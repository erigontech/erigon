// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

var engineXTestCommand = cli.Command{
	Action:    engineXTestCmd,
	Name:      "enginextest",
	Usage:     "Executes engine-x test fixtures using the existing EngineXTestRunner",
	ArgsUsage: "<path>",
	Description: "Each test runs in its own short-lived Erigon node whose datadir lives\n" +
		"under $TMPDIR (default /tmp). On a journaled filesystem like ext4 the\n" +
		"per-tester create/unlink work serialises through the journal and\n" +
		"dominates wall time. Pointing $TMPDIR at a RAM-backed filesystem cuts\n" +
		"wall time by ~2× on Linux. Easiest cross-platform setup:\n" +
		"\n" +
		"    RAMDISK=$(./tools/create-ramdisk)\n" +
		"    TMPDIR=$RAMDISK evm enginextest ...\n" +
		"\n" +
		"On Linux you can also use /dev/shm directly (TMPDIR=/dev/shm), which is\n" +
		"a tmpfs sized at half of RAM by default. ~5GB free is plenty for the\n" +
		"full EEST engine_x set.",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "pre-alloc-dir",
			Usage:    "Directory containing engine-x pre-alloc JSON files",
			Required: true,
		},
		&JSONOutputFlag,
		&RunFlag,
		&VerbosityFlag,
		&WorkersFlag,
	},
}

type engineXNamedTest struct {
	name string
	def  engineapitester.EngineXTestDefinition
}

type engineXGroupKey struct {
	fork engineapitester.Fork
	hash engineapitester.PreAllocHash
}

func engineXTestCmd(cliCtx *cli.Context) error {
	if cliCtx.Int(VerbosityFlag.Name) > 0 {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(cliCtx.Int(VerbosityFlag.Name)), log.StderrHandler))
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	}

	path := cliCtx.Args().First()
	if path == "" {
		return errors.New("path argument required")
	}
	preAllocDir := cliCtx.String("pre-alloc-dir")
	if preAllocDir == "" {
		return errors.New("--pre-alloc-dir is required")
	}
	re, err := regexp.Compile(cliCtx.String(RunFlag.Name))
	if err != nil {
		return fmt.Errorf("invalid --run regex: %w", err)
	}
	workers := cliCtx.Int(WorkersFlag.Name)
	if workers <= 0 {
		// Knee of the wall-time vs. RAM curve on the full EEST engine_x set
		// (~64k tests / ~36k groups): workers=8 reaches plateau speedup at
		// ~4.3GB peak RSS. Higher values barely improve wall time and
		// values >24 risk MDBX virtual-memory exhaustion.
		workers = 8
	}

	ctx, cancel := context.WithCancel(cliCtx.Context)
	defer cancel()

	groups, totalTests, err := loadEngineXGroups(path, re)
	if err != nil {
		return err
	}
	if workers > len(groups) && len(groups) > 0 {
		workers = len(groups)
	}
	fmt.Fprintf(os.Stderr, "Collected %d tests across %d (fork, preAllocHash) groups; running with %d workers\n", totalTests, len(groups), workers)

	if totalTests == 0 {
		report(cliCtx, nil)
		return nil
	}

	runner, err := engineapitester.NewEngineXTestRunner(ctx, log.Root(), preAllocDir)
	if err != nil {
		return fmt.Errorf("create runner: %w", err)
	}
	defer func() {
		cerr := runner.Close()
		if cerr != nil {
			fmt.Fprintf(os.Stderr, "runner.Close: %v\n", cerr)
		}
	}()

	groupKeys := make([]engineXGroupKey, 0, len(groups))
	for k := range groups {
		groupKeys = append(groupKeys, k)
	}

	groupCh := make(chan engineXGroupKey)
	resultCh := make(chan testResult, totalTests)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range groupCh {
				runEngineXGroup(ctx, runner, key, groups[key], resultCh)
			}
		}()
	}

	go func() {
		defer close(groupCh)
		for _, key := range groupKeys {
			select {
			case <-ctx.Done():
				return
			case groupCh <- key:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	results := make([]testResult, 0, totalTests)
	for r := range resultCh {
		results = append(results, r)
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })

	report(cliCtx, results)
	return nil
}

// loadEngineXGroups walks path for JSON files, parses each, filters tests by
// the regex, and groups them by (fork, preAllocHash). Each group is the unit
// of execution given to a worker — a worker creates one EngineApiTester for
// the group's (fork, preAllocHash), runs all tests in the group sequentially
// on that tester, then evicts it.
//
// Files under any pre_alloc/ directory are skipped: those are pre-allocation
// inputs (consumed via --pre-alloc-dir), not test fixtures, and they don't
// follow the engine-x JSON schema. Every other JSON in the tree is expected
// to be a valid engine-x fixture; an unmarshal error is treated as a hard
// failure rather than silently skipped.
func loadEngineXGroups(path string, re *regexp.Regexp) (map[engineXGroupKey][]engineXNamedTest, int, error) {
	files := collectFiles(path)
	groups := make(map[engineXGroupKey][]engineXNamedTest)
	total := 0
	for _, fname := range files {
		if isUnderPreAlloc(fname) {
			continue
		}
		src, err := os.ReadFile(fname)
		if err != nil {
			return nil, 0, fmt.Errorf("read %s: %w", fname, err)
		}
		var tests map[string]engineapitester.EngineXTestDefinition
		err = json.Unmarshal(src, &tests)
		if err != nil {
			return nil, 0, fmt.Errorf("unmarshal %s: %w", fname, err)
		}
		for name, def := range tests {
			if !re.MatchString(name) {
				continue
			}
			key := engineXGroupKey{fork: def.Fork, hash: def.PreAllocHash}
			groups[key] = append(groups[key], engineXNamedTest{name: name, def: def})
			total++
		}
	}
	return groups, total, nil
}

// isUnderPreAlloc reports whether any directory component of p is named
// "pre_alloc" — those are pre-allocation inputs, not test fixtures.
func isUnderPreAlloc(p string) bool {
	for _, c := range strings.Split(filepath.ToSlash(p), "/") {
		if c == "pre_alloc" {
			return true
		}
	}
	return false
}

// runEngineXGroup executes every test in the group sequentially on a single
// tester (created lazily by the runner) then evicts the tester to free its
// node and temp directory before returning. Results are streamed to resultCh
// so the parent goroutine can collect across all workers.
func runEngineXGroup(
	ctx context.Context,
	runner *engineapitester.EngineXTestRunner,
	key engineXGroupKey,
	tests []engineXNamedTest,
	resultCh chan<- testResult,
) {
	defer func() {
		err := runner.Evict(key.fork, key.hash)
		if err != nil {
			fmt.Fprintf(os.Stderr, "evict fork=%s preAllocHash=%s: %v\n", key.fork, key.hash, err)
		}
	}()
	for _, t := range tests {
		r := testResult{Name: t.name, Pass: true}
		err := runner.Run(ctx, t.def)
		if err != nil {
			r.Pass = false
			r.Error = err.Error()
		}
		resultCh <- r
	}
}
