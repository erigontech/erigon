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
	"runtime/pprof"
	"sort"
	"strings"
	"sync"

	"github.com/felixge/fgprof"
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
		&TimeFlag,
		&VerbosityFlag,
		&WorkersFlag,
		&cli.StringFlag{
			Name:  "pprof.cpu",
			Usage: "directory in which to write one CPU profile per engine API request",
		},
		&cli.StringFlag{
			Name:  "fgprof",
			Usage: "directory in which to write one fgprof wall-clock profile per engine API request",
		},
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
	workers := cliCtx.Uint64(WorkersFlag.Name)
	if workers == 0 {
		return fmt.Errorf("--%s must be >= 1", WorkersFlag.Name)
	}
	cpuProfileDir := cliCtx.String("pprof.cpu")
	wallProfileDir := cliCtx.String("fgprof")
	profilingEnabled := cpuProfileDir != "" || wallProfileDir != ""
	if profilingEnabled && workers > 1 {
		// pprof.StartCPUProfile and fgprof.Start are process-global, so
		// concurrent workers cannot produce isolated per-request profiles.
		return fmt.Errorf("--pprof.cpu/--fgprof require --workers=1 (got %d)", workers)
	}

	ctx, cancel := context.WithCancel(cliCtx.Context)
	defer cancel()

	groups, totalTests, err := loadEngineXGroups(path, re)
	if err != nil {
		return err
	}
	if workers > uint64(len(groups)) && len(groups) > 0 {
		workers = uint64(len(groups))
	}
	fmt.Fprintf(os.Stderr, "Collected %d tests across %d (fork, preAllocHash) groups; running with %d workers\n", totalTests, len(groups), workers)

	if totalTests == 0 {
		report(cliCtx, nil)
		return nil
	}

	var runnerOpts []engineapitester.EngineXTestRunnerOption
	if profilingEnabled {
		if cpuProfileDir != "" {
			err := os.MkdirAll(cpuProfileDir, 0o755)
			if err != nil {
				return fmt.Errorf("create cpu profile dir: %w", err)
			}
		}
		if wallProfileDir != "" {
			err := os.MkdirAll(wallProfileDir, 0o755)
			if err != nil {
				return fmt.Errorf("create wall profile dir: %w", err)
			}
		}
		runnerOpts = append(runnerOpts, engineapitester.WithRequestProfileHook(makeRequestProfileHook(cpuProfileDir, wallProfileDir)))
	}

	runner, err := engineapitester.NewEngineXTestRunner(ctx, log.Root(), preAllocDir, runnerOpts...)
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

	timeIt := cliCtx.Bool(TimeFlag.Name)
	var wg sync.WaitGroup
	for w := uint64(0); w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range groupCh {
				runEngineXGroup(ctx, runner, key, groups[key], timeIt, resultCh)
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
	if timeIt {
		printTimings(results)
	}
	return nil
}

// printTimings writes per-test wall-time to stderr sorted descending by Time.
// Goes to stderr so JSON mode (--jsonout to stdout) isn't polluted.
func printTimings(results []testResult) {
	timed := make([]testResult, 0, len(results))
	for _, r := range results {
		if r.Stats != nil {
			timed = append(timed, r)
		}
	}
	sort.Slice(timed, func(i, j int) bool {
		return timed[i].Stats.Time > timed[j].Stats.Time
	})
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "=== test timings (descending) ===")
	for _, r := range timed {
		fmt.Fprintf(os.Stderr, "  %12s  %s\n", r.Stats.Time, r.Name)
	}
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
// so the parent goroutine can collect across all workers. When timeIt is
// true, each test's single Run is wrapped in timedExec so wall-time and
// memstats land on the JSON output.
func runEngineXGroup(
	ctx context.Context,
	runner *engineapitester.EngineXTestRunner,
	key engineXGroupKey,
	tests []engineXNamedTest,
	timeIt bool,
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
		// timedExec(bench=false): single execution with memstats. We
		// deliberately don't expose bench=true (testing.Benchmark loop) for
		// engine_x tests because NewPayload + FCU are idempotent for a block
		// hash the EL has already processed — iterations 2..N would
		// short-circuit through the already-known-block fast path instead of
		// re-executing.
		testCtx := engineapitester.ContextWithTestName(ctx, t.name)
		var err error
		if timeIt {
			var stats execStats
			_, stats, err = timedExec(false, func() ([]byte, uint64, error) {
				return nil, 0, runner.Run(testCtx, t.def)
			})
			if err == nil {
				r.Stats = &stats
			}
		} else {
			err = runner.Run(testCtx, t.def)
		}
		if err != nil {
			r.Pass = false
			r.Error = err.Error()
		}
		resultCh <- r
	}
}

// safeFilenameRe matches characters that are unsafe in filenames across
// macOS/Linux/Windows. Replaced with '_' in profileFilename so paths derived
// from EEST test names (which contain `[`, `]`, `:`, `/`, ` `, etc.) remain
// usable.
var safeFilenameRe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func profileFilename(dir, kind, id, ext string) string {
	clean := safeFilenameRe.ReplaceAllString(id, "_")
	return filepath.Join(dir, kind+"__"+clean+"."+ext)
}

// makeRequestProfileHook returns a hook that writes one CPU profile and/or
// one fgprof profile per engine API request, keyed by the request kind +
// per-request id supplied by the runner. The hook holds an internal mutex so
// concurrent invocations serialise on the process-global pprof state — in
// practice callers should pair profiling with --workers=1.
func makeRequestProfileHook(cpuDir, wallDir string) engineapitester.RequestProfileHook {
	var mu sync.Mutex
	return func(kind, id string) func() {
		mu.Lock()
		var cpuFile, wallFile *os.File
		var stopFgprof func() error
		if cpuDir != "" {
			path := profileFilename(cpuDir, kind, id, "pprof.cpu")
			f, err := os.Create(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "create cpu profile %s: %v\n", path, err)
			} else {
				err = pprof.StartCPUProfile(f)
				if err != nil {
					fmt.Fprintf(os.Stderr, "start cpu profile %s: %v\n", path, err)
					f.Close()
				} else {
					cpuFile = f
				}
			}
		}
		if wallDir != "" {
			path := profileFilename(wallDir, kind, id, "fgprof")
			f, err := os.Create(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "create fgprof %s: %v\n", path, err)
			} else {
				stopFgprof = fgprof.Start(f, fgprof.FormatPprof)
				wallFile = f
			}
		}
		return func() {
			defer mu.Unlock()
			if cpuFile != nil {
				pprof.StopCPUProfile()
				cpuFile.Close()
			}
			if stopFgprof != nil {
				if err := stopFgprof(); err != nil {
					fmt.Fprintf(os.Stderr, "stop fgprof: %v\n", err)
				}
				if wallFile != nil {
					wallFile.Close()
				}
			}
		}
	}
}
