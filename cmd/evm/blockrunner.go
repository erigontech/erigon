// Copyright 2023 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"bufio"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

var blockTestCommand = cli.Command{
	Action:    blockTestCmd,
	Name:      "blocktest",
	Usage:     "Executes the given blockchain tests. Filenames can be fed via standard input (batch mode) or as an argument (one-off execution).",
	ArgsUsage: "<path>",
	Flags: []cli.Flag{
		&DumpFlag,
		&JSONOutputFlag,
		&RunFlag,
		&VerbosityFlag,
		&WorkersFlag,
	},
}

func blockTestCmd(ctx *cli.Context) error {
	path := ctx.Args().First()

	// Set up logging
	if ctx.Int(VerbosityFlag.Name) > 0 {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(ctx.Int(VerbosityFlag.Name)), log.StderrHandler))
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	}

	if len(path) != 0 {
		collected := collectFiles(path)
		workers := ctx.Int(WorkersFlag.Name)
		if workers <= 0 {
			workers = 1
		}
		results, err := runBlockTestsParallel(ctx, collected, workers)
		if err != nil {
			return err
		}
		report(ctx, results)
		return nil
	}
	// Otherwise, read filenames from stdin and execute back-to-back.
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fname := scanner.Text()
		if len(fname) == 0 {
			return nil
		}
		results, err := runBlockTest(ctx, fname)
		if err != nil {
			return err
		}
		report(ctx, results)
	}
	return nil
}

// fileResult holds the results from processing a single fixture file.
type fileResult struct {
	index   int
	results []testResult
	err     error
}

func runBlockTestsParallel(ctx *cli.Context, files []string, workers int) ([]testResult, error) {
	if workers == 1 {
		results := make([]testResult, 0, len(files)*4) // pre-allocate: most files have a few tests
		for _, fname := range files {
			r, err := runBlockTest(ctx, fname)
			if err != nil {
				return nil, err
			}
			results = append(results, r...)
		}
		return results, nil
	}
	var (
		wg     sync.WaitGroup
		fileCh = make(chan struct {
			index int
			fname string
		}, len(files))
		resultCh = make(chan fileResult, len(files))
	)
	for i, fname := range files {
		fileCh <- struct {
			index int
			fname string
		}{i, fname}
	}
	close(fileCh)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range fileCh {
				r, err := runBlockTest(ctx, item.fname)
				resultCh <- fileResult{index: item.index, results: r, err: err}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	ordered := make([]fileResult, len(files))
	for fr := range resultCh {
		if fr.err != nil {
			return nil, fr.err
		}
		ordered[fr.index] = fr
	}
	// Pre-estimate total results
	total := 0
	for _, fr := range ordered {
		total += len(fr.results)
	}
	results := make([]testResult, 0, total)
	for _, fr := range ordered {
		results = append(results, fr.results...)
	}
	return results, nil
}

// collectFiles walks the given path and returns all JSON files.
// If path is a file, it returns that file directly.
func collectFiles(path string) []string {
	info, err := os.Stat(path)
	if err != nil {
		return nil
	}

	if !info.IsDir() {
		return []string{path}
	}

	// Pre-allocate with a reasonable estimate to avoid repeated slice growth
	out := make([]string, 0, 256)
	err = filepath.WalkDir(path, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".json" {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error walking path %s: %v\n", path, err)
	}

	return out
}

func runBlockTest(ctx *cli.Context, fname string) ([]testResult, error) {
	src, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}

	var tests map[string]*testutil.BlockTest
	if err = json.Unmarshal(src, &tests); err != nil {
		return nil, err
	}

	re, err := regexp.Compile(ctx.String(RunFlag.Name))
	if err != nil {
		return nil, fmt.Errorf("invalid regex -%s: %v", RunFlag.Name, err)
	}

	// Pull out keys to sort and ensure tests are run in order
	keys := slices.Sorted(maps.Keys(tests))

	// Run all the tests
	results := make([]testResult, 0, len(keys))
	for _, name := range keys {
		if !re.MatchString(name) {
			continue
		}

		result := &testResult{Name: name, Pass: true}
		if err := tests[name].RunCLI(); err != nil {
			result.Pass = false
			result.Error = err.Error()
		}

		results = append(results, *result)
	}

	return results, nil
}
