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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

var blockTestCommand = cli.Command{
	Action:    blockTestCmd,
	Name:      "blocktest",
	Usage:     "Executes the given blockchain tests. Paths (files or directories) are given as arguments, or filenames fed via standard input (batch mode).",
	ArgsUsage: "<path>...",
	Flags: []cli.Flag{
		&DumpFlag,
		&JSONOutputFlag,
		&RunFlag,
		&ExcludeFlag,
		&VerbosityFlag,
		&WorkersFlag,
	},
}

func blockTestCmd(_ context.Context, ctx *cli.Command) error {
	// Set up logging
	if ctx.Int(VerbosityFlag.Name) > 0 {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(ctx.Int(VerbosityFlag.Name)), log.StderrHandler))
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	}

	workers := ctx.Uint64(WorkersFlag.Name)
	if workers == 0 {
		return fmt.Errorf("--%s must be >= 1", WorkersFlag.Name)
	}
	filter, err := compileTestFilter(ctx.String(RunFlag.Name), ctx.StringSlice(ExcludeFlag.Name))
	if err != nil {
		return err
	}

	var incomplete incompleteRun
	if ctx.Args().Present() {
		files, err := collectArgFiles(ctx)
		if err != nil {
			return exitIncomplete(err)
		}
		collected := filter.filterFiles(files)
		results, err := runBlockTestsParallel(ctx, collected, workers, filter, &incomplete)
		if err != nil {
			return err
		}
		report(ctx, results)
		return incomplete.status(anyFailed(results))
	}
	// Otherwise, read filenames from stdin and execute back-to-back.
	runFile := incomplete.wrap(func(path string) ([]testResult, error) {
		return runBlockTest(ctx, path, filter)
	})
	failed := false
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fname := scanner.Text()
		if len(fname) == 0 {
			break
		}
		// A file that cannot be read or decoded has no results to report.
		if results, _ := runFile(fname); results != nil {
			report(ctx, results)
			failed = failed || anyFailed(results)
		}
	}
	return incomplete.status(failed)
}

// fileResult holds the results from processing a single fixture file.
type fileResult struct {
	index   int
	results []testResult
	err     error
}

func runBlockTestsParallel(ctx *cli.Command, files []string, workers uint64, filter testFilter, incomplete *incompleteRun) ([]testResult, error) {
	return runTestFilesParallel(files, workers, incomplete.wrap(func(path string) ([]testResult, error) {
		return runBlockTest(ctx, path, filter)
	}))
}

func runTestFilesParallel(files []string, workers uint64, runner func(string) ([]testResult, error)) ([]testResult, error) {
	if workers == 1 {
		results := make([]testResult, 0, len(files))
		for _, path := range files {
			r, err := runner(path)
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
			path  string
		}, len(files))
		resultCh = make(chan fileResult, len(files))
	)
	for i, path := range files {
		fileCh <- struct {
			index int
			path  string
		}{i, path}
	}
	close(fileCh)

	for range workers {
		wg.Go(func() {
			for item := range fileCh {
				r, err := runner(item.path)
				resultCh <- fileResult{index: item.index, results: r, err: err}
			}
		})
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

// The exit statuses of a blocktest or enginetest run in which a fixture
// failed, and of one that could not run all of its input.
const (
	exitFailedRun     = 1
	exitIncompleteRun = 2
)

// incompleteRun records the fixture files and fixtures a run could not decode.
type incompleteRun struct {
	seen atomic.Bool
}

// wrap reports a file or fixture that runner cannot decode on stderr and
// keeps the run going with the results it has.
func (r *incompleteRun) wrap(runner func(string) ([]testResult, error)) func(string) ([]testResult, error) {
	return func(path string) ([]testResult, error) {
		results, err := runner(path)
		if err != nil {
			r.seen.Store(true)
			fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
		}
		return results, nil
	}
}

// err returns the error that exits with exitIncompleteRun, or nil if the run
// ran all of its input.
func (r *incompleteRun) err() error {
	if !r.seen.Load() {
		return nil
	}
	return exitIncomplete(errors.New("could not run all of the input"))
}

// status returns the error that sets the run's exit status: exitIncompleteRun
// when it could not run all of its input, else exitFailedRun when a fixture
// failed, else nil.
func (r *incompleteRun) status(failed bool) error {
	if err := r.err(); err != nil {
		return err
	}
	if failed {
		return cli.Exit("", exitFailedRun)
	}
	return nil
}

func anyFailed(results []testResult) bool {
	for _, r := range results {
		if !r.Pass {
			return true
		}
	}
	return false
}

// exitIncomplete reports err on stderr and returns the error that exits with
// exitIncompleteRun; erigon's exit handler prints nothing for an exit code.
func exitIncomplete(err error) error {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	return cli.Exit("", exitIncompleteRun)
}

// collectArgFiles returns the fixture files under every path argument, or an
// error for a path that cannot be read.
func collectArgFiles(ctx *cli.Command) ([]string, error) {
	files := make([]string, 0, ctx.Args().Len())
	for _, path := range ctx.Args().Slice() {
		found, err := jsonFiles(path)
		if err != nil {
			return nil, err
		}
		files = append(files, found...)
	}
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	return files, nil
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

// jsonFiles is collectFiles for blocktest and enginetest: it returns the walk's
// error and follows a path that is a symlink to a directory, which WalkDir does
// not descend into.
func jsonFiles(path string) ([]string, error) {
	root, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{path}, nil
	}
	var out []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".json" {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

// runBlockTest runs the fixtures in fname. Its error names a fixture that
// cannot be decoded, which also gets a failed result, or the file itself.
func runBlockTest(ctx *cli.Command, fname string, filter testFilter) ([]testResult, error) {
	src, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	names, fixtures, err := splitFixtures(src)
	if err != nil {
		return nil, err
	}

	results := make([]testResult, 0, len(names))
	var unreadable []error
	for _, name := range names {
		if !filter.includeCase(fname, name) {
			continue
		}
		test := new(testutil.BlockTest)
		if err := json.Unmarshal(fixtures[name], test); err != nil {
			results = append(results, unreadableFixture(name, err))
			unreadable = append(unreadable, fmt.Errorf("fixture %s: %w", name, err))
			continue
		}

		result := &testResult{Name: name, Pass: true}
		if err := test.RunCLI(); err != nil {
			result.Pass = false
			result.Error = err.Error()
		}
		result.Rejections = rejections(test.Rejections)

		results = append(results, *result)
	}

	return results, errors.Join(unreadable...)
}

// splitFixtures splits a fixture file into its fixtures and their sorted
// names, so a fixture that cannot be decoded fails alone.
func splitFixtures(src []byte) ([]string, map[string]json.RawMessage, error) {
	var fixtures map[string]json.RawMessage
	if err := json.Unmarshal(src, &fixtures); err != nil {
		return nil, nil, err
	}
	return slices.Sorted(maps.Keys(fixtures)), fixtures, nil
}

func unreadableFixture(name string, err error) testResult {
	return testResult{Name: name, Error: err.Error(), Rejections: rejections(nil)}
}
