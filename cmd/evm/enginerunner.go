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

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

var engineTestCommand = cli.Command{
	Action:    engineTestCmd,
	Name:      "enginetest",
	Usage:     "Executes the given engine API tests (blockchain_tests_engine fixtures)",
	ArgsUsage: "<path>...",
	Flags: []cli.Flag{
		&JSONOutputFlag,
		&RunFlag,
		&ExcludeFlag,
		&VerbosityFlag,
		&WorkersFlag,
	},
}

func engineTestCmd(_ context.Context, ctx *cli.Command) error {
	if ctx.Int(VerbosityFlag.Name) > 0 {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(ctx.Int(VerbosityFlag.Name)), log.StderrHandler))
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	}

	if !ctx.Args().Present() {
		return errors.New("path argument required")
	}
	workers := ctx.Uint64(WorkersFlag.Name)
	if workers == 0 {
		return fmt.Errorf("--%s must be >= 1", WorkersFlag.Name)
	}
	filter, err := compileTestFilter(ctx.String(RunFlag.Name), ctx.StringSlice(ExcludeFlag.Name))
	if err != nil {
		return err
	}

	files, err := collectArgFiles(ctx)
	if err != nil {
		return exitIncomplete(err)
	}
	files = filter.filterFiles(files)
	var incomplete incompleteRun
	results, err := runTestFilesParallel(files, workers, incomplete.wrap(func(fname string) ([]testResult, error) {
		return runEngineTest(ctx, fname, filter)
	}))
	if err != nil {
		return err
	}
	report(ctx, results)
	return incomplete.status(anyFailed(results))
}

// runEngineTest runs the fixtures in fname. Its error names a fixture that
// cannot be decoded, which also gets a failed result, or the file itself.
func runEngineTest(ctx *cli.Command, fname string, filter testFilter) ([]testResult, error) {
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
		test := new(engineapitester.EngineTest)
		if err := json.Unmarshal(fixtures[name], test); err != nil {
			results = append(results, unreadableFixture(name, err))
			unreadable = append(unreadable, fmt.Errorf("fixture %s: %w", name, err))
			continue
		}
		result := testResult{Name: name, Pass: true}
		if err := test.RunCLI(); err != nil {
			result.Pass = false
			result.Error = err.Error()
		}
		result.Rejections = rejections(test.Rejections)
		results = append(results, result)
	}
	return results, errors.Join(unreadable...)
}
