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
	"maps"
	"os"
	"slices"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

var difficultyTestCommand = cli.Command{
	Action:    difficultyTestCmd,
	Name:      "difficultytest",
	Usage:     "Executes difficulty tests.",
	ArgsUsage: "<path>",
	Flags: []cli.Flag{
		&JSONOutputFlag,
		&RunFlag,
		&ExcludeFlag,
		&WorkersFlag,
	},
}

func difficultyTestCmd(_ context.Context, ctx *cli.Command) error {
	path := ctx.Args().First()
	if path == "" {
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
	results, err := runTestFilesParallel(
		filter.filterFiles(collectFiles(path)),
		workers,
		func(path string) ([]testResult, error) {
			return runDifficultyTest(path, filter)
		},
	)
	if err != nil {
		return err
	}
	report(ctx, results)
	return nil
}

func runDifficultyTest(path string, filter testFilter) ([]testResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var groups map[string]map[string]json.RawMessage
	if err := json.Unmarshal(data, &groups); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	results := make([]testResult, 0, len(groups))
	for _, group := range slices.Sorted(maps.Keys(groups)) {
		forks := groups[group]
		for _, fork := range slices.Sorted(maps.Keys(forks)) {
			if fork == "_info" {
				continue
			}
			var tests map[string]testutil.DifficultyTest
			if err := json.Unmarshal(forks[fork], &tests); err != nil {
				return nil, err
			}
			config, supported := testforks.Forks[fork]
			for _, name := range slices.Sorted(maps.Keys(tests)) {
				fullName := group + "/" + fork + "/" + name
				if !filter.includeCase(path, fullName) {
					continue
				}
				var err error
				if supported {
					test := tests[name]
					err = test.Run(config)
				} else {
					err = testforks.UnsupportedForkError{Name: fork}
				}
				result := testResult{Name: fullName, Pass: err == nil}
				if err != nil {
					result.Error = err.Error()
				}
				results = append(results, result)
			}
		}
	}
	return results, nil
}
