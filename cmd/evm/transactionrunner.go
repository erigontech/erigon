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

	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

var transactionTestCommand = cli.Command{
	Action:    transactionTestCmd,
	Name:      "transactiontest",
	Usage:     "Executes transaction tests.",
	ArgsUsage: "<path>",
	Flags: []cli.Flag{
		&JSONOutputFlag,
		&RunFlag,
		&ExcludeFlag,
		&WorkersFlag,
	},
}

func transactionTestCmd(_ context.Context, ctx *cli.Command) error {
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
			return runTransactionTest(path, filter)
		},
	)
	if err != nil {
		return err
	}
	report(ctx, results)
	return nil
}

func runTransactionTest(path string, filter testFilter) ([]testResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var tests map[string]testutil.TransactionTest
	if err := json.Unmarshal(data, &tests); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	names := slices.Sorted(maps.Keys(tests))
	results := make([]testResult, 0, len(names))
	for _, name := range names {
		if !filter.includeCase(path, name) {
			continue
		}
		test := tests[name]
		err := test.Run(chainspec.Mainnet.Config.ChainID)
		result := testResult{Name: name, Pass: err == nil}
		if err != nil {
			result.Error = err.Error()
		}
		results = append(results, result)
	}
	return results, nil
}
