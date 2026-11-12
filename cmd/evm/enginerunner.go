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
	"errors"
	"fmt"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/node/ethconfig"
)

var engineTestCommand = cli.Command{
	Action:    engineTestCmd,
	Name:      "enginetest",
	Usage:     "Executes the given engine API tests (blockchain_tests_engine fixtures)",
	ArgsUsage: "<path>...",
	Flags: []cli.Flag{
		&BALReportFlag,
		&BALSequentialFlag,
		&JSONOutputFlag,
		&RunFlag,
		&ExcludeFlag,
		&SyncingTimeoutFlag,
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
	if ctx.Duration(SyncingTimeoutFlag.Name) <= 0 {
		return fmt.Errorf("--%s must be positive", SyncingTimeoutFlag.Name)
	}
	filter, err := compileTestFilter(ctx.String(RunFlag.Name), ctx.StringSlice(ExcludeFlag.Name))
	if err != nil {
		return err
	}

	reportPath := runnerExecutionPathReporter(ctx)
	files, err := collectArgFiles(ctx)
	if err != nil {
		return exitIncomplete(err)
	}
	files = filter.filterFiles(files)
	var incomplete incompleteRun
	results := incomplete.run(files, workers, func(fname string) ([]testResult, error) {
		return runEngineTest(ctx, fname, filter, reportPath)
	})
	report(ctx, results)
	return incomplete.status(anyFailed(results))
}

// runEngineTest runs the blockchain engine test fixtures in fname.
func runEngineTest(ctx *cli.Command, fname string, filter testFilter, reportPath func(ethconfig.BlockExecutionPath)) ([]testResult, error) {
	return runFixtures(ctx, fname, filter, reportPath, func() (fixture, *testutil.BlockTest) {
		test := &engineapitester.EngineTest{SyncingTimeout: ctx.Duration(SyncingTimeoutFlag.Name)}
		return test, &test.BlockTest
	})
}
