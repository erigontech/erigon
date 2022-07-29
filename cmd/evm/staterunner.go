// Copyright 2017 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/tests"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli"
)

var stateTestCommand = cli.Command{
	Action:    stateTestCmd,
	Name:      "statetest",
	Usage:     "executes the given state tests",
	ArgsUsage: "<file>",
}

// StatetestResult contains the execution status after running a state test, any
// error that might have occurred and a dump of the final state if requested.
type StatetestResult struct {
	Name  string      `json:"name"`
	Pass  bool        `json:"pass"`
	Fork  string      `json:"fork"`
	Error string      `json:"error,omitempty"`
	State *state.Dump `json:"state,omitempty"`
}

func stateTestCmd(ctx *cli.Context) error {
	if len(ctx.Args().First()) == 0 {
		return errors.New("path-to-test argument required")
	}
	// Configure the go-ethereum logger
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	//glogger := log.NewGlogHandler(log.StreamHandler(os.Stderr, log.TerminalFormat(false)))
	//glogger.Verbosity(log.Lvl(ctx.GlobalInt(VerbosityFlag.Name)))

	// Configure the EVM logger
	config := &vm.LogConfig{
		DisableMemory:     ctx.GlobalBool(DisableMemoryFlag.Name),
		DisableStack:      ctx.GlobalBool(DisableStackFlag.Name),
		DisableStorage:    ctx.GlobalBool(DisableStorageFlag.Name),
		DisableReturnData: ctx.GlobalBool(DisableReturnDataFlag.Name),
	}
	var (
		tracer   vm.Tracer
		debugger *vm.StructLogger
	)
	switch {
	case ctx.GlobalBool(MachineFlag.Name):
		tracer = vm.NewJSONLogger(config, os.Stderr)

	case ctx.GlobalBool(DebugFlag.Name):
		debugger = vm.NewStructLogger(config)
		tracer = debugger

	default:
		debugger = vm.NewStructLogger(config)
	}
	// Load the test content from the input file
	src, err := os.ReadFile(ctx.Args().First())
	if err != nil {
		return err
	}
	var stateTests map[string]tests.StateTest
	if err = json.Unmarshal(src, &stateTests); err != nil {
		return err
	}

	// Iterate over all the stateTests, run them and aggregate the results
	results := make([]StatetestResult, 0, len(stateTests))
	if err := aggregateResultsFromStateTests(ctx, stateTests, results, tracer, debugger); err != nil {
		return err
	}

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
	return nil
}

func aggregateResultsFromStateTests(
	ctx *cli.Context,
	stateTests map[string]tests.StateTest,
	results []StatetestResult,
	tracer vm.Tracer,
	debugger *vm.StructLogger,
) error {
	// Iterate over all the stateTests, run them and aggregate the results
	cfg := vm.Config{
		Tracer: tracer,
		Debug:  ctx.GlobalBool(DebugFlag.Name) || ctx.GlobalBool(MachineFlag.Name),
	}

	db := memdb.New()
	defer db.Close()

	tx, txErr := db.BeginRw(context.Background())
	if txErr != nil {
		return txErr
	}
	defer tx.Rollback()

	for key, test := range stateTests {
		for _, st := range test.Subtests() {
			// Run the test and aggregate the result
			result := &StatetestResult{Name: key, Fork: st.Fork, Pass: true}

			var root common.Hash
			var calcRootErr error

			statedb, err := test.Run(&params.Rules{}, tx, st, cfg)
			// print state root for evmlab tracing
			root, calcRootErr = trie.CalcRoot("", tx)
			if err == nil && calcRootErr != nil {
				err = calcRootErr
			}
			if err != nil {
				// Test failed, mark as so and dump any state to aid debugging
				result.Pass, result.Error = false, err.Error()
			}

			/*
				if result.Error != "" {
					if ctx.GlobalBool(DumpFlag.Name) && statedb != nil {
						tx, err1 := tds.Database().Begin(context.Background(), ethdb.RO)
						if err1 != nil {
							return fmt.Errorf("transition cannot open tx: %v", err1)
						}
						dump := state.NewDumper(tx, tds.GetBlockNr()).DefaultRawDump()
						tx.Rollback()
						result.State = &dump
					}
				}
			*/

			// print state root for evmlab tracing
			if ctx.GlobalBool(MachineFlag.Name) && statedb != nil {
				_, printErr := fmt.Fprintf(os.Stderr, "{\"stateRoot\": \"%x\"}\n", root.Bytes())
				if printErr != nil {
					log.Warn("Failed to write to stderr", "err", printErr)
				}
			}

			results = append(results, *result)

			// Print any structured logs collected
			if ctx.GlobalBool(DebugFlag.Name) {
				if debugger != nil {
					_, printErr := fmt.Fprintln(os.Stderr, "#### TRACE ####")
					if printErr != nil {
						log.Warn("Failed to write to stderr", "err", printErr)
					}
					vm.WriteTrace(os.Stderr, debugger.StructLogs())
				}
			}
		}
	}
	return nil
}
