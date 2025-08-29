// Copyright 2017 The go-ethereum Authors
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
	"fmt"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/eth/tracers/logger"
	"github.com/erigontech/erigon/tests"
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
	Name  string       `json:"name"`
	Pass  bool         `json:"pass"`
	Root  *common.Hash `json:"stateRoot,omitempty"`
	Fork  string       `json:"fork"`
	Error string       `json:"error,omitempty"`
	State *state.Dump  `json:"state,omitempty"`
	Stats *execStats   `json:"benchStats,omitempty"`
}

func stateTestCmd(ctx *cli.Context) error {
	machineFriendlyOutput := ctx.Bool(MachineFlag.Name)
	if machineFriendlyOutput {
		log.Root().SetHandler(log.DiscardHandler())
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
	}

	// Configure the EVM logger
	config := &logger.LogConfig{
		DisableMemory:     ctx.Bool(DisableMemoryFlag.Name),
		DisableStack:      ctx.Bool(DisableStackFlag.Name),
		DisableStorage:    ctx.Bool(DisableStorageFlag.Name),
		DisableReturnData: ctx.Bool(DisableReturnDataFlag.Name),
	}
	cfg := vm.Config{}
	if machineFriendlyOutput {
		cfg.Tracer = logger.NewJSONLogger(config, os.Stderr).Tracer().Hooks
	} else if ctx.Bool(DebugFlag.Name) {
		cfg.Tracer = logger.NewStructLogger(config).Tracer().Hooks
	}

	if len(ctx.Args().First()) != 0 {
		return runStateTest(ctx.Args().First(), cfg, ctx.Bool(MachineFlag.Name), ctx.Bool(BenchFlag.Name))
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fname := scanner.Text()
		if len(fname) == 0 {
			return nil
		}
		if err := runStateTest(fname, cfg, ctx.Bool(MachineFlag.Name), ctx.Bool(BenchFlag.Name)); err != nil {
			return err
		}
	}
	return nil
}

// runStateTest loads the state-test given by fname, and executes the test.
func runStateTest(fname string, cfg vm.Config, jsonOut bool, bench bool) error {
	// Load the test content from the input file
	src, err := os.ReadFile(fname)
	if err != nil {
		return err
	}
	var stateTests map[string]tests.StateTest
	if err = json.Unmarshal(src, &stateTests); err != nil {
		return err
	}

	// Iterate over all the stateTests, run them and aggregate the results
	results, err := aggregateResultsFromStateTests(stateTests, cfg, jsonOut, bench)
	if err != nil {
		return err
	}

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
	return nil
}

func aggregateResultsFromStateTests(
	stateTests map[string]tests.StateTest, cfg vm.Config,
	jsonOut bool, bench bool) ([]StatetestResult, error) {
	dirs := datadir.New(filepath.Join(os.TempDir(), "erigon-statetest"))

	db := temporaltest.NewTestDB(nil, dirs)
	defer db.Close()

	tx, txErr := db.BeginTemporalRw(context.Background())
	if txErr != nil {
		return nil, txErr
	}
	defer tx.Rollback()
	results := make([]StatetestResult, 0, len(stateTests))

	for key, test := range stateTests {
		for _, st := range test.Subtests() {
			// Run the test and aggregate the result
			result := &StatetestResult{Name: key, Fork: st.Fork, Pass: true}

			statedb, root, err := test.Run(tx, st, cfg, dirs)
			if err != nil {
				// Test failed, mark as so and dump any state to aid debugging
				result.Pass, result.Error = false, err.Error()
			}

			// print state root for evmlab tracing
			if statedb != nil {
				result.Root = &root
				if jsonOut {
					_, printErr := fmt.Fprintf(os.Stderr, "{\"stateRoot\": \"%#x\"}\n", root.Bytes())
					if printErr != nil {
						log.Warn("Failed to write to stderr", "err", printErr)
					}
				}
			}

			// if benchmark requested rerun test w/o verification and collect stats
			if bench {
				_, stats, _ := timedExec(true, func() ([]byte, uint64, error) {
					_, _, gasUsed, _ := test.RunNoVerify(tx, st, cfg, dirs)
					return nil, gasUsed, nil
				})

				result.Stats = &stats
			}

			results = append(results, *result)
		}
	}
	return results, nil
}
