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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/c2h5oh/datasize"
	mdbx2 "github.com/erigontech/mdbx-go/mdbx"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/temporal"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/tests"
	"github.com/ledgerwatch/erigon/turbo/trie"
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
	Name  string          `json:"name"`
	Pass  bool            `json:"pass"`
	Root  *libcommon.Hash `json:"stateRoot,omitempty"`
	Fork  string          `json:"fork"`
	Error string          `json:"error,omitempty"`
	State *state.Dump     `json:"state,omitempty"`
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
	cfg := vm.Config{
		Debug: ctx.Bool(DebugFlag.Name) || ctx.Bool(MachineFlag.Name),
	}
	if machineFriendlyOutput {
		cfg.Tracer = logger.NewJSONLogger(config, os.Stderr)
	} else if ctx.Bool(DebugFlag.Name) {
		cfg.Tracer = logger.NewStructLogger(config)
	}

	if len(ctx.Args().First()) != 0 {
		return runStateTest(ctx.Args().First(), cfg, ctx.Bool(MachineFlag.Name))
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fname := scanner.Text()
		if len(fname) == 0 {
			return nil
		}
		if err := runStateTest(fname, cfg, ctx.Bool(MachineFlag.Name)); err != nil {
			return err
		}
	}
	return nil
}

// runStateTest loads the state-test given by fname, and executes the test.
func runStateTest(fname string, cfg vm.Config, jsonOut bool) error {
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
	results, err := aggregateResultsFromStateTests(stateTests, cfg, jsonOut)
	if err != nil {
		return err
	}

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
	return nil
}

func aggregateResultsFromStateTests(
	stateTests map[string]tests.StateTest, cfg vm.Config,
	jsonOut bool) ([]StatetestResult, error) {
	dirs := datadir.New(filepath.Join(os.TempDir(), "erigon-statetest"))
	//this DB is shared. means:
	// - faster sequential tests: don't need create/delete db
	// - less parallelism: multiple processes can open same DB but only 1 can create rw-transaction (other will wait when 1-st finish)
	_db := mdbx.NewMDBX(log.New()).
		Path(dirs.Chaindata).
		Flags(func(u uint) uint {
			return u | mdbx2.UtterlyNoSync | mdbx2.NoMetaSync | mdbx2.NoMemInit | mdbx2.WriteMap
		}).
		GrowthStep(1 * datasize.MB).
		MustOpen()
	defer _db.Close()

	agg, err := libstate.NewAggregator(context.Background(), dirs, config3.HistoryV3AggregationStep, _db, log.New())
	if err != nil {
		return nil, err
	}
	defer agg.Close()

	db, err := temporal.New(_db, agg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tx, txErr := db.BeginRw(context.Background())
	if txErr != nil {
		return nil, txErr
	}
	defer tx.Rollback()
	results := make([]StatetestResult, 0, len(stateTests))

	for key, test := range stateTests {
		for _, st := range test.Subtests() {
			// Run the test and aggregate the result
			result := &StatetestResult{Name: key, Fork: st.Fork, Pass: true}

			var root libcommon.Hash
			var calcRootErr error

			statedb, err := test.Run(tx, st, cfg)
			// print state root for evmlab tracing
			root, calcRootErr = trie.CalcRoot("", tx)
			if err == nil && calcRootErr != nil {
				err = calcRootErr
			}
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
			results = append(results, *result)
		}
	}
	return results, nil
}
