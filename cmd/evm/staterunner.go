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
	"regexp"
	"sync"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	"github.com/erigontech/erigon/execution/vm"
)

var stateTestCommand = cli.Command{
	Action:    stateTestCmd,
	Name:      "statetest",
	Usage:     "Executes the given state tests. Filenames can be fed via standard input (batch mode) or as an argument (one-off execution).",
	ArgsUsage: "<file>",
	Flags: []cli.Flag{
		&BenchFlag,
		&DebugFlag,
		&DumpFlag,
		&JSONOutputFlag,
		&MachineFlag,
		&RunFlag,
		&WorkersFlag,
		&DisableMemoryFlag,
		&DisableStackFlag,
		&DisableStorageFlag,
		&DisableReturnDataFlag,
	},
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
		EnableMemory:     !ctx.Bool(DisableMemoryFlag.Name),
		DisableStack:     ctx.Bool(DisableStackFlag.Name),
		DisableStorage:   ctx.Bool(DisableStorageFlag.Name),
		EnableReturnData: !ctx.Bool(DisableReturnDataFlag.Name),
	}
	cfg := vm.Config{}
	if machineFriendlyOutput {
		cfg.Tracer = logger.NewJSONLogger(config, os.Stderr).Tracer().Hooks
	} else if ctx.Bool(DebugFlag.Name) {
		cfg.Tracer = logger.NewStructLogger(config).Tracer().Hooks
	}

	workers := ctx.Uint64(WorkersFlag.Name)
	if workers == 0 {
		return fmt.Errorf("--%s must be >= 1", WorkersFlag.Name)
	}

	path := ctx.Args().First()
	if len(path) != 0 {
		collected := collectFiles(path)
		results, err := runStateTestsParallel(ctx, cfg, collected, workers)
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
		results, err := runStateTest(ctx, cfg, fname)
		if err != nil {
			return err
		}
		report(ctx, results)
	}
	return nil
}

func runStateTestsParallel(ctx *cli.Context, cfg vm.Config, files []string, workers uint64) ([]testResult, error) {
	if workers == 1 {
		results := make([]testResult, 0, len(files)*4) // pre-allocate
		for _, fname := range files {
			r, err := runStateTest(ctx, cfg, fname)
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

	for w := uint64(0); w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range fileCh {
				r, err := runStateTest(ctx, cfg, item.fname)
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

// runStateTest loads the state-test given by fname, and executes the test.
func runStateTest(ctx *cli.Context, cfg vm.Config, fname string) ([]testResult, error) {
	src, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	var stateTests map[string]testutil.StateTest
	if err = json.Unmarshal(src, &stateTests); err != nil {
		return nil, err
	}

	re, err := regexp.Compile(ctx.String(RunFlag.Name))
	if err != nil {
		return nil, fmt.Errorf("invalid regex -%s: %v", RunFlag.Name, err)
	}

	bench := ctx.Bool(BenchFlag.Name)
	// Emit the per-test stateRoot line on stderr only when running sequentially.
	// In parallel mode (workers > 1) the stderr writes from concurrent goroutines
	// interleave non-deterministically, which defeats differential fuzzing tools
	// (e.g. goevmlab) that rely on per-test ordering.
	emitStateRoot := ctx.Uint64(WorkersFlag.Name) == 1
	results := make([]testResult, 0, len(stateTests))

	for key, test := range stateTests {
		if !re.MatchString(key) {
			continue
		}
		for _, st := range test.Subtests() {
			result := &testResult{Name: key, Fork: st.Fork, Pass: true}

			// Create fresh DB per subtest to avoid state pollution
			tmpDir, err := os.MkdirTemp("", "erigon-statetest-*")
			if err != nil {
				result.Pass, result.Error = false, err.Error()
				results = append(results, *result)
				continue
			}
			dirs := datadir.New(tmpDir)
			db := temporaltest.NewTestDB(nil, dirs)

			err = db.UpdateTemporal(context.Background(), func(tx kv.TemporalRwTx) error {
				statedb, root, err := test.Run(nil, tx, st, cfg, dirs)
				if err != nil {
					result.Pass, result.Error = false, err.Error()
				}
				if statedb != nil {
					h := common.Hash(root)
					result.Root = &h
					if emitStateRoot {
						if _, printErr := fmt.Fprintf(os.Stderr, "{\"stateRoot\": \"%#x\"}\n", h.Bytes()); printErr != nil {
							log.Warn("Failed to write to stderr", "err", printErr)
						}
					}
				}
				if bench {
					_, stats, _ := timedExec(true, func() ([]byte, uint64, error) {
						_, _, gasUsed, _ := test.RunNoVerify(nil, tx, st, cfg, dirs)
						return nil, gasUsed, nil
					})
					result.Stats = &stats
				}
				return nil
			})
			if err != nil && result.Pass {
				result.Pass, result.Error = false, err.Error()
			}

			db.Close()
			dir.RemoveAll(tmpDir)

			results = append(results, *result)
		}
	}
	return results, nil
}
