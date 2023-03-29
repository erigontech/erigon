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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	goruntime "runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	common2 "github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/evm/internal/compiler"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/runtime"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/params"
)

var runCommand = cli.Command{
	Action:      runCmd,
	Name:        "run",
	Usage:       "run arbitrary evm binary",
	ArgsUsage:   "<code>",
	Description: `The run command runs arbitrary EVM code.`,
}

// readGenesis will read the given JSON format genesis file and return
// the initialized Genesis structure
func readGenesis(genesisPath string) *types.Genesis {
	// Make sure we have a valid genesis JSON
	//genesisPath := ctx.Args().First()
	if len(genesisPath) == 0 {
		utils.Fatalf("Must supply path to genesis JSON file")
	}
	file, err := os.Open(genesisPath)
	if err != nil {
		utils.Fatalf("Failed to read genesis file: %v", err)
	}
	defer func(file *os.File) {
		closeErr := file.Close()
		if closeErr != nil {
			log.Warn("Failed to close file", "err", closeErr)
		}
	}(file)

	genesis := new(types.Genesis)
	if err := json.NewDecoder(file).Decode(genesis); err != nil {
		utils.Fatalf("invalid genesis file: %v", err)
	}
	return genesis
}

type execStats struct {
	time           time.Duration // The execution time.
	allocs         int64         // The number of heap allocations during execution.
	bytesAllocated int64         // The cumulative number of bytes allocated during execution.
}

func timedExec(bench bool, execFunc func() ([]byte, uint64, error)) (output []byte, gasLeft uint64, stats execStats, err error) {
	if bench {
		result := testing.Benchmark(func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				output, gasLeft, err = execFunc()
			}
		})

		// Get the average execution time from the benchmarking result.
		// There are other useful stats here that could be reported.
		stats.time = time.Duration(result.NsPerOp())
		stats.allocs = result.AllocsPerOp()
		stats.bytesAllocated = result.AllocedBytesPerOp()
	} else {
		var memStatsBefore, memStatsAfter goruntime.MemStats
		common2.ReadMemStats(&memStatsBefore)
		startTime := time.Now()
		output, gasLeft, err = execFunc()
		stats.time = time.Since(startTime)
		common2.ReadMemStats(&memStatsAfter)
		stats.allocs = int64(memStatsAfter.Mallocs - memStatsBefore.Mallocs)
		stats.bytesAllocated = int64(memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc)
	}

	return output, gasLeft, stats, err
}

func runCmd(ctx *cli.Context) error {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	//glogger := log.NewGlogHandler(log.StreamHandler(os.Stderr, log.TerminalFormat(false)))
	//glogger.Verbosity(log.Lvl(ctx.GlobalInt(VerbosityFlag.Name)))
	//log.Root().SetHandler(glogger)
	logconfig := &logger.LogConfig{
		DisableMemory:     ctx.Bool(DisableMemoryFlag.Name),
		DisableStack:      ctx.Bool(DisableStackFlag.Name),
		DisableStorage:    ctx.Bool(DisableStorageFlag.Name),
		DisableReturnData: ctx.Bool(DisableReturnDataFlag.Name),
		Debug:             ctx.Bool(DebugFlag.Name),
	}

	var (
		tracer        vm.EVMLogger
		debugLogger   *logger.StructLogger
		statedb       *state.IntraBlockState
		chainConfig   *chain.Config
		sender        = libcommon.BytesToAddress([]byte("sender"))
		receiver      = libcommon.BytesToAddress([]byte("receiver"))
		genesisConfig *types.Genesis
	)
	if ctx.Bool(MachineFlag.Name) {
		tracer = logger.NewJSONLogger(logconfig, os.Stdout)
	} else if ctx.Bool(DebugFlag.Name) {
		debugLogger = logger.NewStructLogger(logconfig)
		tracer = debugLogger
	} else {
		debugLogger = logger.NewStructLogger(logconfig)
	}
	db := memdb.New("")
	if ctx.String(GenesisFlag.Name) != "" {
		gen := readGenesis(ctx.String(GenesisFlag.Name))
		core.MustCommitGenesis(gen, db, "")
		genesisConfig = gen
		chainConfig = gen.Config
	} else {
		genesisConfig = new(types.Genesis)
	}
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	statedb = state.New(state.NewPlainStateReader(tx))
	if ctx.String(SenderFlag.Name) != "" {
		sender = libcommon.HexToAddress(ctx.String(SenderFlag.Name))
	}
	statedb.CreateAccount(sender, true)

	if ctx.String(ReceiverFlag.Name) != "" {
		receiver = libcommon.HexToAddress(ctx.String(ReceiverFlag.Name))
	}

	var code []byte
	codeFileFlag := ctx.String(CodeFileFlag.Name)
	codeFlag := ctx.String(CodeFlag.Name)

	// The '--code' or '--codefile' flag overrides code in state
	if codeFileFlag != "" || codeFlag != "" {
		var hexcode []byte
		if codeFileFlag != "" {
			var err error
			// If - is specified, it means that code comes from stdin
			if codeFileFlag == "-" {
				//Try reading from stdin
				if hexcode, err = io.ReadAll(os.Stdin); err != nil {
					fmt.Printf("Could not load code from stdin: %v\n", err)
					os.Exit(1)
				}
			} else {
				// Codefile with hex assembly
				if hexcode, err = os.ReadFile(codeFileFlag); err != nil {
					fmt.Printf("Could not load code from file: %v\n", err)
					os.Exit(1)
				}
			}
		} else {
			hexcode = []byte(codeFlag)
		}
		hexcode = bytes.TrimSpace(hexcode)
		if len(hexcode)%2 != 0 {
			fmt.Printf("Invalid input length for hex data (%d)\n", len(hexcode))
			os.Exit(1)
		}
		code = hexutility.MustDecodeHex(string(hexcode))
	} else if fn := ctx.Args().First(); len(fn) > 0 {
		// EASM-file to compile
		src, err := os.ReadFile(fn)
		if err != nil {
			return err
		}
		bin, err := compiler.Compile(fn, src, false)
		if err != nil {
			return err
		}
		code = common.Hex2Bytes(bin)
	}
	initialGas := ctx.Uint64(GasFlag.Name)
	if genesisConfig.GasLimit != 0 {
		initialGas = genesisConfig.GasLimit
	}
	value, _ := uint256.FromBig(utils.BigFlagValue(ctx, ValueFlag.Name))
	gasPrice, _ := uint256.FromBig(utils.BigFlagValue(ctx, PriceFlag.Name))
	runtimeConfig := runtime.Config{
		Origin:      sender,
		State:       statedb,
		GasLimit:    initialGas,
		GasPrice:    gasPrice,
		Value:       value,
		Difficulty:  genesisConfig.Difficulty,
		Time:        new(big.Int).SetUint64(genesisConfig.Timestamp),
		Coinbase:    genesisConfig.Coinbase,
		BlockNumber: new(big.Int).SetUint64(genesisConfig.Number),
		EVMConfig: vm.Config{
			Tracer: tracer,
			Debug:  ctx.Bool(DebugFlag.Name) || ctx.Bool(MachineFlag.Name),
		},
	}

	if cpuProfilePath := ctx.String(CPUProfileFlag.Name); cpuProfilePath != "" {
		f, err := os.Create(cpuProfilePath)
		if err != nil {
			fmt.Println("could not create CPU profile: ", err)
			os.Exit(1)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Println("could not start CPU profile: ", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	if chainConfig != nil {
		runtimeConfig.ChainConfig = chainConfig
	} else {
		runtimeConfig.ChainConfig = params.AllProtocolChanges
	}

	var hexInput []byte
	if inputFileFlag := ctx.String(InputFileFlag.Name); inputFileFlag != "" {
		var err error
		if hexInput, err = os.ReadFile(inputFileFlag); err != nil {
			fmt.Printf("could not load input from file: %v\n", err)
			os.Exit(1)
		}
	} else {
		hexInput = []byte(ctx.String(InputFlag.Name))
	}
	input := hexutility.MustDecodeHex(string(bytes.TrimSpace(hexInput)))

	var execFunc func() ([]byte, uint64, error)
	if ctx.Bool(CreateFlag.Name) {
		input = append(code, input...)
		execFunc = func() ([]byte, uint64, error) {
			output, _, gasLeft, err := runtime.Create(input, &runtimeConfig, 0)
			return output, gasLeft, err
		}
	} else {
		if len(code) > 0 {
			statedb.SetCode(receiver, code)
		}
		execFunc = func() ([]byte, uint64, error) {
			return runtime.Call(receiver, input, &runtimeConfig)
		}
	}

	bench := ctx.Bool(BenchFlag.Name)
	output, leftOverGas, stats, err := timedExec(bench, execFunc)

	if ctx.Bool(DumpFlag.Name) {
		rules := &chain.Rules{}
		if chainConfig != nil {
			rules = chainConfig.Rules(runtimeConfig.BlockNumber.Uint64(), runtimeConfig.Time.Uint64())
		}
		if err = statedb.CommitBlock(rules, state.NewNoopWriter()); err != nil {
			fmt.Println("Could not commit state: ", err)
			os.Exit(1)
		}
		historyV3, err := kvcfg.HistoryV3.Enabled(tx)
		if err != nil {
			return err
		}
		fmt.Println(string(state.NewDumper(tx, 0, historyV3).DefaultDump()))
	}

	if memProfilePath := ctx.String(MemProfileFlag.Name); memProfilePath != "" {
		f, err := os.Create(memProfilePath)
		if err != nil {
			fmt.Println("could not create memory profile: ", err)
			os.Exit(1)
		}
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Println("could not write memory profile: ", err)
			os.Exit(1)
		}
		closeErr := f.Close()
		if closeErr != nil {
			log.Warn("Failed to close file", "err", closeErr)
		}
	}

	if ctx.Bool(DebugFlag.Name) {
		if debugLogger != nil {
			_, printErr := fmt.Fprintln(os.Stderr, "#### TRACE ####")
			if printErr != nil {
				log.Warn("Failed to print to stderr", "err", printErr)
			}
			logger.WriteTrace(os.Stderr, debugLogger.StructLogs())
		}
		_, printErr := fmt.Fprintln(os.Stderr, "#### LOGS ####")
		if printErr != nil {
			log.Warn("Failed to print to stderr", "err", printErr)
		}
		logger.WriteLogs(os.Stderr, statedb.Logs())
	}

	if bench || ctx.Bool(StatDumpFlag.Name) {
		_, printErr := fmt.Fprintf(os.Stderr, `EVM gas used:    %d
execution time:  %v
allocations:     %d
allocated bytes: %d
`, initialGas-leftOverGas, stats.time, stats.allocs, stats.bytesAllocated)
		if printErr != nil {
			log.Warn("Failed to print to stderr", "err", printErr)
		}
	}
	if tracer == nil {
		fmt.Printf("0x%x\n", output)
		if err != nil {
			fmt.Printf(" error: %v\n", err)
		}
	}

	return nil
}
