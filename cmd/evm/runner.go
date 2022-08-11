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
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/urfave/cli"

	"github.com/ledgerwatch/erigon/cmd/evm/internal/compiler"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/runtime"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
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
func readGenesis(genesisPath string) *core.Genesis {
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

	genesis := new(core.Genesis)
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
	logconfig := &vm.LogConfig{
		DisableMemory:     ctx.GlobalBool(DisableMemoryFlag.Name),
		DisableStack:      ctx.GlobalBool(DisableStackFlag.Name),
		DisableStorage:    ctx.GlobalBool(DisableStorageFlag.Name),
		DisableReturnData: ctx.GlobalBool(DisableReturnDataFlag.Name),
		Debug:             ctx.GlobalBool(DebugFlag.Name),
	}

	var (
		tracer        vm.Tracer
		debugLogger   *vm.StructLogger
		statedb       *state.IntraBlockState
		chainConfig   *params.ChainConfig
		sender        = common.BytesToAddress([]byte("sender"))
		receiver      = common.BytesToAddress([]byte("receiver"))
		genesisConfig *core.Genesis
	)
	if ctx.GlobalBool(MachineFlag.Name) {
		tracer = vm.NewJSONLogger(logconfig, os.Stdout)
	} else if ctx.GlobalBool(DebugFlag.Name) {
		debugLogger = vm.NewStructLogger(logconfig)
		tracer = debugLogger
	} else {
		debugLogger = vm.NewStructLogger(logconfig)
	}
	db := memdb.New()
	if ctx.GlobalString(GenesisFlag.Name) != "" {
		gen := readGenesis(ctx.GlobalString(GenesisFlag.Name))
		gen.MustCommit(db)
		genesisConfig = gen
		chainConfig = gen.Config
	} else {
		genesisConfig = new(core.Genesis)
	}
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	statedb = state.New(state.NewPlainStateReader(tx))
	if ctx.GlobalString(SenderFlag.Name) != "" {
		sender = common.HexToAddress(ctx.GlobalString(SenderFlag.Name))
	}
	statedb.CreateAccount(sender, true)

	if ctx.GlobalString(ReceiverFlag.Name) != "" {
		receiver = common.HexToAddress(ctx.GlobalString(ReceiverFlag.Name))
	}

	var code []byte
	codeFileFlag := ctx.GlobalString(CodeFileFlag.Name)
	codeFlag := ctx.GlobalString(CodeFlag.Name)

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
		code = common.FromHex(string(hexcode))
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
	initialGas := ctx.GlobalUint64(GasFlag.Name)
	if genesisConfig.GasLimit != 0 {
		initialGas = genesisConfig.GasLimit
	}
	value, _ := uint256.FromBig(utils.GlobalBig(ctx, ValueFlag.Name))
	runtimeConfig := runtime.Config{
		Origin:      sender,
		State:       statedb,
		GasLimit:    initialGas,
		GasPrice:    utils.GlobalBig(ctx, PriceFlag.Name),
		Value:       value,
		Difficulty:  genesisConfig.Difficulty,
		Time:        new(big.Int).SetUint64(genesisConfig.Timestamp),
		Coinbase:    genesisConfig.Coinbase,
		BlockNumber: new(big.Int).SetUint64(genesisConfig.Number),
		EVMConfig: vm.Config{
			Tracer: tracer,
			Debug:  ctx.GlobalBool(DebugFlag.Name) || ctx.GlobalBool(MachineFlag.Name),
		},
	}

	if cpuProfilePath := ctx.GlobalString(CPUProfileFlag.Name); cpuProfilePath != "" {
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
		runtimeConfig.ChainConfig = params.AllEthashProtocolChanges
	}

	var hexInput []byte
	if inputFileFlag := ctx.GlobalString(InputFileFlag.Name); inputFileFlag != "" {
		var err error
		if hexInput, err = os.ReadFile(inputFileFlag); err != nil {
			fmt.Printf("could not load input from file: %v\n", err)
			os.Exit(1)
		}
	} else {
		hexInput = []byte(ctx.GlobalString(InputFlag.Name))
	}
	input := common.FromHex(string(bytes.TrimSpace(hexInput)))

	var execFunc func() ([]byte, uint64, error)
	if ctx.GlobalBool(CreateFlag.Name) {
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

	bench := ctx.GlobalBool(BenchFlag.Name)
	output, leftOverGas, stats, err := timedExec(bench, execFunc)

	if ctx.GlobalBool(DumpFlag.Name) {
		rules := &params.Rules{}
		if chainConfig != nil {
			rules = chainConfig.Rules(runtimeConfig.BlockNumber.Uint64())
		}
		if err = statedb.CommitBlock(rules, state.NewNoopWriter()); err != nil {
			fmt.Println("Could not commit state: ", err)
			os.Exit(1)
		}
		fmt.Println(string(state.NewDumper(tx, 0).DefaultDump()))
	}

	if memProfilePath := ctx.GlobalString(MemProfileFlag.Name); memProfilePath != "" {
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

	if ctx.GlobalBool(DebugFlag.Name) {
		if debugLogger != nil {
			_, printErr := fmt.Fprintln(os.Stderr, "#### TRACE ####")
			if printErr != nil {
				log.Warn("Failed to print to stderr", "err", printErr)
			}
			vm.WriteTrace(os.Stderr, debugLogger.StructLogs())
		}
		_, printErr := fmt.Fprintln(os.Stderr, "#### LOGS ####")
		if printErr != nil {
			log.Warn("Failed to print to stderr", "err", printErr)
		}
		vm.WriteLogs(os.Stderr, statedb.Logs())
	}

	if bench || ctx.GlobalBool(StatDumpFlag.Name) {
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
