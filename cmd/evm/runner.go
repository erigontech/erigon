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
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/evm/internal/compiler"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/cmd/utils/flags"
	"github.com/erigontech/erigon/core/genesiswrite"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/core/vm/runtime"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/eth/tracers/logger"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
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
	Time           time.Duration `json:"time"`           // The execution time.
	Allocs         int64         `json:"allocs"`         // The number of heap allocations during execution.
	BytesAllocated int64         `json:"bytesAllocated"` // The cumulative number of bytes allocated during execution.
	GasUsed        uint64        `json:"gasUsed"`        // the amount of gas used during execution
}

func timedExec(bench bool, execFunc func() ([]byte, uint64, error)) (output []byte, stats execStats, err error) {
	if bench {
		// Do one warm-up run
		output, gasUsed, err := execFunc()
		result := testing.Benchmark(func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				haveOutput, haveGasUsed, haveErr := execFunc()
				if !bytes.Equal(haveOutput, output) {
					panic(fmt.Sprintf("output differs\nhave %x\nwant %x\n", haveOutput, output))
				}
				if haveGasUsed != gasUsed {
					panic(fmt.Sprintf("gas differs, have %v want %v", haveGasUsed, gasUsed))
				}
				if haveErr != err {
					panic(fmt.Sprintf("err differs, have %v want %v", haveErr, err))
				}
			}
		})

		// Get the average execution time from the benchmarking result.
		// There are other useful stats here that could be reported.
		stats.Time = time.Duration(result.NsPerOp())
		stats.Allocs = result.AllocsPerOp()
		stats.BytesAllocated = result.AllocedBytesPerOp()
		stats.GasUsed = gasUsed
	} else {
		var memStatsBefore, memStatsAfter goruntime.MemStats
		dbg.ReadMemStats(&memStatsBefore)
		startTime := time.Now()
		output, stats.GasUsed, err = execFunc()
		stats.Time = time.Since(startTime)
		dbg.ReadMemStats(&memStatsAfter)
		stats.Allocs = int64(memStatsAfter.Mallocs - memStatsBefore.Mallocs)
		stats.BytesAllocated = int64(memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc)
	}

	return output, stats, err
}

func runCmd(ctx *cli.Context) error {
	machineFriendlyOutput := ctx.Bool(MachineFlag.Name)
	if machineFriendlyOutput {
		log.Root().SetHandler(log.DiscardHandler())
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(ctx.Int(VerbosityFlag.Name)), log.StderrHandler))
	}
	logconfig := &logger.LogConfig{
		DisableMemory:     ctx.Bool(DisableMemoryFlag.Name),
		DisableStack:      ctx.Bool(DisableStackFlag.Name),
		DisableStorage:    ctx.Bool(DisableStorageFlag.Name),
		DisableReturnData: ctx.Bool(DisableReturnDataFlag.Name),
		Debug:             ctx.Bool(DebugFlag.Name),
	}

	var (
		tracer        *tracers.Tracer
		debugLogger   *logger.StructLogger
		statedb       *state.IntraBlockState
		chainConfig   *chain.Config
		sender        = common.BytesToAddress([]byte("sender"))
		receiver      = common.BytesToAddress([]byte("receiver"))
		genesisConfig *types.Genesis
	)
	if machineFriendlyOutput {
		tracer = logger.NewJSONLogger(logconfig, os.Stderr).Tracer()
	} else if ctx.Bool(DebugFlag.Name) {
		debugLogger = logger.NewStructLogger(logconfig)
		tracer = debugLogger.Tracer()
	} else {
		debugLogger = logger.NewStructLogger(logconfig)
	}
	db := memdb.New(os.TempDir(), kv.ChainDB)
	defer db.Close()
	if ctx.String(GenesisFlag.Name) != "" {
		gen := readGenesis(ctx.String(GenesisFlag.Name))
		genesiswrite.MustCommitGenesis(gen, db, datadir.New(""), log.Root())
		genesisConfig = gen
		chainConfig = gen.Config
	} else {
		genesisConfig = new(types.Genesis)
	}
	agg, err := dbstate.NewAggregator(context.Background(), datadir.New(os.TempDir()), config3.DefaultStepSize, db, log.New())
	if err != nil {
		return err
	}
	defer agg.Close()
	tdb, err := temporal.New(db, agg)
	if err != nil {
		return err
	}
	tx, err := tdb.BeginTemporalRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sd, err := dbstate.NewSharedDomains(tx, log.Root())
	if err != nil {
		return err
	}
	defer sd.Close()
	stateReader := state.NewReaderV3(sd.AsGetter(tx))
	statedb = state.New(stateReader)
	if ctx.String(SenderFlag.Name) != "" {
		sender = common.HexToAddress(ctx.String(SenderFlag.Name))
	}
	statedb.CreateAccount(sender, true)

	if ctx.String(ReceiverFlag.Name) != "" {
		receiver = common.HexToAddress(ctx.String(ReceiverFlag.Name))
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
		code = hexutil.MustDecodeHex(string(hexcode))
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
	value, _ := uint256.FromBig(flags.GlobalBig(ctx, ValueFlag.Name))
	gasPrice, _ := uint256.FromBig(flags.GlobalBig(ctx, PriceFlag.Name))
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
	}

	if tracer != nil {
		runtimeConfig.EVMConfig.Tracer = tracer.Hooks
	}
	runtimeConfig.EVMConfig.JumpDestCache = vm.NewJumpDestCache(16)

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
		runtimeConfig.ChainConfig = chain.AllProtocolChanges
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
	input := hexutil.MustDecodeHex(string(bytes.TrimSpace(hexInput)))

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
			output, gasLeft, err := runtime.Call(receiver, input, &runtimeConfig)
			return output, initialGas - gasLeft, err
		}
	}

	bench := ctx.Bool(BenchFlag.Name)
	output, stats, err := timedExec(bench, execFunc)

	if ctx.Bool(DumpFlag.Name) {
		rules := &chain.Rules{}
		if chainConfig != nil {
			blockContext := evmtypes.BlockContext{
				BlockNumber: runtimeConfig.BlockNumber.Uint64(),
				Time:        runtimeConfig.Time.Uint64(),
			}
			rules = blockContext.Rules(chainConfig)
		}
		if err = statedb.CommitBlock(rules, state.NewNoopWriter()); err != nil {
			fmt.Println("Could not commit state: ", err)
			os.Exit(1)
		}
		fmt.Println(string(state.NewDumper(tx, rawdbv3.TxNums, 0).DefaultDump()))
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
`, stats.GasUsed, stats.Time, stats.Allocs, stats.BytesAllocated)
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
