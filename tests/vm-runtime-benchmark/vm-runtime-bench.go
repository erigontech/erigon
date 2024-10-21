package main

import (
	"flag"
	"fmt"
	"math"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	_ "unsafe"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/runtime"
	"github.com/ledgerwatch/erigon/crypto"
)

var bytecodeStore string = ""
var preserveState bool = false
var csv bool = false

// Initialize some constant calldata of 128KB, 2^17 bytes.
// This means, if we offset between 0th and 2^16th byte, we can fetch between 0 and 2^16 bytes (64KB)
// In consequence, we need args to memory-copying OPCODEs to be between 0 and 2^16, 2^16 fits in a PUSH3,
// which we'll be using to generate arguments for those OPCODEs.
var calldata = []byte(strings.Repeat("{", 1<<17))

// sets defaults on the config
func setDefaults(cfg *runtime.Config, b *testing.B) {
	// cfg.State, _ = state.New(types.EmptyRootHash, state.NewDatabase(rawdb.NewMemoryDatabase()), nil)
	_, tx := memdb.NewTestTx(b)
	cfg.State = state.New(state.NewPlainState(tx, 1, nil))
	var (
		origin   = libcommon.HexToAddress("origin")
		coinbase = libcommon.HexToAddress("coinbase")
		contract = libcommon.HexToAddress("contract")
	)
	cfg.Origin = origin
	cfg.State.CreateAccount(origin, true)
	cfg.Coinbase = coinbase
	cfg.State.CreateAccount(coinbase, true)
	cfg.State.CreateAccount(contract, true)

	if cfg.ChainConfig == nil {
		cfg.ChainConfig = &chain.Config{
			ChainID:               big.NewInt(1),
			HomesteadBlock:        new(big.Int),
			TangerineWhistleBlock: new(big.Int),
			SpuriousDragonBlock:   new(big.Int),
			ByzantiumBlock:        new(big.Int),
			ConstantinopleBlock:   new(big.Int),
			PetersburgBlock:       new(big.Int),
			IstanbulBlock:         new(big.Int),
			MuirGlacierBlock:      new(big.Int),
			BerlinBlock:           new(big.Int),
			LondonBlock:           new(big.Int),
			ArrowGlacierBlock:     new(big.Int),
			GrayGlacierBlock:      new(big.Int),
			ShanghaiTime:          new(big.Int),
			CancunTime:            new(big.Int),
			PragueTime:            new(big.Int),
			OsakaTime:             new(big.Int),
		}
	}

	if cfg.Difficulty == nil {
		cfg.Difficulty = new(big.Int)
	}
	if cfg.Time == nil {
		cfg.Time = big.NewInt(time.Now().Unix())
	}
	if cfg.GasLimit == 0 {
		cfg.GasLimit = math.MaxUint64
	}
	if cfg.GasPrice == nil {
		cfg.GasPrice = new(uint256.Int)
	}
	if cfg.Value == nil {
		cfg.Value = new(uint256.Int)
	}
	if cfg.BlockNumber == nil {
		cfg.BlockNumber = new(big.Int)
	}
	if cfg.GetHashFn == nil {
		cfg.GetHashFn = func(n uint64) libcommon.Hash {
			return libcommon.BytesToHash(crypto.Keccak256([]byte(new(big.Int).SetUint64(n).String())))
		}
	}
}

func BenchmarkBytecodeExecution(b *testing.B) {
	b.ReportAllocs()

	bytecode := libcommon.Hex2Bytes(bytecodeStore)
	cfg := new(runtime.Config)
	setDefaults(cfg, b)

	var snapshotId int

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshotId = cfg.State.Snapshot()
		if _, _, err := runtime.Execute(bytecode, calldata, cfg, 0); err != nil {
			fmt.Fprintln(os.Stderr, err)
			b.Fail()
		}
		cfg.State.RevertToSnapshot(snapshotId)
	}
}

func BenchmarkBytecodeExecutionNonModyfing(b *testing.B) {
	b.ReportAllocs()

	bytecode := libcommon.Hex2Bytes(bytecodeStore)
	cfg := new(runtime.Config)
	setDefaults(cfg, b)

	sender := vm.AccountRef(cfg.Origin)
	contract := libcommon.HexToAddress("contract")

	vmenv := runtime.NewEnv(cfg)
	cfg.State.SetCode(contract, bytecode)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := vmenv.Call(sender, contract, calldata, cfg.GasLimit, cfg.Value, false); err != nil {
			fmt.Fprintln(os.Stderr, err)
			b.Fail()
		}
	}
}

func runBenchmark(samples int) {
	if csv {
		fmt.Println("SampleId, ops, ns/op, mem allocs/op, mem bytes/op")
	} else {
		fmt.Println("Results of benchmarking EVM bytecode execution:")
	}

	if preserveState {
		for i := 0; i < samples; i++ {
			result := testing.Benchmark(BenchmarkBytecodeExecution)
			outputResults(i, result)
		}
	} else {
		for i := 0; i < samples; i++ {
			result := testing.Benchmark(BenchmarkBytecodeExecutionNonModyfing)
			outputResults(i, result)
		}
	}
}

func outputResults(sampleId int, r testing.BenchmarkResult) {
	if csv {
		fmt.Printf("%v,%v,%v,%v,%v\n", sampleId, r.N, r.NsPerOp(), r.AllocsPerOp(), r.AllocedBytesPerOp())
	} else {
		fmt.Printf("%v: %v %v\n", sampleId, r.String(), r.MemString())
	}
}

func main() {
	bytecodePtr := flag.String("bytecode", "", "EVM bytecode to execute and measure, e.g. 61FFFF600020 (mandatory)")
	calldataPtr := flag.String("calldata", "", "Calldata to pass to the EVM bytecode")
	samplesPtr := flag.Int("samples", 1, "Number of measured repetitions of execution")
	preserveStatePtr := flag.Bool("preserveState", false, "Preserve state between executions, in case of a state modifying bytecode, adds overhead for snapshotting")
	csvPtr := flag.Bool("csv", true, "Output results in CSV format (default: true)")

	flag.Parse()

	bytecodeStore = *bytecodePtr
	samples := *samplesPtr
	preserveState = *preserveStatePtr
	csv = *csvPtr

	if bytecodeStore == "" {
		fmt.Println("Please provide a bytecode to execute")
		os.Exit(1)
	}

	if *calldataPtr != "" {
		calldata = libcommon.Hex2Bytes(*calldataPtr)
		if len(calldata) == 0 {
			fmt.Println("Invalid calldata provided")
			os.Exit(1)
		}
	}

	runBenchmark(samples)
}
