// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package tracetest

import (
	"bytes"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/tests"

	// Force-load native and js packages, to trigger registration
	_ "github.com/ledgerwatch/erigon/eth/tracers/js"
	_ "github.com/ledgerwatch/erigon/eth/tracers/native"
)

type callContext struct {
	Number     math.HexOrDecimal64   `json:"number"`
	Difficulty *math.HexOrDecimal256 `json:"difficulty"`
	Time       math.HexOrDecimal64   `json:"timestamp"`
	GasLimit   math.HexOrDecimal64   `json:"gasLimit"`
	Miner      libcommon.Address     `json:"miner"`
}

// callLog is the result of LOG opCode
type callLog struct {
	Address libcommon.Address `json:"address"`
	Topics  []libcommon.Hash  `json:"topics"`
	Data    hexutility.Bytes  `json:"data"`
}

// callTrace is the result of a callTracer run.
type callTrace struct {
	From     libcommon.Address `json:"from"`
	Gas      *hexutil.Uint64   `json:"gas"`
	GasUsed  *hexutil.Uint64   `json:"gasUsed"`
	To       libcommon.Address `json:"to,omitempty"`
	Input    hexutility.Bytes  `json:"input"`
	Output   hexutility.Bytes  `json:"output,omitempty"`
	Error    string            `json:"error,omitempty"`
	Revertal string            `json:"revertReason,omitempty"`
	Calls    []callTrace       `json:"calls,omitempty"`
	Logs     []callLog         `json:"logs,omitempty"`
	Value    *hexutil.Big      `json:"value,omitempty"`
	// Gencodec adds overridden fields at the end
	Type string `json:"type"`
}

// callTracerTest defines a single test to check the call tracer against.
type callTracerTest struct {
	Genesis      *types.Genesis  `json:"genesis"`
	Context      *callContext    `json:"context"`
	Input        string          `json:"input"`
	TracerConfig json.RawMessage `json:"tracerConfig"`
	Result       *callTrace      `json:"result"`
}

// Iterates over all the input-output datasets in the tracer test harness and
// runs the JavaScript tracers against them.
func TestCallTracerLegacy(t *testing.T) {
	testCallTracer("callTracerLegacy", "call_tracer_legacy", t)
}

func TestCallTracerNative(t *testing.T) {
	testCallTracer("callTracer", "call_tracer", t)
}

func TestCallTracerNativeWithLog(t *testing.T) {
	testCallTracer("callTracer", "call_tracer_withLog", t)
}

func testCallTracer(tracerName string, dirPath string, t *testing.T) {
	isLegacy := strings.HasSuffix(dirPath, "_legacy")
	files, err := os.ReadDir(filepath.Join("testdata", dirPath))
	if err != nil {
		t.Fatalf("failed to retrieve tracer test suite: %v", err)
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		file := file // capture range variable
		t.Run(camel(strings.TrimSuffix(file.Name(), ".json")), func(t *testing.T) {
			t.Parallel()

			var (
				test = new(callTracerTest)
			)
			// Call tracer test found, read if from disk
			if blob, err := os.ReadFile(filepath.Join("testdata", dirPath, file.Name())); err != nil {
				t.Fatalf("failed to read testcase: %v", err)
			} else if err := json.Unmarshal(blob, test); err != nil {
				t.Fatalf("failed to parse testcase: %v", err)
			}
			tx, err := types.UnmarshalTransactionFromBinary(common.FromHex(test.Input))
			if err != nil {
				t.Fatalf("failed to parse testcase input: %v", err)
			}
			// Configure a blockchain with the given prestate
			var (
				signer    = types.MakeSigner(test.Genesis.Config, uint64(test.Context.Number))
				origin, _ = signer.Sender(tx)
				txContext = evmtypes.TxContext{
					Origin:   origin,
					GasPrice: tx.GetPrice(),
				}
				context = evmtypes.BlockContext{
					CanTransfer: core.CanTransfer,
					Transfer:    core.Transfer,
					Coinbase:    test.Context.Miner,
					BlockNumber: uint64(test.Context.Number),
					Time:        uint64(test.Context.Time),
					Difficulty:  (*big.Int)(test.Context.Difficulty),
					GasLimit:    uint64(test.Context.GasLimit),
				}
				rules = test.Genesis.Config.Rules(context.BlockNumber, context.Time)
			)
			m := stages.Mock(t)
			dbTx, err := m.DB.BeginRw(m.Ctx)
			require.NoError(t, err)
			defer dbTx.Rollback()
			statedb, _ := tests.MakePreState(rules, dbTx, test.Genesis.Alloc, uint64(test.Context.Number))
			if test.Genesis.BaseFee != nil {
				context.BaseFee, _ = uint256.FromBig(test.Genesis.BaseFee)
			}
			tracer, err := tracers.New(tracerName, new(tracers.Context), test.TracerConfig)
			if err != nil {
				t.Fatalf("failed to create call tracer: %v", err)
			}
			evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Debug: true, Tracer: tracer})
			msg, err := tx.AsMessage(*signer, test.Genesis.BaseFee, rules)
			if err != nil {
				t.Fatalf("failed to prepare transaction for tracing: %v", err)
			}
			vmRet, err := core.ApplyMessage(evm, msg, new(core.GasPool).AddGas(tx.GetGas()).AddDataGas(tx.GetDataGas()), true /* refunds */, false /* gasBailout */)
			if err != nil {
				t.Fatalf("failed to execute transaction: %v", err)
			}
			// Retrieve the trace result and compare against the expected.
			res, err := tracer.GetResult()
			if err != nil {
				t.Fatalf("failed to retrieve trace result: %v", err)
			}
			// The legacy javascript calltracer marshals json in js, which
			// is not deterministic (as opposed to the golang json encoder).
			if isLegacy {
				// This is a tweak to make it deterministic. Can be removed when
				// we remove the legacy tracer.
				var x callTrace
				json.Unmarshal(res, &x)
				res, _ = json.Marshal(x)
			}
			want, err := json.Marshal(test.Result)
			if err != nil {
				t.Fatalf("failed to marshal test: %v", err)
			}
			if string(want) != string(res) {
				t.Fatalf("trace mismatch\n have: %v\n want: %v\n", string(res), string(want))
			}
			// Sanity check: compare top call's gas used against vm result
			type simpleResult struct {
				GasUsed hexutil.Uint64
			}
			var topCall simpleResult
			if err := json.Unmarshal(res, &topCall); err != nil {
				t.Fatalf("failed to unmarshal top calls gasUsed: %v", err)
			}
			if uint64(topCall.GasUsed) != vmRet.UsedGas {
				t.Fatalf("top call has invalid gasUsed. have: %d want: %d", topCall.GasUsed, vmRet.UsedGas)
			}
		})
	}
}

func BenchmarkTracers(b *testing.B) {
	files, err := os.ReadDir(filepath.Join("testdata", "call_tracer"))
	if err != nil {
		b.Fatalf("failed to retrieve tracer test suite: %v", err)
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		file := file // capture range variable
		b.Run(camel(strings.TrimSuffix(file.Name(), ".json")), func(b *testing.B) {
			blob, err := os.ReadFile(filepath.Join("testdata", "call_tracer", file.Name()))
			if err != nil {
				b.Fatalf("failed to read testcase: %v", err)
			}
			test := new(callTracerTest)
			if err := json.Unmarshal(blob, test); err != nil {
				b.Fatalf("failed to parse testcase: %v", err)
			}
			benchTracer(b, "callTracer", test)
		})
	}
}

func benchTracer(b *testing.B, tracerName string, test *callTracerTest) {
	// Configure a blockchain with the given prestate
	tx, err := types.DecodeTransaction(rlp.NewStream(bytes.NewReader(common.FromHex(test.Input)), 0))
	if err != nil {
		b.Fatalf("failed to parse testcase input: %v", err)
	}
	signer := types.MakeSigner(test.Genesis.Config, uint64(test.Context.Number))
	rules := &chain.Rules{}
	msg, err := tx.AsMessage(*signer, nil, rules)
	if err != nil {
		b.Fatalf("failed to prepare transaction for tracing: %v", err)
	}
	origin, _ := signer.Sender(tx)
	txContext := evmtypes.TxContext{
		Origin:   origin,
		GasPrice: tx.GetPrice(),
	}
	context := evmtypes.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		Coinbase:    test.Context.Miner,
		BlockNumber: uint64(test.Context.Number),
		Time:        uint64(test.Context.Time),
		Difficulty:  (*big.Int)(test.Context.Difficulty),
		GasLimit:    uint64(test.Context.GasLimit),
	}
	m := stages.Mock(b)
	dbTx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(b, err)
	defer dbTx.Rollback()
	statedb, _ := tests.MakePreState(rules, dbTx, test.Genesis.Alloc, uint64(test.Context.Number))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracer, err := tracers.New(tracerName, new(tracers.Context), nil)
		if err != nil {
			b.Fatalf("failed to create call tracer: %v", err)
		}
		evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Debug: true, Tracer: tracer})
		snap := statedb.Snapshot()
		st := core.NewStateTransition(evm, msg, new(core.GasPool).AddGas(tx.GetGas()).AddDataGas(tx.GetDataGas()))
		if _, err = st.TransitionDb(true /* refunds */, false /* gasBailout */); err != nil {
			b.Fatalf("failed to execute transaction: %v", err)
		}
		if _, err = tracer.GetResult(); err != nil {
			b.Fatal(err)
		}
		statedb.RevertToSnapshot(snap)
	}
}

// TestZeroValueToNotExitCall tests the calltracer(s) on the following:
// Tx to A, A calls B with zero value. B does not already exist.
// Expected: that enter/exit is invoked and the inner call is shown in the result
func TestZeroValueToNotExitCall(t *testing.T) {
	var to = libcommon.HexToAddress("0x00000000000000000000000000000000deadbeef")
	privkey, err := crypto.HexToECDSA("0000000000000000deadbeef00000000000000000000000000000000deadbeef")
	if err != nil {
		t.Fatalf("err %v", err)
	}
	signer := types.LatestSigner(params.MainnetChainConfig)
	tx, err := types.SignNewTx(privkey, *signer, &types.LegacyTx{
		GasPrice: uint256.NewInt(0),
		CommonTx: types.CommonTx{
			Gas: 50000,
			To:  &to,
		},
	})
	if err != nil {
		t.Fatalf("err %v", err)
	}
	origin, _ := signer.Sender(tx)
	txContext := evmtypes.TxContext{
		Origin:   origin,
		GasPrice: uint256.NewInt(1),
	}
	context := evmtypes.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		Coinbase:    libcommon.Address{},
		BlockNumber: 8000000,
		Time:        5,
		Difficulty:  big.NewInt(0x30000),
		GasLimit:    uint64(6000000),
	}
	var code = []byte{
		byte(vm.PUSH1), 0x0, byte(vm.DUP1), byte(vm.DUP1), byte(vm.DUP1), // in and outs zero
		byte(vm.DUP1), byte(vm.PUSH1), 0xff, byte(vm.GAS), // value=0,address=0xff, gas=GAS
		byte(vm.CALL),
	}
	var alloc = types.GenesisAlloc{
		to: types.GenesisAccount{
			Nonce: 1,
			Code:  code,
		},
		origin: types.GenesisAccount{
			Nonce:   0,
			Balance: big.NewInt(500000000000000),
		},
	}
	rules := params.MainnetChainConfig.Rules(context.BlockNumber, context.Time)
	m := stages.Mock(t)
	dbTx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer dbTx.Rollback()

	statedb, _ := tests.MakePreState(rules, dbTx, alloc, context.BlockNumber)
	// Create the tracer, the EVM environment and run it
	tracer, err := tracers.New("callTracer", nil, nil)
	if err != nil {
		t.Fatalf("failed to create call tracer: %v", err)
	}
	evm := vm.NewEVM(context, txContext, statedb, params.MainnetChainConfig, vm.Config{Debug: true, Tracer: tracer})
	msg, err := tx.AsMessage(*signer, nil, rules)
	if err != nil {
		t.Fatalf("failed to prepare transaction for tracing: %v", err)
	}
	st := core.NewStateTransition(evm, msg, new(core.GasPool).AddGas(tx.GetGas()).AddDataGas(tx.GetDataGas()))
	if _, err = st.TransitionDb(true /* refunds */, false /* gasBailout */); err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}
	// Retrieve the trace result and compare against the etalon
	res, err := tracer.GetResult()
	if err != nil {
		t.Fatalf("failed to retrieve trace result: %v", err)
	}
	wantStr := `{"from":"0x682a80a6f560eec50d54e63cbeda1c324c5f8d1b","gas":"0x7148","gasUsed":"0x54d8","to":"0x00000000000000000000000000000000deadbeef","input":"0x","calls":[{"from":"0x00000000000000000000000000000000deadbeef","gas":"0x6cbf","gasUsed":"0x0","to":"0x00000000000000000000000000000000000000ff","input":"0x","value":"0x0","type":"CALL"}],"value":"0x0","type":"CALL"}`
	if string(res) != wantStr {
		t.Fatalf("trace mismatch\n have: %v\n want: %v\n", string(res), wantStr)
	}
}
