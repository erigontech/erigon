// Copyright 2021 The go-ethereum Authors
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

package tracetest

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/math"

	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/js"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type callContext struct {
	Number     math.HexOrDecimal64 `json:"number"`
	Difficulty *uint256.Int        `json:"difficulty"`
	Time       math.HexOrDecimal64 `json:"timestamp"`
	GasLimit   math.HexOrDecimal64 `json:"gasLimit"`
	BaseFee    *uint256.Int        `json:"baseFeePerGas"`
	Miner      common.Address      `json:"miner"`
}

// callLog is the result of LOG opCode
type callLog struct {
	Index    hexutil.Uint64 `json:"index"`
	Address  common.Address `json:"address"`
	Topics   []common.Hash  `json:"topics"`
	Data     hexutil.Bytes  `json:"data"`
	Position hexutil.Uint   `json:"position"`
}

// callTrace is the result of a callTracer run.
type callTrace struct {
	From     common.Address  `json:"from"`
	Gas      *hexutil.Uint64 `json:"gas"`
	GasUsed  *hexutil.Uint64 `json:"gasUsed"`
	To       common.Address  `json:"to,omitempty"`
	Input    hexutil.Bytes   `json:"input"`
	Output   hexutil.Bytes   `json:"output,omitempty"`
	Error    string          `json:"error,omitempty"`
	Revertal string          `json:"revertReason,omitempty"`
	Calls    []callTrace     `json:"calls,omitempty"`
	Logs     []callLog       `json:"logs,omitempty"`
	Value    *hexutil.Big    `json:"value,omitempty"`
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
	if testing.Short() {
		t.Skip("slow test")
	}
	testCallTracer("callTracerLegacy", "call_tracer_legacy", t)
}

func TestCallTracerNative(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	testCallTracer("callTracer", "call_tracer", t)
}

func TestCallTracerNativeWithLog(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	testCallTracer("callTracer", "call_tracer_withLog", t)
}

func testCallTracer(tracerName string, dirPath string, t *testing.T) {
	isLegacy := strings.HasSuffix(dirPath, "_legacy")
	files, err := dir.ReadDir(filepath.Join("testdata", dirPath))
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
			tx, err := types.UnmarshalTransactionFromBinary(common.FromHex(test.Input), false /* blobTxnsAreWrappedWithBlobs */)
			if err != nil {
				t.Fatalf("failed to parse testcase input: %v", err)
			}
			// Configure a blockchain with the given prestate
			signer := types.MakeSigner(test.Genesis.Config, uint64(test.Context.Number), uint64(test.Context.Time))
			context := evmtypes.BlockContext{
				CanTransfer: protocol.CanTransfer,
				Transfer:    misc.Transfer,
				Coinbase:    accounts.InternAddress(test.Context.Miner),
				BlockNumber: uint64(test.Context.Number),
				Time:        uint64(test.Context.Time),
				GasLimit:    uint64(test.Context.GasLimit),
			}
			if test.Context.Difficulty != nil {
				context.Difficulty = *test.Context.Difficulty
			}
			if test.Context.BaseFee != nil {
				baseFee := test.Context.BaseFee
				context.BaseFee = *baseFee
			}
			rules := context.Rules(test.Genesis.Config)

			m := execmoduletester.New(t)
			dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
			require.NoError(t, err)
			defer dbTx.Rollback()
			statedb, err := testutil.MakePreState(rules, dbTx, test.Genesis.Alloc, uint64(test.Context.Number))
			require.NoError(t, err)
			tracer, err := tracers.New(tracerName, new(tracers.Context), test.TracerConfig)
			if err != nil {
				t.Fatalf("failed to create call tracer: %v", err)
			}
			statedb.SetHooks(tracer.Hooks)
			msg, err := tx.AsMessage(*signer, test.Context.BaseFee, rules)
			if err != nil {
				t.Fatalf("failed to prepare transaction for tracing: %v", err)
			}
			txContext := protocol.NewEVMTxContext(msg)
			evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Tracer: tracer.Hooks})
			tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
			vmRet, err := protocol.ApplyMessage(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()), true /* refunds */, false /* gasBailout */, nil /* engine */)
			if err != nil {
				t.Fatalf("failed to execute transaction: %v", err)
			}
			tracer.OnTxEnd(&types.Receipt{GasUsed: vmRet.ReceiptGasUsed}, err)
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
				err = json.Unmarshal(res, &x)
				require.NoError(t, err)
				res, err = json.Marshal(x)
				require.NoError(t, err)
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
			if uint64(topCall.GasUsed) != vmRet.ReceiptGasUsed {
				t.Fatalf("top call has invalid gasUsed. have: %d want: %d", topCall.GasUsed, vmRet.ReceiptGasUsed)
			}
		})
	}
}

// evmLog0 is a 5-byte EVM snippet that emits a zero-topic, zero-data LOG0.
var evmLog0 = []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.LOG0)}

// evmRevert is a 5-byte EVM snippet that REVERTs with no return data.
var evmRevert = []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.REVERT)}

// evmCallTo returns a 34-byte EVM snippet that CALLs a 20-byte address whose last byte is addr.
// The return value is discarded (POP).
func evmCallTo(addr byte) []byte {
	return []byte{
		byte(vm.PUSH1), 0x00, // retSize
		byte(vm.PUSH1), 0x00, // retOffset
		byte(vm.PUSH1), 0x00, // argsSize
		byte(vm.PUSH1), 0x00, // argsOffset
		byte(vm.PUSH1), 0x00, // value
		byte(vm.PUSH20),
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, addr,
		byte(vm.GAS), byte(vm.CALL),
		byte(vm.POP),
	}
}

// TestCallTracerWithLogPositionAfterRevert verifies that the position field in callTracer logs
// correctly counts a reverted subcall:
//   - LOG_A emitted before the subcall → position 0x0
//   - CALL to addrB which REVERTs (recorded in calls[] even on revert)
//   - LOG_B emitted after the reverted subcall → position 0x1
func TestCallTracerWithLogPositionAfterRevert(t *testing.T) {
	var (
		addrA = common.HexToAddress("0x00000000000000000000000000000000000000aa")
		addrB = common.HexToAddress("0x00000000000000000000000000000000000000bb")
	)

	codeB := evmRevert

	codeA := evmLog0
	codeA = append(codeA, evmCallTo(0xbb)...)
	codeA = append(codeA, evmLog0...)
	codeA = append(codeA, byte(vm.STOP))

	privkey, err := crypto.HexToECDSA("0000000000000000deadbeef00000000000000000000000000000000deadbeef")
	if err != nil {
		t.Fatalf("err %v", err)
	}
	signer := types.LatestSigner(chainspec.Mainnet.Config)
	tx, err := types.SignNewTx(privkey, *signer, &types.LegacyTx{
		GasPrice: *uint256.NewInt(0),
		CommonTx: types.CommonTx{
			GasLimit: 100000,
			To:       &addrA,
		},
	})
	if err != nil {
		t.Fatalf("err %v", err)
	}
	origin, _ := signer.Sender(tx)
	txContext := evmtypes.TxContext{
		Origin:   origin,
		GasPrice: *uint256.NewInt(1),
	}
	context := evmtypes.BlockContext{
		CanTransfer: protocol.CanTransfer,
		Transfer:    misc.Transfer,
		Coinbase:    accounts.ZeroAddress,
		BlockNumber: 8000000,
		Time:        5,
		Difficulty:  *uint256.NewInt(0x30000),
		GasLimit:    uint64(6000000),
	}
	alloc := types.GenesisAlloc{
		addrA: types.GenesisAccount{Nonce: 1, Code: codeA},
		addrB: types.GenesisAccount{Nonce: 1, Code: codeB},
		origin.Value(): types.GenesisAccount{
			Nonce:   0,
			Balance: big.NewInt(500000000000000),
		},
	}
	rules := context.Rules(chainspec.Mainnet.Config)
	m := execmoduletester.New(t)
	dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(t, err)
	defer dbTx.Rollback()
	statedb, _ := testutil.MakePreState(rules, dbTx, alloc, context.BlockNumber)

	tracer, err := tracers.New("callTracer", nil, json.RawMessage(`{"withLog":true}`))
	if err != nil {
		t.Fatalf("failed to create call tracer: %v", err)
	}
	statedb.SetHooks(tracer.Hooks)
	evm := vm.NewEVM(context, txContext, statedb, chainspec.Mainnet.Config, vm.Config{Tracer: tracer.Hooks})
	msg, err := tx.AsMessage(*signer, nil, rules)
	if err != nil {
		t.Fatalf("failed to prepare transaction for tracing: %v", err)
	}
	tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
	st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
	vmRet, err := st.Execute(true /* refunds */, false /* gasBailout */)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}
	tracer.OnTxEnd(&types.Receipt{GasUsed: vmRet.ReceiptGasUsed}, err)

	res, err := tracer.GetResult()
	if err != nil {
		t.Fatalf("failed to retrieve trace result: %v", err)
	}
	var result callTrace
	if err := json.Unmarshal(res, &result); err != nil {
		t.Fatalf("failed to unmarshal trace: %v", err)
	}

	require.Len(t, result.Logs, 2, "expected 2 logs in top frame")
	require.Equal(t, hexutil.Uint(0), result.Logs[0].Position, "LOG_A: emitted before subcall, position should be 0x0")
	require.Equal(t, hexutil.Uint(1), result.Logs[1].Position, "LOG_B: emitted after reverted subcall, position should be 0x1")
	require.Len(t, result.Calls, 1, "expected 1 subcall")
	require.Equal(t, "execution reverted", result.Calls[0].Error, "subcall should be reverted")
}

// TestCallTracerWithLogPositionMixedSubcalls verifies that position counts both successful
// and reverted subcalls:
//   - LOG_A before any subcall → position 0x0
//   - CALL addrB (succeeds) → calls[] len becomes 1
//   - CALL addrC (reverts) → calls[] len becomes 2
//   - LOG_B after both → position 0x2
func TestCallTracerWithLogPositionMixedSubcalls(t *testing.T) {
	var (
		addrA = common.HexToAddress("0x00000000000000000000000000000000000000aa")
		addrB = common.HexToAddress("0x00000000000000000000000000000000000000bb")
		addrC = common.HexToAddress("0x00000000000000000000000000000000000000cc")
	)

	codeB := []byte{byte(vm.STOP)} // succeeds
	codeC := evmRevert

	codeA := evmLog0
	codeA = append(codeA, evmCallTo(0xbb)...)
	codeA = append(codeA, evmCallTo(0xcc)...)
	codeA = append(codeA, evmLog0...)
	codeA = append(codeA, byte(vm.STOP))

	privkey, err := crypto.HexToECDSA("0000000000000000deadbeef00000000000000000000000000000000deadbeef")
	if err != nil {
		t.Fatalf("err %v", err)
	}
	signer := types.LatestSigner(chainspec.Mainnet.Config)
	tx, err := types.SignNewTx(privkey, *signer, &types.LegacyTx{
		GasPrice: *uint256.NewInt(0),
		CommonTx: types.CommonTx{GasLimit: 100000, To: &addrA},
	})
	if err != nil {
		t.Fatalf("err %v", err)
	}
	origin, _ := signer.Sender(tx)
	context := evmtypes.BlockContext{
		CanTransfer: protocol.CanTransfer,
		Transfer:    misc.Transfer,
		Coinbase:    accounts.ZeroAddress,
		BlockNumber: 8000000,
		Time:        5,
		Difficulty:  *uint256.NewInt(0x30000),
		GasLimit:    uint64(6000000),
	}
	alloc := types.GenesisAlloc{
		addrA: types.GenesisAccount{Nonce: 1, Code: codeA},
		addrB: types.GenesisAccount{Nonce: 1, Code: codeB},
		addrC: types.GenesisAccount{Nonce: 1, Code: codeC},
		origin.Value(): types.GenesisAccount{
			Nonce:   0,
			Balance: big.NewInt(500000000000000),
		},
	}
	rules := context.Rules(chainspec.Mainnet.Config)
	m := execmoduletester.New(t)
	dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(t, err)
	defer dbTx.Rollback()
	statedb, _ := testutil.MakePreState(rules, dbTx, alloc, context.BlockNumber)

	tracer, err := tracers.New("callTracer", nil, json.RawMessage(`{"withLog":true}`))
	if err != nil {
		t.Fatalf("failed to create call tracer: %v", err)
	}
	statedb.SetHooks(tracer.Hooks)
	evm := vm.NewEVM(context, evmtypes.TxContext{Origin: origin, GasPrice: *uint256.NewInt(1)}, statedb, chainspec.Mainnet.Config, vm.Config{Tracer: tracer.Hooks})
	msg, err := tx.AsMessage(*signer, nil, rules)
	if err != nil {
		t.Fatalf("failed to prepare transaction for tracing: %v", err)
	}
	tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
	st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
	vmRet, err := st.Execute(true, false)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}
	tracer.OnTxEnd(&types.Receipt{GasUsed: vmRet.ReceiptGasUsed}, err)

	res, err := tracer.GetResult()
	if err != nil {
		t.Fatalf("failed to retrieve trace result: %v", err)
	}
	var result callTrace
	if err := json.Unmarshal(res, &result); err != nil {
		t.Fatalf("failed to unmarshal trace: %v", err)
	}

	require.Len(t, result.Logs, 2, "expected 2 logs in top frame")
	require.Equal(t, hexutil.Uint(0), result.Logs[0].Position, "LOG_A: before any subcall, position should be 0x0")
	require.Equal(t, hexutil.Uint(2), result.Logs[1].Position, "LOG_B: after success+revert subcalls, position should be 0x2")
	require.Len(t, result.Calls, 2, "expected 2 subcalls")
	require.Empty(t, result.Calls[0].Error, "first subcall (B) should succeed")
	require.Equal(t, "execution reverted", result.Calls[1].Error, "second subcall (C) should revert")
}

// TestCallTracerWithLogPositionInCreate verifies that position is tracked correctly inside
// a CREATE frame:
//   - Factory (addrA) runs CREATE with init code that:
//   - emits LOG_A → position 0x0 (no subcalls yet)
//   - CALLs addrC (succeeds)
//   - emits LOG_B → position 0x1
func TestCallTracerWithLogPositionInCreate(t *testing.T) {
	var (
		addrA = common.HexToAddress("0x00000000000000000000000000000000000000aa")
		addrC = common.HexToAddress("0x00000000000000000000000000000000000000cc")
	)

	// Init code: LOG_A, CALL addrC (succeeds), LOG_B, RETURN empty runtime
	initCode := evmLog0
	initCode = append(initCode, evmCallTo(0xcc)...)
	initCode = append(initCode, evmLog0...)
	initCode = append(initCode, byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.RETURN))
	initLen := byte(len(initCode))

	// Factory code (15 bytes): CODECOPY init code into memory, then CREATE
	//   CODECOPY(destOffset=0, codeOffset=factoryLen, size=initLen)
	//   CREATE(value=0, offset=0, size=initLen)
	factoryLen := byte(15) // length of the factory code itself
	codeA := []byte{
		byte(vm.PUSH1), initLen, // size
		byte(vm.PUSH1), factoryLen, // offset of init code in codeA
		byte(vm.PUSH1), 0x00, // memory dest
		byte(vm.CODECOPY),
		byte(vm.PUSH1), initLen, // size
		byte(vm.PUSH1), 0x00, // memory offset
		byte(vm.PUSH1), 0x00, // value
		byte(vm.CREATE),
		byte(vm.STOP),
	}
	codeA = append(codeA, initCode...)

	privkey, err := crypto.HexToECDSA("0000000000000000deadbeef00000000000000000000000000000000deadbeef")
	if err != nil {
		t.Fatalf("err %v", err)
	}
	signer := types.LatestSigner(chainspec.Mainnet.Config)
	tx, err := types.SignNewTx(privkey, *signer, &types.LegacyTx{
		GasPrice: *uint256.NewInt(0),
		CommonTx: types.CommonTx{GasLimit: 200000, To: &addrA},
	})
	if err != nil {
		t.Fatalf("err %v", err)
	}
	origin, _ := signer.Sender(tx)
	context := evmtypes.BlockContext{
		CanTransfer: protocol.CanTransfer,
		Transfer:    misc.Transfer,
		Coinbase:    accounts.ZeroAddress,
		BlockNumber: 8000000,
		Time:        5,
		Difficulty:  *uint256.NewInt(0x30000),
		GasLimit:    uint64(6000000),
	}
	alloc := types.GenesisAlloc{
		addrA: types.GenesisAccount{Nonce: 1, Code: codeA},
		addrC: types.GenesisAccount{Nonce: 1, Code: []byte{byte(vm.STOP)}},
		origin.Value(): types.GenesisAccount{
			Nonce:   0,
			Balance: big.NewInt(500000000000000),
		},
	}
	rules := context.Rules(chainspec.Mainnet.Config)
	m := execmoduletester.New(t)
	dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(t, err)
	defer dbTx.Rollback()
	statedb, _ := testutil.MakePreState(rules, dbTx, alloc, context.BlockNumber)

	tracer, err := tracers.New("callTracer", nil, json.RawMessage(`{"withLog":true}`))
	if err != nil {
		t.Fatalf("failed to create call tracer: %v", err)
	}
	statedb.SetHooks(tracer.Hooks)
	evm := vm.NewEVM(context, evmtypes.TxContext{Origin: origin, GasPrice: *uint256.NewInt(1)}, statedb, chainspec.Mainnet.Config, vm.Config{Tracer: tracer.Hooks})
	msg, err := tx.AsMessage(*signer, nil, rules)
	if err != nil {
		t.Fatalf("failed to prepare transaction for tracing: %v", err)
	}
	tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
	st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
	vmRet, err := st.Execute(true, false)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}
	tracer.OnTxEnd(&types.Receipt{GasUsed: vmRet.ReceiptGasUsed}, err)

	res, err := tracer.GetResult()
	if err != nil {
		t.Fatalf("failed to retrieve trace result: %v", err)
	}
	var result callTrace
	if err := json.Unmarshal(res, &result); err != nil {
		t.Fatalf("failed to unmarshal trace: %v", err)
	}

	require.Len(t, result.Calls, 1, "expected CREATE subcall from factory")
	createFrame := result.Calls[0]
	require.Equal(t, "CREATE", createFrame.Type, "subcall type should be CREATE")
	require.Len(t, createFrame.Logs, 2, "expected 2 logs inside CREATE frame")
	require.Equal(t, hexutil.Uint(0), createFrame.Logs[0].Position, "LOG_A in CREATE: before subcall, position should be 0x0")
	require.Equal(t, hexutil.Uint(1), createFrame.Logs[1].Position, "LOG_B in CREATE: after subcall, position should be 0x1")
}

func BenchmarkTracers(b *testing.B) {
	files, err := dir.ReadDir(filepath.Join("testdata", "call_tracer"))
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
	tx, err := types.DecodeTransaction(common.FromHex(test.Input))
	if err != nil {
		b.Fatalf("failed to parse testcase input: %v", err)
	}
	signer := types.MakeSigner(test.Genesis.Config, uint64(test.Context.Number), uint64(test.Context.Time))
	context := evmtypes.BlockContext{
		CanTransfer: protocol.CanTransfer,
		Transfer:    misc.Transfer,
		Coinbase:    accounts.InternAddress(test.Context.Miner),
		BlockNumber: uint64(test.Context.Number),
		Time:        uint64(test.Context.Time),
		Difficulty:  *test.Context.Difficulty,
		GasLimit:    uint64(test.Context.GasLimit),
	}
	rules := context.Rules(test.Genesis.Config)
	msg, err := tx.AsMessage(*signer, nil, rules)
	if err != nil {
		b.Fatalf("failed to prepare transaction for tracing: %v", err)
	}
	origin, _ := signer.Sender(tx)
	baseFee := test.Context.BaseFee
	txContext := evmtypes.TxContext{
		Origin:   origin,
		GasPrice: tx.GetEffectiveGasTip(baseFee),
	}
	m := execmoduletester.New(b)
	dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(b, err)
	defer dbTx.Rollback()
	statedb, _ := testutil.MakePreState(rules, dbTx, test.Genesis.Alloc, uint64(test.Context.Number))

	b.ReportAllocs()
	for b.Loop() {
		tracer, err := tracers.New(tracerName, new(tracers.Context), nil)
		if err != nil {
			b.Fatalf("failed to create call tracer: %v", err)
		}
		evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Tracer: tracer.Hooks})
		snap := statedb.PushSnapshot()
		st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
		if _, err = st.Execute(true /* refunds */, false /* gasBailout */); err != nil {
			b.Fatalf("failed to execute transaction: %v", err)
		}
		if _, err = tracer.GetResult(); err != nil {
			b.Fatal(err)
		}
		statedb.RevertToSnapshot(snap, nil)
		statedb.PopSnapshot(snap)
	}
}

// TestZeroValueToNotExitCall tests the calltracer(s) on the following:
// txn to A, A calls B with zero value. B does not already exist.
// Expected: that enter/exit is invoked and the inner call is shown in the result
func TestZeroValueToNotExitCall(t *testing.T) {
	var to = common.HexToAddress("0x00000000000000000000000000000000deadbeef")
	privkey, err := crypto.HexToECDSA("0000000000000000deadbeef00000000000000000000000000000000deadbeef")
	if err != nil {
		t.Fatalf("err %v", err)
	}
	signer := types.LatestSigner(chainspec.Mainnet.Config)
	tx, err := types.SignNewTx(privkey, *signer, &types.LegacyTx{
		GasPrice: *uint256.NewInt(0),
		CommonTx: types.CommonTx{
			GasLimit: 50000,
			To:       &to,
		},
	})
	if err != nil {
		t.Fatalf("err %v", err)
	}
	origin, _ := signer.Sender(tx)
	txContext := evmtypes.TxContext{
		Origin:   origin,
		GasPrice: *uint256.NewInt(1),
	}
	context := evmtypes.BlockContext{
		CanTransfer: protocol.CanTransfer,
		Transfer:    misc.Transfer,
		Coinbase:    accounts.ZeroAddress,
		BlockNumber: 8000000,
		Time:        5,
		Difficulty:  *uint256.NewInt(0x30000),
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
		origin.Value(): types.GenesisAccount{
			Nonce:   0,
			Balance: big.NewInt(500000000000000),
		},
	}
	rules := context.Rules(chainspec.Mainnet.Config)
	m := execmoduletester.New(t)
	dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(t, err)
	defer dbTx.Rollback()

	statedb, _ := testutil.MakePreState(rules, dbTx, alloc, context.BlockNumber)
	// Create the tracer, the EVM environment and run it
	tracer, err := tracers.New("callTracer", nil, nil)
	if err != nil {
		t.Fatalf("failed to create call tracer: %v", err)
	}
	statedb.SetHooks(tracer.Hooks)
	evm := vm.NewEVM(context, txContext, statedb, chainspec.Mainnet.Config, vm.Config{Tracer: tracer.Hooks})
	msg, err := tx.AsMessage(*signer, nil, rules)
	if err != nil {
		t.Fatalf("failed to prepare transaction for tracing: %v", err)
	}
	tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
	st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
	vmRet, err := st.Execute(true /* refunds */, false /* gasBailout */)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}
	tracer.OnTxEnd(&types.Receipt{GasUsed: vmRet.ReceiptGasUsed}, err)
	// Retrieve the trace result and compare against the etalon
	res, err := tracer.GetResult()
	if err != nil {
		t.Fatalf("failed to retrieve trace result: %v", err)
	}
	wantStr := `{"from":"0x682a80a6f560eec50d54e63cbeda1c324c5f8d1b","gas":"0xc350","gasUsed":"0x54d8","to":"0x00000000000000000000000000000000deadbeef","input":"0x","calls":[{"from":"0x00000000000000000000000000000000deadbeef","gas":"0x6cbf","gasUsed":"0x0","to":"0x00000000000000000000000000000000000000ff","input":"0x","value":"0x0","type":"CALL"}],"value":"0x0","type":"CALL"}`
	if string(res) != wantStr {
		t.Fatalf("trace mismatch\n have: %v\n want: %v\n", string(res), wantStr)
	}
}
