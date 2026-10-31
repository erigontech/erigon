// Copyright 2026 The Erigon Authors
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

package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/transactions"
)

func frameTraceTestEnv(t *testing.T) (*vm.EVM, *types.FrameTransaction) {
	t.Helper()
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)
	sender, target := common.HexToAddress("0x1000"), common.HexToAddress("0x2000")
	require.NoError(t, ibs.SetBalance(accounts.InternAddress(sender), uint256.Int{10_000_000}, tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(sender), program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(target), []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))
	blockCtx := evmtypes.BlockContext{CanTransfer: protocol.CanTransfer, Transfer: misc.Transfer, GasLimit: 30_000_000, BlockNumber: 1}
	evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, cfg, vm.Config{NoBaseFee: true})
	tx := &types.FrameTransaction{
		ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
			{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 10_000}},
		},
	}
	return evm, tx
}

func traceFrameRPC(t *testing.T, evm *vm.EVM, tx *types.FrameTransaction, name string, cfg json.RawMessage) json.RawMessage {
	t.Helper()
	msg, err := tx.AsMessage(types.Signer{}, &evm.Context.BaseFee, evm.ChainRules())
	require.NoError(t, err)
	traceConfig := &config.TraceConfig{Tracer: &name, TracerConfig: &cfg}
	var out bytes.Buffer
	_, err = transactions.TraceTx(context.Background(), nil, tx, msg, evm.Context, protocol.NewEVMTxContext(msg), uint256.NewInt(1), common.Hash{}, 0, evm.IntraBlockState(), traceConfig, evm.ChainConfig(), jsonstream.New(&out), time.Second*10, nil)
	require.NoError(t, err)
	return out.Bytes()
}

type frameCallTrace struct {
	Type    string            `json:"type"`
	From    common.Address    `json:"from"`
	To      common.Address    `json:"to"`
	Gas     hexutil.Uint64    `json:"gas"`
	GasUsed hexutil.Uint64    `json:"gasUsed"`
	Error   string            `json:"error"`
	Calls   []frameCallTrace  `json:"calls"`
	Logs    []json.RawMessage `json:"logs"`
}

func TestFrameCallTracer(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	var trace frameCallTrace
	require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "callTracer", json.RawMessage(`{}`)), &trace))
	require.Equal(t, "CALL", trace.Type)
	require.Equal(t, tx.SenderAddress, trace.From)
	require.Equal(t, vm.FrameEntryPoint.Value(), trace.To)
	require.EqualValues(t, tx.GetGasLimit(), trace.Gas)
	require.Empty(t, trace.Error)
	require.Len(t, trace.Calls, len(tx.Frames))
	for i, frame := range tx.Frames {
		require.Equal(t, frame.ResolvedTarget(tx.SenderAddress), trace.Calls[i].To)
		require.EqualValues(t, frame.Limits.Total(), trace.Calls[i].Gas)
	}
	require.Equal(t, "STATICCALL", trace.Calls[0].Type)
	require.Equal(t, vm.FrameEntryPoint.Value(), trace.Calls[0].From)
	require.Equal(t, tx.SenderAddress, trace.Calls[1].From)
	require.EqualValues(t, 107, trace.Calls[0].GasUsed)
	require.EqualValues(t, params.ColdAccountAccessCostEIP8038, trace.Calls[1].GasUsed)
}

func TestFrameCallTracerDefaultVerification(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	tx.SenderAddress = crypto.PubkeyToAddress(key.PublicKey)
	require.NoError(t, evm.IntraBlockState().SetBalance(accounts.InternAddress(tx.SenderAddress), uint256.Int{10_000_000}, tracing.BalanceChangeUnspecified))
	tx.Signatures = []types.FrameSignature{{Scheme: types.FrameSignatureSecp256k1}}
	hash := tx.SigningHash(nil)
	sig, err := crypto.Sign(hash[:], key)
	require.NoError(t, err)
	tx.Signatures[0].Signature = append([]byte{sig[64]}, sig[:64]...)
	var trace frameCallTrace
	require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "callTracer", json.RawMessage(`{}`)), &trace))
	require.Len(t, trace.Calls, 2)
	require.Equal(t, "STATICCALL", trace.Calls[0].Type)
	require.Equal(t, tx.SenderAddress, trace.Calls[0].To)
	require.EqualValues(t, params.WarmStorageReadCostEIP2929, trace.Calls[0].GasUsed)
	require.Empty(t, trace.Calls[0].Calls)
	require.Empty(t, trace.Calls[0].Error)
}

func TestFrameCallTracerOnlyTopCall(t *testing.T) {
	for _, tc := range []struct {
		op      vm.OpCode
		onlyTop bool
	}{
		{vm.CALL, false},
		{vm.CALL, true},
		{vm.SELFDESTRUCT, false},
		{vm.SELFDESTRUCT, true},
	} {
		t.Run(tc.op.String()+"/"+map[bool]string{false: "nested", true: "top"}[tc.onlyTop], func(t *testing.T) {
			evm, tx := frameTraceTestEnv(t)
			tx.Frames[1].Limits.Execution = 20_000
			target := accounts.InternAddress(*tx.Frames[1].Target)
			child := common.HexToAddress("0x3000")
			require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(child), []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))
			code := program.New().Call(uint256.NewInt(1_000), child, 0, 0, 0, 0, 0).Op(vm.POP, vm.STOP).Bytes()
			if tc.op == vm.SELFDESTRUCT {
				code = program.New().Push(child).Op(vm.SELFDESTRUCT).Bytes()
			}
			require.NoError(t, evm.IntraBlockState().SetCode(target, code, tracing.CodeChangeUnspecified))
			cfg, err := json.Marshal(map[string]bool{"onlyTopCall": tc.onlyTop})
			require.NoError(t, err)
			var trace frameCallTrace
			require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "callTracer", cfg), &trace))
			require.Empty(t, trace.Error)
			require.Len(t, trace.Calls, 2)
			require.Empty(t, trace.Calls[1].Error)
			if tc.onlyTop {
				require.Empty(t, trace.Calls[1].Calls)
			} else {
				require.Len(t, trace.Calls[1].Calls, 1)
				require.Equal(t, tc.op.String(), trace.Calls[1].Calls[0].Type)
				require.Equal(t, child, trace.Calls[1].Calls[0].To)
			}
		})
	}
}

func TestFrameCallTracerPrecompileFrame(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	identity := common.HexToAddress("0x4")
	tx.Frames[1].Target = &identity
	tx.Frames[1].Data = []byte("hello")
	var trace frameCallTrace
	require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "callTracer", json.RawMessage(`{"includePrecompiles":false}`)), &trace))
	require.Len(t, trace.Calls, 2)
	require.Equal(t, identity, trace.Calls[1].To)
	require.Empty(t, trace.Calls[1].Error)
}

func TestFrameCallTracerStateGas(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	target := accounts.InternAddress(*tx.Frames[1].Target)
	require.NoError(t, evm.IntraBlockState().SetCode(target, program.New().Sstore(0, 1).Op(vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
	tx.Frames[1].Limits = mdgas.MdGas{Execution: 100_000, State: params.StateGasPerStorageSet}
	var trace frameCallTrace
	require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "callTracer", json.RawMessage(`{}`)), &trace))
	require.Len(t, trace.Calls, 2)
	require.EqualValues(t, tx.Frames[1].Limits.Total(), trace.Calls[1].Gas)
	intrinsic, _, _, err := tx.GasLimits()
	require.NoError(t, err)
	require.EqualValues(t, uint64(trace.GasUsed)-intrinsic-uint64(trace.Calls[0].GasUsed), trace.Calls[1].GasUsed)
}

func TestFrameCallTracerAtomicBatch(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	target := *tx.Frames[1].Target
	failure := common.HexToAddress("0x3000")
	require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(target), program.New().Push0().Push0().Op(vm.LOG0, vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(failure), program.New().Push0().Push0().Op(vm.REVERT).Bytes(), tracing.CodeChangeUnspecified))
	tx.Frames[1].Flags = types.FrameAtomicBatch
	tx.Frames = append(tx.Frames,
		types.Frame{Mode: types.FrameSender, Target: &failure, Flags: types.FrameAtomicBatch, Limits: mdgas.MdGas{Execution: 10_000}},
		types.Frame{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 10_000}},
		types.Frame{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 10_000}},
	)
	var trace frameCallTrace
	require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "callTracer", json.RawMessage(`{"withLog":true}`)), &trace))
	require.Len(t, trace.Calls, len(tx.Frames))
	require.NotEmpty(t, trace.Error)
	require.Empty(t, trace.Calls[1].Error)
	require.Empty(t, trace.Calls[1].Logs)
	require.Equal(t, "execution reverted", trace.Calls[2].Error)
	require.Equal(t, "frame skipped", trace.Calls[3].Error)
	require.Zero(t, trace.Calls[3].GasUsed)
	require.Empty(t, trace.Calls[3].Calls)
	require.Empty(t, trace.Calls[4].Error)
	require.Len(t, trace.Calls[4].Logs, 1)
}

func TestFramePrestateTracer(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	target := *tx.Frames[1].Target
	require.NoError(t, evm.IntraBlockState().SetCode(accounts.InternAddress(target), program.New().Push0().Op(vm.SLOAD, vm.POP, vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
	require.NoError(t, evm.IntraBlockState().SetState(accounts.InternAddress(target), accounts.InternKey(common.Hash{}), uint256.Int{1}))
	var trace map[common.Address]struct {
		Storage map[common.Hash]common.Hash `json:"storage"`
	}
	require.NotPanics(t, func() {
		require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "prestateTracer", json.RawMessage(`{}`)), &trace))
	})
	require.Contains(t, trace, tx.SenderAddress)
	require.Contains(t, trace, target)
	require.Equal(t, common.HexToHash("0x1"), trace[target].Storage[common.Hash{}])
	require.NotContains(t, trace, types.CreateAddress(tx.SenderAddress, tx.Nonce))
	require.NotContains(t, trace, vm.FrameEntryPoint.Value())
}

func TestFramePrestateTracerKeyedNonce(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	tx.NonceKeys = []uint256.Int{{5}}
	tx.Nonce = 1
	manager := accounts.InternAddress(types.FrameNonceManager)
	slot := types.FrameNonceSlot(tx.SenderAddress, tx.NonceKeys[0])
	require.NoError(t, evm.IntraBlockState().SetNonce(manager, 1, tracing.NonceChangeUnspecified))
	require.NoError(t, evm.IntraBlockState().SetState(manager, accounts.InternKey(slot), uint256.Int{1}))
	var trace map[common.Address]struct {
		Storage map[common.Hash]common.Hash `json:"storage"`
	}
	require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "prestateTracer", json.RawMessage(`{}`)), &trace))
	require.Contains(t, trace, types.FrameNonceManager)
	require.Equal(t, common.HexToHash("0x1"), trace[types.FrameNonceManager].Storage[slot])
}

func traceParityFrame(t *testing.T, evm *vm.EVM, tx *types.FrameTransaction, traceVM bool) (*TraceCallResult, *types.Receipt) {
	t.Helper()
	result := &TraceCallResult{Trace: []*ParityTrace{}}
	if traceVM {
		result.VmTrace = &VmTrace{Ops: []*VmTraceOp{}}
	}
	tracer := (&OeTracer{r: result}).Tracer()
	vmConfig := evm.Config()
	vmConfig.Tracer = tracer.Hooks
	evm.ResetBetweenBlocks(evm.Context, evm.TxContext, evm.IntraBlockState(), vmConfig, evm.ChainRules())
	msg, err := tx.AsMessage(types.Signer{}, nil, evm.ChainRules())
	require.NoError(t, err)
	tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
	executed, err := protocol.ApplyMessage(evm, msg, protocol.NewGasPool(30_000_000, 0), true, false, nil)
	require.NoError(t, err)
	receipt := protocol.MakeReceipt(uint256.NewInt(1), common.Hash{}, tx, executed.ReceiptGasUsed, executed, evm.IntraBlockState(), evm)
	tracer.EmitTxEnd(receipt, executed.TxnGasUsage, nil)
	return result, receipt
}

func TestFrameParityTracer(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	result, receipt := traceParityFrame(t, evm, tx, true)
	require.Len(t, result.Trace, 3)
	require.Equal(t, []int{}, result.Trace[0].TraceAddress)
	require.Equal(t, []int{0}, result.Trace[1].TraceAddress)
	require.Equal(t, []int{1}, result.Trace[2].TraceAddress)
	require.Equal(t, 2, result.Trace[0].Subtraces)
	require.Empty(t, result.VmTrace.Code)
	require.Len(t, result.VmTrace.Ops, 2)
	for i, op := range result.VmTrace.Ops {
		require.Equal(t, i, op.Pc)
		require.Empty(t, op.Op, "a synthetic frame operation executes no opcode")
		require.EqualValues(t, tx.Frames[i].Limits.Total(), op.Cost)
		require.EqualValues(t, tx.Frames[i].Limits.Total()-receipt.FrameReceipts[i].GasUsed.Total(), op.Ex.GasRemaining)
		require.NotNil(t, op.Sub)
		require.NotEmpty(t, op.Sub.Code)
		require.Nil(t, op.Sub.Ops[len(op.Sub.Ops)-1].Sub)
		for _, executed := range op.Sub.Ops {
			require.NotEmpty(t, executed.Op)
		}
	}
}

func TestFrameParityTracerStackResults(t *testing.T) {
	for _, tc := range []struct {
		op   vm.OpCode
		code []byte
		want string
	}{
		{vm.TXPARAM, program.New().Push(9).Op(vm.TXPARAM, vm.STOP).Bytes(), "0x2"},
		{vm.FRAMEPARAM, program.New().Push0().Push(1).Op(vm.FRAMEPARAM, vm.STOP).Bytes(), "0x2000"},
		{vm.SIGPARAM, program.New().Push(3).Push0().Op(vm.SIGPARAM, vm.STOP).Bytes(), "0x3"},
		{vm.FRAMEDATALOAD, program.New().Push(1).Push(1).Op(vm.FRAMEDATALOAD, vm.STOP).Bytes(), "0xbbcc" + strings.Repeat("00", 30)},
	} {
		t.Run(tc.op.String(), func(t *testing.T) {
			evm, tx := frameTraceTestEnv(t)
			tx.Frames[1].Data = []byte{0xaa, 0xbb, 0xcc}
			tx.Signatures = []types.FrameSignature{{Scheme: types.FrameSignatureArbitrary, Signature: []byte{0xdd, 0xee, 0xff}}}
			target := accounts.InternAddress(*tx.Frames[1].Target)
			require.NoError(t, evm.IntraBlockState().SetCode(target, tc.code, tracing.CodeChangeUnspecified))
			result, receipt := traceParityFrame(t, evm, tx, true)
			require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)
			ops := result.VmTrace.Ops[1].Sub.Ops
			op := ops[len(ops)-2]
			require.Equal(t, tc.op.String(), op.Op)
			require.NotNil(t, op.Ex)
			require.Equal(t, []string{tc.want}, op.Ex.Push)
		})
	}
}

func TestFrameParityTracerMemoryCopies(t *testing.T) {
	for _, tc := range []struct {
		op   vm.OpCode
		want string
	}{
		{vm.FRAMEDATACOPY, "0xbbcc0000"},
		{vm.SIGDATACOPY, "0xeeff0000"},
	} {
		t.Run(tc.op.String(), func(t *testing.T) {
			evm, tx := frameTraceTestEnv(t)
			tx.Frames[1].Data = []byte{0xaa, 0xbb, 0xcc}
			tx.Signatures = []types.FrameSignature{
				{Scheme: types.FrameSignatureArbitrary, Signature: []byte{0x99}},
				{Scheme: types.FrameSignatureArbitrary, Signature: []byte{0xdd, 0xee, 0xff}},
			}
			code := program.New().Push(1).Push(4).Push(1).Push(32).Op(tc.op, vm.STOP).Bytes()
			target := accounts.InternAddress(*tx.Frames[1].Target)
			require.NoError(t, evm.IntraBlockState().SetCode(target, code, tracing.CodeChangeUnspecified))
			result, receipt := traceParityFrame(t, evm, tx, true)
			require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)
			ops := result.VmTrace.Ops[1].Sub.Ops
			op := ops[len(ops)-2]
			require.Equal(t, tc.op.String(), op.Op)
			require.NotNil(t, op.Ex)
			require.Equal(t, &VmTraceMem{Off: 32, Data: tc.want}, op.Ex.Mem)
		})
	}
}

func TestFrameParityTracerStateGas(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	target := accounts.InternAddress(*tx.Frames[1].Target)
	require.NoError(t, evm.IntraBlockState().SetCode(target, program.New().Sstore(0, 1).Op(vm.STOP).Bytes(), tracing.CodeChangeUnspecified))
	tx.Frames[1].Limits = mdgas.MdGas{Execution: 100_000, State: params.StateGasPerStorageSet}
	result, receipt := traceParityFrame(t, evm, tx, true)
	require.Equal(t, hexutil.U256(*uint256.NewInt(tx.GetGasLimit())), result.Trace[0].Action.(*CallTraceAction).Gas)
	child := result.Trace[2]
	require.Equal(t, hexutil.U256(*uint256.NewInt(tx.Frames[1].Limits.Total())), child.Action.(*CallTraceAction).Gas)
	require.Equal(t, (*hexutil.U256)(uint256.NewInt(receipt.FrameReceipts[1].GasUsed.Total())), child.Result.(*TraceResult).GasUsed)
}

func TestFrameParityTracerFailedFrame(t *testing.T) {
	for _, tc := range []struct {
		name      string
		code      []byte
		value     uint64
		wantError string
		output    []byte
		vmFrames  int
	}{
		{name: "invalid opcode", code: []byte{byte(vm.INVALID)}, wantError: "Bad instruction", vmFrames: 2},
		{name: "insufficient balance", code: []byte{byte(vm.STOP)}, value: 10_000_001, wantError: "Reverted", vmFrames: 1},
		{name: "revert", code: revertInit, wantError: "Reverted", output: common.FromHex("0xdeadbeef"), vmFrames: 2},
	} {
		for name, traceVM := range map[string]bool{"trace": false, "trace and vmTrace": true} {
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				evm, tx := frameTraceTestEnv(t)
				target := accounts.InternAddress(*tx.Frames[1].Target)
				require.NoError(t, evm.IntraBlockState().SetCode(target, tc.code, tracing.CodeChangeUnspecified))
				tx.Frames[1].Value.SetUint64(tc.value)
				result, receipt := traceParityFrame(t, evm, tx, traceVM)
				require.EqualValues(t, types.ReceiptStatusFailed, receipt.Status)
				require.Len(t, result.Trace, 3)
				require.NotEmpty(t, result.Trace[0].Error)
				require.NotNil(t, result.Trace[0].Result)
				require.Equal(t, (*hexutil.U256)(uint256.NewInt(receipt.GasUsed)), result.Trace[0].Result.(*TraceResult).GasUsed)
				frame := result.Trace[2]
				require.Equal(t, []int{1}, frame.TraceAddress)
				require.Zero(t, frame.Subtraces)
				require.Equal(t, tc.wantError, frame.Error)
				if traceVM {
					require.Len(t, result.VmTrace.Ops, tc.vmFrames)
				}
				if tc.output == nil {
					require.Nil(t, frame.Result)
				} else {
					require.IsType(t, &TraceResult{}, frame.Result)
					reverted := frame.Result.(*TraceResult)
					require.Equal(t, hexutil.Bytes(tc.output), reverted.Output)
					require.Equal(t, (*hexutil.U256)(uint256.NewInt(receipt.FrameReceipts[1].GasUsed.Total())), reverted.GasUsed)
				}
			})
		}
	}
}

func TestFrameParityTracerPrecompileFrame(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	identity := common.HexToAddress("0x4")
	tx.Frames[1].Target = &identity
	tx.Frames[1].Data = []byte("hello")
	result, _ := traceParityFrame(t, evm, tx, true)
	require.Len(t, result.Trace, 3)
	require.Equal(t, []int{1}, result.Trace[2].TraceAddress)
	require.Equal(t, identity, result.Trace[2].Action.(*CallTraceAction).To)
}

func TestFrameFlatCallTracerPrecompileFrame(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	identity := common.HexToAddress("0x4")
	tx.Frames[1].Target = &identity
	tx.Frames[1].Data = []byte("hello")
	var result []struct {
		TraceAddress []int `json:"traceAddress"`
	}
	require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "flatCallTracer", json.RawMessage(`{}`)), &result))
	require.Len(t, result, 3)
	require.Equal(t, []int{1}, result[2].TraceAddress)
}

func TestFrameFlatCallTracerFailedFrame(t *testing.T) {
	evm, tx := frameTraceTestEnv(t)
	target := accounts.InternAddress(*tx.Frames[1].Target)
	require.NoError(t, evm.IntraBlockState().SetCode(target, []byte{byte(vm.INVALID)}, tracing.CodeChangeUnspecified))
	var result []struct {
		Error  string `json:"error"`
		Result *struct {
			GasUsed hexutil.Uint64 `json:"gasUsed"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(traceFrameRPC(t, evm, tx, "flatCallTracer", json.RawMessage(`{}`)), &result))
	require.Len(t, result, 3)
	require.NotEmpty(t, result[0].Error)
	require.NotNil(t, result[0].Result)
	require.NotZero(t, result[0].Result.GasUsed)
	require.Nil(t, result[2].Result)
}
