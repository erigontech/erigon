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

package transactions

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/js"
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native" // registers callTracer
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func TestTraceTxnGasUsage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tracer string
		hexGas bool
	}{
		{name: "opcode"},
		{name: "call", tracer: "callTracer", hexGas: true},
		{name: "legacy call", tracer: "callTracerLegacy", hexGas: true},
		{name: "js context", tracer: `{fault: function() {}, result: function(ctx) {
			return {gasUsed: ctx.gasUsed, regularGasUsed: ctx.regularGasUsed,
				stateGasUsed: ctx.stateGasUsed, gasRefund: ctx.gasRefund};
		}}`},
	} {
		for _, fork := range []struct {
			name        string
			amsterdam   bool
			txnGasUsage mdgas.TxnGasUsage
		}{
			{name: "Amsterdam", amsterdam: true, txnGasUsage: mdgas.TxnGasUsage{BlockExecutionGasUsed: 30_000, BlockStateGasUsed: 12_000, GasRefund: 5_000}},
			{name: "pre-Amsterdam", txnGasUsage: mdgas.TxnGasUsage{BlockExecutionGasUsed: 30_000, BlockStateGasUsed: 12_000, GasRefund: 5_000}},
			{name: "zero state and refund", amsterdam: true, txnGasUsage: mdgas.TxnGasUsage{BlockExecutionGasUsed: 37_000}},
		} {
			t.Run(tc.name+"/"+fork.name, func(t *testing.T) {
				var output bytes.Buffer
				stream := jsonstream.New(&output)
				cfg := &tracersConfig.TraceConfig{}
				if tc.tracer != "" {
					cfg.Tracer = &tc.tracer
				}
				tracer, streaming, cancel, err := AssembleTracer(t.Context(), cfg, common.Hash{}, nil,
					common.Hash{}, 0, stream, time.Second)
				require.NoError(t, err)
				defer cancel()
				ibs := state.New(state.NewNoopReader())
				defer ibs.Close()
				txnGasUsage := fork.txnGasUsage
				chainConfig := *chain.AllProtocolChanges
				if !fork.amsterdam {
					chainConfig.AmsterdamTime = nil
				}
				err = ExecuteTraceTx(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, cfg,
					&chainConfig, stream, tracer, streaming, nil, func(evm *vm.EVM, refunds bool) (*evmtypes.ExecutionResult, error) {
						tracer.OnTxStart(evm.GetVMContext(), types.NewTransaction(0, common.Address{}, nil, 100_000, nil, nil), accounts.ZeroAddress)
						tracer.EmitEnter(0, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
							mdgas.MdGas{Execution: 80_000, State: 10_000}, uint256.Int{}, nil)
						tracer.EmitEnter(1, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
							mdgas.MdGas{Execution: 10_000, State: 10_000}, uint256.Int{}, nil)
						tracer.EmitExit(1, nil, mdgas.MdGasUsage{Execution: 500, State: 1_000}, nil, false)
						tracer.EmitExit(0, nil, mdgas.MdGasUsage{Execution: 2_000, State: 1_000}, nil, false)
						tracer.EmitTxEnd(&types.Receipt{GasUsed: 37_000}, txnGasUsage, nil)
						return &evmtypes.ExecutionResult{ReceiptGasUsed: 37_000, TxnGasUsage: txnGasUsage}, nil
					})
				require.NoError(t, err)
				require.NoError(t, stream.Flush())
				var result map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(output.Bytes(), &result))
				gasField := "gasUsed"
				if streaming {
					gasField = "gas"
				}
				for field, want := range map[string]uint64{
					gasField: 37_000, "regularGasUsed": txnGasUsage.BlockExecutionGasUsed,
					"stateGasUsed": txnGasUsage.BlockStateGasUsed, "gasRefund": txnGasUsage.GasRefund,
				} {
					if field != gasField && !fork.amsterdam {
						require.NotContains(t, result, field)
						continue
					}
					require.Contains(t, result, field)
					if tc.hexGas {
						require.JSONEq(t, fmt.Sprintf(`"0x%x"`, want), string(result[field]), field)
					} else {
						require.JSONEq(t, fmt.Sprint(want), string(result[field]), field)
					}
				}
				if tc.tracer == "callTracer" {
					var calls []map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(result["calls"], &calls))
					require.Len(t, calls, 1)
					require.JSONEq(t, `"0x1f4"`, string(calls[0]["gasUsed"]))
					require.NotContains(t, calls[0], "regularGasUsed")
					if fork.amsterdam {
						require.JSONEq(t, `"0x3e8"`, string(calls[0]["stateGasUsed"]))
					} else {
						require.NotContains(t, calls[0], "stateGasUsed")
					}
					require.NotContains(t, calls[0], "gasRefund")
				}
			})
		}
	}
}

func TestTraceTxCompletionV2(t *testing.T) {
	name := "testTxCompletionV2"
	var received mdgas.TxnGasUsage
	var receivedReceipt *types.Receipt
	var completionErr error
	var calls int
	tracers.RegisterLookup(false, func(code string, _ *tracers.Context, _ json.RawMessage) (*tracers.Tracer, error) {
		if code != name {
			return nil, errors.New("unknown tracer")
		}
		return &tracers.Tracer{
			Hooks: &tracing.Hooks{OnTxEndV2: func(receipt *types.Receipt, txnGasUsage mdgas.TxnGasUsage, err error) {
				received = txnGasUsage
				receivedReceipt = receipt
				completionErr = err
				calls++
			}},
			GetResult: func() (json.RawMessage, error) { return json.RawMessage(`{}`), nil },
			Stop:      func(error) {},
		}, nil
	})
	for _, gasLimit := range []uint64{200_000, 100} {
		t.Run(fmt.Sprintf("gas=%d", gasLimit), func(t *testing.T) {
			calls = 0
			received = mdgas.TxnGasUsage{}
			receivedReceipt = nil
			completionErr = nil
			sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
			recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, ibs.SetCode(recipient, []byte{
				byte(vm.PUSH1), 1, byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP),
			}, tracing.CodeChangeUnspecified))
			msg := types.NewMessage(sender, recipient, 0, uint256.NewInt(0), gasLimit,
				uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0), nil, nil, false, false, true, false, nil)
			blockCtx := evmtypes.BlockContext{CanTransfer: protocol.CanTransfer, Transfer: misc.Transfer, GasLimit: 1_000_000}
			txnGasUsage, err := TraceTx(t.Context(), nil, nil, msg, blockCtx, protocol.NewEVMTxContext(msg),
				uint256.NewInt(0), common.Hash{}, 0, ibs, &tracersConfig.TraceConfig{Tracer: &name},
				chain.AllProtocolChanges, jsonstream.New(io.Discard), time.Second, nil)
			require.Equal(t, 1, calls)
			require.Equal(t, received, txnGasUsage)
			if gasLimit == 100 {
				require.Error(t, err)
				require.ErrorIs(t, err, completionErr)
				require.Zero(t, received)
				require.Nil(t, receivedReceipt)
				return
			}
			require.NoError(t, err)
			require.NoError(t, completionErr)
			require.NotNil(t, receivedReceipt)
			require.EqualValues(t, params.StateGasPerStorageSet, received.BlockStateGasUsed)
			require.Equal(t, receivedReceipt.GasUsed, received.BlockExecutionGasUsed+received.BlockStateGasUsed)
			require.Zero(t, received.GasRefund)
			require.False(t, ibs.AccessedAddr(recipient), "tracing does not build a block access list")
		})
	}
}

func assembleWithLogConfig(t *testing.T, cfg *logger.LogConfig, tracerName *string) error {
	t.Helper()
	_, _, cancel, err := AssembleTracer(
		t.Context(),
		&tracersConfig.TraceConfig{LogConfig: cfg, Tracer: tracerName},
		common.Hash{}, nil, common.Hash{}, 0,
		jsonstream.New(io.Discard),
		time.Second,
	)
	cancel()
	return err
}

// A tracer that fails before writing writes nothing, so the response's "result" can be taken back.
func TestWriteTracerResultWritesNothingOnError(t *testing.T) {
	var buf bytes.Buffer
	s := jsonstream.New(&buf)
	tracer := &tracers.Tracer{MarshalFastJSONTo: func(*jsonstream.Stream) error { return errors.New("stopped") }}
	require.EqualError(t, writeTracerResult(tracer, s), "stopped")
	require.Empty(t, s.Buffer())
}

// execution-apis gives the opcode logger's limit a minimum of 0, and a negative
// one would suppress every step, so it must be refused rather than served as an
// empty trace.
func TestAssembleTracerRejectsNegativeLimit(t *testing.T) {
	err := assembleWithLogConfig(t, &logger.LogConfig{Limit: -1}, nil)

	var invalidParams *rpc.InvalidParamsError
	require.ErrorAs(t, err, &invalidParams)
}

func TestAssembleTracerAcceptsNonNegativeLimit(t *testing.T) {
	for _, limit := range []int{0, 1, 1000} {
		require.NoError(t, assembleWithLogConfig(t, &logger.LogConfig{Limit: limit}, nil))
	}
}

// The limit belongs to the opcode logger, and execution-apis says a named tracer
// ignores it, so it must not turn into an error there.
func TestAssembleTracerIgnoresLimitForNamedTracer(t *testing.T) {
	callTracer := "callTracer"
	require.NoError(t, assembleWithLogConfig(t, &logger.LogConfig{Limit: -1}, &callTracer))
}

// On the versioned path the next transaction index of the same state reads the finalized
// writes, and the log index keeps counting across transactions.
func TestFinalizeCallTxPublishesWritesToNextTx(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x0201"))
	key := accounts.InternKey(common.HexToHash("0x11"))
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	ibs.SetVersionMap(state.NewVersionMap(nil))
	ibs.SetNoConflictDetection()
	ibs.SetTxContext(0, 0)
	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(7)))
	ibs.AddLog(&types.Log{Address: addr.Value()})
	require.NoError(t, ethapi.FinalizeCallTx(ibs, &chain.Rules{}))

	ibs.SetTxContext(0, 1)
	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(7), got)
	ibs.AddLog(&types.Log{Address: addr.Value()})
	require.Equal(t, hexutil.Uint(1), ibs.Logs()[1].Index)
}

type emptyAccountReader struct {
	*state.NoopReader
	addr accounts.Address
}

func (r emptyAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.addr {
		acc := accounts.NewAccount()
		return &acc, nil
	}
	return nil, nil
}

// An existing account a transaction touches and leaves empty is gone for the next transaction
// of the same request (EIP-161), as with FinalizeTx on the materializing path.
func TestFinalizeCallTxRemovesTouchedEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x0202"))
	ibs := state.New(emptyAccountReader{NoopReader: state.NewNoopReader(), addr: addr})
	defer ibs.Close()
	ibs.SetVersionMap(state.NewVersionMap(nil))
	ibs.SetNoConflictDetection()
	ibs.SetTxContext(1, 0)
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	require.NoError(t, ibs.TouchAccount(addr))
	require.NoError(t, ethapi.FinalizeCallTx(ibs, &chain.Rules{IsSpuriousDragon: true}))

	ibs.SetTxContext(1, 1)
	exists, err = ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
}

// An account deleted under EIP-161 by one transaction, recreated empty by the next (a zero-value
// call to a precompile) and deleted again, stays gone for the transaction after that.
func TestFinalizeCallTxKeepsARecreatedEmptyAccountDeleted(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x0203"))
	ibs := state.New(emptyAccountReader{NoopReader: state.NewNoopReader(), addr: addr})
	defer ibs.Close()
	ibs.SetVersionMap(state.NewVersionMap(nil))
	ibs.SetNoConflictDetection()
	rules := &chain.Rules{IsSpuriousDragon: true}

	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.TouchAccount(addr))
	require.NoError(t, ethapi.FinalizeCallTx(ibs, rules))

	ibs.SetTxContext(1, 1)
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, ibs.CreateAccount(addr, false))
	require.NoError(t, ibs.TouchAccount(addr))
	require.NoError(t, ethapi.FinalizeCallTx(ibs, rules))

	ibs.SetTxContext(1, 2)
	exists, err = ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
}

func BenchmarkTraceTxAccountReads(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			sender := accounts.InternAddress(common.HexToAddress("0x1111"))
			recipient := accounts.InternAddress(common.HexToAddress("0x2222"))
			code := make([]byte, 0, count*5+1)
			for i := range count {
				code = append(code, byte(vm.PUSH2), byte((i+0x3000)>>8), byte(i+0x3000), byte(vm.BALANCE), byte(vm.POP))
			}
			code = append(code, byte(vm.STOP))
			ibs := state.NewPooled(state.NewNoopReader())
			defer state.ReleasePooled(ibs)
			msg := types.NewMessage(sender, recipient, 0, uint256.NewInt(0), 10_000_000,
				uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0), nil, nil, false, false, true, false, nil)
			txn := types.NewTransaction(0, recipient.Value(), uint256.NewInt(0), 10_000_000, uint256.NewInt(0), nil)
			blockCtx := evmtypes.BlockContext{CanTransfer: protocol.CanTransfer, Transfer: misc.Transfer, GasLimit: 30_000_000}
			name := "callTracer"
			config := &tracersConfig.TraceConfig{Tracer: &name}
			b.ReportAllocs()
			for b.Loop() {
				ibs.Reset()
				VersionedCallState(ibs)
				if err := ibs.SetCode(recipient, code, tracing.CodeChangeUnspecified); err != nil {
					b.Fatal(err)
				}
				_, err := TraceTx(b.Context(), nil, txn, msg, blockCtx, protocol.NewEVMTxContext(msg),
					uint256.NewInt(0), common.Hash{}, 0, ibs, config, chain.TestChainOsakaConfig,
					jsonstream.New(io.Discard), time.Minute, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
