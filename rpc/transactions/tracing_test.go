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

// countingReader records how often the inner reader is consulted and hands back
// buffers it reuses, the way a real reader backed by a pooled buffer does.
type countingReader struct {
	accountReads, storageReads, codeReads, codeSizeReads int
	account                                              *accounts.Account
	codeBuf                                              []byte
}

func (c *countingReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	c.accountReads++
	return c.account, nil
}

// The value follows the key, so a cache that loses the key replays the wrong slot.
func (c *countingReader) ReadAccountStorage(_ accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	c.storageReads++
	return *uint256.NewInt(7 + uint64(key.Value()[31])), true, nil
}

func (c *countingReader) ReadAccountCode(accounts.Address) ([]byte, error) {
	c.codeReads++
	c.codeBuf = append(c.codeBuf[:0], byte(c.codeReads), 0x60, 0x00)
	return c.codeBuf, nil
}

func (c *countingReader) ReadAccountCodeSize(accounts.Address) (int, error) {
	c.codeSizeReads++
	return 3, nil
}

func (c *countingReader) ReadAccountDataForDebug(accounts.Address) (*accounts.Account, error) {
	return c.account, nil
}
func (c *countingReader) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }
func (c *countingReader) SetTrace(bool, string)                                   {}
func (c *countingReader) Trace() bool                                             { return false }
func (c *countingReader) TracePrefix() string                                     { return "" }

func TestMemoReaderServesRepeatedReadsFromItsCache(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.Hash{})
	other := accounts.InternKey(common.Hash{31: 1})
	inner := &countingReader{account: &accounts.Account{Nonce: 3}}
	m := newMemoReader(inner)

	first, err := m.ReadAccountData(addr)
	require.NoError(t, err)
	first.Nonce = 99

	second, err := m.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, 1, inner.accountReads, "a repeated account read must not reach the inner reader")
	require.Equal(t, uint64(3), second.Nonce, "mutating a returned account must not change the cache")

	// Two slots of one account, so a cache keyed on the address alone would answer the second
	// with the first one's value.
	for range 2 {
		v, found, err := m.ReadAccountStorage(addr, key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, *uint256.NewInt(7), v)

		w, found, err := m.ReadAccountStorage(addr, other)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, *uint256.NewInt(8), w)

		n, err := m.ReadAccountCodeSize(addr)
		require.NoError(t, err)
		require.Equal(t, 3, n)
	}
	require.Equal(t, 2, inner.storageReads, "a repeated storage read must not reach the inner reader")
	require.Equal(t, 1, inner.codeSizeReads, "a repeated code-size read must not reach the inner reader")
}

func TestMemoReaderKeepsCodeAfterTheInnerBufferIsReused(t *testing.T) {
	first := accounts.InternAddress(common.HexToAddress("0x01"))
	second := accounts.InternAddress(common.HexToAddress("0x02"))
	inner := &countingReader{account: &accounts.Account{}}
	m := newMemoReader(inner)

	want, err := m.ReadAccountCode(first)
	require.NoError(t, err)
	snapshot := bytes.Clone(want)

	_, err = m.ReadAccountCode(second)
	require.NoError(t, err)
	require.Equal(t, snapshot, want, "the inner reader reusing its buffer must not rewrite a slice already returned")

	got, err := m.ReadAccountCode(first)
	require.NoError(t, err)
	require.Equal(t, 2, inner.codeReads, "a repeated code read must not reach the inner reader")
	require.Equal(t, snapshot, got, "the inner reader reusing its buffer must not rewrite a cached entry")
}
