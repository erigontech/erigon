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
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native" // registers callTracer
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func TestTraceTxCompletionV2(t *testing.T) {
	name := "testTxCompletionV2"
	var received *mdgas.TxGasUsage
	var receivedReceipt *types.Receipt
	var completionErr error
	var calls int
	tracers.RegisterLookup(false, func(code string, _ *tracers.Context, _ json.RawMessage) (*tracers.Tracer, error) {
		if code != name {
			return nil, errors.New("unknown tracer")
		}
		return &tracers.Tracer{
			Hooks: &tracing.Hooks{OnTxEndV2: func(receipt *types.Receipt, gasUsed *mdgas.TxGasUsage, err error) {
				received = gasUsed
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
			received = nil
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
			gasUsed, err := TraceTx(t.Context(), nil, nil, msg, blockCtx, protocol.NewEVMTxContext(msg),
				uint256.NewInt(0), common.Hash{}, 0, ibs, &tracersConfig.TraceConfig{Tracer: &name},
				chain.AllProtocolChanges, jsonstream.New(io.Discard), time.Second, nil)
			require.Equal(t, 1, calls)
			if gasLimit == 100 {
				require.Error(t, err)
				require.ErrorIs(t, err, completionErr)
				require.Nil(t, received)
				require.Nil(t, receivedReceipt)
				return
			}
			require.NoError(t, err)
			require.NoError(t, completionErr)
			require.NotNil(t, received)
			require.NotNil(t, receivedReceipt)
			require.Equal(t, gasUsed, receivedReceipt.GasUsed)
			require.EqualValues(t, params.StateGasPerStorageSet, received.BlockStateGasUsed)
			require.Equal(t, gasUsed, received.BlockExecutionGasUsed+received.BlockStateGasUsed)
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
