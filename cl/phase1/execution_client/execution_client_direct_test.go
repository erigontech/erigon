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

package execution_client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

type newPayloadModuleStub struct {
	execmodule.ExecutionModule
	inserted        []*types.Block
	validationCalls int
}

func (s *newPayloadModuleStub) InsertBlocks(_ context.Context, blocks []*types.Block) (execmodule.ExecutionStatus, error) {
	s.inserted = append(s.inserted, blocks...)
	return execmodule.ExecutionStatusSuccess, nil
}

func (s *newPayloadModuleStub) CurrentHeader(context.Context) (*types.Header, error) {
	return &types.Header{}, nil
}

func (s *newPayloadModuleStub) ValidateChain(context.Context, common.Hash, uint64) (execmodule.ValidationResult, error) {
	s.validationCalls++
	return execmodule.ValidationResult{ValidationStatus: execmodule.ExecutionStatusSuccess}, nil
}

func TestDirectNewPayloadTransactionEncoding(t *testing.T) {
	transactions, err := types.MarshalTransactionsBinary(types.Transactions{
		&types.LegacyTx{},
		&types.DynamicFeeTransaction{},
		&types.DynamicFeeTransaction{CommonTx: types.CommonTx{Data: make([]byte, 64)}},
	})
	require.NoError(t, err)
	wrappedShort, err := rlp.EncodeToBytes(transactions[1])
	require.NoError(t, err)
	wrappedLong, err := rlp.EncodeToBytes(transactions[2])
	require.NoError(t, err)

	for _, tc := range []struct {
		name         string
		transactions [][]byte
		wrapped      bool
	}{
		{name: "empty"},
		{name: "legacy", transactions: transactions[:1]},
		{name: "typed", transactions: transactions[1:]},
		{name: "mixed", transactions: transactions},
		{name: "wrapped short", transactions: [][]byte{wrappedShort}, wrapped: true},
		{name: "wrapped long", transactions: [][]byte{wrappedLong}, wrapped: true},
		{name: "wrapped after canonical", transactions: [][]byte{transactions[0], transactions[1], wrappedLong}, wrapped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			module := &newPayloadModuleStub{}
			client, err := NewExecutionClientDirect(chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil)
			require.NoError(t, err)

			payload := cltypes.NewEth1Block(clparams.FuluVersion, &clparams.MainnetBeaconConfig)
			payload.BlockNumber = 1
			payload.Extra = solid.NewExtraData()
			payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
			payload.Transactions = solid.NewTransactionsSSZFromTransactions(tc.transactions)
			parentRoot := common.Hash{}
			payload.BlockHash, err = payload.ComputeBlockHash(&parentRoot, cltypes.ComputeExecutionRequestHash(nil), nil)
			require.NoError(t, err)

			status, err := client.NewPayload(t.Context(), payload, &parentRoot, nil, nil)
			if tc.wrapped {
				require.EqualValues(t, PayloadStatusInvalidated, status)
				require.Error(t, err)
				require.Empty(t, module.inserted)
				require.Zero(t, module.validationCalls)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, PayloadStatusValidated, status)
			require.Len(t, module.inserted, 1)
			require.Len(t, module.inserted[0].Transactions(), len(tc.transactions))
			require.Equal(t, 1, module.validationCalls)
		})
	}
}

func TestRetryAssembleBlockReturnsFirstSuccess(t *testing.T) {
	calls := 0
	id, err := retryAssembleBlock(t.Context(), 3, time.Millisecond, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		if calls < 3 {
			return 0, chainreader.ErrExecutionBusy
		}
		return 7, nil
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), id)
	require.Equal(t, 3, calls)
}

func TestRetryAssembleBlockStopsOnRejection(t *testing.T) {
	rejected := errors.New("withdrawals before shanghai")
	calls := 0
	_, err := retryAssembleBlock(t.Context(), 30, time.Hour, time.Hour, func(context.Context) (uint64, error) {
		calls++
		return 0, rejected
	})

	// Only contention settles by waiting; a rejection answers the same way however often it is
	// asked, so retrying it just burns the slot.
	require.ErrorIs(t, err, rejected)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockGivesUpAfterAttempts(t *testing.T) {
	calls := 0
	_, err := retryAssembleBlock(t.Context(), 2, time.Millisecond, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, chainreader.ErrExecutionBusy)
	require.Equal(t, 2, calls)
}

func TestRetryAssembleBlockStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, time.Hour, func(context.Context) (uint64, error) {
		calls++
		cancel()
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockDoesNotStartWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, time.Hour, func(context.Context) (uint64, error) {
		calls++
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}

func TestRetryAssembleBlockRejectsNoAttempts(t *testing.T) {
	_, err := retryAssembleBlock(t.Context(), 0, time.Millisecond, time.Millisecond, func(context.Context) (uint64, error) {
		return 1, nil
	})
	require.EqualError(t, err, "assemble block requires at least one attempt")
}

// The usual contention is the forkchoice update's own commit, which ends within tens of
// milliseconds, so the first retries come quickly; longer contention backs off to the cap.
func TestAssembleRetryDelayBacksOffToTheCap(t *testing.T) {
	var delays []time.Duration
	for retry := range 7 {
		delays = append(delays, assembleRetryDelay(retry, 10*time.Millisecond, 200*time.Millisecond))
	}
	require.Equal(t, []time.Duration{
		10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond,
		160 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond,
	}, delays)
}

// One busy answer costs the first, short delay, not the cap.
func TestRetryAssembleBlockRetriesOneBusyAnswerQuickly(t *testing.T) {
	calls := 0
	start := time.Now()
	id, err := retryAssembleBlock(t.Context(), 30, 10*time.Millisecond, 5*time.Second, func(context.Context) (uint64, error) {
		calls++
		if calls == 1 {
			return 0, chainreader.ErrExecutionBusy
		}
		return 7, nil
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), id)
	require.Less(t, time.Since(start), time.Second)
}
