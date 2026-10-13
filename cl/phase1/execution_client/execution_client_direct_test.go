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
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

func TestRetryAssembleBlockReturnsFirstSuccess(t *testing.T) {
	calls := 0
	id, err := retryAssembleBlock(t.Context(), 3, time.Millisecond, func(context.Context) (uint64, error) {
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
	_, err := retryAssembleBlock(t.Context(), 30, time.Hour, func(context.Context) (uint64, error) {
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
	_, err := retryAssembleBlock(t.Context(), 2, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, chainreader.ErrExecutionBusy)
	require.Equal(t, 2, calls)
}

func TestRetryAssembleBlockStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, func(context.Context) (uint64, error) {
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
	_, err := retryAssembleBlock(ctx, 30, time.Hour, func(context.Context) (uint64, error) {
		calls++
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}

func TestRetryAssembleBlockRejectsNoAttempts(t *testing.T) {
	_, err := retryAssembleBlock(t.Context(), 0, time.Millisecond, func(context.Context) (uint64, error) {
		return 1, nil
	})
	require.EqualError(t, err, "assemble block requires at least one attempt")
}

type newPayloadModuleStub struct {
	execmodule.ExecutionModule
	inserted int
}

func (s *newPayloadModuleStub) InsertBlocks(context.Context, []*types.Block) (execmodule.ExecutionStatus, error) {
	s.inserted++
	return execmodule.ExecutionStatusSuccess, nil
}

func (s *newPayloadModuleStub) CurrentHeader(context.Context) (*types.Header, error) {
	return nil, nil
}

func newDirectClientForNewPayload(t *testing.T) (*ExecutionClientDirect, *newPayloadModuleStub) {
	t.Helper()
	module := &newPayloadModuleStub{}
	client, err := NewExecutionClientDirect(chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil)
	require.NoError(t, err)
	return client, module
}

func gloasPayloadWithBlobTx(t *testing.T, blobHashes ...common.Hash) *cltypes.Eth1Block {
	t.Helper()
	txn := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{CommonTx: types.CommonTx{GasLimit: 21_000, To: &common.Address{1}}},
		BlobVersionedHashes:   blobHashes,
	}
	raw, err := types.MarshalTransactionsBinary(types.Transactions{txn})
	require.NoError(t, err)
	payload := gloas(&clparams.MainnetBeaconConfig, []byte{0xc0})
	payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions(raw)
	payload.BlobGasUsed = misc.GetBlobGasUsed(len(blobHashes))
	bal, err := DecodeAndValidateBlockAccessList(payload)
	require.NoError(t, err)
	payload.BlockHash, err = payload.ComputeBlockHash(&common.Hash{}, cltypes.ComputeExecutionRequestHash(nil), bal)
	require.NoError(t, err)
	return payload
}

func TestDirectNewPayloadRejectsBlobHashesTheBidDidNotCommitTo(t *testing.T) {
	v, w := common.Hash{0x01, 0x0a}, common.Hash{0x01, 0x0b}
	for _, tc := range []struct {
		name      string
		inPayload []common.Hash
		expected  []common.Hash
	}{
		{"missing", []common.Hash{v}, []common.Hash{}},
		{"different", []common.Hash{v}, []common.Hash{w}},
		{"reordered", []common.Hash{v, w}, []common.Hash{w, v}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, module := newDirectClientForNewPayload(t)

			status, err := client.NewPayload(t.Context(), gloasPayloadWithBlobTx(t, tc.inPayload...), &common.Hash{}, tc.expected, nil)

			require.ErrorIs(t, err, misc.ErrMismatchBlobHashes)
			require.EqualValues(t, PayloadStatusInvalidated, status)
			require.Zero(t, module.inserted)
		})
	}
}

func TestDirectNewPayloadAcceptsMatchingBlobHashes(t *testing.T) {
	v := common.Hash{0x01, 0x0a}
	client, module := newDirectClientForNewPayload(t)

	status, err := client.NewPayload(t.Context(), gloasPayloadWithBlobTx(t, v), &common.Hash{}, []common.Hash{v}, nil)

	require.NoError(t, err)
	require.EqualValues(t, PayloadStatusNotValidated, status)
	require.Equal(t, 1, module.inserted)
}

func TestDirectNewPayloadTreatsNilBlobHashesAsACallerError(t *testing.T) {
	client, module := newDirectClientForNewPayload(t)

	status, err := client.NewPayload(t.Context(), gloasPayloadWithBlobTx(t, common.Hash{0x01}), &common.Hash{}, nil, nil)

	require.ErrorIs(t, err, misc.ErrNilBlobHashes)
	require.EqualValues(t, PayloadStatusNone, status)
	require.Zero(t, module.inserted)
}
