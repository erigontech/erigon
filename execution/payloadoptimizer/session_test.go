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

package payloadoptimizer_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

type optimizerBackend struct {
	assemble func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error)
	get      func(context.Context, uint64) (execmodule.AssembledBlockResult, error)
	discard  func(uint64)
}

type panicMarshalTransaction struct {
	types.Transaction
}

func (*panicMarshalTransaction) MarshalBinary(io.Writer) error {
	panic("malformed transaction")
}

func (b *optimizerBackend) AssembleBlock(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
	return b.assemble(ctx, params)
}

func (b *optimizerBackend) GetAssembledBlock(ctx context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
	return b.get(ctx, payloadID)
}

func (b *optimizerBackend) DiscardAssembledBlock(payloadID uint64) {
	if b.discard != nil {
		b.discard(payloadID)
	}
}

func TestOrderflowUpdateRejectsNilTransactions(t *testing.T) {
	var typedNil *types.LegacyTx

	for name, tx := range map[string]types.Transaction{
		"interface nil": nil,
		"typed nil":     typedNil,
	} {
		t.Run(name, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{tx})
				require.Error(t, err)
			})
		})
	}
}

func TestOrderflowUpdateReturnsMarshalPanicsAsErrors(t *testing.T) {
	require.NotPanics(t, func() {
		_, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{new(panicMarshalTransaction)})
		require.Error(t, err)
	})
}

func TestSessionAppliesForkSpecificBlobOrderflowShape(t *testing.T) {
	backendErr := errors.New("backend reached")
	for _, tc := range []struct {
		name      string
		amsterdam bool
		wrapper   byte
		wantGate  bool
	}{
		{name: "pre-Amsterdam v0", wrapper: 0},
		{name: "pre-Amsterdam v1", wrapper: 1, wantGate: true},
		{name: "Amsterdam v0", amsterdam: true, wrapper: 0, wantGate: true},
		{name: "Amsterdam v1", amsterdam: true, wrapper: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, fork, requests := baseBuildContextInput()
			if tc.amsterdam {
				slot := uint64(6)
				params.SlotNumber = &slot
			}
			buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
			require.NoError(t, err)
			called := false
			backend := &optimizerBackend{
				assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
					called = true
					return execmodule.AssembleBlockResult{}, backendErr
				},
				get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
					return execmodule.AssembledBlockResult{}, nil
				},
			}
			session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
			require.NoError(t, err)
			update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{candidateBlobWrapper(t, tc.wrapper, 0)})
			require.NoError(t, err)

			_, err = session.Apply(t.Context(), update)
			if tc.wantGate {
				require.ErrorContains(t, err, "wrapper version")
				require.False(t, called)
				return
			}
			require.ErrorIs(t, err, backendErr)
			require.True(t, called)
		})
	}
}

func TestColdSessionApplyPublishesAnImmutableCanonicalCandidate(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	canonicalResult := validColdResult(params, requests, 100)
	canonicalResult.Block.Block.HeaderNoCopy().Root = common.Hash{0x31}
	canonical := canonicalResult.Block
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, got *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			require.NoError(t, ctx.Err())
			require.Equal(t, params.ParentHash, got.ParentHash)
			require.NotNil(t, got.CustomTxnProvider)
			txs, err := got.CustomTxnProvider.ProvideTxns(ctx)
			require.NoError(t, err)
			require.Empty(t, txs)
			return execmodule.AssembleBlockResult{PayloadID: 17}, nil
		},
		get: func(ctx context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
			require.NoError(t, ctx.Err())
			require.Equal(t, uint64(17), payloadID)
			return canonicalResult, nil
		},
	}
	optimizer := payloadoptimizer.New(backend)
	session, err := optimizer.Open(t.Context(), buildCtx)
	require.NoError(t, err)
	_, ok := session.Best()
	require.False(t, ok)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	candidate, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.True(t, candidate.Context().Equal(buildCtx))
	require.Equal(t, canonical.Block.Hash(), candidate.Block().Block.Hash())
	require.Equal(t, uint64(100), candidate.Value().Uint64())

	firstCopy := candidate.Block()
	firstCopy.Block.HeaderNoCopy().GasLimit = 1
	firstCopy.Requests[0].RequestData[0] = 0xff
	candidate.Value().SetUint64(1)

	best, ok := session.Best()
	require.True(t, ok)
	require.Equal(t, misc.CalcGasLimit(baseParentGasLimit, *params.TargetGasLimit), best.Block().Block.GasLimit())
	require.Equal(t, []byte{0x0e}, best.Block().Requests[0].RequestData)
	require.Equal(t, uint64(100), best.Value().Uint64())
	require.True(t, best.Context().Equal(buildCtx))
}

func TestSessionRejectsCandidateForDifferentParent(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	header := &types.Header{
		ParentHash:            common.Hash{0xff},
		Number:                *uint256.NewInt(2),
		GasLimit:              *params.TargetGasLimit,
		Time:                  params.Timestamp,
		MixDigest:             params.PrevRandao,
		Coinbase:              params.SuggestedFeeRecipient,
		ParentBeaconBlockRoot: params.ParentBeaconBlockRoot,
		SlotNumber:            params.SlotNumber,
		Extra:                 params.ExtraData,
	}
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 1}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			return execmodule.AssembledBlockResult{
				Block: &types.BlockWithReceipts{
					Block:    types.NewBlock(header, nil, nil, nil, params.Withdrawals, nil),
					Requests: requests,
				},
				BlockValue: uint256.NewInt(1),
			}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrCandidateContextMismatch)
	_, ok := session.Best()
	require.False(t, ok)
}

func TestApplyDoesNotInstallAfterCallCancellation(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	header := &types.Header{
		ParentHash:            params.ParentHash,
		Number:                *uint256.NewInt(2),
		GasLimit:              *params.TargetGasLimit,
		Time:                  params.Timestamp,
		MixDigest:             params.PrevRandao,
		Coinbase:              params.SuggestedFeeRecipient,
		ParentBeaconBlockRoot: params.ParentBeaconBlockRoot,
		SlotNumber:            params.SlotNumber,
		Extra:                 params.ExtraData,
	}
	started := make(chan struct{})
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			close(started)
			<-ctx.Done()
			return execmodule.AssembleBlockResult{PayloadID: 1}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			return execmodule.AssembledBlockResult{
				Block: &types.BlockWithReceipts{
					Block:    types.NewBlock(header, nil, nil, nil, params.Withdrawals, nil),
					Requests: requests,
				},
				BlockValue: uint256.NewInt(1),
			}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	applyCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(applyCtx, update)
		done <- applyErr
	}()
	<-started
	cancel()

	require.ErrorIs(t, <-done, context.Canceled)
	_, ok := session.Best()
	require.False(t, ok)
}
