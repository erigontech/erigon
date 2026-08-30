package payloadoptimizer_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/types"
)

type optimizerBackend struct {
	assemble func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error)
	get      func(context.Context, uint64) (execmodule.AssembledBlockResult, error)
}

func (b *optimizerBackend) AssembleBlock(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
	return b.assemble(ctx, params)
}

func (b *optimizerBackend) GetAssembledBlock(ctx context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
	return b.get(ctx, payloadID)
}

func TestColdSessionApplyPublishesAnImmutableCanonicalCandidate(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests)
	require.NoError(t, err)
	header := &types.Header{
		ParentHash:            params.ParentHash,
		Number:                *uint256.NewInt(2),
		GasLimit:              *params.TargetGasLimit,
		Time:                  params.Timestamp,
		Root:                  common.Hash{0x31},
		MixDigest:             params.PrevRandao,
		Coinbase:              params.SuggestedFeeRecipient,
		ParentBeaconBlockRoot: params.ParentBeaconBlockRoot,
		SlotNumber:            params.SlotNumber,
		Extra:                 params.ExtraData,
	}
	canonical := &types.BlockWithReceipts{
		Block:    types.NewBlock(header, nil, nil, nil, params.Withdrawals, nil),
		Requests: requests,
	}
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
			return execmodule.AssembledBlockResult{Block: canonical, BlockValue: uint256.NewInt(100)}, nil
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
	require.Equal(t, *params.TargetGasLimit, best.Block().Block.GasLimit())
	require.Equal(t, []byte{0x0e}, best.Block().Requests[0].RequestData)
	require.Equal(t, uint64(100), best.Value().Uint64())
	require.True(t, best.Context().Equal(buildCtx))
}

func TestSessionRejectsCandidateForDifferentParent(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests)
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
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests)
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
