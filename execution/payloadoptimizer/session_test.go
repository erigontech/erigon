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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/protocol/misc"
	protocolparams "github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type optimizerBackend struct {
	assemble func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error)
	get      func(context.Context, uint64) (execmodule.AssembledBlockResult, error)
	discard  func(uint64)
}

type panicMarshalTransaction struct {
	types.Transaction
}

type disguisedCanonicalTransaction struct {
	types.Transaction
	apparentType byte
}

func (tx *disguisedCanonicalTransaction) Type() byte {
	return tx.apparentType
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

func TestNewOrderflowUpdateRejectsCanonicalizedAccountAbstractionType(t *testing.T) {
	aa := &types.AccountAbstractionTransaction{
		ChainID:       uint256.NewInt(1),
		Tip:           uint256.NewInt(1),
		FeeCap:        uint256.NewInt(1),
		BuilderFee:    uint256.NewInt(0),
		NonceKey:      uint256.NewInt(0),
		SenderAddress: accounts.InternAddress(common.Address{1}),
	}
	tx := &disguisedCanonicalTransaction{Transaction: aa, apparentType: types.LegacyTxType}

	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{tx})
	require.ErrorIs(t, err, payloadoptimizer.ErrAccountAbstractionUnsupported)
	require.Empty(t, update.Transactions())
}

func TestNewOrderflowUpdateRevalidatesCanonicalBlobSidecar(t *testing.T) {
	wrapper := candidateBlobWrapper(t, 0, 0)
	tx := &disguisedCanonicalTransaction{Transaction: wrapper, apparentType: types.LegacyTxType}

	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{tx})
	require.ErrorContains(t, err, "has no sidecar")
	require.Empty(t, update.Transactions())
}

func TestNewOrderflowUpdateAuthenticatesBeforeBlobProofVerification(t *testing.T) {
	wrapper := candidateBlobWrapper(t, 0, 0)
	wrapper.Tx.V = uint256.Int{}
	wrapper.Tx.R = uint256.Int{}
	wrapper.Tx.S = uint256.Int{}
	wrapper.Proofs[0][0] ^= 0xff

	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{wrapper})
	require.ErrorContains(t, err, "authenticate orderflow transaction")
	require.Empty(t, update.Transactions())
}

func TestSessionApplyRejectsBadBlobProofBeforeBackend(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, clparams.ElectraVersion, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	called := false
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			called = true
			return execmodule.AssembleBlockResult{}, errors.New("backend reached")
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	wrapper := candidateBlobWrapper(t, 0, 0)
	wrapper.Proofs[0][0] ^= 0xff
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{wrapper})
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorContains(t, err, "proof verification")
	require.False(t, called)
}

func TestSessionApplyAppliesFuluBlobCountBeforeProofVerification(t *testing.T) {
	backendErr := errors.New("backend reached")
	for _, tc := range []struct {
		name          string
		stateVersion  clparams.StateVersion
		wrapper       byte
		blobCount     int
		maxBlobs      uint64
		wantForkError bool
	}{
		{name: "Fulu rejects seven v1 blobs", stateVersion: clparams.FuluVersion, wrapper: 1, blobCount: 7, maxBlobs: protocolparams.MaxBlobsPerTxn, wantForkError: true},
		{name: "Fulu accepts six v1 blobs", stateVersion: clparams.FuluVersion, wrapper: 1, blobCount: int(protocolparams.MaxBlobsPerTxn), maxBlobs: protocolparams.MaxBlobsPerTxn},
		{name: "Electra accepts seven v0 blobs below block cap", stateVersion: clparams.ElectraVersion, wrapper: 0, blobCount: 7, maxBlobs: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, _, requests := baseBuildContextInput()
			config := testBeaconConfigFor(tc.stateVersion)
			config.MaxBlobsPerBlock = tc.maxBlobs
			config.MaxBlobsPerBlockElectra = tc.maxBlobs
			params.TargetGasLimit = nil
			targetGasLimit := uint64(31_000_000)
			buildCtx, err := payloadoptimizer.NewBuildContext(params, config, 64, requests, baseParentGasLimit, payloadoptimizer.BuildDefaults{TargetGasLimit: &targetGasLimit, MaxBlobsPerBlock: &tc.maxBlobs})
			require.NoError(t, err)
			called := 0
			backend := &optimizerBackend{
				assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
					called++
					return execmodule.AssembleBlockResult{}, backendErr
				},
				get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
					return execmodule.AssembledBlockResult{}, nil
				},
			}
			session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
			require.NoError(t, err)
			update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{blobWrapperWithCount(t, tc.wrapper, tc.blobCount)})
			require.NoError(t, err)

			_, err = session.Apply(t.Context(), update)
			if tc.wantForkError {
				require.ErrorContains(t, err, "more than")
				require.Zero(t, called)
				return
			}
			require.ErrorIs(t, err, backendErr)
			require.Equal(t, 1, called)
		})
	}
}

func blobWrapperWithCount(t *testing.T, version byte, count int) *types.BlobTxWrapper {
	t.Helper()
	wrapper := candidateBlobWrapper(t, version, 0)
	firstHash := wrapper.Tx.BlobVersionedHashes[0]
	firstBlob := wrapper.Blobs[0]
	firstCommitment := wrapper.Commitments[0]
	proofsPerBlob := 1
	if version == 1 {
		proofsPerBlob = int(protocolparams.CellsPerExtBlob)
	}
	firstProofs := append([]types.KZGProof(nil), wrapper.Proofs[:proofsPerBlob]...)
	wrapper.Tx.BlobVersionedHashes = make([]common.Hash, count)
	wrapper.Blobs = make([]types.Blob, count)
	wrapper.Commitments = make([]types.KZGCommitment, count)
	wrapper.Proofs = make([]types.KZGProof, count*proofsPerBlob)
	for i := range count {
		wrapper.Tx.BlobVersionedHashes[i] = firstHash
		wrapper.Blobs[i] = firstBlob
		wrapper.Commitments[i] = firstCommitment
		copy(wrapper.Proofs[i*proofsPerBlob:], firstProofs)
	}
	return signOrderflowTransaction(t, wrapper).(*types.BlobTxWrapper)
}

func TestSessionAppliesForkSpecificBlobOrderflowShape(t *testing.T) {
	backendErr := errors.New("backend reached")
	for _, tc := range []struct {
		name         string
		stateVersion clparams.StateVersion
		wrapper      byte
		wantGate     bool
	}{
		{name: "Electra v0", stateVersion: clparams.ElectraVersion, wrapper: 0},
		{name: "Electra v1", stateVersion: clparams.ElectraVersion, wrapper: 1, wantGate: true},
		{name: "Fulu v0", stateVersion: clparams.FuluVersion, wrapper: 0, wantGate: true},
		{name: "Fulu v1", stateVersion: clparams.FuluVersion, wrapper: 1},
		{name: "Gloas v0", stateVersion: clparams.GloasVersion, wrapper: 0, wantGate: true},
		{name: "Gloas v1", stateVersion: clparams.GloasVersion, wrapper: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, fork, requests := baseBuildContextInput()
			if tc.stateVersion >= clparams.GloasVersion {
				slot := uint64(6)
				params.SlotNumber = &slot
			}
			buildCtx, err := newTestBuildContext(params, tc.stateVersion, fork, requests, baseParentGasLimit)
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
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
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

	canonicalResult.Block.Block.HeaderNoCopy().GasLimit = 2
	canonicalResult.Block.Receipts[0].GasUsed = 2
	canonicalResult.BlockValue.SetUint64(2)
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
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
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
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
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
