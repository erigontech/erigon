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
	"math"
	"sync/atomic"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	protocolparams "github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
)

func signOrderflowTransaction(t *testing.T, transaction types.Transaction) types.Transaction {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signed, err := types.SignTx(transaction, *types.LatestSignerForChainID(transaction.GetChainID()), key)
	require.NoError(t, err)
	return signed
}

func TestCanceledQueuedApplyNeverCallsBackend(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
			}
			return execmodule.AssembleBlockResult{Busy: true}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("unexpected collection")
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	firstDone := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		firstDone <- applyErr
	}()
	<-firstStarted
	queuedCtx, cancel := context.WithCancel(t.Context())
	queuedDone := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(queuedCtx, update)
		queuedDone <- applyErr
	}()
	cancel()

	select {
	case err := <-queuedDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled queued apply did not return")
	}
	require.Equal(t, uint64(1), calls.Load())
	close(releaseFirst)
	require.ErrorIs(t, <-firstDone, payloadoptimizer.ErrBackendBusy)
}

func TestSessionRetriesBusyCollectionAndDiscardsPayload(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	var gets atomic.Uint64
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 91}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			if gets.Add(1) < 3 {
				return execmodule.AssembledBlockResult{Busy: true}, nil
			}
			return validColdResult(params, requests, 1), nil
		},
		discard: func(id uint64) {
			require.Equal(t, uint64(91), id)
			discards.Add(1)
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	candidate, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.Equal(t, uint64(3), gets.Load())
	require.Equal(t, uint64(1), discards.Load())
}

func TestSessionDiscardsAnIdReturnedWithBusy(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 7, Busy: true}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("unexpected collection")
			return execmodule.AssembledBlockResult{}, nil
		},
		discard: func(id uint64) {
			require.Equal(t, uint64(7), id)
			discards.Add(1)
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrBackendBusy)
	require.Equal(t, uint64(1), discards.Load())
}

func TestSessionDiscardsAnIdReturnedWithAssemblyError(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	backendErr := errors.New("assembly failed after allocating an id")
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 9}, backendErr
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("unexpected collection")
			return execmodule.AssembledBlockResult{}, nil
		},
		discard: func(id uint64) {
			require.Equal(t, uint64(9), id)
			discards.Add(1)
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, backendErr)
	require.Equal(t, uint64(1), discards.Load())
}

func TestSessionBusyRetryStopsOnCancellationAndDiscards(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	var gets atomic.Uint64
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 8}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			gets.Add(1)
			return execmodule.AssembledBlockResult{Busy: true}, nil
		},
		discard: func(uint64) { discards.Add(1) },
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	applyCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, err = session.Apply(applyCtx, update)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Positive(t, gets.Load())
	require.Equal(t, uint64(1), discards.Load())
}

func TestPermanentBusyReturnsAndLaterApplyAdvances(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	var assemblies atomic.Uint64
	var firstGets atomic.Uint64
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: assemblies.Add(1)}, nil
		},
		get: func(_ context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
			if payloadID == 1 {
				firstGets.Add(1)
				return execmodule.AssembledBlockResult{Busy: true}, nil
			}
			return validColdResult(params, requests, 2), nil
		},
		discard: func(uint64) { discards.Add(1) },
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	firstDone := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(context.Background(), update)
		firstDone <- applyErr
	}()

	select {
	case err := <-firstDone:
		require.ErrorIs(t, err, payloadoptimizer.ErrBackendBusy)
	case <-time.After(150 * time.Millisecond):
		require.NoError(t, session.Close())
		<-firstDone
		t.Fatal("permanent Busy did not reach a finite terminal result")
	}
	require.LessOrEqual(t, firstGets.Load(), uint64(8))
	second, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, uint64(2), discards.Load())
}

func TestCloseWaitsForActiveApplyCleanup(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	started := make(chan struct{})
	discarded := make(chan struct{})
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			close(started)
			<-ctx.Done()
			return execmodule.AssembleBlockResult{PayloadID: 12}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("collection must not start after close")
			return execmodule.AssembledBlockResult{}, nil
		},
		discard: func(uint64) { close(discarded) },
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	applyDone := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		applyDone <- applyErr
	}()
	<-started

	require.NoError(t, session.Close())
	require.ErrorIs(t, <-applyDone, context.Canceled)
	select {
	case <-discarded:
	default:
		t.Fatal("Close returned before payload cleanup")
	}
}

func TestParentCancellationPreventsPromotionAndHidesExistingBest(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)

	t.Run("before promotion", func(t *testing.T) {
		parentCtx, cancelParent := context.WithCancel(t.Context())
		backend := &optimizerBackend{
			assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				return execmodule.AssembleBlockResult{PayloadID: 20}, nil
			},
			get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
				cancelParent()
				return validColdResult(params, requests, 1), nil
			},
		}
		session, err := payloadoptimizer.New(backend).Open(parentCtx, buildCtx)
		require.NoError(t, err)
		update, err := payloadoptimizer.NewOrderflowUpdate(nil)
		require.NoError(t, err)

		_, err = session.Apply(t.Context(), update)
		require.ErrorIs(t, err, context.Canceled)
		_, ok := session.Best()
		require.False(t, ok)
	})

	t.Run("after best", func(t *testing.T) {
		parentCtx, cancelParent := context.WithCancel(t.Context())
		backend := &optimizerBackend{
			assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				return execmodule.AssembleBlockResult{PayloadID: 21}, nil
			},
			get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
				return validColdResult(params, requests, 1), nil
			},
		}
		session, err := payloadoptimizer.New(backend).Open(parentCtx, buildCtx)
		require.NoError(t, err)
		update, err := payloadoptimizer.NewOrderflowUpdate(nil)
		require.NoError(t, err)
		candidate, err := session.Apply(t.Context(), update)
		require.NoError(t, err)
		require.NotNil(t, candidate)
		cancelParent()

		_, ok := session.Best()
		require.False(t, ok)
	})
}

func TestApplyCancellationAfterCollectionPreventsPromotion(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	applyCtx, cancelApply := context.WithCancel(t.Context())
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 22}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			result := validColdResult(params, requests, 1)
			cancelApply()
			return result, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(applyCtx, update)
	require.ErrorIs(t, err, context.Canceled)
	_, ok := session.Best()
	require.False(t, ok)
}

func TestCloseDuringCollectionPreventsPromotion(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	closeDone := make(chan error, 1)
	var session *payloadoptimizer.Session
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 23}, nil
		},
		get: func(ctx context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
			go func() { closeDone <- session.Close() }()
			<-ctx.Done()
			return validColdResult(params, requests, 1), nil
		},
	}
	session, err = payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, <-closeDone)
	_, ok := session.Best()
	require.False(t, ok)
}

func TestOrderflowProviderHonorsAmountAcrossCalls(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	txs := types.Transactions{
		signOrderflowTransaction(t, &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21_000}}),
		signOrderflowTransaction(t, &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 2, GasLimit: 21_000}}),
	}
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			first, err := params.CustomTxnProvider.ProvideTxns(ctx, txnprovider.WithAmount(1))
			require.NoError(t, err)
			second, err := params.CustomTxnProvider.ProvideTxns(ctx, txnprovider.WithAmount(1))
			require.NoError(t, err)
			exhausted, err := params.CustomTxnProvider.ProvideTxns(ctx,
				txnprovider.WithAmount(1),
				txnprovider.WithTxnIdsFilter(mapset.NewThreadUnsafeSet([32]byte(txs[0].Hash()), [32]byte(txs[1].Hash()))),
			)
			require.NoError(t, err)
			require.Equal(t, uint64(1), first[0].GetNonce())
			require.Equal(t, uint64(2), second[0].GetNonce())
			require.Empty(t, exhausted)
			return execmodule.AssembleBlockResult{Busy: true}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(txs)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrBackendBusy)
}

func TestOrderflowProviderRetainsTransactionsForDynamicFilters(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	txs := make(types.Transactions, 51)
	for i := range txs {
		txs[i] = signOrderflowTransaction(t, &types.LegacyTx{CommonTx: types.CommonTx{Nonce: uint64(50 - i), GasLimit: 21_000}})
	}
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			filter := mapset.NewThreadUnsafeSet[[32]byte]()
			retained := params.CustomTxnProvider.(builder.RetainedTxnProvider)
			firstBatch, err := retained.ProvideRetainedTxns(ctx, txnprovider.WithAmount(50), txnprovider.WithTxnIdsFilter(filter))
			require.NoError(t, err)
			require.False(t, firstBatch.PassComplete)
			first := firstBatch.Transactions
			require.Len(t, first, 50)
			require.Equal(t, uint64(50), first[0].GetNonce())
			require.Equal(t, uint64(1), first[49].GetNonce())
			for _, transaction := range first {
				filter.Remove([32]byte(transaction.Hash()))
			}

			secondBatch, err := retained.ProvideRetainedTxns(ctx, txnprovider.WithAmount(50), txnprovider.WithTxnIdsFilter(filter))
			require.NoError(t, err)
			require.True(t, secondBatch.PassComplete)
			second := secondBatch.Transactions
			require.Len(t, second, 1)
			require.Equal(t, uint64(0), second[0].GetNonce())
			thirdBatch, err := retained.ProvideRetainedTxns(ctx, txnprovider.WithAmount(50), txnprovider.WithTxnIdsFilter(filter))
			require.NoError(t, err)
			require.False(t, thirdBatch.PassComplete)
			third := thirdBatch.Transactions
			require.Len(t, third, 50)
			require.Equal(t, uint64(50), third[0].GetNonce())
			require.Equal(t, uint64(1), third[49].GetNonce())
			return execmodule.AssembleBlockResult{Busy: true}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(txs)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrBackendBusy)
}

func TestOrderflowProviderHonorsBlobAndRlpBudgets(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	baseWrapper := types.MakeWrappedBlobTxn(uint256.NewInt(1))
	baseWrapper.Tx.BlobVersionedHashes = baseWrapper.Tx.BlobVersionedHashes[:1]
	baseWrapper.Blobs = baseWrapper.Blobs[:1]
	baseWrapper.Commitments = baseWrapper.Commitments[:1]
	baseWrapper.Proofs = baseWrapper.Proofs[:1]
	oneBlob := func(nonce uint64) *types.BlobTxWrapper {
		wrapper := types.CopyTxs(types.Transactions{baseWrapper})[0].(*types.BlobTxWrapper)
		wrapper.Tx.Nonce = nonce
		return signOrderflowTransaction(t, wrapper).(*types.BlobTxWrapper)
	}
	firstBlob, secondBlob := oneBlob(1), oneBlob(2)
	small := signOrderflowTransaction(t, &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 3, GasLimit: 21_000, Data: []byte{0x01}}})
	large := signOrderflowTransaction(t, &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 4, GasLimit: 21_000, Data: make([]byte, 512)}})
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			blobLimited, err := params.CustomTxnProvider.ProvideTxns(ctx,
				txnprovider.WithAmount(2),
				txnprovider.WithGasTarget(mdgas.NewFullMdGas(math.MaxUint64, math.MaxUint64, protocolparams.GasPerBlob)),
			)
			require.NoError(t, err)
			require.Len(t, blobLimited, 2)
			require.Equal(t, firstBlob.Hash(), blobLimited[0].Hash())
			require.Equal(t, small.Hash(), blobLimited[1].Hash())

			blobFilter := mapset.NewThreadUnsafeSet(
				[32]byte(firstBlob.Hash()), [32]byte(small.Hash()), [32]byte(large.Hash()),
			)
			completedPass, err := params.CustomTxnProvider.ProvideTxns(ctx,
				txnprovider.WithAmount(1),
				txnprovider.WithTxnIdsFilter(blobFilter),
				txnprovider.WithGasTarget(mdgas.NewFullMdGas(math.MaxUint64, math.MaxUint64, protocolparams.GasPerBlob)),
			)
			require.NoError(t, err)
			require.Empty(t, completedPass)
			secondTry, err := params.CustomTxnProvider.ProvideTxns(ctx,
				txnprovider.WithAmount(1),
				txnprovider.WithTxnIdsFilter(blobFilter),
				txnprovider.WithGasTarget(mdgas.NewFullMdGas(math.MaxUint64, math.MaxUint64, protocolparams.GasPerBlob)),
			)
			require.NoError(t, err)
			require.Len(t, secondTry, 1)
			require.Equal(t, secondBlob.Hash(), secondTry[0].Hash())

			rlpLimited, err := params.CustomTxnProvider.ProvideTxns(ctx,
				txnprovider.WithAmount(2),
				txnprovider.WithTxnIdsFilter(mapset.NewThreadUnsafeSet([32]byte(firstBlob.Hash()), [32]byte(secondBlob.Hash()))),
				txnprovider.WithAvailableRlpSpace(small.EncodingSize()+rlp.ListPrefixLen(small.EncodingSize())),
			)
			require.NoError(t, err)
			require.Len(t, rlpLimited, 1)
			require.Equal(t, small.Hash(), rlpLimited[0].Hash())
			return execmodule.AssembleBlockResult{Busy: true}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{firstBlob, secondBlob, small, large})
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrBackendBusy)
}

func TestOrderflowProviderChargesCanonicalRlpElementCost(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	transaction := signOrderflowTransaction(t, types.NewTransaction(0, common.Address{1}, uint256.NewInt(1), 21_000, uint256.NewInt(1), []byte{1}))
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			tooSmall, err := params.CustomTxnProvider.(builder.RetainedTxnProvider).ProvideRetainedTxns(ctx,
				txnprovider.WithAmount(1),
				txnprovider.WithAvailableRlpSpace(transaction.EncodingSize()),
			)
			require.NoError(t, err)
			require.Empty(t, tooSmall.Transactions)
			require.True(t, tooSmall.PassComplete)

			canonicalCost := transaction.EncodingSize() + rlp.ListPrefixLen(transaction.EncodingSize())
			exact, err := params.CustomTxnProvider.(builder.RetainedTxnProvider).ProvideRetainedTxns(ctx,
				txnprovider.WithAmount(1),
				txnprovider.WithAvailableRlpSpace(canonicalCost),
			)
			require.NoError(t, err)
			require.Len(t, exact.Transactions, 1)
			require.True(t, exact.PassComplete)
			return execmodule.AssembleBlockResult{Busy: true}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{transaction})
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrBackendBusy)
}

func TestOpenRejectsTypedNilBackend(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	var backend *optimizerBackend

	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.Error(t, err)
	require.Nil(t, session)
}

func TestNewOrderflowUpdateRequiresCompleteBlobSidecars(t *testing.T) {
	validV0 := signOrderflowTransaction(t, types.MakeWrappedBlobTxn(uint256.NewInt(1))).(*types.BlobTxWrapper)
	validV1 := signOrderflowTransaction(t, types.MakeV1WrappedBlobTxn(uint256.NewInt(1))).(*types.BlobTxWrapper)
	missingSidecar := types.CopyTxs(types.Transactions{validV0})[0].(*types.BlobTxWrapper)
	missingSidecar.Blobs = nil
	missingSidecar.Commitments = nil
	missingSidecar.Proofs = nil
	unknownVersion := types.CopyTxs(types.Transactions{validV0})[0].(*types.BlobTxWrapper)
	unknownVersion.WrapperVersion = 2
	badHash := types.CopyTxs(types.Transactions{validV0})[0].(*types.BlobTxWrapper)
	badHash.Tx.BlobVersionedHashes[0][1] ^= 0xff

	for _, transaction := range (types.Transactions{validV0, validV1}) {
		update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{transaction})
		require.NoError(t, err)
		require.Len(t, update.Transactions(), 1)
	}
	for _, transaction := range (types.Transactions{&validV0.Tx, missingSidecar, unknownVersion, badHash}) {
		update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{transaction})
		require.Error(t, err)
		require.Empty(t, update.Transactions())
	}
}

func TestCloseCancelsActiveApplyAndPreventsLaterUse(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	started := make(chan struct{})
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			close(started)
			<-ctx.Done()
			return execmodule.AssembleBlockResult{}, ctx.Err()
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("GetAssembledBlock called after canceled assembly")
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		done <- applyErr
	}()
	<-started

	require.NoError(t, session.Close())
	require.NoError(t, session.Close())
	require.ErrorIs(t, <-done, context.Canceled)
	_, ok := session.Best()
	require.False(t, ok)
	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrSessionClosed)
}

func TestSessionKeepsOnlyStrictlyImprovedCandidates(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	values := []uint64{100, 90, 110}
	var next atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: next.Add(1)}, nil
		},
		get: func(_ context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
			result := validColdResult(params, requests, values[payloadID-1])
			result.Block.Block.HeaderNoCopy().Root[0] = byte(payloadID)
			return result, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	first, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.NotNil(t, first)
	second, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.Nil(t, second)
	best, ok := session.Best()
	require.True(t, ok)
	require.Equal(t, uint64(100), best.Value().Uint64())
	third, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.NotNil(t, third)
	best, ok = session.Best()
	require.True(t, ok)
	require.Equal(t, uint64(110), best.Value().Uint64())
}

func TestSessionReportsColdBackendStates(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	backendErr := errors.New("backend failed")
	tests := map[string]struct {
		assemble execmodule.AssembleBlockResult
		get      execmodule.AssembledBlockResult
		errAt    string
		want     error
	}{
		"assemble busy":  {assemble: execmodule.AssembleBlockResult{Busy: true}, want: payloadoptimizer.ErrBackendBusy},
		"unknown":        {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Unknown: true}, want: payloadoptimizer.ErrUnknownPayload},
		"not ready":      {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, want: payloadoptimizer.ErrPayloadNotReady},
		"nil block":      {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{}}, want: payloadoptimizer.ErrPayloadNotReady},
		"nil header":     {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{Block: new(types.Block)}}, want: payloadoptimizer.ErrPayloadNotReady},
		"assemble error": {errAt: "assemble", want: backendErr},
		"get error":      {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, errAt: "get", want: backendErr},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var discards atomic.Uint64
			backend := &optimizerBackend{
				assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
					if test.errAt == "assemble" {
						return execmodule.AssembleBlockResult{}, backendErr
					}
					return test.assemble, nil
				},
				get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
					if test.errAt == "get" {
						return execmodule.AssembledBlockResult{}, backendErr
					}
					return test.get, nil
				},
				discard: func(uint64) { discards.Add(1) },
			}
			session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
			require.NoError(t, err)
			update, err := payloadoptimizer.NewOrderflowUpdate(nil)
			require.NoError(t, err)

			_, err = session.Apply(t.Context(), update)
			require.ErrorIs(t, err, test.want)
			if test.assemble.PayloadID == 0 {
				require.Zero(t, discards.Load())
			} else {
				require.Equal(t, uint64(1), discards.Load())
			}
		})
	}
}

func TestSessionDiscardsAContextMismatchedCandidate(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	result := validColdResult(params, requests, 1)
	result.Block.Block.HeaderNoCopy().ParentHash[0]++
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 10}, nil
		},
		get:     func(context.Context, uint64) (execmodule.AssembledBlockResult, error) { return result, nil },
		discard: func(uint64) { discards.Add(1) },
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrCandidateContextMismatch)
	require.Equal(t, uint64(1), discards.Load())
}

func TestOrderflowUpdateOwnsTransactions(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	tx, err := types.SignTx(
		&types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21_000, Data: []byte{0x01}}},
		*types.LatestSignerForChainID(nil),
		key,
	)
	require.NoError(t, err)
	wantSender, err := tx.Sender(*types.LatestSignerForChainID(nil))
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{tx})
	require.NoError(t, err)
	tx.(*types.LegacyTx).Data[0] = 0xff
	tx.SetSender(accounts.InternAddress(common.Address{0xff}))

	first := update.Transactions()
	require.Equal(t, []byte{0x01}, first[0].GetData())
	gotSender, ok := first[0].GetSender()
	require.True(t, ok)
	require.Equal(t, wantSender, gotSender)
	first[0].(*types.LegacyTx).Data[0] = 0xee
	first[0].SetSender(accounts.InternAddress(common.Address{0xee}))
	require.Equal(t, []byte{0x01}, update.Transactions()[0].GetData())
	gotSender, ok = update.Transactions()[0].GetSender()
	require.True(t, ok)
	require.Equal(t, wantSender, gotSender)
}

func TestOrderflowUpdateReauthenticatesCachedSender(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	chainID := uint256.NewInt(1)
	tx, err := types.SignTx(
		types.NewTransaction(0, common.Address{1}, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil),
		*types.LatestSignerForChainID(chainID),
		key,
	)
	require.NoError(t, err)
	want, err := tx.Sender(*types.LatestSignerForChainID(chainID))
	require.NoError(t, err)
	tx.SetSender(accounts.InternAddress(common.Address{0xff}))

	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{tx})
	require.NoError(t, err)
	got, ok := update.Transactions()[0].GetSender()
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestNewOrderflowUpdateRejectsAccountAbstraction(t *testing.T) {
	t.Parallel()

	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{new(types.AccountAbstractionTransaction)})
	require.ErrorIs(t, err, payloadoptimizer.ErrAccountAbstractionUnsupported)
	require.Empty(t, update.Transactions())
}

func TestSessionSerializesConcurrentApply(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := newTestBuildContext(params, testStateVersion(params), fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Uint64
	var active atomic.Int64
	var maxActive atomic.Int64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			current := active.Add(1)
			for previous := maxActive.Load(); current > previous && !maxActive.CompareAndSwap(previous, current); previous = maxActive.Load() {
			}
			call := calls.Add(1)
			if call == 1 {
				close(firstStarted)
				<-releaseFirst
			}
			active.Add(-1)
			return execmodule.AssembleBlockResult{PayloadID: call}, nil
		},
		get: func(_ context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
			return validColdResult(params, requests, payloadID), nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	done := make(chan error, 2)
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		done <- applyErr
	}()
	<-firstStarted
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		done <- applyErr
	}()
	require.Never(t, func() bool { return maxActive.Load() > 1 }, 50*time.Millisecond, time.Millisecond)
	close(releaseFirst)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.Equal(t, int64(1), maxActive.Load())
}
