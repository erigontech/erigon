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

package forkchoice

import (
	"context"
	"errors"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

const lockScopeTimeout = 5 * time.Second

// blockingEngine returns a mock whose NewPayload signals entry and then blocks until
// the returned release channel is closed, so a test can hold OnBlock inside the EL call.
func blockingEngine(tb testing.TB, times int, status execution_client.PayloadStatus, retErr error) (*execution_client.MockExecutionEngine, chan struct{}, chan struct{}) {
	tb.Helper()
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(tb))
	entered := make(chan struct{}, times)
	release := make(chan struct{})
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(times).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			entered <- struct{}{}
			<-release
			return status, retErr
		})
	return engine, entered, release
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lockScopeTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// OnBlock must not hold f.mu across the EL NewPayload call: an unrelated fork-choice
// writer has to be able to make progress while the EL is still working.
func TestOnBlockYieldsForkChoiceLockDuringNewPayload(t *testing.T) {
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	store, block := buildExAnteStorePendingLast(t, engine)

	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- store.OnBlock(context.Background(), block, true, true, false) }()
	awaitSignal(t, elEntered, "NewPayload to start")

	tickDone := make(chan struct{})
	go func() {
		store.OnTick(48)
		close(tickDone)
	}()
	awaitSignal(t, tickDone, "OnTick to acquire the fork-choice lock while NewPayload is blocked")

	close(releaseEL)
	select {
	case err := <-onBlockDone:
		require.NoError(t, err)
	case <-time.After(lockScopeTimeout):
		t.Fatal("OnBlock did not finish after the EL was released")
	}

	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, ok := store.forkGraph.GetHeader(blockRoot)
	require.True(t, ok, "block should have been added once the EL validated it")
	require.True(t, store.verifiedExecutionPayload.Contains(blockRoot))
}

// A GetHead that runs while f.mu is released caches a head computed without the
// incoming block, so OnBlock has to drop that cache again after it resumes.
func TestOnBlockResetsCachedHeadAfterNewPayload(t *testing.T) {
	// A VALID status with an error is rejected before markPayloadStatus, which would
	// otherwise reset the head cache itself and mask a missing invalidation here.
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, errors.New("el unavailable"))
	store, block := buildExAnteStorePendingLast(t, engine)

	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- store.OnBlock(context.Background(), block, true, true, false) }()
	awaitSignal(t, elEntered, "NewPayload to start")

	headCached := make(chan struct{})
	go func() {
		defer close(headCached)
		_, _, err := store.GetHead(nil)
		require.NoError(t, err)
	}()
	awaitSignal(t, headCached, "GetHead to populate the head cache")

	store.mu.RLock()
	cachedHead := store.headHash
	store.mu.RUnlock()
	require.NotEqual(t, common.Hash{}, cachedHead, "GetHead should have cached a head during the released-lock window")

	close(releaseEL)
	require.Error(t, <-onBlockDone)

	store.mu.RLock()
	defer store.mu.RUnlock()
	require.Equal(t, common.Hash{}, store.headHash, "head cached during the EL call must be invalidated")
	require.Equal(t, cltypes.PayloadStatusPending, store.headPayloadStatus)
}

// Finality can move while f.mu is released, so the finalized-descendant checks that
// gated entry into OnBlock have to be redone before the block is committed.
func TestOnBlockRechecksFinalityAfterNewPayload(t *testing.T) {
	t.Run("finalized past the block", func(t *testing.T) {
		store, block, run := startOnBlockInsideEL(t, execution_client.PayloadStatusValidated, nil)
		// Epoch 1 starts at slot 32, above the block's slot.
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1, Root: block.Block.ParentRoot})
		require.NoError(t, run())
		requireBlockNotAdded(t, store, block)
	})

	t.Run("finalized onto another branch", func(t *testing.T) {
		store, block, run := startOnBlockInsideEL(t, execution_client.PayloadStatusValidated, nil)
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 0, Root: common.HexToHash("0xdead")})
		require.ErrorIs(t, run(), ErrNotFinalizedDescendant)
		requireBlockNotAdded(t, store, block)
	})
}

// startOnBlockInsideEL parks an OnBlock call inside the EL NewPayload call with f.mu
// released. run releases the EL and returns OnBlock's error.
func startOnBlockInsideEL(t *testing.T, status execution_client.PayloadStatus, retErr error) (*ForkChoiceStore, *cltypes.SignedBeaconBlock, func() error) {
	t.Helper()
	engine, elEntered, releaseEL := blockingEngine(t, 1, status, retErr)
	store, block := buildExAnteStorePendingLast(t, engine)
	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- store.OnBlock(context.Background(), block, true, true, false) }()
	awaitSignal(t, elEntered, "NewPayload to start")
	return store, block, func() error {
		close(releaseEL)
		select {
		case err := <-onBlockDone:
			return err
		case <-time.After(lockScopeTimeout):
			t.Fatal("OnBlock did not finish after the EL was released")
			return nil
		}
	}
}

func requireBlockNotAdded(t *testing.T, store *ForkChoiceStore, block *cltypes.SignedBeaconBlock) {
	t.Helper()
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, ok := store.forkGraph.GetHeader(blockRoot)
	require.False(t, ok, "block must not be committed once finality moved past it")
	store.mu.RLock()
	defer store.mu.RUnlock()
	require.NotContains(t, store.headSet, common.Hash(blockRoot))
}

// Two equivocating blocks can both clear the entry admission check before either is
// inserted, so the guard has to be redone after the EL call. Whichever finishes second
// must be rejected.
func TestOnBlockRejectsEquivocationRegisteredDuringNewPayload(t *testing.T) {
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	store, sibling := buildExAnteStorePendingLast(t, engine)

	// Same slot and proposer as the sibling, but a different root.
	parked := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(parked, diffBlockd4Enc, int(clparams.AltairVersion)))
	parked.Block.StateRoot = common.HexToHash("0xfeed")
	siblingRoot, err := sibling.Block.HashSSZ()
	require.NoError(t, err)
	parkedRoot, err := parked.Block.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, siblingRoot, parkedRoot, "the two blocks must have distinct roots")
	require.Equal(t, sibling.Block.Slot, parked.Block.Slot)
	require.Equal(t, sibling.Block.ProposerIndex, parked.Block.ProposerIndex)

	// Park one block inside the EL with f.mu released, so nothing is inserted yet.
	done := make(chan error, 1)
	go func() {
		done <- store.OnBlockWithEquivocationCheck(context.Background(), parked, true, true, false)
	}()
	awaitSignal(t, elEntered, "NewPayload to start for the equivocating block")

	// Register the sibling while the parked caller is still in the EL.
	require.NoError(t, store.OnBlock(context.Background(), sibling, false, true, false))
	_, ok := store.forkGraph.GetHeader(siblingRoot)
	require.True(t, ok, "the sibling must be registered before the EL call returns")

	close(releaseEL)
	select {
	case err := <-done:
		require.ErrorContains(t, err, "conflicts with a previously validated proposal",
			"the equivocating block must be rejected by the equivocation guard, not incidentally")
	case <-time.After(lockScopeTimeout):
		t.Fatal("OnBlockWithEquivocationCheck did not finish")
	}
	_, ok = store.forkGraph.GetHeader(parkedRoot)
	require.False(t, ok, "the equivocating block must not be inserted")
}

// Finality moving during the EL call must not swallow the EL's own verdict: the error is
// still reported, it is only the commit that is dropped.
func TestOnBlockKeepsELVerdictWhenFinalityMovesDuringNewPayload(t *testing.T) {
	t.Run("engine error", func(t *testing.T) {
		store, block, run := startOnBlockInsideEL(t, execution_client.PayloadStatusNone, errors.New("el unavailable"))
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1, Root: block.Block.ParentRoot})

		require.ErrorIs(t, run(), ErrNewPayloadNoStatus)
		requireBlockNotAdded(t, store, block)
	})
}

// A NOT_VALIDATED verdict is kept even when finality drops the block: the payload really
// is unvalidated, so the root stays optimistic. Cleanup is not finality-driven — it waits
// for a later validated payload with a higher execution block number.
func TestOnBlockKeepsNotValidatedVerdictWhenFinalityMovesDuringNewPayload(t *testing.T) {
	store, block, run := startOnBlockInsideEL(t, execution_client.PayloadStatusNotValidated, nil)
	store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1, Root: block.Block.ParentRoot})

	require.NoError(t, run())
	requireBlockNotAdded(t, store, block)

	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.True(t, store.IsRootOptimistic(blockRoot), "an unvalidated payload stays optimistic")

	block.Block.Body.ExecutionPayload.BlockNumber++
	require.NoError(t, store.optimisticStore.ValidateBlock(common.HexToHash("0xfeed"), block.Block))
	require.False(t, store.IsRootOptimistic(blockRoot), "a later validated payload sweeps the entry")
}

// The VALID result must be published before the admission token is released. Pinning f.mu
// means the finishing caller cannot publish after relock, so if the marker is set by the
// time the token comes free, it was published under the token.
func TestNewPayloadPublishesValidatedBeforeReleasingAdmission(t *testing.T) {
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	store, block := buildExAnteStorePendingLast(t, engine)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- store.OnBlock(context.Background(), block, true, true, false) }()
	awaitSignal(t, elEntered, "NewPayload to start")

	// Pin f.mu so the caller cannot reach any post-relock publishing.
	pinned, unpin := make(chan struct{}), make(chan struct{})
	go func() {
		store.mu.Lock()
		close(pinned)
		<-unpin
		store.mu.Unlock()
	}()
	awaitSignal(t, pinned, "the competing writer to take the fork-choice lock")
	close(releaseEL)

	// Sending into the admission channel blocks until the caller hands the token back.
	store.payloadValidationAdmission <- struct{}{}
	require.True(t, store.verifiedExecutionPayload.Contains(blockRoot),
		"the validated payload must be published before the admission token is released")
	<-store.payloadValidationAdmission

	close(unpin)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(lockScopeTimeout):
		t.Fatal("OnBlock did not finish")
	}
}

// The INVALIDATED verdict must be published under the admission token too. Without it,
// callers queued for the same payload each resend it to the EL, because the invalid gate
// they check on entry is only refreshed once the first caller reacquires f.mu.
func TestNewPayloadPublishesInvalidatedBeforeReleasingAdmission(t *testing.T) {
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusInvalidated, nil)
	store, block := buildExAnteStorePendingLast(t, engine)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	executionBlockHash := block.Block.Body.ExecutionPayload.BlockHash

	done := make(chan struct{})
	go func() {
		defer close(done)
		store.mu.Lock()
		defer store.mu.Unlock()
		_, _ = store.newPayloadForBlockWhileYieldingForkChoiceLock(context.Background(),
			blockRoot, executionBlockHash, block.Block.Body.ExecutionPayload,
			&block.Block.ParentRoot, nil, nil)
	}()
	awaitSignal(t, elEntered, "NewPayload to start")

	// Pin f.mu so the caller cannot reach anything it would record after relock.
	pinned, unpin := make(chan struct{}), make(chan struct{})
	go func() {
		store.mu.Lock()
		close(pinned)
		<-unpin
		store.mu.Unlock()
	}()
	awaitSignal(t, pinned, "the competing writer to take the fork-choice lock")
	close(releaseEL)

	// Sending into the admission channel blocks until the caller hands the token back.
	store.payloadValidationAdmission <- struct{}{}
	status, ok := store.executionPayloadStatus.Get(executionBlockHash)
	require.True(t, ok, "the invalid verdict must be published before the token is released")
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
	<-store.payloadValidationAdmission

	close(unpin)
	awaitSignal(t, done, "the validating caller to finish")
}

// A caller that wins admission only after someone else validated the same payload
// must not send it to the EL a second time.
func TestNewPayloadForBlockWhileYieldingLockSkipsValidatedPayload(t *testing.T) {
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	verified, err := lru.New[common.Hash, struct{}](8)
	require.NoError(t, err)
	f := &ForkChoiceStore{engine: engine, verifiedExecutionPayload: verified}
	blockRoot := common.HexToHash("0xabc")
	verified.Add(blockRoot, struct{}{})

	f.mu.Lock()
	status, err := f.newPayloadForBlockWhileYieldingForkChoiceLock(context.Background(),
		blockRoot, common.Hash{}, nil, nil, nil, nil)
	locked := f.mu.TryLock()
	f.mu.Unlock()

	require.NoError(t, err)
	require.EqualValues(t, execution_client.PayloadStatusValidated, status)
	require.False(t, locked, "the fork-choice lock must be held again when the helper returns")
}

// A cancelled EL call must still release f.mu, propagate the same error as before,
// and commit nothing.
func TestOnBlockCancelledNewPayloadLeavesStoreConsistent(t *testing.T) {
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	elEntered := make(chan struct{})
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *cltypes.Eth1Block, _ *common.Hash, _ []common.Hash, _ []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			close(elEntered)
			<-ctx.Done()
			return execution_client.PayloadStatusNone, ctx.Err()
		})
	store, block := buildExAnteStorePendingLast(t, engine)

	ctx, cancel := context.WithCancel(context.Background())
	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- store.OnBlock(ctx, block, true, true, false) }()
	awaitSignal(t, elEntered, "NewPayload to start")
	cancel()

	select {
	case err := <-onBlockDone:
		require.ErrorIs(t, err, ErrNewPayloadNoStatus)
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(lockScopeTimeout):
		t.Fatal("OnBlock did not finish after the EL call was cancelled")
	}

	require.True(t, store.mu.TryLock(), "the fork-choice lock must be free after a cancelled EL call")
	store.mu.Unlock()
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, ok := store.forkGraph.GetHeader(blockRoot)
	require.False(t, ok, "a block whose EL validation was cancelled must not be committed")
	require.False(t, store.verifiedExecutionPayload.Contains(blockRoot))
}

// Two OnBlock calls racing on the same block must both succeed and leave the payload
// validated, whichever order they win admission in.
func TestConcurrentOnBlockForSameBlockStaysConsistent(t *testing.T) {
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			return execution_client.PayloadStatusValidated, nil
		})
	store, block := buildExAnteStorePendingLast(t, engine)

	done := make(chan error, 2)
	for range 2 {
		go func() { done <- store.OnBlock(context.Background(), block, true, true, false) }()
	}
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(lockScopeTimeout):
			t.Fatal("concurrent OnBlock did not finish")
		}
	}

	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.True(t, store.verifiedExecutionPayload.Contains(blockRoot))
	_, ok := store.forkGraph.GetHeader(blockRoot)
	require.True(t, ok)
	status, ok := store.executionPayloadStatus.Get(block.Block.Body.ExecutionPayload.BlockHash)
	require.True(t, ok)
	require.EqualValues(t, execution_client.PayloadStatusValidated, status)
}
