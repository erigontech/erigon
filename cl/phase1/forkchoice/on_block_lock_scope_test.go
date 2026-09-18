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
	"sync/atomic"
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

func noDerivedHash() (common.Hash, bool) { return common.Hash{}, false }

// selfConsistentPayload makes the payload name its own derived hash, which is what a
// caller must do before any invalid verdict for it can be cached.
func selfConsistentPayload(t *testing.T, block *cltypes.SignedBeaconBlock) func() (common.Hash, bool) {
	t.Helper()
	payload := block.Block.Body.ExecutionPayload
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	payload.Extra = solid.NewExtraData()
	executionHash, err := payload.ComputeBlockHash(&block.Block.ParentRoot, common.Hash{}, nil)
	require.NoError(t, err)
	payload.BlockHash = executionHash
	return func() (common.Hash, bool) { return executionHash, true }
}

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
	select {
	case store.payloadValidationAdmission <- struct{}{}:
	case <-time.After(lockScopeTimeout):
		t.Fatal("timed out waiting for the admission token to be released")
	}
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
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusInvalidated, errors.New("bad block"))
	store, block := buildExAnteStorePendingLast(t, engine)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	derived := selfConsistentPayload(t, block)
	done := make(chan struct{})
	go func() {
		defer close(done)
		store.mu.Lock()
		defer store.mu.Unlock()
		_, _ = store.newPayloadForBlockWhileYieldingForkChoiceLock(context.Background(),
			blockRoot, func() error { return nil }, derived,
			block.Block.Body.ExecutionPayload, &block.Block.ParentRoot, nil, nil)
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
	select {
	case store.payloadValidationAdmission <- struct{}{}:
	case <-time.After(lockScopeTimeout):
		t.Fatal("timed out waiting for the admission token to be released")
	}
	status, ok := store.payloadStatusByRoot.Get(blockRoot)
	require.True(t, ok, "the invalid verdict must be published before the token is released")
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
	<-store.payloadValidationAdmission

	close(unpin)
	awaitSignal(t, done, "the validating caller to finish")
}

// getHead snapshots the justified checkpoint before taking f.mu. If it moved by the time
// the lock is held, the head computed from the old one must be discarded rather than cached.
func TestGetHeadOnceDiscardsSupersededCheckpoint(t *testing.T) {
	store, _ := buildExAnteStorePendingLast(t, nil)
	stale := store.justifiedCheckpoint.Load().(solid.Checkpoint)
	store.justifiedCheckpoint.Store(solid.Checkpoint{Epoch: stale.Epoch + 1, Root: stale.Root})

	_, _, ok, err := store.getHeadOnce(nil, stale)
	require.NoError(t, err)
	require.False(t, ok, "a superseded checkpoint must not produce a head")

	store.mu.RLock()
	defer store.mu.RUnlock()
	require.Equal(t, common.Hash{}, store.headHash, "nothing may be cached from the stale checkpoint")
}

// A caller that queues for the admission token can go stale while it waits. It must not
// spend an EL call on a block that is no longer admissible, because that call serializes on
// the shared token and delays unrelated payloads behind it.
func TestNewPayloadSkipsELWhenBlockWentStaleWhileQueued(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)
	store, block := buildExAnteStorePendingLast(t, engine)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	store.mu.Lock()
	status, err := store.newPayloadForBlockWhileYieldingForkChoiceLock(context.Background(),
		blockRoot, func() error { return errBlockAtFinalizedHorizon }, noDerivedHash,
		block.Block.Body.ExecutionPayload, &block.Block.ParentRoot, nil, nil)
	store.mu.Unlock()

	require.ErrorIs(t, err, errBlockAtFinalizedHorizon)
	require.EqualValues(t, execution_client.PayloadStatusNone, status)
}

// Invalid is terminal. A root invalidated while the EL call was in flight must not be
// reported as validated to a queued caller, nor have its validated marker revived.
func TestNewPayloadKeepsInvalidAheadOfValidated(t *testing.T) {
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	store, block := buildExAnteStorePendingLast(t, engine)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		store.mu.Lock()
		defer store.mu.Unlock()
		_, _ = store.newPayloadForBlockWhileYieldingForkChoiceLock(context.Background(),
			blockRoot, func() error { return nil }, noDerivedHash,
			block.Block.Body.ExecutionPayload, &block.Block.ParentRoot, nil, nil)
	}()
	awaitSignal(t, elEntered, "NewPayload to start")

	// The root is invalidated while the EL is still working on it.
	store.payloadStatusByRoot.Add(blockRoot, execution_client.PayloadStatusInvalidated)
	close(releaseEL)
	awaitSignal(t, done, "the validating caller to finish")

	require.False(t, store.verifiedExecutionPayload.Contains(blockRoot),
		"a root invalidated during the call must not gain a validated marker")
}

// Two beacon blocks can carry the same execution payload. The invalid verdict must be
// cached against the payload, not only the beacon root, or the second one resends it.
func TestNewPayloadPublishesInvalidatedAgainstThePayload(t *testing.T) {
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusInvalidated, errors.New("bad block"))
	store, block := buildExAnteStorePendingLast(t, engine)
	payload := block.Block.Body.ExecutionPayload
	derived := selfConsistentPayload(t, block)
	executionHash := payload.BlockHash

	done := make(chan struct{})
	go func() {
		defer close(done)
		store.mu.Lock()
		defer store.mu.Unlock()
		_, _ = store.newPayloadForBlockWhileYieldingForkChoiceLock(context.Background(),
			common.HexToHash("0xaaaa"), func() error { return nil }, derived,
			payload, &block.Block.ParentRoot, nil, nil)
	}()
	awaitSignal(t, elEntered, "NewPayload to start")

	// Pin f.mu so nothing can be recorded after relock.
	pinned, unpin := make(chan struct{}), make(chan struct{})
	go func() {
		store.mu.Lock()
		close(pinned)
		<-unpin
		store.mu.Unlock()
	}()
	awaitSignal(t, pinned, "the competing writer to take the fork-choice lock")
	close(releaseEL)

	select {
	case store.payloadValidationAdmission <- struct{}{}:
	case <-time.After(lockScopeTimeout):
		t.Fatal("timed out waiting for the admission token to be released")
	}
	status, ok := store.executionPayloadStatus.Get(executionHash)
	require.True(t, ok, "the verdict must be cached against the payload before the token is released")
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
	<-store.payloadValidationAdmission

	close(unpin)
	awaitSignal(t, done, "the validating caller to finish")

	// A different beacon root carrying the same payload must reuse that verdict. The mock
	// allows one call only, so a resend fails here.
	store.mu.Lock()
	second, err := store.newPayloadForBlockWhileYieldingForkChoiceLock(context.Background(),
		common.HexToHash("0xbbbb"), func() error { return nil }, derived,
		payload, &block.Block.ParentRoot, nil, nil)
	store.mu.Unlock()
	require.NoError(t, err)
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, second)
}

// The verdict must also survive eviction from the bounded status caches: the authoritative
// invalid set outlives them and has to be consulted too.
func TestNewPayloadHonoursTheAuthoritativeInvalidSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)
	store, block := buildExAnteStorePendingLast(t, engine)
	payload := block.Block.Body.ExecutionPayload
	derived := selfConsistentPayload(t, block)

	// Only the authoritative set knows, as it would be once the LRUs evicted the entry.
	store.invalidatedExecutionPayloads.Store(payload.BlockHash, struct{}{})

	store.mu.Lock()
	status, err := store.newPayloadForBlockWhileYieldingForkChoiceLock(context.Background(),
		common.HexToHash("0xcccc"), func() error { return nil }, derived,
		payload, &block.Block.ParentRoot, nil, nil)
	store.mu.Unlock()
	require.NoError(t, err)
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
}

// stillAdmissible has to run after the admission token is acquired, not before. Running it
// earlier would re-open the window where a block goes stale while queued.
func TestNewPayloadChecksAdmissionOnlyAfterWinningTheToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)
	store, block := buildExAnteStorePendingLast(t, engine)

	// The channel is created lazily on first use, so make sure it exists before taking it.
	store.payloadValidationOnce.Do(func() { store.payloadValidationAdmission = make(chan struct{}, 1) })
	store.payloadValidationAdmission <- struct{}{}

	var checked atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	locked, done := make(chan struct{}), make(chan error, 1)
	go func() {
		store.mu.Lock()
		close(locked)
		_, err := store.newPayloadForBlockWhileYieldingForkChoiceLock(ctx,
			common.HexToHash("0xbbbb"), func() error {
				checked.Store(true)
				return nil
			}, noDerivedHash, block.Block.Body.ExecutionPayload, &block.Block.ParentRoot, nil, nil)
		store.mu.Unlock()
		done <- err
	}()

	// Taking f.mu proves the helper released it, so it is now parked on the token.
	awaitSignal(t, locked, "the caller to take the fork-choice lock")
	store.mu.Lock()
	store.mu.Unlock()
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, errPayloadValidationAdmission)
	case <-time.After(lockScopeTimeout):
		t.Fatal("the caller did not return after cancellation")
	}
	require.False(t, checked.Load(), "admission was checked before the token was won")
	<-store.payloadValidationAdmission
}

// A payload whose claimed hash does not match its contents is rejected for naming the
// wrong payload. That verdict says nothing about either identity, so repeating the
// submission must keep reaching the EL and must never blacklist the claimed hash.
func TestOnBlockMalformedHashIsNotCachedAgainstTheClaimedHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(2).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			return execution_client.PayloadStatusInvalidated, errors.New("mismatching hash")
		})
	store, block := buildExAnteStorePendingLast(t, engine)
	payload := block.Block.Body.ExecutionPayload
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	payload.Extra = solid.NewExtraData()
	claimed := payload.BlockHash
	derivedHash, err := payload.ComputeBlockHash(&block.Block.ParentRoot, common.Hash{}, nil)
	require.NoError(t, err)
	require.NotEqual(t, claimed, derivedHash, "the payload must not name its own hash")

	for range 2 {
		require.ErrorContains(t, store.OnBlock(context.Background(), block, true, true, false),
			"invalid execution payload hash")
	}

	_, cached := store.GetRecentExecutionPayloadStatus(claimed)
	require.False(t, cached, "a hash the payload never owned must not be blacklisted")
	require.False(t, store.executionHashMarkedInvalid(claimed))
	require.False(t, store.rootMarkedInvalid(common.Hash(mustRoot(t, block))))
}

func mustRoot(t *testing.T, block *cltypes.SignedBeaconBlock) common.Hash {
	t.Helper()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return root
}

// The global admission token must never be held waiting on f.mu: whoever holds the lock
// may be in a slow EL call of its own, and every other payload would queue behind it.
func TestNewPayloadDoesNotWaitForTheLockWhileHoldingTheToken(t *testing.T) {
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	store, block := buildExAnteStorePendingLast(t, engine)

	// Occupy the token so the caller below parks on it with f.mu already released.
	store.payloadValidationOnce.Do(func() { store.payloadValidationAdmission = make(chan struct{}, 1) })
	store.payloadValidationAdmission <- struct{}{}

	var checked atomic.Bool
	locked, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		store.mu.Lock()
		close(locked)
		_, _ = store.newPayloadForBlockWhileYieldingForkChoiceLock(context.Background(),
			mustRoot(t, block), func() error {
				checked.Store(true)
				return nil
			}, noDerivedHash, block.Block.Body.ExecutionPayload, &block.Block.ParentRoot, nil, nil)
		store.mu.Unlock()
	}()

	// Taking f.mu proves the caller released it, and holding it is the contention the
	// caller must not wait on once it wins the token.
	awaitSignal(t, locked, "the caller to take the fork-choice lock")
	store.mu.Lock()
	<-store.payloadValidationAdmission

	awaitSignal(t, elEntered, "NewPayload to start while the fork-choice lock is held")
	require.False(t, checked.Load(), "admission must be skipped rather than wait for the lock")
	store.mu.Unlock()
	close(releaseEL)
	awaitSignal(t, done, "the caller to finish")
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
		blockRoot, func() error { return nil }, noDerivedHash,
		&cltypes.Eth1Block{}, nil, nil, nil)
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
