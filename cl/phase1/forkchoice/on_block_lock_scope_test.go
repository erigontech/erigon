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

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	dasmock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

const lockScopeTimeout = 5 * time.Second

// buildOnBlockLockScopeStore returns a store holding the first two ex-ante blocks and
// the third block, still unprocessed, so a test can drive one OnBlock through the EL.
func buildOnBlockLockScopeStore(tb testing.TB, engine execution_client.ExecutionEngine) (*ForkChoiceStore, *cltypes.SignedBeaconBlock) {
	tb.Helper()
	ctx := context.Background()
	cfg := &clparams.MainnetBeaconConfig
	sd := synced_data.NewSyncedDataManager(cfg, true)
	b3a := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	bc2 := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	bd4 := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	require.NoError(tb, utils.DecodeSSZSnappy(b3a, diffBlock3aEnc, int(clparams.AltairVersion)))
	require.NoError(tb, utils.DecodeSSZSnappy(bc2, diffBlockc2Enc, int(clparams.AltairVersion)))
	require.NoError(tb, utils.DecodeSSZSnappy(bd4, diffBlockd4Enc, int(clparams.AltairVersion)))
	anchor := state2.New(cfg)
	require.NoError(tb, utils.DecodeSSZSnappy(anchor, diffAnchorEnc, int(clparams.AltairVersion)))
	gs, err := initial_state.GetGenesisState(tb.Context(), 1)
	require.NoError(tb, err)
	clk := eth_clock.NewEthereumClock(gs.GenesisTime(), gs.GenesisValidatorsRoot(), cfg)
	bs := blob_storage.NewBlobStore(mdbxtest.NewTestDB(tb, dbcfg.ChainDB), afero.NewMemMapFs())
	forkGraphDisk, err := fork_graph.NewForkGraphDisk(anchor, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(tb, err)
	store, err := NewForkChoiceStore(clk, anchor, engine, pool.NewOperationsPool(cfg),
		forkGraphDisk, beaconevents.NewEventEmitter(), sd, bs,
		public_keys_registry.NewInMemoryPublicKeysRegistry(), validator_params.NewValidatorParams(), false, nil)
	require.NoError(tb, err)
	store.OnTick(0)
	store.OnTick(12)
	require.NoError(tb, store.OnBlock(ctx, b3a, false, true, false))
	store.OnTick(36)
	require.NoError(tb, store.OnBlock(ctx, bc2, false, true, false))
	return store, bd4
}

// blockingEngine returns a mock whose NewPayload signals entry and then blocks until
// the returned release channel is closed, so a test can hold OnBlock inside the EL call.
func blockingEngine(tb testing.TB, times int) (*execution_client.MockExecutionEngine, chan struct{}, chan struct{}) {
	tb.Helper()
	return blockingEngineReturning(tb, times, execution_client.PayloadStatusValidated, nil)
}

func blockingEngineReturning(tb testing.TB, times int, status execution_client.PayloadStatus, retErr error) (*execution_client.MockExecutionEngine, chan struct{}, chan struct{}) {
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
	engine, elEntered, releaseEL := blockingEngine(t, 1)
	store, block := buildOnBlockLockScopeStore(t, engine)

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
	require.True(t, store.IsPayloadVerified(blockRoot))
}

// A GetHead that runs while f.mu is released caches a head computed without the
// incoming block, so OnBlock has to drop that cache again after it resumes.
func TestOnBlockResetsCachedHeadAfterNewPayload(t *testing.T) {
	engine, elEntered, releaseEL := blockingEngine(t, 1)
	store, block := buildOnBlockLockScopeStore(t, engine)

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
	require.NoError(t, <-onBlockDone)

	store.mu.RLock()
	defer store.mu.RUnlock()
	require.Equal(t, common.Hash{}, store.headHash, "head cached during the EL call must be invalidated")
	require.Equal(t, cltypes.PayloadStatusPending, store.headPayloadStatus)
}

// Finality can move while f.mu is released, so the finalized-descendant checks that
// gated entry into OnBlock have to be redone before the block is committed.
func TestOnBlockRechecksFinalityAfterNewPayload(t *testing.T) {
	t.Run("finalized past the block", func(t *testing.T) {
		store, block, run := startOnBlockInsideEL(t)
		// Epoch 1 starts at slot 32, above the block's slot.
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1, Root: block.Block.ParentRoot})
		require.NoError(t, run())
		requireBlockNotAdded(t, store, block)
	})

	t.Run("finalized onto another branch", func(t *testing.T) {
		store, block, run := startOnBlockInsideEL(t)
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 0, Root: common.HexToHash("0xdead")})
		require.ErrorIs(t, run(), ErrNotFinalizedDescendant)
		requireBlockNotAdded(t, store, block)
	})
}

// startOnBlockInsideEL parks an OnBlock call inside the EL NewPayload call with f.mu
// released. run releases the EL and returns OnBlock's error.
func startOnBlockInsideEL(t *testing.T) (*ForkChoiceStore, *cltypes.SignedBeaconBlock, func() error) {
	t.Helper()
	return startOnBlockInsideELReturning(t, execution_client.PayloadStatusValidated, nil)
}

func startOnBlockInsideELReturning(t *testing.T, status execution_client.PayloadStatus, retErr error) (*ForkChoiceStore, *cltypes.SignedBeaconBlock, func() error) {
	t.Helper()
	engine, elEntered, releaseEL := blockingEngineReturning(t, 1, status, retErr)
	store, block := buildOnBlockLockScopeStore(t, engine)
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

// blockWithBlobCommitment makes the block take the EL GetBlobs branch. The body edit
// invalidates the block against its state root, so callers must assert on how OnBlock
// returns rather than on a successful import.
func blockWithBlobCommitment(t *testing.T, store *ForkChoiceStore, block *cltypes.SignedBeaconBlock) *cltypes.SignedBeaconBlock {
	t.Helper()
	peerDas := dasmock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsArchivedMode().Return(false).AnyTimes()
	store.InitPeerDas(peerDas)
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	return block
}

// blockingBlobEngine returns a mock whose GetBlobs signals entry and then blocks.
func blockingBlobEngine(t *testing.T) (*execution_client.MockExecutionEngine, chan struct{}, chan struct{}) {
	t.Helper()
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	engine.EXPECT().
		GetBlobs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, []common.Hash, clparams.StateVersion) ([][]byte, [][][]byte, error) {
			entered <- struct{}{}
			<-release
			return nil, nil, nil
		})
	return engine, entered, release
}

// GetBlobs is a blocking EL call on the same pre-Gloas path, so OnBlock must not hold
// f.mu across it either.
func TestOnBlockYieldsForkChoiceLockDuringGetBlobs(t *testing.T) {
	engine, blobsEntered, releaseBlobs := blockingBlobEngine(t)
	store, block := buildOnBlockLockScopeStore(t, engine)
	block = blockWithBlobCommitment(t, store, block)

	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- store.OnBlock(context.Background(), block, false, true, true) }()
	awaitSignal(t, blobsEntered, "GetBlobs to start")

	tickDone := make(chan struct{})
	go func() {
		store.OnTick(48)
		close(tickDone)
	}()
	awaitSignal(t, tickDone, "OnTick to acquire the fork-choice lock while GetBlobs is blocked")

	close(releaseBlobs)
	select {
	case <-onBlockDone:
	case <-time.After(lockScopeTimeout):
		t.Fatal("OnBlock did not finish after GetBlobs was released")
	}
}

// GetBlobs runs with newPayload=false, where the post-NewPayload recheck never runs, so
// the finalized-descendant checks have to be redone after this yield too.
func TestOnBlockRechecksFinalityAfterGetBlobs(t *testing.T) {
	t.Run("finalized past the block", func(t *testing.T) {
		store, block, run := startOnBlockInsideGetBlobs(t)
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1, Root: block.Block.ParentRoot})
		require.NoError(t, run())
		requireBlockNotAdded(t, store, block)
	})

	t.Run("finalized onto another branch", func(t *testing.T) {
		store, block, run := startOnBlockInsideGetBlobs(t)
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 0, Root: common.HexToHash("0xdead")})
		require.ErrorIs(t, run(), ErrNotFinalizedDescendant)
		requireBlockNotAdded(t, store, block)
	})
}

func startOnBlockInsideGetBlobs(t *testing.T) (*ForkChoiceStore, *cltypes.SignedBeaconBlock, func() error) {
	t.Helper()
	engine, blobsEntered, releaseBlobs := blockingBlobEngine(t)
	store, block := buildOnBlockLockScopeStore(t, engine)
	block = blockWithBlobCommitment(t, store, block)
	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- store.OnBlock(context.Background(), block, false, true, true) }()
	awaitSignal(t, blobsEntered, "GetBlobs to start")
	return store, block, func() error {
		close(releaseBlobs)
		select {
		case err := <-onBlockDone:
			return err
		case <-time.After(lockScopeTimeout):
			t.Fatal("OnBlock did not finish after GetBlobs was released")
			return nil
		}
	}
}

// Finality moving during the EL call must not swallow the EL's own verdict: an invalid
// payload still has to be reported and recorded, it is only the commit that is dropped.
func TestOnBlockKeepsELVerdictWhenFinalityMovesDuringNewPayload(t *testing.T) {
	t.Run("invalidated", func(t *testing.T) {
		store, block, run := startOnBlockInsideELReturning(t, execution_client.PayloadStatusInvalidated, nil)
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1, Root: block.Block.ParentRoot})

		require.ErrorContains(t, run(), "block is invalid")
		status, ok := store.executionPayloadStatus.Get(block.Block.Body.ExecutionPayload.BlockHash)
		require.True(t, ok, "the EL verdict must still be recorded")
		require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
		requireBlockNotAdded(t, store, block)
	})

	t.Run("engine error", func(t *testing.T) {
		store, block, run := startOnBlockInsideELReturning(t, execution_client.PayloadStatusNone, errors.New("el unavailable"))
		store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1, Root: block.Block.ParentRoot})

		require.ErrorIs(t, run(), ErrNewPayloadNoStatus)
		requireBlockNotAdded(t, store, block)
	})
}

// A NOT_VALIDATED verdict is kept even when finality drops the block: the payload really
// is unvalidated, so the root stays optimistic. Cleanup is not finality-driven — it waits
// for a later validated payload with a higher execution block number.
func TestOnBlockKeepsNotValidatedVerdictWhenFinalityMovesDuringNewPayload(t *testing.T) {
	store, block, run := startOnBlockInsideELReturning(t, execution_client.PayloadStatusNotValidated, nil)
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

// A caller that wins admission only after someone else validated the same payload
// must not send it to the EL a second time.
func TestNewPayloadWhileYieldingLockSkipsValidatedPayload(t *testing.T) {
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	f := &ForkChoiceStore{engine: engine}

	f.mu.Lock()
	status, err := f.newPayloadWhileYieldingForkChoiceLock(context.Background(),
		func() bool { return true }, nil, nil, nil, nil)
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
	store, block := buildOnBlockLockScopeStore(t, engine)

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
	require.False(t, store.IsPayloadVerified(blockRoot))
}

// Two OnBlock calls racing on the same block must both succeed and leave the payload
// validated, whichever order they win admission in.
func TestConcurrentOnBlockForSameBlockStaysConsistent(t *testing.T) {
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		MinTimes(1).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			return execution_client.PayloadStatusValidated, nil
		})
	store, block := buildOnBlockLockScopeStore(t, engine)

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
	require.True(t, store.IsPayloadVerified(blockRoot))
	_, ok := store.forkGraph.GetHeader(blockRoot)
	require.True(t, ok)
	status, ok := store.executionPayloadStatus.Get(block.Block.Body.ExecutionPayload.BlockHash)
	require.True(t, ok)
	require.EqualValues(t, execution_client.PayloadStatusValidated, status)
}
