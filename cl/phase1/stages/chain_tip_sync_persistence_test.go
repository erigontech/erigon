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

package stages

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	das_mock_services "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

type blockWriteErrorDB struct {
	kv.RwDB
	err          error
	failures     int
	writes       int
	onWriteError func()
}

func (db *blockWriteErrorDB) Update(ctx context.Context, fn func(kv.RwTx) error) error {
	db.writes++
	if db.writes <= db.failures {
		if db.onWriteError != nil {
			db.onWriteError()
		}
		return db.err
	}
	return db.RwDB.Update(ctx, fn)
}

func TestChainTipBatchDoesNotRejectAcceptedBlockAfterWriteFailure(t *testing.T) {
	cfg, graph, _, _, block, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	graph.insertOnAdd = true
	cfg.indiciesDB = &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 1}
	cfg.chainTipRejections = newChainTipRejections(time.Hour, nil)
	cfg.chainTipRejections.lastLog = time.Now()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	rejected := make(map[common.Hash]error)

	reachedTarget := processChainTipBatch(t.Context(), cfg, Args{targetSlot: block.Block.Slot}, []*cltypes.SignedBeaconBlock{block}, make(map[common.Hash]struct{}), rejected)

	require.False(t, reachedTarget)
	_, accepted := cfg.forkChoice.GetHeader(root)
	require.True(t, accepted)
	require.NotNil(t, cfg.pendingBlockWrite)
	require.Empty(t, rejected, "a failed write must not classify an accepted block as rejected")
	require.Empty(t, cfg.chainTipRejections.reasons)
}

func TestChainTipListenerRetriesWriteWithoutAnotherBatch(t *testing.T) {
	cfg, graph, _, _, block, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	graph.insertOnAdd = true
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 2}
	cfg.indiciesDB = db
	peerDas := das_mock_services.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().BackfillCustodyColumns(block.Block.Slot, block.Block.Slot).Times(1)
	cfg.peerDas = peerDas
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{block}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = listenToIncomingBlocksUntilANewBlockIsReceived(ctx, log.Root(), cfg, Args{targetSlot: block.Block.Slot}, respCh, make(chan error))

	require.NoError(t, err)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		storedRoot, err := beacon_indicies.ReadBlockRootByStateRoot(tx, block.Block.StateRoot)
		require.NoError(t, err)
		require.Equal(t, common.Hash(root), storedRoot)
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot, "reaching the target in fork choice must not skip the failed write")
		require.Equal(t, block.Block.Slot, *slot)
		body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, root))
		require.NoError(t, err)
		require.NotEmpty(t, body)
		return nil
	}))
	require.Equal(t, 3, db.writes)
	require.Equal(t, 1, graph.added[root], "retry storage without importing the accepted block again")
}

func TestChainTipSyncRetriesWriteAfterStageCancellation(t *testing.T) {
	cfg, graph, _, _, block, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	graph.insertOnAdd = true
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 100}
	cfg.indiciesDB = db
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{block}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	db.onWriteError = cancel
	args := Args{targetSlot: block.Block.Slot, hasDownloaded: true, peers: 1}

	err = listenToIncomingBlocksUntilANewBlockIsReceived(ctx, log.Root(), cfg, args, respCh, make(chan error))

	require.ErrorIs(t, err, context.Canceled)
	args.seenSlot = cfg.forkChoice.HighestSeen()
	require.Equal(t, args.targetSlot, args.seenSlot)
	stage := ConsensusClStages().Stages[ChainTipSync]
	require.Equal(t, ChainTipSync, stage.TransitionFunc(cfg, args, err))

	db.failures = db.writes
	require.NoError(t, stage.ActionFunc(t.Context(), log.Root(), cfg, args))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot, "a new stage must finish the write even when fork choice reached the target")
		return nil
	}))
	require.Equal(t, ForkChoice, stage.TransitionFunc(cfg, args, nil))
	require.Equal(t, 1, graph.added[root])
}

func TestChainTipSyncRetriesWriteFromForwardSync(t *testing.T) {
	cfg, graph, block, engine := newForwardSyncPersistenceFixture(t)
	collector := &gloasCollectorTest{}
	cfg.blockCollector = collector
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 1}
	cfg.indiciesDB = db
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	progress, err := processDownloadedBlockBatches(t.Context(), log.Root(), cfg, block.Block.Slot-1, true, []*cltypes.SignedBeaconBlock{block}, nil)
	require.ErrorIs(t, err, db.err)
	require.ErrorIs(t, err, network2.ErrUnattributableProcess, "a local write failure must not ban the serving peer")
	require.Equal(t, block.Block.Slot-1, progress)
	require.Zero(t, collector.calls)
	args := Args{seenSlot: cfg.forkChoice.HighestSeen(), targetSlot: block.Block.Slot}
	require.Equal(t, args.targetSlot, args.seenSlot)
	require.Equal(t, ChainTipSync, ConsensusClStages().Stages[ForwardSync].TransitionFunc(cfg, args, ErrForwardSyncStale))
	unpublished := args
	unpublished.hasDownloaded, unpublished.headUnpublished = true, true
	unpublished.peers = 1
	unpublished.targetSlot++
	require.Equal(t, ChainTipSync, ConsensusClStages().Stages[ForwardSync].TransitionFunc(cfg, unpublished, err), "finish the pending write before publishing the accepted head")

	require.NoError(t, chainTipSync(t.Context(), log.Root(), cfg, args))
	require.Nil(t, cfg.pendingBlockWrite)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot, "the stage transition must preserve a failed forward-sync write")
		return nil
	}))
	require.Equal(t, 1, graph.added[root])
	require.Zero(t, engine.newPayloadCalls, "forward sync leaves execution insertion to the collector")
	require.Equal(t, 1, collector.calls, "the handoff must preserve execution insertion along with the database write")
	require.Equal(t, 1, collector.flushCalls)
}

func TestForwardSyncRetainsAcceptedCopyForExecutionInsertion(t *testing.T) {
	cfg, _, block, _ := newForwardSyncPersistenceFixture(t)
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), block, false, true, false))
	collector := &gloasCollectorTest{}
	cfg.blockCollector = collector
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 1}
	cfg.indiciesDB = db
	received := *block
	received.Signature[0] ^= 1

	_, err := processDownloadedBlockBatches(t.Context(), log.Root(), cfg, block.Block.Slot-1, true, []*cltypes.SignedBeaconBlock{&received}, nil)
	require.ErrorIs(t, err, db.err)
	require.NoError(t, retryPendingBlockWrite(t.Context(), cfg))
	require.Equal(t, 1, collector.calls)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		header, _, err := beacon_indicies.ReadSignedHeaderByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.Equal(t, block.Signature, header.Signature)
		return nil
	}))
}

type cancelingBlockCollector struct {
	gloasCollectorTest
	cancel context.CancelFunc
}

func (c *cancelingBlockCollector) AddBlock(block *cltypes.BeaconBlock) error {
	if c.cancel != nil {
		c.cancel()
	}
	return c.gloasCollectorTest.AddBlock(block)
}

func TestChainTipSyncRetainsForwardSyncInsertionAfterCollectorFailure(t *testing.T) {
	cfg, _, block, _ := newForwardSyncPersistenceFixture(t)
	db := &blockWriteErrorDB{RwDB: cfg.indiciesDB, err: errors.New("database write failed"), failures: 1}
	cfg.indiciesDB = db
	_, err := processDownloadedBlockBatches(t.Context(), log.Root(), cfg, block.Block.Slot-1, true, []*cltypes.SignedBeaconBlock{block}, nil)
	require.ErrorIs(t, err, db.err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	collector := &cancelingBlockCollector{
		gloasCollectorTest: gloasCollectorTest{err: errors.New("collector write failed")},
		cancel:             cancel,
	}
	cfg.blockCollector = collector
	args := Args{seenSlot: cfg.forkChoice.HighestSeen(), targetSlot: block.Block.Slot}
	err = chainTipSync(ctx, log.Root(), cfg, args)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, collector.calls)
	require.NotNil(t, cfg.pendingBlockWrite)
	require.Equal(t, ChainTipSync, ConsensusClStages().Stages[ChainTipSync].TransitionFunc(cfg, args, err))

	collector.cancel = nil
	collector.err = nil
	require.NoError(t, chainTipSync(t.Context(), log.Root(), cfg, args))
	require.Nil(t, cfg.pendingBlockWrite)
	require.Equal(t, 2, collector.calls)
	require.Equal(t, 1, collector.flushCalls)
}

func newForwardSyncPersistenceFixture(t *testing.T) (*Cfg, *chainTipBatchForkGraph, *cltypes.SignedBeaconBlock, *testExecutionEngine) {
	t.Helper()
	engine := &testExecutionEngine{supportInsertion: true, payloadStatus: execution_client.PayloadStatusValidated}
	anchorSlot := clparams.MainnetBeaconConfig.FuluForkEpoch * clparams.MainnetBeaconConfig.SlotsPerEpoch
	fixture := newLockOrderFixtureWithConfig(t, anchorSlot, engine, common.Hash{1}, false)
	cfg := fixture.cfg

	block := cltypes.NewSignedBeaconBlock(cfg.beaconCfg, clparams.FuluVersion)
	block.Block.Slot = fixture.headState.Slot() + 1
	block.Block.ParentRoot = fixture.headRoot
	block.Block.Body.ExecutionPayload.ParentHash = common.Hash{1}
	block.Block.Body.ExecutionPayload.BlockHash = common.Hash{2}
	block.Block.Body.ExecutionPayload.BlockNumber = 2
	postState, err := fixture.headState.Copy()
	require.NoError(t, err)
	require.NoError(t, postState.SetSlot(block.Block.Slot))
	checkpoint := cfg.forkChoice.FinalizedCheckpoint()
	postState.SetPreviousJustifiedCheckpoint(checkpoint)
	postState.SetCurrentJustifiedCheckpoint(checkpoint)
	postState.SetFinalizedCheckpoint(checkpoint)
	block.Block.StateRoot, err = postState.HashSSZ()
	require.NoError(t, err)

	baseGraph, err := fork_graph.NewForkGraphDisk(fixture.headState, fixture.syncedData, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := &chainTipBatchForkGraph{
		ForkGraph:   baseGraph,
		parents:     make(map[common.Hash]*cltypes.SignedBeaconBlock),
		parentState: postState,
		envelopes:   make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
		added:       make(map[common.Hash]int),
		insertOnAdd: true,
	}
	cfg.forkChoice, err = forkchoice.NewForkChoiceStore(
		cfg.ethClock, fixture.headState, engine, pool.NewOperationsPool(cfg.beaconCfg),
		graph, cfg.emitter, fixture.syncedData, nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(), validator_params.NewValidatorParams(), false, nil,
	)
	require.NoError(t, err)
	cfg.executionClient = engine
	cfg.indiciesDB = mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	cfg.forkChoice.OnTick((fixture.headState.GenesisTime() + block.Block.Slot*cfg.beaconCfg.SecondsPerSlot) * 1000)
	return cfg, graph, block, engine
}
