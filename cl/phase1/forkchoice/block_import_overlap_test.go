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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

type productionTestForkGraph interface {
	fork_graph.ForkGraph
	retainedBlockGuard
	ownedStateReplayer
}

type serialBlockImportForkGraph struct {
	productionTestForkGraph
}

type signalAdmissionChecksForkGraph struct {
	productionTestForkGraph
	checks chan struct{}
}

func (g *signalAdmissionChecksForkGraph) AnchorSlot() uint64 {
	g.checks <- struct{}{}
	return g.productionTestForkGraph.AnchorSlot()
}

func signalAdmissionChecks(test *overlapTestStore) chan struct{} {
	checks := make(chan struct{}, 8)
	test.store.mu.Lock()
	test.store.forkGraph = &signalAdmissionChecksForkGraph{productionTestForkGraph: test.store.forkGraph.(productionTestForkGraph), checks: checks}
	test.store.mu.Unlock()
	return checks
}

func (g serialBlockImportForkGraph) BeginChainSegment(*cltypes.SignedBeaconBlock, common.Hash, bool) (*fork_graph.ChainSegment, fork_graph.ChainSegmentInsertionResult, error) {
	return nil, fork_graph.ReplayRequired, nil
}

type verifiedBlockImportForkGraph struct {
	productionTestForkGraph
	root common.Hash
}

type overlapLightClientForkGraph struct {
	productionTestForkGraph
	update    *cltypes.LightClientUpdate
	commitErr error
	committed bool
}

func (g *overlapLightClientForkGraph) CommitChainSegment(segment *fork_graph.ChainSegment) (*state.CachingBeaconState, fork_graph.ChainSegmentInsertionResult, error) {
	if g.commitErr != nil {
		g.committed = true
		return nil, fork_graph.LogisticError, g.commitErr
	}
	postState, status, err := g.productionTestForkGraph.CommitChainSegment(segment)
	g.committed = true
	return postState, status, err
}

func (g *overlapLightClientForkGraph) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	if g.committed {
		return g.update
	}
	return nil
}

func (g *overlapLightClientForkGraph) NewestLightClientFinalityUpdate() *cltypes.LightClientUpdate {
	return g.NewestLightClientUpdate()
}

func (g verifiedBlockImportForkGraph) PayloadAccepted(root common.Hash) (bool, bool) {
	return root == g.root, root == g.root
}

func (g verifiedBlockImportForkGraph) HasEnvelope(root common.Hash) bool {
	return root == g.root
}

type overlapTestStore struct {
	store   *ForkChoiceStore
	graph   fork_graph.ForkGraph
	manager *synced_data.SyncedDataManager
	events  chan *beaconevents.EventStream
}

func newOverlapTestStore(t *testing.T, anchor *state.CachingBeaconState, engine execution_client.ExecutionEngine, serial bool) *overlapTestStore {
	t.Helper()
	cfg := anchor.BeaconConfig()
	manager := synced_data.NewSyncedDataManager(cfg, true)
	graph, err := fork_graph.NewForkGraphDisk(anchor, manager, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{Beacon: true})
	require.NoError(t, err)
	storeGraph := graph
	if serial {
		storeGraph = serialBlockImportForkGraph{productionTestForkGraph: graph.(productionTestForkGraph)}
	}
	store, err := NewForkChoiceStore(
		eth_clock.NewEthereumClock(anchor.GenesisTime(), anchor.GenesisValidatorsRoot(), cfg),
		anchor,
		engine,
		pool.NewOperationsPool(cfg),
		storeGraph,
		beaconevents.NewEventEmitter(),
		manager,
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)
	store.SetSynced(true)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(anchor, graph.AnchorRoot()))
	events := make(chan *beaconevents.EventStream, 1024)
	subscription := store.emitters.State().Subscribe(events)
	t.Cleanup(subscription.Unsubscribe)
	return &overlapTestStore{store: store, graph: graph, manager: manager, events: events}
}

func newReturningEngine(t testing.TB, status execution_client.PayloadStatus, retErr error) *execution_client.MockExecutionEngine {
	t.Helper()
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(status, retErr)
	return engine
}

func tickOverlapStore(test *overlapTestStore, block *cltypes.SignedBeaconBlock) {
	cfg := test.store.beaconCfg
	test.store.OnTick((test.store.genesisTime+block.Block.Slot*cfg.SecondsPerSlot)*1000 + 100)
}

func publishOverlapHead(t *testing.T, test *overlapTestStore, root common.Hash) {
	t.Helper()
	test.store.mu.RLock()
	current := test.graph.CurrentStateLocked(root)
	require.NotNil(t, current)
	err := test.manager.OnHeadStateWithBlockRoot(current, root)
	test.store.mu.RUnlock()
	require.NoError(t, err)
}

func importOverlapBlock(t *testing.T, test *overlapTestStore, block *cltypes.SignedBeaconBlock) common.Hash {
	return importOverlapBlockWithValidation(t, test, block, true)
}

func importOverlapBlockWithValidation(t *testing.T, test *overlapTestStore, block *cltypes.SignedBeaconBlock, fullValidation bool) common.Hash {
	t.Helper()
	tickOverlapStore(test, block)
	require.NoError(t, test.store.OnBlock(t.Context(), block, true, fullValidation, false))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	publishOverlapHead(t, test, root)
	return root
}

func importOverlapSetupBlock(t *testing.T, test *overlapTestStore, block *cltypes.SignedBeaconBlock) {
	t.Helper()
	tickOverlapStore(test, block)
	require.NoError(t, test.store.OnBlock(t.Context(), block, false, false, false))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	publishOverlapHead(t, test, root)
}

func newNonBoundaryOverlapTestStore(t *testing.T, blocks []*cltypes.SignedBeaconBlock, anchor *state.CachingBeaconState, engine execution_client.ExecutionEngine, serial bool) (*overlapTestStore, *cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	test := newOverlapTestStore(t, anchor, engine, serial)
	importOverlapSetupBlock(t, test, blocks[0])
	drainBlockImportEvents(t, test.events)
	setupRoot, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	block := blocks[1]
	require.Equal(t, blocks[0].Block.Slot/test.store.beaconCfg.SlotsPerEpoch, block.Block.Slot/test.store.beaconCfg.SlotsPerEpoch)
	tickOverlapStore(test, block)
	return test, block, setupRoot
}

func currentParentAttestingIndices(t *testing.T, test *overlapTestStore, block *cltypes.SignedBeaconBlock, attestation *solid.Attestation) []uint64 {
	t.Helper()
	test.store.mu.Lock()
	defer test.store.mu.Unlock()
	parent := test.graph.CurrentStateLocked(block.Block.ParentRoot)
	require.NotNil(t, parent)
	indices, err := parent.GetAttestingIndicies(attestation, true)
	require.NoError(t, err)
	return indices
}

func cloneOverlapFixture(t *testing.T, block *cltypes.SignedBeaconBlock) *cltypes.SignedBeaconBlock {
	t.Helper()
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	clone := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, block.Version())
	require.NoError(t, clone.DecodeSSZ(encoded, int(block.Version())))
	return clone
}

func assertOverlapBlockNotPublished(t *testing.T, test *overlapTestStore, block *cltypes.SignedBeaconBlock, newest *cltypes.LightClientUpdate) {
	t.Helper()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, hasHeader := test.graph.GetHeader(root)
	require.False(t, hasHeader)
	_, hasBlock := test.graph.GetBlock(root)
	require.False(t, hasBlock)
	_, hasRewards := test.graph.GetBlockRewards(root)
	require.False(t, hasRewards)
	_, hasBootstrap := test.graph.GetLightClientBootstrap(root)
	require.False(t, hasBootstrap)
	require.Same(t, newest, test.graph.NewestLightClientUpdate())
}

func assertForkChoiceLockFree(t *testing.T, store *ForkChoiceStore) {
	t.Helper()
	locked := make(chan struct{})
	go func() {
		store.mu.Lock()
		_ = store.highestSeen.Load()
		store.mu.Unlock()
		close(locked)
	}()
	awaitSignal(t, locked, "the fork-choice lock")
}

func assertOnBlockWaiting(t *testing.T, store *ForkChoiceStore, done <-chan error) {
	t.Helper()
	assertForkChoiceLockFree(t, store)
	select {
	case err := <-done:
		t.Fatalf("OnBlock completed before the open overlap: %v", err)
	default:
	}
}

func TestBlockImportOverlapRejectsCloneRootMismatch(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	wrongRoot := common.Hash(blockRoot)
	wrongRoot[0] ^= 0xff

	test.store.mu.Lock()
	overlap := test.store.startBlockImportOverlapLocked(block, wrongRoot, true)
	test.store.mu.Unlock()
	require.NotNil(t, overlap)
	overlap.join()
	require.ErrorContains(t, overlap.err, "cloned block root mismatch")
	require.Nil(t, overlap.segment.PostState())
	require.False(t, test.graph.IsBlockInvalid(wrongRoot))
	require.False(t, test.graph.IsBlockInvalid(blockRoot))
}

func TestBlockImportOverlapPreparesDuringNewPayload(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	test, block, setupRoot := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	prepared := make(chan struct{})
	test.store.blockImportPrepareHook = func() { close(prepared) }
	before, err := captureBlockImportSnapshot(test.store, []common.Hash{test.graph.AnchorRoot(), setupRoot}, []uint64{state.Epoch(anchor)}, setupRoot, drainBlockImportEvents(t, test.events))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- test.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, elEntered, "NewPayload")
	awaitSignal(t, prepared, "post-state prepare while NewPayload is blocked")

	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	after, err := captureBlockImportSnapshot(test.store, []common.Hash{test.graph.AnchorRoot(), setupRoot}, []uint64{state.Epoch(anchor)}, setupRoot, drainBlockImportEvents(t, test.events))
	require.NoError(t, err)
	before.CurrentStateRoot = nil
	require.Equal(t, before, after)
	assertOverlapBlockNotPublished(t, test, block, nil)
	require.Nil(t, test.graph.CurrentStateLocked(block.Block.ParentRoot))
	require.Empty(t, test.events)

	close(releaseEL)
	require.NoError(t, <-done)
	_, ok := test.graph.GetHeader(root)
	require.True(t, ok)

	serialBlocks, serialAnchor, _ := tests.GetBellatrixRandom()
	serial, serialBlock, serialSetupRoot := newNonBoundaryOverlapTestStore(t, serialBlocks, serialAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), true)
	serialRoot := importOverlapBlock(t, serial, serialBlock)
	gotSnapshot, err := captureBlockImportSnapshot(test.store, []common.Hash{test.graph.AnchorRoot(), setupRoot, root}, []uint64{state.Epoch(anchor), block.Block.Slot / test.store.beaconCfg.SlotsPerEpoch}, root, drainBlockImportEvents(t, test.events))
	require.NoError(t, err)
	wantSnapshot, err := captureBlockImportSnapshot(serial.store, []common.Hash{serial.graph.AnchorRoot(), serialSetupRoot, serialRoot}, []uint64{state.Epoch(serialAnchor), serialBlock.Block.Slot / serial.store.beaconCfg.SlotsPerEpoch}, serialRoot, drainBlockImportEvents(t, serial.events))
	require.NoError(t, err)
	require.Equal(t, wantSnapshot, gotSnapshot)
}

func TestBlockImportOverlapQueuesLightClientEventsOnCommit(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		commitErr error
	}{
		{name: "success"},
		{name: "commit error", commitErr: errors.New("commit failed")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			blocks, anchor, _ := tests.GetBellatrixRandom()
			test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
			graph := &overlapLightClientForkGraph{
				productionTestForkGraph: test.graph.(productionTestForkGraph),
				update:                  cltypes.NewLightClientUpdate(clparams.BellatrixVersion, &clparams.MainnetBeaconConfig),
				commitErr:               testCase.commitErr,
			}
			test.store.forkGraph = graph

			err := test.store.OnBlock(t.Context(), block, true, true, false)
			if testCase.commitErr != nil {
				require.ErrorIs(t, err, testCase.commitErr)
			} else {
				require.NoError(t, err)
			}
			require.True(t, graph.committed)

			events := drainBlockImportEvents(t, test.events)
			finalityEvents := 0
			optimisticEvents := 0
			for _, event := range events {
				switch event.Type {
				case beaconevents.StateLightClientFinalityUpdate:
					finalityEvents++
				case beaconevents.StateLightClientOptimisticUpdate:
					optimisticEvents++
				}
			}
			if testCase.commitErr == nil {
				require.Equal(t, 1, finalityEvents)
				require.Equal(t, 1, optimisticEvents)
			} else {
				require.Zero(t, finalityEvents)
				require.Zero(t, optimisticEvents)
			}
		})
	}
}

func TestBlockImportOverlapMatchesSerial(t *testing.T) {
	fixtures := []struct {
		name string
		load func() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState)
	}{
		{name: "bellatrix", load: tests.GetBellatrixRandom},
		{name: "capella", load: tests.GetCapellaRandom},
		{name: "electra", load: tests.GetElectraRandom},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			serialBlocks, serialAnchor, _ := fixture.load()
			overlapBlocks, overlapAnchor, _ := fixture.load()
			fullValidation := true
			if fixture.name == "electra" {
				fullValidation = false
				for _, blocks := range [][]*cltypes.SignedBeaconBlock{serialBlocks, overlapBlocks} {
					for index, block := range blocks {
						if index > 0 {
							parentRoot, err := blocks[index-1].Block.HashSSZ()
							require.NoError(t, err)
							block.Block.ParentRoot = parentRoot
						}
						block.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
						block.Block.Body.ExecutionPayload.BlobGasUsed = 0
						block.Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)
					}
				}
			}
			serial := newOverlapTestStore(t, serialAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), true)
			overlap := newOverlapTestStore(t, overlapAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
			var hookCount atomic.Int64
			overlap.store.blockImportPrepareHook = func() { hookCount.Add(1) }
			serialRoots := []common.Hash{serial.graph.AnchorRoot()}
			overlapRoots := []common.Hash{overlap.graph.AnchorRoot()}
			epochs := []uint64{state.Epoch(serialAnchor)}
			epochStarts := 0
			epochBoundaryBlocks := 0
			parentSlot := overlapAnchor.Slot()
			for index := range serialBlocks {
				serialBlock := serialBlocks[index]
				overlapBlock := overlapBlocks[index]
				if overlapBlock.Block.Slot/overlap.store.beaconCfg.SlotsPerEpoch > parentSlot/overlap.store.beaconCfg.SlotsPerEpoch {
					epochBoundaryBlocks++
				}
				if serialBlock.Block.Slot%serial.store.beaconCfg.SlotsPerEpoch == 0 {
					if epochStarts == 0 {
						tickOverlapStore(serial, serialBlock)
						tickOverlapStore(overlap, overlapBlock)
						require.NoError(t, serial.store.precomputeNextEpochState(t.Context(), serialBlock.Block.Slot))
						require.NoError(t, overlap.store.precomputeNextEpochState(t.Context(), overlapBlock.Block.Slot))
					}
					epochStarts++
				}
				serialRoot := importOverlapBlockWithValidation(t, serial, serialBlock, fullValidation)
				overlapRoot := importOverlapBlockWithValidation(t, overlap, overlapBlock, fullValidation)
				serialRoots = append(serialRoots, serialRoot)
				overlapRoots = append(overlapRoots, overlapRoot)
				epoch := serialBlock.Block.Slot / serial.store.beaconCfg.SlotsPerEpoch
				if epochs[len(epochs)-1] != epoch {
					epochs = append(epochs, epoch)
				}
				want, err := captureBlockImportSnapshot(serial.store, serialRoots, epochs, serialRoot, drainBlockImportEvents(t, serial.events))
				require.NoError(t, err)
				got, err := captureBlockImportSnapshot(overlap.store, overlapRoots, epochs, overlapRoot, drainBlockImportEvents(t, overlap.events))
				require.NoError(t, err)
				require.Equal(t, want, got, "slot %d", serialBlock.Block.Slot)
				assertOverlapForkGraphArtifactsEqual(t, serial, overlap, serialRoot, overlapRoot, serialBlock.Block.Slot)
				require.NotNil(t, overlap.graph.CurrentStateLocked(overlapRoot))
				parentSlot = overlapBlock.Block.Slot
			}
			require.Equal(t, int64(len(overlapBlocks)-epochBoundaryBlocks), hookCount.Load())
			if fixture.name != "electra" {
				require.Positive(t, epochStarts, "fixture must cover an epoch-first block")
			}
			t.Logf("%s covers %d blocks and %d epoch-first blocks", fixture.name, len(overlapBlocks), epochStarts)
		})
	}
}

func assertOverlapForkGraphArtifactsEqual(t *testing.T, serial, overlap *overlapTestStore, serialRoot, overlapRoot common.Hash, slot uint64) {
	t.Helper()
	serialHeader, serialHasHeader := serial.graph.GetHeader(serialRoot)
	overlapHeader, overlapHasHeader := overlap.graph.GetHeader(overlapRoot)
	require.Equal(t, serialHasHeader, overlapHasHeader)
	require.Equal(t, serialHeader, overlapHeader)
	serialBlock, serialHasBlock := serial.graph.GetBlock(serialRoot)
	overlapBlock, overlapHasBlock := overlap.graph.GetBlock(overlapRoot)
	require.Equal(t, serialHasBlock, overlapHasBlock)
	if serialHasBlock {
		serialStoredRoot, err := serialBlock.Block.HashSSZ()
		require.NoError(t, err)
		overlapStoredRoot, err := overlapBlock.Block.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, serialStoredRoot, overlapStoredRoot)
	}
	serialRewards, serialHasRewards := serial.graph.GetBlockRewards(serialRoot)
	overlapRewards, overlapHasRewards := overlap.graph.GetBlockRewards(overlapRoot)
	require.Equal(t, serialHasRewards, overlapHasRewards)
	require.Equal(t, serialRewards, overlapRewards)
	serialBootstrap, serialHasBootstrap := serial.graph.GetLightClientBootstrap(serialRoot)
	overlapBootstrap, overlapHasBootstrap := overlap.graph.GetLightClientBootstrap(overlapRoot)
	require.Equal(t, serialHasBootstrap, overlapHasBootstrap)
	require.Equal(t, serialBootstrap, overlapBootstrap)
	require.Equal(t, serial.graph.NewestLightClientUpdate(), overlap.graph.NewestLightClientUpdate())
	require.Equal(t, serial.graph.NewestLightClientFinalityUpdate(), overlap.graph.NewestLightClientFinalityUpdate())
	period := serial.store.beaconCfg.SyncCommitteePeriod(slot)
	require.Equal(t, serial.graph.GetLightClientUpdates(period, period+1, serialRoot), overlap.graph.GetLightClientUpdates(period, period+1, overlapRoot))
	serialCurrent, serialPrevious, serialHasCommittees := serial.graph.GetSyncCommittees(period)
	overlapCurrent, overlapPrevious, overlapHasCommittees := overlap.graph.GetSyncCommittees(period)
	require.Equal(t, serialHasCommittees, overlapHasCommittees)
	require.Equal(t, serialCurrent, overlapCurrent)
	require.Equal(t, serialPrevious, overlapPrevious)
	epoch := slot / serial.store.beaconCfg.SlotsPerEpoch
	serialParticipation, serialErr := serial.graph.GetCurrentParticipationIndicies(epoch)
	overlapParticipation, overlapErr := overlap.graph.GetCurrentParticipationIndicies(epoch)
	require.Equal(t, serialErr, overlapErr)
	require.Equal(t, serialParticipation, overlapParticipation)
	serialParticipation, serialErr = serial.graph.GetPreviousParticipationIndicies(epoch)
	overlapParticipation, overlapErr = overlap.graph.GetPreviousParticipationIndicies(epoch)
	require.Equal(t, serialErr, overlapErr)
	require.Equal(t, serialParticipation, overlapParticipation)
}

func TestBlockImportOverlapPriorEpochBlockUsesCommitTime(t *testing.T) {
	const targetIndex = 30
	overlapBlocks, overlapAnchor, _ := tests.GetBellatrixRandom()
	serialBlocks, serialAnchor, _ := tests.GetBellatrixRandom()
	overlap := newOverlapTestStore(t, overlapAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	serial := newOverlapTestStore(t, serialAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), true)
	overlapRoots := []common.Hash{overlap.graph.AnchorRoot()}
	serialRoots := []common.Hash{serial.graph.AnchorRoot()}
	for index := range targetIndex {
		importOverlapSetupBlock(t, overlap, overlapBlocks[index])
		importOverlapSetupBlock(t, serial, serialBlocks[index])
	}

	block := overlapBlocks[targetIndex]
	serialBlock := serialBlocks[targetIndex]
	lastSlot := overlap.store.beaconCfg.SlotsPerEpoch - 1
	require.Equal(t, lastSlot, block.Block.Slot%overlap.store.beaconCfg.SlotsPerEpoch)
	engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	overlap.store.engine = engine
	tickOverlapStore(overlap, block)
	var hookCount atomic.Int64
	prepared := make(chan struct{})
	overlap.store.blockImportPrepareHook = func() {
		hookCount.Add(1)
		close(prepared)
	}
	done := make(chan error, 1)
	go func() { done <- overlap.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	awaitSignal(t, prepared, "prepare")

	nextSlot := block.Block.Slot + 1
	require.Zero(t, nextSlot%overlap.store.beaconCfg.SlotsPerEpoch)
	overlap.store.OnTick((overlap.store.genesisTime+nextSlot*overlap.store.beaconCfg.SecondsPerSlot)*1000 + 100)
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, int64(1), hookCount.Load())
	overlapRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	overlapRoots = append(overlapRoots, overlapRoot)
	publishOverlapHead(t, overlap, overlapRoot)

	serial.store.OnTick((serial.store.genesisTime+nextSlot*serial.store.beaconCfg.SecondsPerSlot)*1000 + 100)
	require.NoError(t, serial.store.OnBlock(t.Context(), serialBlock, true, true, false))
	serialRoot, err := serialBlock.Block.HashSSZ()
	require.NoError(t, err)
	serialRoots = append(serialRoots, serialRoot)
	publishOverlapHead(t, serial, serialRoot)

	epochs := []uint64{state.Epoch(overlapAnchor)}
	got, err := captureBlockImportSnapshot(overlap.store, overlapRoots, epochs, overlapRoot, drainBlockImportEvents(t, overlap.events))
	require.NoError(t, err)
	want, err := captureBlockImportSnapshot(serial.store, serialRoots, epochs, serialRoot, drainBlockImportEvents(t, serial.events))
	require.NoError(t, err)
	require.Equal(t, want, got)
	assertOverlapForkGraphArtifactsEqual(t, serial, overlap, serialRoot, overlapRoot, block.Block.Slot)
}

func TestBlockImportOverlapSkipsPrecomputedEpochState(t *testing.T) {
	const targetIndex = 32
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, entered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	released := false
	defer func() {
		if !released {
			close(releaseEL)
		}
	}()
	test := newOverlapTestStore(t, anchor, engine, false)
	for index := range targetIndex {
		importOverlapSetupBlock(t, test, blocks[index])
	}
	block := blocks[targetIndex]
	test.store.mu.RLock()
	parentState := test.graph.CurrentStateLocked(block.Block.ParentRoot)
	require.NotNil(t, parentState)
	parentSlot := parentState.Slot()
	precomputed, err := parentState.Copy()
	test.store.mu.RUnlock()
	require.NoError(t, err)
	require.Equal(t, parentSlot/test.store.beaconCfg.SlotsPerEpoch, block.Block.Slot/test.store.beaconCfg.SlotsPerEpoch)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(precomputed, block.Block.Slot))
	_, err = precomputed.HashSSZ()
	require.NoError(t, err)
	tickOverlapStore(test, block)
	test.graph.SetNextEpochState(block.Block.ParentRoot, precomputed)
	var hookCount atomic.Int64
	test.store.blockImportPrepareHook = func() { hookCount.Add(1) }
	skipCounter := blockImportOverlapCounter.WithLabelValues("skipped_precomputed_epoch_state")
	skipBefore := skipCounter.GetValue()
	done := make(chan error, 1)
	go func() { done <- test.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	test.store.mu.RLock()
	parentCurrentDuringEL := test.graph.CurrentStateLocked(block.Block.ParentRoot) != nil
	test.store.mu.RUnlock()
	close(releaseEL)
	released = true
	require.NoError(t, awaitOnBlock(t, done))

	require.True(t, parentCurrentDuringEL)
	require.Zero(t, hookCount.Load())
	require.Equal(t, 1.0, skipCounter.GetValue()-skipBefore)
}

func TestBlockImportOverlapEpochBoundaryAcceptsPrecomputeDuringEL(t *testing.T) {
	const targetIndex = 31
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, entered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusInvalidated, errors.New("invalid payload"))
	released := false
	defer func() {
		if !released {
			close(releaseEL)
		}
	}()
	test := newOverlapTestStore(t, anchor, engine, false)
	for index := range targetIndex {
		importOverlapSetupBlock(t, test, blocks[index])
	}
	block := blocks[targetIndex]
	invalidSibling := cloneOverlapFixture(t, block)
	invalidSibling.Block.Body.ExecutionPayload.BlockHash[0] ^= 1
	require.Zero(t, block.Block.Slot%test.store.beaconCfg.SlotsPerEpoch)
	tickOverlapStore(test, block)
	var hookCount atomic.Int64
	test.store.blockImportPrepareHook = func() { hookCount.Add(1) }
	outcomes := metrics.GetOrCreateCounterVec("caplin_next_epoch_state_outcomes_total", []string{"outcome"})
	hitBefore := outcomes.WithLabelValues("hit").GetValue()
	lateBefore := outcomes.WithLabelValues("late").GetValue()
	skipCounter := blockImportOverlapCounter.WithLabelValues("skipped_epoch_boundary")
	skipBefore := skipCounter.GetValue()
	newest := test.graph.NewestLightClientUpdate()
	done := make(chan error, 1)
	go func() { done <- test.store.OnBlock(t.Context(), invalidSibling, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	require.NoError(t, test.store.precomputeNextEpochState(t.Context(), block.Block.Slot))
	close(releaseEL)
	released = true
	require.ErrorIs(t, awaitOnBlock(t, done), ErrBlockInvalid)
	assertOverlapBlockNotPublished(t, test, invalidSibling, newest)

	test.store.engine = newReturningEngine(t, execution_client.PayloadStatusValidated, nil)
	require.NoError(t, test.store.OnBlock(t.Context(), block, true, true, false))
	require.Zero(t, outcomes.WithLabelValues("late").GetValue()-lateBefore)
	require.Equal(t, 1.0, outcomes.WithLabelValues("hit").GetValue()-hitBefore)
	require.Zero(t, hookCount.Load())
	require.Equal(t, 2.0, skipCounter.GetValue()-skipBefore)
}

func TestBlockImportOverlapRetryPreservesPrecomputedEpochState(t *testing.T) {
	const targetIndex = 31
	overlapBlocks, overlapAnchor, _ := tests.GetBellatrixRandom()
	serialBlocks, serialAnchor, _ := tests.GetBellatrixRandom()
	overlap := newOverlapTestStore(t, overlapAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	serial := newOverlapTestStore(t, serialAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), true)
	overlapRoots := []common.Hash{overlap.graph.AnchorRoot()}
	serialRoots := []common.Hash{serial.graph.AnchorRoot()}
	for index := range targetIndex {
		importOverlapSetupBlock(t, overlap, overlapBlocks[index])
		importOverlapSetupBlock(t, serial, serialBlocks[index])
	}

	block := overlapBlocks[targetIndex]
	serialBlock := serialBlocks[targetIndex]
	invalidSibling := cloneOverlapFixture(t, block)
	invalidSibling.Block.Body.ExecutionPayload.BlockHash[0] ^= 1
	require.Zero(t, block.Block.Slot%overlap.store.beaconCfg.SlotsPerEpoch)
	tickOverlapStore(overlap, block)
	tickOverlapStore(serial, serialBlock)
	require.NoError(t, overlap.store.precomputeNextEpochState(t.Context(), block.Block.Slot))
	require.NoError(t, serial.store.precomputeNextEpochState(t.Context(), serialBlock.Block.Slot))
	var hookCount atomic.Int64
	overlap.store.blockImportPrepareHook = func() { hookCount.Add(1) }
	outcomes := metrics.GetOrCreateCounterVec("caplin_next_epoch_state_outcomes_total", []string{"outcome"})
	hitBefore := outcomes.WithLabelValues("hit").GetValue()
	lateBefore := outcomes.WithLabelValues("late").GetValue()
	overlap.store.engine = newReturningEngine(t, execution_client.PayloadStatusInvalidated, errors.New("invalid payload"))
	require.ErrorIs(t, overlap.store.OnBlock(t.Context(), invalidSibling, true, true, false), ErrBlockInvalid)
	hitAfterInvalid := outcomes.WithLabelValues("hit").GetValue()
	overlap.store.mu.RLock()
	parentCurrentAfterInvalid := overlap.graph.CurrentStateLocked(block.Block.ParentRoot) != nil
	overlap.store.mu.RUnlock()
	assertOverlapBlockNotPublished(t, overlap, invalidSibling, overlap.graph.NewestLightClientUpdate())

	overlap.store.engine = newReturningEngine(t, execution_client.PayloadStatusValidated, nil)
	require.NoError(t, overlap.store.OnBlock(t.Context(), block, true, true, false))
	require.Equal(t, 1.0, outcomes.WithLabelValues("hit").GetValue()-hitAfterInvalid)
	require.Zero(t, hitAfterInvalid-hitBefore)
	require.Zero(t, outcomes.WithLabelValues("late").GetValue()-lateBefore)
	require.True(t, parentCurrentAfterInvalid)
	require.Zero(t, hookCount.Load())
	overlapRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	overlapRoots = append(overlapRoots, overlapRoot)
	publishOverlapHead(t, overlap, overlapRoot)

	require.NoError(t, serial.store.OnBlock(t.Context(), serialBlock, true, true, false))
	serialRoot, err := serialBlock.Block.HashSSZ()
	require.NoError(t, err)
	serialRoots = append(serialRoots, serialRoot)
	publishOverlapHead(t, serial, serialRoot)

	epochs := []uint64{state.Epoch(overlapAnchor), block.Block.Slot / overlap.store.beaconCfg.SlotsPerEpoch}
	got, err := captureBlockImportSnapshot(overlap.store, overlapRoots, epochs, overlapRoot, drainBlockImportEvents(t, overlap.events))
	require.NoError(t, err)
	want, err := captureBlockImportSnapshot(serial.store, serialRoots, epochs, serialRoot, drainBlockImportEvents(t, serial.events))
	require.NoError(t, err)
	require.Equal(t, want, got)
	assertOverlapForkGraphArtifactsEqual(t, serial, overlap, serialRoot, overlapRoot, block.Block.Slot)
}

func TestBlockImportOverlapDiscardPaths(t *testing.T) {
	testsCases := []struct {
		name      string
		status    execution_client.PayloadStatus
		engineErr error
		committed bool
	}{
		{name: "invalid", status: execution_client.PayloadStatusInvalidated, engineErr: errors.New("invalid payload")},
		{name: "none", status: execution_client.PayloadStatusNone, engineErr: errors.New("EL unavailable")},
		{name: "accepted", status: execution_client.PayloadStatusNotValidated, committed: true},
	}
	for _, testCase := range testsCases {
		t.Run(testCase.name, func(t *testing.T) {
			blocks, anchor, _ := tests.GetBellatrixRandom()
			engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
			engine.EXPECT().NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(testCase.status, testCase.engineErr)
			test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
			if testCase.status == execution_client.PayloadStatusInvalidated {
				blockHash, err := block.Block.Body.ExecutionPayload.ComputeBlockHash(&block.Block.ParentRoot, common.Hash{}, nil)
				require.NoError(t, err)
				block.Block.Body.ExecutionPayload.BlockHash = blockHash
			}
			var joined atomic.Bool
			test.store.blockImportPrepareHook = func() { joined.Store(true) }
			newest := test.graph.NewestLightClientUpdate()
			err := test.store.OnBlock(t.Context(), block, true, true, false)
			require.True(t, joined.Load())
			assertForkChoiceLockFree(t, test.store)
			if testCase.committed {
				require.NoError(t, err)
				root, hashErr := block.Block.HashSSZ()
				require.NoError(t, hashErr)
				_, ok := test.graph.GetHeader(root)
				require.True(t, ok)
				require.True(t, test.store.optimisticStore.IsOptimistic(root))
				return
			}
			require.Error(t, err)
			assertOverlapBlockNotPublished(t, test, block, newest)
			if testCase.status == execution_client.PayloadStatusInvalidated {
				require.Error(t, test.store.OnBlock(t.Context(), block, true, true, false))
				return
			}
			test.store.engine = newReturningEngine(t, execution_client.PayloadStatusValidated, nil)
			require.NoError(t, test.store.OnBlock(t.Context(), block, true, true, false))
		})
	}
}

// importSameRootDuplicateDuringOverlap imports a mutated copy of the overlapped block while the overlap waits in prepare.
func importSameRootDuplicateDuringOverlap(t *testing.T, name string, mutate func(*cltypes.SignedBeaconBlock)) (*overlapTestStore, *cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, entered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	prepareEntered := make(chan struct{})
	releasePrepare := make(chan struct{})
	test.store.blockImportPrepareHook = func() {
		close(prepareEntered)
		<-releasePrepare
	}
	legitDone := make(chan error, 1)
	go func() { legitDone <- test.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	awaitSignal(t, prepareEntered, "prepare")

	duplicate := cloneOverlapFixture(t, block)
	mutate(duplicate)
	checks := signalAdmissionChecks(test)
	duplicateDone := make(chan error, 1)
	go func() { duplicateDone <- test.store.OnBlock(t.Context(), duplicate, true, true, false) }()
	awaitSignal(t, checks, name+" admission")
	close(releaseEL)
	awaitSignal(t, checks, name+" admission after payload validation")
	awaitSignal(t, checks, name+" post-payload admission")
	assertOnBlockWaiting(t, test.store, duplicateDone)
	close(releasePrepare)
	require.NoError(t, <-legitDone)
	require.NoError(t, <-duplicateDone)

	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, hasHeader := test.graph.GetHeader(root)
	require.True(t, hasHeader)
	_, hasBlock := test.graph.GetBlock(root)
	require.True(t, hasBlock)
	require.Len(t, drainBlockImportEvents(t, test.events), 1)
	return test, block, root
}

func TestBlockImportOverlapSameRootForgedDuplicateWaits(t *testing.T) {
	test, block, root := importSameRootDuplicateDuringOverlap(t, "forged duplicate", func(forged *cltypes.SignedBeaconBlock) {
		forged.Signature[5] ^= 0xff
	})
	require.False(t, test.graph.IsBlockInvalid(root))
	stored, _ := test.graph.GetBlock(root)
	require.Equal(t, block.Signature, stored.Signature)
}

func TestBlockImportOverlapSameRootValidDuplicateWaits(t *testing.T) {
	importSameRootDuplicateDuringOverlap(t, "valid duplicate", func(*cltypes.SignedBeaconBlock) {})
}

func TestBlockImportOverlapAbandonReleasesSameRootWaiter(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, entered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusNone, errors.New("EL unavailable"))
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	prepared := make(chan struct{})
	test.store.blockImportPrepareHook = func() { close(prepared) }
	overlapDone := make(chan error, 1)
	go func() { overlapDone <- test.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	awaitSignal(t, prepared, "prepare")

	checks := signalAdmissionChecks(test)
	duplicateDone := make(chan error, 1)
	go func() { duplicateDone <- test.store.OnBlock(t.Context(), block, false, true, false) }()
	awaitSignal(t, checks, "duplicate admission")
	assertOnBlockWaiting(t, test.store, duplicateDone)
	close(releaseEL)
	require.ErrorIs(t, <-overlapDone, ErrNewPayloadNoStatus)
	require.NoError(t, <-duplicateDone)

	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, hasHeader := test.graph.GetHeader(root)
	require.True(t, hasHeader)
	_, hasBlock := test.graph.GetBlock(root)
	require.True(t, hasBlock)
	require.Len(t, drainBlockImportEvents(t, test.events), 1)
}

func TestBlockImportOverlapWaiterInvalidatesCachedHead(t *testing.T) {
	blocks, anchor, _ := tests.GetCapellaRandom()
	block := cloneOverlapFixture(t, blocks[0])
	block.Block.Body.Attestations = solid.NewDynamicListSSZ[*solid.Attestation](int(anchor.BeaconConfig().MaxAttestations))
	test := newOverlapTestStore(t, anchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	tickOverlapStore(test, block)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	root := common.Hash(blockRoot)
	overlap := &blockImportOverlap{settled: make(chan struct{})}
	test.store.mu.Lock()
	test.store.blockImportsInFlight = map[common.Hash]chan struct{}{root: overlap.settled}
	test.store.mu.Unlock()
	t.Cleanup(func() {
		test.store.mu.Lock()
		test.store.settleBlockImportOverlapLocked(root, overlap)
		test.store.mu.Unlock()
	})

	checks := signalAdmissionChecks(test)
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- test.store.OnBlock(t.Context(), block, false, false, false) }()
	awaitSignal(t, checks, "waiter admission")
	assertOnBlockWaiting(t, test.store, waiterDone)

	test.store.mu.Lock()
	test.store.settleBlockImportOverlapLocked(root, overlap)
	test.store.headHash = test.graph.AnchorRoot()
	test.store.mu.Unlock()
	require.NoError(t, awaitOnBlock(t, waiterDone))

	head, _, err := test.store.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, root, head)
}

func TestBlockImportOverlapCancelledWaiterReturnsBeforeOwner(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, entered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	released := false
	defer func() {
		if !released {
			close(releaseEL)
		}
	}()
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	prepared := make(chan struct{})
	test.store.blockImportPrepareHook = func() { close(prepared) }
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- test.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	awaitSignal(t, prepared, "prepare")

	checks := signalAdmissionChecks(test)
	waiterCtx, cancel := context.WithCancel(t.Context())
	waiterBlock := cloneOverlapFixture(t, block)
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- test.store.OnBlock(waiterCtx, waiterBlock, false, true, false) }()
	awaitSignal(t, checks, "waiter admission")
	assertOnBlockWaiting(t, test.store, waiterDone)
	cancel()
	waiterErr := awaitOnBlock(t, waiterDone)
	require.ErrorIs(t, waiterErr, ErrPayloadValidationInterrupted)
	require.ErrorIs(t, waiterErr, context.Canceled)
	select {
	case err := <-ownerDone:
		t.Fatalf("owner completed before NewPayload was released: %v", err)
	default:
	}

	close(releaseEL)
	released = true
	require.NoError(t, <-ownerDone)
}

func TestBlockImportOverlapCancellationJoinsPrepare(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusNone, context.Canceled)
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	prepared := make(chan struct{})
	test.store.blockImportPrepareHook = func() { close(prepared) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- test.store.OnBlock(ctx, block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	awaitSignal(t, prepared, "prepare")
	cancel()
	close(release)
	require.ErrorIs(t, <-done, ErrPayloadValidationInterrupted)
	assertOverlapBlockNotPublished(t, test, block, nil)
	assertForkChoiceLockFree(t, test.store)
	test.store.engine = newReturningEngine(t, execution_client.PayloadStatusValidated, nil)
	require.NoError(t, test.store.OnBlock(t.Context(), block, true, true, false))
}

func TestBlockImportOverlapAdmissionRecheckDropsSegment(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	finalizedBefore := test.store.finalizedCheckpoint.Load()
	prepared := make(chan struct{})
	test.store.blockImportPrepareHook = func() { close(prepared) }
	done := make(chan error, 1)
	go func() { done <- test.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	awaitSignal(t, prepared, "prepare")
	test.store.mu.Lock()
	test.store.finalizedCheckpoint.Store(solid.Checkpoint{
		Epoch: block.Block.Slot/test.store.beaconCfg.SlotsPerEpoch + 1,
		Root:  test.graph.AnchorRoot(),
	})
	test.store.mu.Unlock()
	close(release)
	require.NoError(t, <-done)
	assertOverlapBlockNotPublished(t, test, block, nil)
	assertForkChoiceLockFree(t, test.store)
	test.store.finalizedCheckpoint.Store(finalizedBefore)
	require.NoError(t, test.store.OnBlock(t.Context(), block, true, true, false))
}

func TestBlockImportOverlapCLInvalidAndPreparePanic(t *testing.T) {
	t.Run("CL invalid", func(t *testing.T) {
		blocks, anchor, _ := tests.GetBellatrixRandom()
		test, validBlock, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
		block := cloneOverlapFixture(t, validBlock)
		block.Block.ProposerIndex = 1 << 30
		err := test.store.OnBlock(t.Context(), block, true, true, false)
		require.ErrorIs(t, err, ErrBlockInvalid)
		root, hashErr := block.Block.HashSSZ()
		require.NoError(t, hashErr)
		require.True(t, test.graph.IsBlockInvalid(root))
		assertOverlapBlockNotPublished(t, test, block, nil)
		assertForkChoiceLockFree(t, test.store)
		require.NoError(t, test.store.OnBlock(t.Context(), validBlock, true, true, false))
	})

	t.Run("prepare panic", func(t *testing.T) {
		blocks, anchor, _ := tests.GetBellatrixRandom()
		test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
		test.store.blockImportPrepareHook = func() { panic("prepare hook panic") }
		err := test.store.OnBlock(t.Context(), block, true, true, false)
		require.ErrorContains(t, err, "prepare hook panic")
		root, hashErr := block.Block.HashSSZ()
		require.NoError(t, hashErr)
		require.False(t, test.graph.IsBlockInvalid(root))
		assertOverlapBlockNotPublished(t, test, block, nil)
		assertForkChoiceLockFree(t, test.store)
		test.store.blockImportPrepareHook = nil
		require.NoError(t, test.store.OnBlock(t.Context(), block, true, true, false))
	})
}

func TestBlockImportOverlapBadProposerSignatureKeepsRootValid(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	test, validBlock, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	forged := cloneOverlapFixture(t, validBlock)
	forged.Signature[5] ^= 0xff
	var prepared bool
	test.store.blockImportPrepareHook = func() { prepared = true }

	err := test.store.OnBlock(t.Context(), forged, true, true, false)
	require.ErrorIs(t, err, ErrBlockInvalid)
	require.ErrorIs(t, err, eth2.ErrInvalidBlockSignature)
	require.True(t, prepared)
	root, err := validBlock.Block.HashSSZ()
	require.NoError(t, err)
	require.False(t, test.graph.IsBlockInvalid(root))
	assertOverlapBlockNotPublished(t, test, forged, nil)

	require.NoError(t, test.store.OnBlock(t.Context(), validBlock, true, true, false))
	require.False(t, test.graph.IsBlockInvalid(root))
	_, hasHeader := test.graph.GetHeader(root)
	require.True(t, hasHeader)
}

func TestBlockImportOverlapELPanicJoinsAndRelocks(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			panic("EL panic")
		},
	)
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	var joined atomic.Bool
	test.store.blockImportPrepareHook = func() { joined.Store(true) }
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_ = test.store.OnBlock(t.Context(), block, true, true, false)
	}()
	require.Equal(t, "EL panic", <-panicked)
	require.True(t, joined.Load())
	assertForkChoiceLockFree(t, test.store)
	assertOverlapBlockNotPublished(t, test, block, nil)
	test.store.engine = newReturningEngine(t, execution_client.PayloadStatusValidated, nil)
	require.NoError(t, test.store.OnBlock(t.Context(), block, true, true, false))
}

func TestBlockImportOverlapReadersUseSyncedHead(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusNone, errors.New("EL unavailable"))
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	prepared := make(chan struct{})
	test.store.blockImportPrepareHook = func() { close(prepared) }
	done := make(chan error, 1)
	go func() { done <- test.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	awaitSignal(t, prepared, "prepare")
	parentRoot := block.Block.ParentRoot
	wantRoot := blocks[0].Block.StateRoot
	requireParentReaders := func() {
		t.Helper()
		got, err := test.store.GetStateAtBlockRoot(parentRoot)
		require.NoError(t, err)
		requireStateHash(t, got, wantRoot)
		require.NoError(t, test.store.ViewStateAtBlockRoot(parentRoot, func(view *state.CachingBeaconState) error {
			requireStateHash(t, view, wantRoot)
			return nil
		}))
		owned, err := test.store.GetOwnedStateAtBlockRoot(parentRoot)
		require.NoError(t, err)
		requireStateHash(t, owned, wantRoot)
		called := false
		require.NoError(t, test.store.ViewCurrentStateAtBlockRoot(parentRoot, func(*state.CachingBeaconState) error {
			called = true
			return nil
		}))
		require.False(t, called)
	}
	requireParentReaders()
	close(release)
	require.Error(t, <-done)
	requireParentReaders()
}

func requireStateHash(t *testing.T, got *state.CachingBeaconState, want common.Hash) {
	t.Helper()
	require.NotNil(t, got)
	gotRoot, err := got.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, common.Hash(gotRoot))
}

func TestBlockImportOverlapSlowPrepareReleasesLockAndAdmission(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine := newReturningEngine(t, execution_client.PayloadStatusValidated, nil)
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	prepareEntered := make(chan struct{})
	releasePrepare := make(chan struct{})
	test.store.blockImportPrepareHook = func() {
		close(prepareEntered)
		<-releasePrepare
	}
	done := make(chan error, 1)
	go func() { done <- test.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, prepareEntered, "prepare hook")
	assertForkChoiceLockFree(t, test.store)
	admitted := make(chan error, 1)
	go func() {
		_, err := test.store.NewPayloadWithAdmission(t.Context(), nil, nil, nil, nil)
		admitted <- err
	}()
	require.NoError(t, <-admitted)
	close(releasePrepare)
	require.NoError(t, <-done)
}

func TestBlockImportOverlapCloneDoesNotRaceOriginal(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
	prepared := make(chan struct{})
	test.store.blockImportPrepareHook = func() { close(prepared) }
	done := make(chan error, 1)
	go func() { done <- test.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	stop := make(chan struct{})
	var hashes sync.WaitGroup
	hashes.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = block.Block.HashSSZ()
				_ = engine_types.ExecutionPayloadFromSSZBlock(block.Block.Body.ExecutionPayload, block.Version())
				_, _ = block.Block.Body.ExecutionPayload.ComputeBlockHash(&block.Block.ParentRoot, common.Hash{}, nil)
			}
		}
	})
	awaitSignal(t, prepared, "prepare")
	close(stop)
	hashes.Wait()
	close(release)
	require.NoError(t, <-done)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	current := test.graph.CurrentStateLocked(root)
	require.NotNil(t, current)
	stateRoot, err := current.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, block.Block.StateRoot, common.Hash(stateRoot))
}

func TestBlockImportOverlapGateUsesSerialPath(t *testing.T) {
	testsCases := []struct {
		name        string
		configure   func(*overlapTestStore, *cltypes.SignedBeaconBlock)
		publishHead bool
	}{
		{name: "verified payload", publishHead: true, configure: func(test *overlapTestStore, block *cltypes.SignedBeaconBlock) {
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			test.store.forkGraph = verifiedBlockImportForkGraph{productionTestForkGraph: test.graph.(productionTestForkGraph), root: root}
		}},
		{name: "non tip", publishHead: true, configure: func(test *overlapTestStore, block *cltypes.SignedBeaconBlock) {
			cfg := test.store.beaconCfg
			test.store.OnTick((test.store.genesisTime+(block.Block.Slot+2)*cfg.SecondsPerSlot)*1000 + 100)
		}},
		{name: "syncing", configure: func(*overlapTestStore, *cltypes.SignedBeaconBlock) {}},
	}
	for _, testCase := range testsCases {
		t.Run(testCase.name, func(t *testing.T) {
			blocks, anchor, _ := tests.GetBellatrixRandom()
			engine := newReturningEngine(t, execution_client.PayloadStatusValidated, nil)
			test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
			if !testCase.publishHead {
				test.manager = synced_data.NewSyncedDataManager(anchor.BeaconConfig(), true)
				test.store.syncedDataManager = test.manager
			}
			testCase.configure(test, block)
			var hooks atomic.Int64
			test.store.blockImportPrepareHook = func() { hooks.Add(1) }
			require.NoError(t, test.store.OnBlock(t.Context(), block, true, true, false))
			require.Zero(t, hooks.Load())
		})
	}
}

func TestBlockImportOverlapReverseInterleavings(t *testing.T) {
	t.Run("tick before commit", func(t *testing.T) {
		blocks, anchor, _ := tests.GetBellatrixRandom()
		engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
		test, block, setupRoot := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
		prepared := make(chan struct{})
		test.store.blockImportPrepareHook = func() { close(prepared) }
		done := make(chan error, 1)
		go func() { done <- test.store.OnBlock(t.Context(), block, true, true, false) }()
		awaitSignal(t, entered, "NewPayload")
		awaitSignal(t, prepared, "prepare")
		cfg := test.store.beaconCfg
		test.store.OnTick((test.store.genesisTime+(block.Block.Slot+1)*cfg.SecondsPerSlot)*1000 + 100)
		close(release)
		require.NoError(t, <-done)

		serialBlocks, serialAnchor, _ := tests.GetBellatrixRandom()
		serial, serialBlock, serialSetupRoot := newNonBoundaryOverlapTestStore(t, serialBlocks, serialAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), true)
		serial.store.OnTick((serial.store.genesisTime+(serialBlock.Block.Slot+1)*cfg.SecondsPerSlot)*1000 + 100)
		require.NoError(t, serial.store.OnBlock(t.Context(), serialBlock, true, true, false))
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		serialRoot, err := serialBlock.Block.HashSSZ()
		require.NoError(t, err)
		got, err := captureBlockImportSnapshot(test.store, []common.Hash{test.graph.AnchorRoot(), setupRoot, root}, []uint64{state.Epoch(anchor)}, root, drainBlockImportEvents(t, test.events))
		require.NoError(t, err)
		want, err := captureBlockImportSnapshot(serial.store, []common.Hash{serial.graph.AnchorRoot(), serialSetupRoot, serialRoot}, []uint64{state.Epoch(serialAnchor)}, serialRoot, drainBlockImportEvents(t, serial.events))
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("duplicate waits for overlap", func(t *testing.T) {
		blocks, anchor, _ := tests.GetBellatrixRandom()
		engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
		test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
		prepared := make(chan struct{})
		test.store.blockImportPrepareHook = func() { close(prepared) }
		firstDone := make(chan error, 1)
		go func() { firstDone <- test.store.OnBlock(t.Context(), block, true, true, false) }()
		awaitSignal(t, entered, "NewPayload")
		awaitSignal(t, prepared, "prepare")
		checks := signalAdmissionChecks(test)
		duplicateDone := make(chan error, 1)
		go func() { duplicateDone <- test.store.OnBlock(t.Context(), block, false, true, false) }()
		awaitSignal(t, checks, "duplicate admission")
		assertOnBlockWaiting(t, test.store, duplicateDone)
		close(release)
		require.NoError(t, <-firstDone)
		require.NoError(t, <-duplicateDone)
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		_, ok := test.graph.GetHeader(root)
		require.True(t, ok)
		require.Len(t, drainBlockImportEvents(t, test.events), 1)
		require.NoError(t, test.store.OnBlock(t.Context(), block, false, true, false))
		require.Empty(t, drainBlockImportEvents(t, test.events))
	})

	t.Run("sibling commits first", func(t *testing.T) {
		blocks, anchor, _ := tests.GetBellatrixRandom()
		engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
		test, original, setupRoot := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
		sibling := cloneOverlapFixture(t, original)
		sibling.Block.Body.Graffiti[0] ^= 1
		prepared := make(chan struct{})
		test.store.blockImportPrepareHook = func() { close(prepared) }
		originalDone := make(chan error, 1)
		go func() { originalDone <- test.store.OnBlock(t.Context(), original, true, true, false) }()
		awaitSignal(t, entered, "NewPayload")
		awaitSignal(t, prepared, "prepare")
		require.NoError(t, test.store.OnBlock(t.Context(), sibling, false, false, false))
		close(release)
		require.NoError(t, <-originalDone)
		originalRoot, err := original.Block.HashSSZ()
		require.NoError(t, err)
		siblingRoot, err := sibling.Block.HashSSZ()
		require.NoError(t, err)
		_, originalStored := test.graph.GetHeader(originalRoot)
		_, siblingStored := test.graph.GetHeader(siblingRoot)
		require.True(t, originalStored)
		require.True(t, siblingStored)
		require.NotNil(t, test.graph.CurrentStateLocked(originalRoot))

		serialBlocks, serialAnchor, _ := tests.GetBellatrixRandom()
		serial := newOverlapTestStore(t, serialAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), true)
		importOverlapSetupBlock(t, serial, serialBlocks[0])
		drainBlockImportEvents(t, serial.events)
		serialOriginal := serialBlocks[1]
		serialSetupRoot, err := serialBlocks[0].Block.HashSSZ()
		require.NoError(t, err)
		serialSibling := cloneOverlapFixture(t, serialOriginal)
		serialSibling.Block.Body.Graffiti[0] ^= 1
		tickOverlapStore(serial, serialOriginal)
		require.NoError(t, serial.store.OnBlock(t.Context(), serialSibling, false, false, false))
		require.NoError(t, serial.store.OnBlock(t.Context(), serialOriginal, true, true, false))
		serialOriginalRoot, err := serialOriginal.Block.HashSSZ()
		require.NoError(t, err)
		serialSiblingRoot, err := serialSibling.Block.HashSSZ()
		require.NoError(t, err)
		got, err := captureBlockImportSnapshot(test.store, []common.Hash{test.graph.AnchorRoot(), setupRoot, siblingRoot, originalRoot}, []uint64{state.Epoch(anchor)}, originalRoot, drainBlockImportEvents(t, test.events))
		require.NoError(t, err)
		want, err := captureBlockImportSnapshot(serial.store, []common.Hash{serial.graph.AnchorRoot(), serialSetupRoot, serialSiblingRoot, serialOriginalRoot}, []uint64{state.Epoch(serialAnchor)}, serialOriginalRoot, drainBlockImportEvents(t, serial.events))
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("set next epoch state", func(t *testing.T) {
		blocks, anchor, _ := tests.GetBellatrixRandom()
		engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
		test, block, _ := newNonBoundaryOverlapTestStore(t, blocks, anchor, engine, false)
		prepared := make(chan struct{})
		test.store.blockImportPrepareHook = func() { close(prepared) }
		done := make(chan error, 1)
		go func() { done <- test.store.OnBlock(t.Context(), block, true, true, false) }()
		awaitSignal(t, entered, "NewPayload")
		awaitSignal(t, prepared, "prepare")
		next, err := anchor.Copy()
		require.NoError(t, err)
		test.graph.SetNextEpochState(block.Block.ParentRoot, next)
		close(release)
		require.NoError(t, <-done)
	})
}

func TestBlockImportOverlapGossipAttestationDuringEL(t *testing.T) {
	overlapBlocks, overlapAnchor, _ := tests.GetBellatrixRandom()
	serialBlocks, serialAnchor, _ := tests.GetBellatrixRandom()
	const targetIndex = 32
	overlap := newOverlapTestStore(t, overlapAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	serial := newOverlapTestStore(t, serialAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), true)
	for index := range targetIndex {
		importOverlapBlock(t, overlap, overlapBlocks[index])
		importOverlapBlock(t, serial, serialBlocks[index])
		drainBlockImportEvents(t, overlap.events)
		drainBlockImportEvents(t, serial.events)
	}
	block := overlapBlocks[targetIndex]
	serialBlock := serialBlocks[targetIndex]
	require.Positive(t, block.Block.Body.Attestations.Len())
	engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	overlap.store.engine = engine
	tickOverlapStore(overlap, block)
	prepared := make(chan struct{})
	overlap.store.blockImportPrepareHook = func() { close(prepared) }
	done := make(chan error, 1)
	go func() { done <- overlap.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	awaitSignal(t, prepared, "prepare")
	require.NoError(t, overlap.store.OnAttestation(block.Block.Body.Attestations.Get(0), false, false))
	close(release)
	require.NoError(t, <-done)

	tickOverlapStore(serial, serialBlock)
	require.NoError(t, serial.store.OnAttestation(serialBlock.Block.Body.Attestations.Get(0), false, false))
	require.NoError(t, serial.store.OnBlock(t.Context(), serialBlock, true, true, false))
	overlapRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	serialRoot, err := serialBlock.Block.HashSSZ()
	require.NoError(t, err)
	got, err := captureBlockImportSnapshot(overlap.store, []common.Hash{overlap.graph.AnchorRoot(), overlapRoot}, []uint64{state.Epoch(overlapAnchor), block.Block.Slot / overlap.store.beaconCfg.SlotsPerEpoch}, overlapRoot, drainBlockImportEvents(t, overlap.events))
	require.NoError(t, err)
	want, err := captureBlockImportSnapshot(serial.store, []common.Hash{serial.graph.AnchorRoot(), serialRoot}, []uint64{state.Epoch(serialAnchor), serialBlock.Block.Slot / serial.store.beaconCfg.SlotsPerEpoch}, serialRoot, drainBlockImportEvents(t, serial.events))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestBlockImportOverlapCompetingVoteDuringEL(t *testing.T) {
	overlapBlocks, overlapAnchor, _ := tests.GetBellatrixRandom()
	serialBlocks, serialAnchor, _ := tests.GetBellatrixRandom()
	const targetIndex = 32
	overlap := newOverlapTestStore(t, overlapAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	serial := newOverlapTestStore(t, serialAnchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), true)
	for index := range targetIndex {
		importOverlapBlock(t, overlap, overlapBlocks[index])
		importOverlapBlock(t, serial, serialBlocks[index])
		drainBlockImportEvents(t, overlap.events)
		drainBlockImportEvents(t, serial.events)
	}
	block := overlapBlocks[targetIndex]
	serialBlock := serialBlocks[targetIndex]
	bodyAttestation := block.Block.Body.Attestations.Get(0)
	serialBodyAttestation := serialBlock.Block.Body.Attestations.Get(0)
	indices := currentParentAttestingIndices(t, overlap, block, bodyAttestation)
	require.NotEmpty(t, indices)
	serialIndices := currentParentAttestingIndices(t, serial, serialBlock, serialBodyAttestation)
	require.Equal(t, indices, serialIndices)
	validatorIndex := indices[0]
	overlap.store.mu.Lock()
	overlap.store.setLatestMessage(validatorIndex, LatestMessage{})
	overlap.store.mu.Unlock()
	serial.store.mu.Lock()
	serial.store.setLatestMessage(validatorIndex, LatestMessage{})
	serial.store.mu.Unlock()
	require.Equal(t, block.Block.ParentRoot, serialBlock.Block.ParentRoot)
	competingRoot := block.Block.ParentRoot
	require.NotEqual(t, bodyAttestation.Data.BeaconBlockRoot, competingRoot)
	competing := bodyAttestation.Copy()
	competing.Data.BeaconBlockRoot = competingRoot
	serialCompeting := serialBodyAttestation.Copy()
	serialCompeting.Data.BeaconBlockRoot = competingRoot

	engine, entered, release := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	overlap.store.engine = engine
	tickOverlapStore(overlap, block)
	prepared := make(chan struct{})
	overlap.store.blockImportPrepareHook = func() { close(prepared) }
	done := make(chan error, 1)
	go func() { done <- overlap.store.OnBlock(t.Context(), block, true, true, false) }()
	awaitSignal(t, entered, "NewPayload")
	awaitSignal(t, prepared, "prepare")
	overlap.store.ProcessAttestingIndicies(competing, []uint64{validatorIndex})
	close(release)
	require.NoError(t, <-done)
	overlapMessage, ok := overlap.store.getLatestMessage(validatorIndex)
	require.True(t, ok)
	require.Equal(t, competingRoot, overlapMessage.Root)

	tickOverlapStore(serial, serialBlock)
	serial.store.ProcessAttestingIndicies(serialCompeting, []uint64{validatorIndex})
	require.NoError(t, serial.store.OnBlock(t.Context(), serialBlock, true, true, false))
	serialMessage, ok := serial.store.getLatestMessage(validatorIndex)
	require.True(t, ok)
	require.Equal(t, competingRoot, serialMessage.Root)
	overlapRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	serialRoot, err := serialBlock.Block.HashSSZ()
	require.NoError(t, err)
	got, err := captureBlockImportSnapshot(overlap.store, []common.Hash{overlap.graph.AnchorRoot(), overlapRoot}, []uint64{state.Epoch(overlapAnchor), block.Block.Slot / overlap.store.beaconCfg.SlotsPerEpoch}, overlapRoot, drainBlockImportEvents(t, overlap.events))
	require.NoError(t, err)
	want, err := captureBlockImportSnapshot(serial.store, []common.Hash{serial.graph.AnchorRoot(), serialRoot}, []uint64{state.Epoch(serialAnchor), serialBlock.Block.Slot / serial.store.beaconCfg.SlotsPerEpoch}, serialRoot, drainBlockImportEvents(t, serial.events))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestBlockImportOverlapParentNotCurrentUsesSerialPath(t *testing.T) {
	blocks, anchor, _ := tests.GetCapellaRandom()
	test := newOverlapTestStore(t, anchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	importOverlapBlock(t, test, blocks[0])
	sibling := cloneOverlapFixture(t, blocks[0])
	sibling.Block.Body.Graffiti[0] ^= 1
	var hooks atomic.Int64
	test.store.blockImportPrepareHook = func() { hooks.Add(1) }
	tickOverlapStore(test, sibling)
	require.NoError(t, test.store.OnBlock(t.Context(), sibling, true, false, false))
	require.Zero(t, hooks.Load())
}

func TestBlockImportOverlapParentNotSyncedHeadUsesSerialPath(t *testing.T) {
	blocks, anchor, _ := tests.GetCapellaRandom()
	test := newOverlapTestStore(t, anchor, newReturningEngine(t, execution_client.PayloadStatusValidated, nil), false)
	tickOverlapStore(test, blocks[0])
	require.NoError(t, test.store.OnBlock(t.Context(), blocks[0], true, true, false))
	parentRoot, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	require.NotNil(t, test.graph.CurrentStateLocked(parentRoot))
	require.NotEqual(t, parentRoot, test.manager.HeadRoot())

	var hooks atomic.Int64
	test.store.blockImportPrepareHook = func() { hooks.Add(1) }
	tickOverlapStore(test, blocks[1])
	require.NoError(t, test.store.OnBlock(t.Context(), blocks[1], true, true, false))
	require.Zero(t, hooks.Load())
}
