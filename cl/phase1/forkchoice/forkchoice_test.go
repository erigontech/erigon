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
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
)

func TestGetFinalizedExecutionHash(t *testing.T) {
	cache, err := lru.New[common.Hash, common.Hash](16)
	require.NoError(t, err)

	gloasBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	gloasBlock.Block.Slot = 1
	gloasRoot := common.Hash{0x01}
	gloasParentBlockHash := common.Hash{0xaa}
	gloasFallbackHash := common.Hash{0xbb}
	gloasBlock.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = gloasParentBlockHash
	cache.Add(gloasRoot, gloasFallbackHash)

	preGloasBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	preGloasBlock.Block.Slot = 2
	preGloasRoot := common.Hash{0x02}
	preGloasExecutionHash := common.Hash{0xcc}
	cache.Add(preGloasRoot, preGloasExecutionHash)

	missingRoot := common.Hash{0x03}
	missingExecutionHash := common.Hash{0xdd}
	cache.Add(missingRoot, missingExecutionHash)

	store := &ForkChoiceStore{
		forkGraph: &getFinalizedExecutionHashForkGraph{
			blocks: map[common.Hash]*cltypes.SignedBeaconBlock{
				gloasRoot:    gloasBlock,
				preGloasRoot: preGloasBlock,
			},
		},
		eth2Roots: cache,
	}

	require.Equal(t, gloasParentBlockHash, store.GetFinalizedExecutionHash(gloasRoot))
	require.Equal(t, preGloasExecutionHash, store.GetFinalizedExecutionHash(preGloasRoot))
	require.Equal(t, missingExecutionHash, store.GetFinalizedExecutionHash(missingRoot))
}

func TestAddChainSegmentDoesNotQueueLightClientEventsOnError(t *testing.T) {
	insertErr := errors.New("invalid block")
	update := &cltypes.LightClientUpdate{}
	store := &ForkChoiceStore{
		forkGraph: &getFinalizedExecutionHashForkGraph{
			afterUpdate:           update,
			addChainSegmentStatus: fork_graph.InvalidBlock,
			addChainSegmentErr:    insertErr,
		},
		emitters: beaconevents.NewEventEmitter(),
	}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.AltairVersion)

	_, status, err := store.addChainSegmentAndQueueLightClientEvents(block, true)

	require.ErrorIs(t, err, insertErr)
	require.Equal(t, fork_graph.InvalidBlock, status)
	require.Empty(t, store.queuedEmits)
}

func TestAddChainSegmentQueuesLightClientEventsOnSuccess(t *testing.T) {
	update := &cltypes.LightClientUpdate{}
	store := &ForkChoiceStore{
		forkGraph: &getFinalizedExecutionHashForkGraph{
			afterUpdate:           update,
			addChainSegmentStatus: fork_graph.Success,
		},
		emitters: beaconevents.NewEventEmitter(),
	}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.AltairVersion)

	_, status, err := store.addChainSegmentAndQueueLightClientEvents(block, true)

	require.NoError(t, err)
	require.Equal(t, fork_graph.Success, status)
	require.Len(t, store.queuedEmits, 1)
}

func TestUpdateCheckpointsPrunesOperationsWithExactFinalizedState(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	finalizedRoot := common.Hash{0x42}
	finalizedState := state.New(&cfg)
	finalizedState.SetSlot(2 * cfg.SlotsPerEpoch)
	validator := solid.NewValidatorFromParameters(
		common.Bytes48{},
		common.Hash{},
		cfg.MaxEffectiveBalance,
		false,
		0,
		0,
		3,
		cfg.FarFutureEpoch,
	)
	finalizedState.AddValidator(validator, cfg.MaxEffectiveBalance)

	operationsPool := pool.NewOperationsPool(&cfg)
	exit := &cltypes.SignedVoluntaryExit{
		VoluntaryExit: &cltypes.VoluntaryExit{ValidatorIndex: 0},
	}
	operationsPool.VoluntaryExitsPool.Insert(0, exit)
	graph := &getFinalizedExecutionHashForkGraph{
		headers: map[common.Hash]*cltypes.BeaconBlockHeader{
			finalizedRoot: {},
		},
		states: map[common.Hash]*state.CachingBeaconState{
			finalizedRoot: finalizedState,
		},
		getStateStarted: make(chan common.Hash, 1),
		getStateRelease: make(chan struct{}),
	}
	store := &ForkChoiceStore{
		forkGraph:      graph,
		operationsPool: operationsPool,
		beaconCfg:      &cfg,
		emitters:       beaconevents.NewEventEmitter(),
	}
	store.justifiedCheckpoint.Store(solid.Checkpoint{})
	store.finalizedCheckpoint.Store(solid.Checkpoint{})

	store.updateCheckpoints(
		solid.Checkpoint{},
		solid.Checkpoint{Epoch: 2, Root: finalizedRoot},
	)
	require.Empty(t, graph.stateRoots())

	drained := make(chan struct{})
	go func() {
		store.drainQueuedWork()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(time.Second):
		close(graph.getStateRelease)
		<-drained
		t.Fatal("drainQueuedWork waited for finalized operation pruning")
	}

	require.Equal(t, finalizedRoot, waitForStateLoad(t, graph.getStateStarted))
	require.True(t, operationsPool.VoluntaryExitsPool.Has(0))
	close(graph.getStateRelease)
	require.Eventually(t, func() bool {
		return !operationsPool.VoluntaryExitsPool.Has(0)
	}, time.Second, time.Millisecond)
	require.Equal(t, []common.Hash{finalizedRoot}, graph.stateRoots())
	require.Equal(t, []bool{true}, graph.stateCopyModes())
	requireOperationPrunerIdle(t, store)
}

func TestAllAttesterSlashingIndicesSeen(t *testing.T) {
	store := &ForkChoiceStore{
		equivocatingIndicies: []byte{0b00000110},
	}

	require.True(t, store.allAttesterSlashingIndicesSeen([]uint64{1, 2}))
	require.False(t, store.allAttesterSlashingIndicesSeen([]uint64{1, 3}))
	require.True(t, store.allAttesterSlashingIndicesSeen(nil))
}

func TestAttesterSlashingIgnoresDisjointIndices(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	store := &ForkChoiceStore{}
	slashing := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{1}),
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{2}),
		},
	}

	err := store.onProcessAttesterSlashing(slashing, state.New(&cfg), false)

	require.ErrorIs(t, err, ErrIgnore)
}

func TestAttesterSlashingSeenCheckPrecedesValidation(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	store := &ForkChoiceStore{
		equivocatingIndicies: []byte{0b00000010},
	}
	slashing := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{1}),
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{1}),
		},
	}

	err := store.onProcessAttesterSlashing(slashing, state.New(&cfg), false)

	require.ErrorIs(t, err, ErrIgnore)
}

func TestAttesterSlashingSeenCheckHandlesUnsortedIndices(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	store := &ForkChoiceStore{
		equivocatingIndicies: []byte{0b00000010},
	}
	slashing := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{2, 1}),
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{1}),
		},
	}

	err := store.onProcessAttesterSlashing(slashing, state.New(&cfg), false)

	require.ErrorIs(t, err, ErrIgnore)
}

func TestAttesterSlashingRejectsIndicesOverLimitBeforeSeenCheck(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	store := &ForkChoiceStore{
		equivocatingIndicies: []byte{0b00000010},
	}
	slashing := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(1, []uint64{2, 1}),
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(1, []uint64{1}),
		},
	}

	err := store.onProcessAttesterSlashing(slashing, state.New(&cfg), false)

	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestOnAttesterSlashingRejectsIncompleteOperation(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	store := &ForkChoiceStore{
		operationsPool: pool.NewOperationsPool(&cfg),
	}

	require.NotPanics(t, func() {
		require.Error(t, store.OnAttesterSlashing(&cltypes.AttesterSlashing{}, false))
	})
}

func TestDrainQueuedWorkRetainsOperationsWhenFinalizedStateIsUnavailable(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	operationsPool := pool.NewOperationsPool(&cfg)
	exit := &cltypes.SignedVoluntaryExit{
		VoluntaryExit: &cltypes.VoluntaryExit{ValidatorIndex: 0},
	}
	operationsPool.VoluntaryExitsPool.Insert(0, exit)
	store := &ForkChoiceStore{
		forkGraph:      &getFinalizedExecutionHashForkGraph{},
		operationsPool: operationsPool,
	}
	store.queueOperationPrune(solid.Checkpoint{Epoch: 1, Root: common.Hash{1}})

	store.drainQueuedWork()

	requireOperationPrunerIdle(t, store)
	require.True(t, operationsPool.VoluntaryExitsPool.Has(0))
}

func TestDrainQueuedWorkSkipsOperationStateLoadWithoutPrunableOperations(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	graph := &getFinalizedExecutionHashForkGraph{}
	operationsPool := pool.NewOperationsPool(&cfg)
	operationsPool.AttestationsPool.Insert(common.Bytes96{1}, &solid.Attestation{})
	store := &ForkChoiceStore{
		forkGraph:      graph,
		operationsPool: operationsPool,
	}
	store.queueOperationPrune(solid.Checkpoint{Epoch: 1, Root: common.Hash{1}})

	store.drainQueuedWork()

	requireOperationPrunerIdle(t, store)
	require.Empty(t, graph.stateRoots())
}

func TestOperationPrunerCoalescesPendingFinalizedRoots(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	firstRoot := common.Hash{1}
	skippedRoot := common.Hash{2}
	latestRoot := common.Hash{3}
	graph := &getFinalizedExecutionHashForkGraph{
		states: map[common.Hash]*state.CachingBeaconState{
			firstRoot:  state.New(&cfg),
			latestRoot: state.New(&cfg),
		},
		getStateStarted: make(chan common.Hash, 3),
		getStateRelease: make(chan struct{}),
	}
	operationsPool := pool.NewOperationsPool(&cfg)
	operationsPool.VoluntaryExitsPool.Insert(0, &cltypes.SignedVoluntaryExit{
		VoluntaryExit: &cltypes.VoluntaryExit{ValidatorIndex: 0},
	})
	store := &ForkChoiceStore{
		forkGraph:      graph,
		operationsPool: operationsPool,
	}

	store.queueOperationPrune(solid.Checkpoint{Epoch: 1, Root: firstRoot})
	store.drainQueuedWork()
	require.Equal(t, firstRoot, waitForStateLoad(t, graph.getStateStarted))

	store.queueOperationPrune(solid.Checkpoint{Epoch: 3, Root: latestRoot})
	store.drainQueuedWork()
	store.queueOperationPrune(solid.Checkpoint{Epoch: 2, Root: skippedRoot})
	store.drainQueuedWork()
	close(graph.getStateRelease)
	require.Equal(t, latestRoot, waitForStateLoad(t, graph.getStateStarted))
	require.Eventually(t, func() bool {
		return len(graph.stateRoots()) == 2
	}, time.Second, time.Millisecond)
	require.Equal(t, []common.Hash{firstRoot, latestRoot}, graph.stateRoots())
	requireOperationPrunerIdle(t, store)
}

func requireOperationPrunerIdle(t *testing.T, store *ForkChoiceStore) {
	t.Helper()
	require.Eventually(t, func() bool {
		store.operationPruneMu.Lock()
		defer store.operationPruneMu.Unlock()
		return !store.operationPruneRunning && !store.operationPrunePending
	}, time.Second, time.Millisecond)
}

func waitForStateLoad(t *testing.T, started <-chan common.Hash) common.Hash {
	t.Helper()
	select {
	case root := <-started:
		return root
	case <-time.After(time.Second):
		t.Fatal("finalized state load did not start")
		return common.Hash{}
	}
}

type getFinalizedExecutionHashForkGraph struct {
	blocks                map[common.Hash]*cltypes.SignedBeaconBlock
	headers               map[common.Hash]*cltypes.BeaconBlockHeader
	states                map[common.Hash]*state.CachingBeaconState
	getStateMu            sync.Mutex
	getStateRoots         []common.Hash
	getStateAlwaysCopy    []bool
	getStateStarted       chan common.Hash
	getStateRelease       chan struct{}
	beforeUpdate          *cltypes.LightClientUpdate
	afterUpdate           *cltypes.LightClientUpdate
	addChainSegmentStatus fork_graph.ChainSegmentInsertionResult
	addChainSegmentErr    error
	addChainSegmentCalled bool
	anchorRoot            common.Hash
	anchorSlot            uint64
	currentJustified      solid.Checkpoint
}

func (g *getFinalizedExecutionHashForkGraph) AddChainSegment(*cltypes.SignedBeaconBlock, bool) (*state.CachingBeaconState, fork_graph.ChainSegmentInsertionResult, error) {
	g.addChainSegmentCalled = true
	return nil, g.addChainSegmentStatus, g.addChainSegmentErr
}

func (g *getFinalizedExecutionHashForkGraph) GetHeader(blockRoot common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	header := g.headers[blockRoot]
	return header, header != nil
}

func (g *getFinalizedExecutionHashForkGraph) GetBlock(blockRoot common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	block := g.blocks[blockRoot]
	return block, block != nil
}

func (g *getFinalizedExecutionHashForkGraph) GetState(blockRoot common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
	g.getStateMu.Lock()
	g.getStateRoots = append(g.getStateRoots, blockRoot)
	g.getStateAlwaysCopy = append(g.getStateAlwaysCopy, alwaysCopy)
	g.getStateMu.Unlock()
	if g.getStateStarted != nil {
		g.getStateStarted <- blockRoot
	}
	if g.getStateRelease != nil {
		<-g.getStateRelease
	}
	state := g.states[blockRoot]
	return state, nil
}

func (g *getFinalizedExecutionHashForkGraph) stateRoots() []common.Hash {
	g.getStateMu.Lock()
	defer g.getStateMu.Unlock()
	return slices.Clone(g.getStateRoots)
}

func (g *getFinalizedExecutionHashForkGraph) stateCopyModes() []bool {
	g.getStateMu.Lock()
	defer g.getStateMu.Unlock()
	return slices.Clone(g.getStateAlwaysCopy)
}

func (g *getFinalizedExecutionHashForkGraph) GetCurrentJustifiedCheckpoint(common.Hash) (solid.Checkpoint, bool) {
	return g.currentJustified, true
}

func (g *getFinalizedExecutionHashForkGraph) GetFinalizedCheckpoint(common.Hash) (solid.Checkpoint, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetSyncCommittees(uint64) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) MarkHeaderAsInvalid(common.Hash) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) AnchorSlot() uint64 {
	return g.anchorSlot
}

func (g *getFinalizedExecutionHashForkGraph) AnchorRoot() common.Hash {
	return g.anchorRoot
}

func (g *getFinalizedExecutionHashForkGraph) Prune(uint64) error {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetBlockRewards(common.Hash) (*eth2.BlockRewardsCollector, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) LowestAvailableSlot() uint64 {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetLightClientBootstrap(common.Hash) (*cltypes.LightClientBootstrap, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	if g.addChainSegmentCalled {
		return g.afterUpdate
	}
	return g.beforeUpdate
}

func (g *getFinalizedExecutionHashForkGraph) GetLightClientUpdate(uint64) (*cltypes.LightClientUpdate, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetBalances(common.Hash) (solid.Uint64ListSSZ, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetInactivitiesScores(common.Hash) (solid.Uint64ListSSZ, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetValidatorSet(common.Hash) (*solid.ValidatorSet, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetCurrentParticipationIndicies(uint64) (*solid.ParticipationBitList, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetPreviousParticipationIndicies(uint64) (*solid.ParticipationBitList, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) DumpBeaconStateOnDisk(common.Hash, *state.CachingBeaconState, bool) error {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) DumpEnvelopeOnDisk(common.Hash, *cltypes.SignedExecutionPayloadEnvelope) error {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) PrepareEnvelopeOnDisk(common.Hash, *cltypes.SignedExecutionPayloadEnvelope, bool) (func() error, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) PendingEnvelopeIndexRoots() ([]common.Hash, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) MarkEnvelopeIndicesCommitted(common.Hash) error {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) HasEnvelope(common.Hash) bool {
	panic("not used")
}
