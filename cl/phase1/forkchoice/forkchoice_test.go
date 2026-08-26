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
	"slices"
	"sync"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

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
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
)

type embeddedPtcVoteForkGraph struct {
	*getFinalizedExecutionHashForkGraph
	postState *state.CachingBeaconState
	envelopes map[common.Hash]bool
}

func (g *embeddedPtcVoteForkGraph) AddChainSegment(block *cltypes.SignedBeaconBlock, _ bool) (*state.CachingBeaconState, fork_graph.ChainSegmentInsertionResult, error) {
	root, err := block.Block.HashSSZ()
	if err != nil {
		return nil, fork_graph.InvalidBlock, err
	}
	g.blocks[root] = block
	g.headers[root] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot, ParentRoot: block.Block.ParentRoot}
	g.addChainSegmentCalled = true
	return g.postState, fork_graph.Success, nil
}

func (g *embeddedPtcVoteForkGraph) HasEnvelope(root common.Hash) bool {
	return g.envelopes[root]
}

func TestOnBlockFromForwardSyncExpandsEmbeddedPtcVotesForDuplicateValidator(t *testing.T) {
	committee := make([]uint64, clparams.MaxPtcSize)
	for i := range committee {
		committee[i] = uint64(i + 1)
	}
	committee[clparams.MaxPtcSize/2] = committee[0]
	selectedPositions := make([]int, clparams.MaxPtcSize/2)
	for i := range selectedPositions {
		selectedPositions[i] = i
	}

	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, committee, selectedPositions)
	require.True(t, store.payloadTimeliness(anchorRoot, false))
	require.True(t, store.payloadDataAvailability(anchorRoot, false))
	require.False(t, store.ShouldBuildOnFull(ForkChoiceNode{Root: anchorRoot, PayloadStatus: cltypes.PayloadStatusFull}, 2))
	head, err := store.GetHeadNode()
	require.NoError(t, err)
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: childRoot, PayloadStatus: cltypes.PayloadStatusEmpty}, head)
}

func TestOnBlockFromForwardSyncAppliesEmbeddedPtcVotesForUniqueValidators(t *testing.T) {
	committee := make([]uint64, clparams.MaxPtcSize)
	for i := range committee {
		committee[i] = uint64(i + 1)
	}
	selectedPositions := make([]int, clparams.MaxPtcSize/2+1)
	for i := range selectedPositions {
		selectedPositions[i] = i
	}

	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, committee, selectedPositions)
	require.True(t, store.payloadTimeliness(anchorRoot, false))
	require.True(t, store.payloadDataAvailability(anchorRoot, false))
	head, err := store.GetHeadNode()
	require.NoError(t, err)
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: childRoot, PayloadStatus: cltypes.PayloadStatusEmpty}, head)
}

func TestOnBlockFromForwardSyncUsesMaxPtcSizeForZeroConfig(t *testing.T) {
	committee := make([]uint64, clparams.MaxPtcSize)
	for i := range committee {
		committee[i] = uint64(i + 1)
	}

	store, anchorRoot, _ := runEmbeddedPtcVoteBlock(t, 0, committee, []int{int(clparams.MaxPtcSize - 1)})
	votes := store.payloadTimelinessVoteValue(anchorRoot)
	require.Equal(t, int8(-1), votes[clparams.MaxPtcSize-1])
	require.Equal(t, int8(1), votes[clparams.MaxPtcSize-2])
}

func runEmbeddedPtcVoteBlock(
	t *testing.T,
	configuredPtcSize uint64,
	committee []uint64,
	selectedPositions []int,
) (*ForkChoiceStore, common.Hash, *cltypes.SignedBeaconBlock) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.PtcSize = configuredPtcSize
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()

	anchor := state.New(&cfg)
	anchor.SetVersion(clparams.GloasVersion)
	require.NoError(t, anchor.SetSlot(1))
	ptcSize := configuredPtcSize
	if ptcSize == 0 {
		ptcSize = clparams.MaxPtcSize
	}
	ptcWindow := solid.NewUint64VectorOfVectors(int((2+cfg.MinSeedLookahead)*cfg.SlotsPerEpoch), int(ptcSize))
	ptc := ptcWindow.Get(int(cfg.SlotsPerEpoch + anchor.Slot()%cfg.SlotsPerEpoch))
	for i, validatorIndex := range committee {
		ptc.Set(i, validatorIndex)
	}
	anchor.SetPtcWindow(ptcWindow)
	anchorRoot, err := anchor.BlockRoot()
	require.NoError(t, err)

	postState, err := anchor.Copy()
	require.NoError(t, err)
	require.NoError(t, postState.SetSlot(2))
	graph := &embeddedPtcVoteForkGraph{
		getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{
			blocks:     make(map[common.Hash]*cltypes.SignedBeaconBlock),
			headers:    map[common.Hash]*cltypes.BeaconBlockHeader{anchorRoot: {Slot: 1}},
			states:     map[common.Hash]*state.CachingBeaconState{anchorRoot: anchor},
			anchorRoot: anchorRoot,
			anchorSlot: 1,
		},
		postState: postState,
		envelopes: map[common.Hash]bool{anchorRoot: true},
	}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	store, err := NewForkChoiceStore(
		clock,
		anchor,
		nil,
		pool.NewOperationsPool(&cfg),
		graph,
		beaconevents.NewEventEmitter(),
		synced_data.NewSyncedDataManager(&cfg, true),
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)
	store.OnTick(2 * cfg.SecondsPerSlot)
	store.payloadStatusByRoot.Add(anchorRoot, execution_client.PayloadStatusValidated)

	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	child.Block.Slot = 2
	child.Block.ParentRoot = anchorRoot
	votes := solid.NewBitVector(int(ptcSize))
	for _, position := range selectedPositions {
		require.NoError(t, votes.SetBitAt(position, true))
	}
	child.Block.Body.PayloadAttestations.Append(&cltypes.PayloadAttestation{
		AggregationBits: votes,
		Data: &cltypes.PayloadAttestationData{
			BeaconBlockRoot:   anchorRoot,
			Slot:              1,
			PayloadPresent:    false,
			BlobDataAvailable: false,
		},
	})

	require.NoError(t, store.OnBlock(context.Background(), child, false, false, false))
	return store, anchorRoot, child
}

type headerOnlyAnchorForkGraph struct {
	fork_graph.ForkGraph
	root common.Hash
	slot uint64
}

type activeParentForkGraph struct {
	fork_graph.ForkGraph
	root  common.Hash
	slot  uint64
	state *state.CachingBeaconState
	copy  bool
}

func (g *activeParentForkGraph) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if root != g.root {
		return nil, false
	}
	return &cltypes.BeaconBlockHeader{Slot: g.slot}, true
}

func (g *activeParentForkGraph) GetState(root common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
	if root != g.root {
		return nil, nil
	}
	g.copy = alwaysCopy
	return g.state, nil
}

func (g *activeParentForkGraph) HasEnvelope(root common.Hash) bool { return root == g.root }
func (*activeParentForkGraph) IsBlockInvalid(common.Hash) bool     { return false }
func (*activeParentForkGraph) PayloadAccepted(common.Hash) (bool, bool) {
	return false, false
}
func (*activeParentForkGraph) IsPayloadUnavailable(common.Hash) bool {
	return false
}

func TestActiveParentsUsesHeadPayloadStatus(t *testing.T) {
	root := common.HexToHash("0xa1")
	parentHash := common.HexToHash("0xb2")
	blockHash := common.HexToHash("0xc3")
	s := state.New(&clparams.MainnetBeaconConfig)
	s.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: parentHash,
		BlockHash:       blockHash,
	})
	payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	payloadStatusByRoot.Add(root, execution_client.PayloadStatusValidated)

	graph := &activeParentForkGraph{root: root, slot: 10, state: s}
	store := &ForkChoiceStore{
		beaconCfg:           &clparams.MainnetBeaconConfig,
		forkGraph:           graph,
		headHash:            root,
		headSlot:            10,
		headPayloadStatus:   cltypes.PayloadStatusEmpty,
		payloadStatusByRoot: payloadStatusByRoot,
	}
	store.proposerBoostRoot.Store(common.Hash{})

	parents := store.ActiveParents(12)
	require.True(t, graph.copy)
	require.Equal(t, []ParentCandidate{{
		Slot:          10,
		BlockRoot:     root,
		ExecutionHash: parentHash,
		ShouldExtend:  false,
	}}, parents)
}

func TestActiveParentsRejectsMissingStateForEmptyHead(t *testing.T) {
	root := common.HexToHash("0xa1")
	graph := &activeParentForkGraph{root: root, slot: 10}
	eth2Roots, err := lru.New[common.Hash, common.Hash](16)
	require.NoError(t, err)
	eth2Roots.Add(root, common.HexToHash("0xc3"))
	store := &ForkChoiceStore{
		beaconCfg:         &clparams.MainnetBeaconConfig,
		forkGraph:         graph,
		eth2Roots:         eth2Roots,
		headHash:          root,
		headSlot:          10,
		headPayloadStatus: cltypes.PayloadStatusEmpty,
	}

	require.Empty(t, store.ActiveParents(12))
}

func (g headerOnlyAnchorForkGraph) AnchorRoot() common.Hash { return g.root }
func (g headerOnlyAnchorForkGraph) AnchorSlot() uint64      { return g.slot }
func (g headerOnlyAnchorForkGraph) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if root == g.root {
		return &cltypes.BeaconBlockHeader{Slot: g.slot}, true
	}
	return nil, false
}

func TestGetHeadNodeCachesHeaderOnlyAnchorFallback(t *testing.T) {
	anchorRoot := common.HexToHash("0xa1")
	store := &ForkChoiceStore{
		forkGraph: headerOnlyAnchorForkGraph{root: anchorRoot, slot: 42},
		beaconCfg: &clparams.MainnetBeaconConfig,
	}
	store.justifiedCheckpoint.Store(solid.Checkpoint{Root: common.HexToHash("0xb2")})

	node, err := store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, anchorRoot, node.Root)
	require.Equal(t, cltypes.PayloadStatusPending, node.PayloadStatus)
	require.Equal(t, anchorRoot, store.headHash)
	require.Equal(t, uint64(42), store.headSlot)
}

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
	require.NoError(t, finalizedState.SetSlot(2*cfg.SlotsPerEpoch))
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
	require.NoError(t, finalizedState.AddValidator(validator, cfg.MaxEffectiveBalance))

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
	hasBlockEquivocation  bool
}

func (f *getFinalizedExecutionHashForkGraph) HasBlockEquivocation(uint64, uint64, common.Hash) bool {
	return f.hasBlockEquivocation
}

func TestOnBlockWithEquivocationCheckRejectsKnownGloasConflict(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	graph := &getFinalizedExecutionHashForkGraph{
		headers:              map[common.Hash]*cltypes.BeaconBlockHeader{root: block.SignedBeaconBlockHeader().Header},
		hasBlockEquivocation: true,
	}
	store := &ForkChoiceStore{forkGraph: graph}

	err = store.OnBlockWithEquivocationCheck(t.Context(), block, true, true, false)
	require.ErrorContains(t, err, "conflicts with a previously validated proposal")
	require.NoError(t, store.OnBlock(t.Context(), block, true, true, false))
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

func (g *getFinalizedExecutionHashForkGraph) HasBlockChildAtOrAfter(blockRoot common.Hash, slot uint64) bool {
	for _, block := range g.blocks {
		if block != nil && block.Block != nil && block.Block.ParentRoot == blockRoot && block.Block.Slot >= slot {
			return true
		}
	}
	return false
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

func (g *getFinalizedExecutionHashForkGraph) IsBlockInvalid(common.Hash) bool {
	return false
}

func (g *getFinalizedExecutionHashForkGraph) MarkPayloadUnavailable(common.Hash) {}
func (g *getFinalizedExecutionHashForkGraph) MarkPayloadAvailable(common.Hash)   {}
func (g *getFinalizedExecutionHashForkGraph) IsPayloadUnavailable(common.Hash) bool {
	return false
}
func (g *getFinalizedExecutionHashForkGraph) MarkPayloadAccepted(common.Hash, bool) {}
func (g *getFinalizedExecutionHashForkGraph) ClearPayloadAccepted(common.Hash)      {}
func (g *getFinalizedExecutionHashForkGraph) PayloadAccepted(common.Hash) (bool, bool) {
	return false, false
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

func (g *getFinalizedExecutionHashForkGraph) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) HasEnvelope(common.Hash) bool {
	return false
}
