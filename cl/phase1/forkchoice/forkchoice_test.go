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
	"fmt"
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
	invalid   map[common.Hash]bool
}

func (g *embeddedPtcVoteForkGraph) MarkHeaderAsInvalid(root common.Hash) {
	if g.invalid == nil {
		g.invalid = map[common.Hash]bool{}
	}
	g.invalid[root] = true
}

func (g *embeddedPtcVoteForkGraph) IsBlockInvalid(root common.Hash) bool {
	return g.invalid[root]
}

func TestHasImportedDescendantAfterCountsDescendantsOnly(t *testing.T) {
	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)

	require.True(t, store.HasImportedDescendantAfter(anchorRoot, child.Block.Slot-1))
	require.False(t, store.HasImportedDescendantAfter(anchorRoot, child.Block.Slot))
	// The anchor precedes the child: an ancestor or another branch never counts.
	require.False(t, store.HasImportedDescendantAfter(childRoot, 0))

	// A deeper descendant counts too.
	grandchild := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.GloasVersion)
	grandchild.Block.Slot = 40
	grandchild.Block.ParentRoot = childRoot
	grandchildRoot, err := grandchild.Block.HashSSZ()
	require.NoError(t, err)
	graph := store.forkGraph.(*embeddedPtcVoteForkGraph)
	graph.blocks[grandchildRoot] = grandchild
	graph.headers[grandchildRoot] = &cltypes.BeaconBlockHeader{Slot: grandchild.Block.Slot, ParentRoot: childRoot}
	store.updateChildren(child.Block.Slot, childRoot, grandchildRoot)
	require.True(t, store.HasImportedDescendantAfter(anchorRoot, grandchild.Block.Slot-1))
}

func TestHasImportedDescendantAfterSkipsBlocksCutOffByAnInvalidPayload(t *testing.T) {
	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)
	require.Equal(t, cltypes.PayloadStatusFull, store.getParentPayloadStatus(child.Block))

	// The child builds on the anchor's payload, so an INVALID verdict leaves no path to it.
	store.MarkPayloadStatus(anchorRoot, common.Hash{0xe1}, execution_client.PayloadStatusInvalidated)
	require.False(t, store.HasImportedDescendantAfter(anchorRoot, 0))
}

func TestHasImportedDescendantAfterKeepsBlocksOnTheEmptyVariantOfAnInvalidPayload(t *testing.T) {
	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)
	anchorBlock, ok := store.forkGraph.GetBlock(anchorRoot)
	require.True(t, ok)
	anchorBlock.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{0xe1}
	require.Equal(t, cltypes.PayloadStatusEmpty, store.getParentPayloadStatus(child.Block))

	store.MarkPayloadStatus(anchorRoot, common.Hash{0xe1}, execution_client.PayloadStatusInvalidated)
	require.True(t, store.HasImportedDescendantAfter(anchorRoot, 0))
}

// A pre-Gloas block has no EMPTY alternative: an invalid one cuts off itself and every child,
// whatever the child's bid claims.
func TestCutOffByInvalidationAtThePreGloasBoundary(t *testing.T) {
	store, _, _ := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)
	graph := store.forkGraph.(*embeddedPtcVoteForkGraph)
	add := func(block *cltypes.SignedBeaconBlock) common.Hash {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		graph.blocks[root] = block
		return root
	}
	parent := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.FuluVersion)
	parent.Block.Slot = 30
	parentRoot := add(parent)
	child := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.GloasVersion)
	child.Block.Slot = 31
	child.Block.ParentRoot = parentRoot
	childRoot := add(child)
	require.Equal(t, cltypes.PayloadStatusEmpty, store.getParentPayloadStatus(child.Block))

	require.False(t, store.cutOffByInvalidation(parentRoot))
	require.False(t, store.cutOffByInvalidation(childRoot))

	graph.MarkHeaderAsInvalid(parentRoot)
	require.True(t, store.cutOffByInvalidation(parentRoot))
	require.True(t, store.cutOffByInvalidation(childRoot))
}

func TestGloasAnchorStartsWithoutPtcVotes(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	anchor := state.New(&cfg)
	anchor.SetVersion(clparams.GloasVersion)
	root, err := anchor.BlockRoot()
	require.NoError(t, err)
	store, err := NewForkChoiceStore(nil, anchor, nil, pool.OperationsPool{}, nil, nil, nil, nil, public_keys_registry.NewInMemoryPublicKeysRegistry(), nil, false, nil)
	require.NoError(t, err)
	for _, votes := range []*sync.Map{&store.payloadTimelinessVote, &store.payloadDataAvailabilityVote} {
		value, ok := votes.Load(common.Hash(root))
		require.True(t, ok)
		require.Equal(t, [clparams.PtcSize]int8{}, value)
	}
}

func TestNewForkChoiceStoreAnchorCheckpointEpoch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	for _, tc := range []struct {
		name       string
		slot       uint64
		headerSlot uint64
		wantEpoch  uint64
	}{
		{name: "advanced to epoch start", slot: 96, headerSlot: 95, wantEpoch: 3},
		{name: "block post-state", slot: 95, headerSlot: 95, wantEpoch: 3},
		{name: "advanced mid-epoch", slot: 95, headerSlot: 94, wantEpoch: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			anchor := state.New(&cfg)
			require.NoError(t, anchor.SetSlot(tc.slot))
			anchor.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: tc.headerSlot})
			anchorRoot, err := anchor.BlockRoot()
			require.NoError(t, err)

			store, err := NewForkChoiceStore(nil, anchor, nil, pool.OperationsPool{}, nil, nil, nil, nil, public_keys_registry.NewInMemoryPublicKeysRegistry(), nil, false, nil)
			require.NoError(t, err)

			want := solid.Checkpoint{Epoch: tc.wantEpoch, Root: anchorRoot}
			require.Equal(t, want, store.FinalizedCheckpoint())
			require.Equal(t, want, store.JustifiedCheckpoint())
		})
	}
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

	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, committee, selectedPositions, true)
	require.True(t, store.payloadTimeliness(anchorRoot, false))
	require.True(t, store.payloadDataAvailability(anchorRoot, false))
	require.False(t, store.ShouldBuildOnFull(ForkChoiceNode{Root: anchorRoot, PayloadStatus: cltypes.PayloadStatusFull}, 2))
	head, _, err := store.GetHeadNode()
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

	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, committee, selectedPositions, true)
	require.True(t, store.payloadTimeliness(anchorRoot, false))
	require.True(t, store.payloadDataAvailability(anchorRoot, false))
	head, _, err := store.GetHeadNode()
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

	store, anchorRoot, _ := runEmbeddedPtcVoteBlock(t, 0, committee, []int{int(clparams.MaxPtcSize - 1)}, true)
	votes := store.payloadTimelinessVoteValue(anchorRoot)
	require.Equal(t, int8(-1), votes[clparams.MaxPtcSize-1])
	require.Zero(t, votes[clparams.MaxPtcSize-2])
}

func TestOnBlockFromForwardSyncAcceptsPersistedParentEnvelopeWithoutEngineStatus(t *testing.T) {
	store, anchorRoot, _ := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)

	require.True(t, store.HasEnvelope(anchorRoot))
}

func TestKnownForwardSyncBlockRequiresValidatedParentForLiveIngress(t *testing.T) {
	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)

	require.ErrorIs(t, store.ValidateBlockForPublishing(child, false), ErrParentEnvelopePending)
	require.ErrorIs(t, store.OnBlock(t.Context(), child, true, true, false), ErrParentEnvelopePending)

	store.payloadStatusByRoot.Add(anchorRoot, execution_client.PayloadStatusValidated)
	require.NoError(t, store.ValidateBlockForPublishing(child, false))
	require.NoError(t, store.OnBlock(t.Context(), child, true, true, false))
}

func runEmbeddedPtcVoteBlock(
	t *testing.T,
	configuredPtcSize uint64,
	committee []uint64,
	selectedPositions []int,
	payloadValidated bool,
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
	parent := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	parent.Block.Slot = anchor.Slot()
	graph := &embeddedPtcVoteForkGraph{
		getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{
			blocks:     map[common.Hash]*cltypes.SignedBeaconBlock{anchorRoot: parent},
			headers:    map[common.Hash]*cltypes.BeaconBlockHeader{anchorRoot: {Slot: 1}},
			states:     make(map[common.Hash]*state.CachingBeaconState),
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
	store.OnTick(2 * cfg.SecondsPerSlot * 1000)
	if payloadValidated {
		store.payloadStatusByRoot.Add(anchorRoot, execution_client.PayloadStatusValidated)
	}

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

func TestNewForkChoiceStorePreservesAnchorExecutionPayloadBuilderIndex(t *testing.T) {
	for _, tc := range []struct {
		name         string
		version      clparams.StateVersion
		builderIndex uint64
		ok           bool
	}{
		{name: "pre-Gloas", version: clparams.FuluVersion},
		{name: "zero index", version: clparams.GloasVersion, ok: true},
		{name: "self build", version: clparams.GloasVersion, builderIndex: clparams.BuilderIndexSelfBuild, ok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.GloasForkEpoch = 0
			cfg.InitializeForkSchedule()
			anchor := state.New(&cfg)
			anchor.SetVersion(tc.version)
			require.NoError(t, anchor.SetSlot(1))
			if tc.ok {
				anchor.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
					BuilderIndex:       tc.builderIndex,
					BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
				})
			}
			anchorRoot, err := anchor.BlockRoot()
			require.NoError(t, err)
			graph := &getFinalizedExecutionHashForkGraph{
				headers:    map[common.Hash]*cltypes.BeaconBlockHeader{anchorRoot: {Slot: anchor.Slot()}},
				anchorRoot: anchorRoot,
				anchorSlot: anchor.Slot(),
			}
			store, err := NewForkChoiceStore(
				eth_clock.NewEthereumClock(0, common.Hash{}, &cfg),
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

			builderIndex, ok := store.AnchorExecutionPayloadBuilderIndex()
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.builderIndex, builderIndex)
			}
		})
	}
}

func TestNewForkChoiceStoreSeedsAnchorExecutionHash(t *testing.T) {
	legacyHash := common.Hash{0x11}
	latestHash := common.Hash{0x22}
	for _, tc := range []struct {
		name    string
		version clparams.StateVersion
		slot    uint64
		want    common.Hash
	}{
		{name: "Fulu genesis", version: clparams.FuluVersion, want: legacyHash},
		{name: "Gloas genesis", version: clparams.GloasVersion, want: latestHash},
		{name: "Gloas checkpoint", version: clparams.GloasVersion, slot: 32, want: latestHash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.GloasForkEpoch = 0
			cfg.InitializeForkSchedule()
			anchor := state.New(&cfg)
			anchor.SetVersion(tc.version)
			require.NoError(t, anchor.SetSlot(tc.slot))
			legacyHeader := cltypes.NewEth1Header(tc.version)
			legacyHeader.BlockHash = legacyHash
			anchor.SetLatestExecutionPayloadHeader(legacyHeader)
			anchor.SetLatestBlockHash(latestHash)
			anchorRoot, err := anchor.BlockRoot()
			require.NoError(t, err)
			graph := &getFinalizedExecutionHashForkGraph{
				headers:    map[common.Hash]*cltypes.BeaconBlockHeader{anchorRoot: {Slot: tc.slot}},
				anchorRoot: anchorRoot,
				anchorSlot: tc.slot,
			}
			store, err := NewForkChoiceStore(
				eth_clock.NewEthereumClock(0, common.Hash{}, &cfg), anchor, nil,
				pool.NewOperationsPool(&cfg), graph, beaconevents.NewEventEmitter(),
				synced_data.NewSyncedDataManager(&cfg, true), nil,
				public_keys_registry.NewInMemoryPublicKeysRegistry(), validator_params.NewValidatorParams(),
				false, nil,
			)
			require.NoError(t, err)

			require.Equal(t, tc.want, store.GetEth1Hash(anchorRoot))
			require.Equal(t, tc.want, store.GetFinalizedExecutionHash(anchorRoot))
			require.Equal(t, tc.want, store.GetFinalizedExecutionHash(common.Hash{}))
		})
	}
}

type headerOnlyAnchorForkGraph struct {
	fork_graph.ForkGraph
	root common.Hash
	slot uint64
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

	node, slot, err := store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, anchorRoot, node.Root)
	require.Equal(t, cltypes.PayloadStatusPending, node.PayloadStatus)
	require.Equal(t, anchorRoot, store.headHash)
	require.Equal(t, uint64(42), store.headSlot)
	require.Equal(t, uint64(42), slot)
}

func TestGetHeadNodeDoesNotFailDuringCacheInvalidation(t *testing.T) {
	anchorRoot := common.HexToHash("0xa1")
	store := &ForkChoiceStore{
		forkGraph: headerOnlyAnchorForkGraph{root: anchorRoot, slot: 42},
		beaconCfg: &clparams.MainnetBeaconConfig,
	}
	store.justifiedCheckpoint.Store(solid.Checkpoint{Root: common.HexToHash("0xb2")})
	require.NoError(t, func() error {
		_, _, err := store.GetHeadNode()
		return err
	}())

	stop := make(chan struct{})
	var invalidators sync.WaitGroup
	for range 16 {
		invalidators.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				store.mu.Lock()
				store.headHash = common.Hash{}
				store.headPayloadStatus = cltypes.PayloadStatusPending
				store.mu.Unlock()
			}
		})
	}
	t.Cleanup(func() {
		close(stop)
		invalidators.Wait()
	})

	for range 1_000 {
		node, slot, err := store.GetHeadNode()
		require.NoError(t, err)
		require.Equal(t, anchorRoot, node.Root)
		require.Equal(t, uint64(42), slot)
	}
}

func TestBlockProcessingTracksQueuedAndOverlappingImports(t *testing.T) {
	store := &ForkChoiceStore{}
	require.False(t, store.BlockProcessing())
	draining := make(chan struct{})
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	store.queuedEmits = []func(){func() {
		close(draining)
		<-resume
	}}
	store.mu.Lock()
	unlock := sync.OnceFunc(store.mu.Unlock)
	results := make(chan error, 2)
	var imports sync.WaitGroup
	t.Cleanup(func() {
		unlock()
		release()
		imports.Wait()
	})
	for range 2 {
		imports.Go(func() {
			results <- store.OnBlock(t.Context(), nil, false, false, false)
		})
	}

	require.Eventually(t, func() bool { return store.blocksProcessing.Load() == 2 }, 5*time.Second, time.Millisecond)
	require.True(t, store.BlockProcessing(), "imports waiting for the store lock must be visible")
	unlock()
	select {
	case <-draining:
	case <-time.After(5 * time.Second):
		t.Fatal("import did not drain its queued work")
	}
	select {
	case err := <-results:
		require.ErrorContains(t, err, "missing beacon block")
	case <-time.After(5 * time.Second):
		t.Fatal("overlapping import did not finish")
	}
	require.True(t, store.BlockProcessing(), "one completed import must not hide another active import")

	release()
	select {
	case err := <-results:
		require.ErrorContains(t, err, "missing beacon block")
	case <-time.After(5 * time.Second):
		t.Fatal("import did not finish after its queued work")
	}
	require.False(t, store.BlockProcessing(), "early returns must release the import count")
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

func TestAttesterSlashingSeenCheckHandlesHugeIndex(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	store := &ForkChoiceStore{}
	slashing := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{1 << 63}),
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{1 << 63}),
		},
	}

	var err error
	require.NotPanics(t, func() {
		err = store.onProcessAttesterSlashing(slashing, state.New(&cfg), false)
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestAttesterSlashingRejectsWhenNoValidatorIsSlashable(t *testing.T) {
	store, s, slashing := attesterSlashingFixture(t, true)

	err := store.onProcessAttesterSlashing(slashing, s, true)

	require.EqualError(t, err, "no slashable validators in intersection")
	require.False(t, store.operationsPool.AttesterSlashingsPool.Has(pool.ComputeKeyForAttesterSlashing(slashing)))
	require.True(t, store.isUnequivocating(0))
	require.True(t, store.isUnequivocating(1))
}

func TestAttesterSlashingAcceptsWhenAnyValidatorIsSlashable(t *testing.T) {
	store, s, slashing := attesterSlashingFixture(t, false)

	err := store.onProcessAttesterSlashing(slashing, s, true)

	require.NoError(t, err)
	require.True(t, store.operationsPool.AttesterSlashingsPool.Has(pool.ComputeKeyForAttesterSlashing(slashing)))
}

func attesterSlashingFixture(t *testing.T, secondValidatorSlashed bool) (*ForkChoiceStore, *state.CachingBeaconState, *cltypes.AttesterSlashing) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	for _, slashed := range []bool{true, secondValidatorSlashed} {
		validator := solid.NewValidatorFromParameters(
			common.Bytes48{},
			common.Hash{},
			cfg.MaxEffectiveBalance,
			slashed,
			0,
			0,
			cfg.FarFutureEpoch,
			cfg.FarFutureEpoch,
		)
		require.NoError(t, s.AddValidator(validator, cfg.MaxEffectiveBalance))
	}
	slashing := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{0, 1}),
			Data: &solid.AttestationData{
				BeaconBlockRoot: common.Hash{1},
				Target:          solid.Checkpoint{Epoch: 0},
			},
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{0, 1}),
			Data: &solid.AttestationData{
				BeaconBlockRoot: common.Hash{2},
				Target:          solid.Checkpoint{Epoch: 0},
			},
		},
	}
	return &ForkChoiceStore{operationsPool: pool.NewOperationsPool(&cfg)}, s, slashing
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

func TestOnAttesterSlashingIgnoresMissingJustifiedStateWhileSyncing(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	root := common.Hash{1}
	loadErr := errors.New("state storage unavailable")
	slashing := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{1}),
			Data: &solid.AttestationData{
				Slot:   1,
				Target: solid.Checkpoint{Epoch: 1},
			},
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, []uint64{1}),
			Data: &solid.AttestationData{
				Slot:   2,
				Target: solid.Checkpoint{Epoch: 1},
			},
		},
	}
	largeIndices := make([]uint64, int(cfg.MaxValidatorsPerCommittee)+1)
	for i := range largeIndices {
		largeIndices[i] = uint64(i)
	}
	largeSlashing := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(int(cfg.MaxValidatorsPerCommittee*cfg.MaxCommitteesPerSlot), largeIndices),
			Data:             slashing.Attestation_1.Data,
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(int(cfg.MaxValidatorsPerCommittee*cfg.MaxCommitteesPerSlot), largeIndices),
			Data:             slashing.Attestation_2.Data,
		},
	}
	slashings := []struct {
		name     string
		slashing *cltypes.AttesterSlashing
	}{
		{name: "single index", slashing: slashing},
		{name: "electra capacity with pre-electra target", slashing: largeSlashing},
	}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "missing"},
		{name: "load error", err: loadErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &ForkChoiceStore{
				forkGraph:         &getFinalizedExecutionHashForkGraph{getStateErr: tc.err},
				operationsPool:    pool.NewOperationsPool(&cfg),
				syncedDataManager: synced_data.NewSyncedDataManager(&cfg, true),
			}
			store.justifiedCheckpoint.Store(solid.Checkpoint{Root: root})

			for _, slashingCase := range slashings {
				t.Run(slashingCase.name, func(t *testing.T) {
					err := store.OnAttesterSlashing(slashingCase.slashing, true)
					require.ErrorIs(t, err, synced_data.ErrNotSynced)
				})
			}
		})
	}
}

func TestOnAttesterSlashingRejectsInvalidSlashingWhileSyncing(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	root := common.Hash{1}
	data := &solid.AttestationData{}
	for _, slashingCase := range []struct {
		name          string
		slashing      *cltypes.AttesterSlashing
		errorContains string
	}{
		{
			name: "non-slashable data",
			slashing: &cltypes.AttesterSlashing{
				Attestation_1: &cltypes.IndexedAttestation{
					AttestingIndices: solid.NewRawUint64List(2048, []uint64{1}),
					Data:             data,
				},
				Attestation_2: &cltypes.IndexedAttestation{
					AttestingIndices: solid.NewRawUint64List(2048, []uint64{1}),
					Data:             data,
				},
			},
			errorContains: "attestation data is not slashable",
		},
		{
			name: "unsorted indices",
			slashing: &cltypes.AttesterSlashing{
				Attestation_1: &cltypes.IndexedAttestation{
					AttestingIndices: solid.NewRawUint64List(2048, []uint64{2, 1}),
					Data: &solid.AttestationData{
						Slot:   1,
						Target: solid.Checkpoint{Epoch: 1},
					},
				},
				Attestation_2: &cltypes.IndexedAttestation{
					AttestingIndices: solid.NewRawUint64List(2048, []uint64{1}),
					Data: &solid.AttestationData{
						Slot:   2,
						Target: solid.Checkpoint{Epoch: 1},
					},
				},
			},
			errorContains: "attesting indices are not sorted or are null",
		},
	} {
		t.Run(slashingCase.name, func(t *testing.T) {
			loadErr := errors.New("state storage unavailable")
			for _, tc := range []struct {
				name      string
				forkGraph *getFinalizedExecutionHashForkGraph
			}{
				{
					name: "available",
					forkGraph: &getFinalizedExecutionHashForkGraph{
						states: map[common.Hash]*state.CachingBeaconState{root: state.New(&cfg)},
					},
				},
				{name: "missing", forkGraph: &getFinalizedExecutionHashForkGraph{}},
				{name: "load error", forkGraph: &getFinalizedExecutionHashForkGraph{getStateErr: loadErr}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					store := &ForkChoiceStore{
						forkGraph:         tc.forkGraph,
						operationsPool:    pool.NewOperationsPool(&cfg),
						syncedDataManager: synced_data.NewSyncedDataManager(&cfg, true),
					}
					store.justifiedCheckpoint.Store(solid.Checkpoint{Root: root})

					err := store.OnAttesterSlashing(slashingCase.slashing, true)
					require.ErrorContains(t, err, slashingCase.errorContains)
					require.NotErrorIs(t, err, ErrIgnore)
					require.NotErrorIs(t, err, synced_data.ErrNotSynced)
				})
			}
		})
	}
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

func TestCheckpointStatesBoundedWithoutFinalization(t *testing.T) {
	checkpoints := checkpointStateCacheTestCheckpoints(maxCheckpointStates + 1)
	store := newCheckpointStateCacheTestStore(t, checkpoints, solid.Checkpoint{})

	for _, checkpoint := range checkpoints {
		_, err := store.getCheckpointState(checkpoint)
		require.NoError(t, err)
	}

	require.Equal(t, maxCheckpointStates, checkpointStateCacheLen(store))
	_, ok := store.checkpointStates.Load(checkpoints[0])
	require.False(t, ok)
}

func TestCheckpointStatesKeepJustStoredState(t *testing.T) {
	checkpoints := checkpointStateCacheTestCheckpoints(maxCheckpointStates + 1)
	justStored := checkpoints[0]
	store := newCheckpointStateCacheTestStore(t, checkpoints, solid.Checkpoint{})

	for _, checkpoint := range checkpoints[1:] {
		_, err := store.getCheckpointState(checkpoint)
		require.NoError(t, err)
	}
	justStoredState, err := store.getCheckpointState(justStored)
	require.NoError(t, err)

	cached, ok := store.checkpointStates.Load(justStored)
	require.True(t, ok)
	require.Same(t, justStoredState, cached)
	require.Equal(t, maxCheckpointStates, checkpointStateCacheLen(store))
}

func TestCheckpointStatesKeepCurrentJustified(t *testing.T) {
	checkpoints := checkpointStateCacheTestCheckpoints(maxCheckpointStates + 2)
	justified := checkpoints[0]
	store := newCheckpointStateCacheTestStore(t, checkpoints, justified)

	justifiedState, err := store.getCheckpointState(justified)
	require.NoError(t, err)
	for _, checkpoint := range checkpoints[1:] {
		_, err := store.getCheckpointState(checkpoint)
		require.NoError(t, err)
	}

	cached, ok := store.checkpointStates.Load(justified)
	require.True(t, ok)
	require.Same(t, justifiedState, cached)
	require.Equal(t, maxCheckpointStates, checkpointStateCacheLen(store))
}

func TestJustifiedPromotionWaitsForCheckpointStateEviction(t *testing.T) {
	current := solid.Checkpoint{Epoch: 1, Root: common.Hash{1}}
	promoted := solid.Checkpoint{Epoch: 2, Root: common.Hash{2}}
	store := newCheckpointStateCacheTestStore(t, nil, current)
	store.finalizedCheckpoint.Store(solid.Checkpoint{})

	requireWaitsForCheckpointStatesMu(t, store, func() {
		store.updateCheckpoints(promoted, solid.Checkpoint{})
	}, func() {
		require.Equal(t, current, store.justifiedCheckpoint.Load().(solid.Checkpoint))
	})
	require.Equal(t, promoted, store.justifiedCheckpoint.Load().(solid.Checkpoint))
}

func TestCheckpointStateEvictionWaitsForJustifiedPromotion(t *testing.T) {
	checkpoint := checkpointStateCacheTestCheckpoints(1)[0]
	store := newCheckpointStateCacheTestStore(t, []solid.Checkpoint{checkpoint}, solid.Checkpoint{})
	var loadedState *checkpointState
	var err error

	requireWaitsForCheckpointStatesMu(t, store, func() {
		loadedState, err = store.getCheckpointState(checkpoint)
	}, nil)
	require.NoError(t, err)

	cached, ok := store.checkpointStates.Load(checkpoint)
	require.True(t, ok)
	require.Same(t, loadedState, cached)
}

func requireWaitsForCheckpointStatesMu(t *testing.T, store *ForkChoiceStore, fn, whileBlocked func()) {
	t.Helper()
	store.checkpointStatesMu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			store.checkpointStatesMu.Unlock()
		}
	}()

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		fn()
		close(done)
	}()
	<-started

	select {
	case <-done:
		t.Fatal("operation completed while checkpointStatesMu was held")
	case <-time.After(100 * time.Millisecond):
	}
	if whileBlocked != nil {
		whileBlocked()
	}

	store.checkpointStatesMu.Unlock()
	unlocked = true
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("operation did not finish after checkpointStatesMu was released")
	}
}

func checkpointStateCacheTestCheckpoints(count int) []solid.Checkpoint {
	checkpoints := make([]solid.Checkpoint, count)
	for i := range checkpoints {
		checkpoints[i] = solid.Checkpoint{Epoch: uint64(i + 1), Root: common.Hash{byte(i + 1)}}
	}
	return checkpoints
}

func newCheckpointStateCacheTestStore(t *testing.T, checkpoints []solid.Checkpoint, justified solid.Checkpoint) *ForkChoiceStore {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	graph := &getFinalizedExecutionHashForkGraph{states: make(map[common.Hash]*state.CachingBeaconState, len(checkpoints))}
	for _, checkpoint := range checkpoints {
		checkpointState := state.New(&cfg)
		require.NoError(t, checkpointState.SetSlot(checkpoint.Epoch*cfg.SlotsPerEpoch))
		graph.states[checkpoint.Root] = checkpointState
	}
	store := &ForkChoiceStore{
		beaconCfg:          &cfg,
		forkGraph:          graph,
		publicKeysRegistry: public_keys_registry.NewInMemoryPublicKeysRegistry(),
	}
	store.justifiedCheckpoint.Store(justified)
	return store
}

func checkpointStateCacheLen(store *ForkChoiceStore) int {
	count := 0
	store.checkpointStates.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
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
	getStateErr           error
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

func (g *getFinalizedExecutionHashForkGraph) HasBlockEquivocation(uint64, uint64, common.Hash) bool {
	return g.hasBlockEquivocation
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
	require.NoError(t, store.OnBlock(t.Context(), block, false, true, false))
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
	if g.getStateErr != nil {
		return nil, g.getStateErr
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

// get_head attaches a child only under the payload variant its bid names, and offers FULL only
// when the payload is available. At the served head, a child on the other variant counts only
// while that variant is not a candidate.
func TestHasImportedDescendantAfterFollowsTheServedVariant(t *testing.T) {
	newStore := func(t *testing.T, childOnFull bool) (*ForkChoiceStore, common.Hash) {
		store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)
		if !childOnFull {
			anchorBlock, ok := store.forkGraph.GetBlock(anchorRoot)
			require.True(t, ok)
			anchorBlock.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{0xe1}
		}
		want := cltypes.PayloadStatusEmpty
		if childOnFull {
			want = cltypes.PayloadStatusFull
		}
		require.Equal(t, want, store.getParentPayloadStatus(child.Block))
		return store, anchorRoot
	}
	serve := func(store *ForkChoiceStore, root common.Hash, status cltypes.PayloadStatus) {
		store.selectedHead.Store(&ForkChoiceNode{Root: root, PayloadStatus: status})
	}

	t.Run("served EMPTY, child on an available FULL", func(t *testing.T) {
		store, anchorRoot := newStore(t, true)
		store.MarkPayloadStatus(anchorRoot, common.Hash{0xe1}, execution_client.PayloadStatusValidated)
		serve(store, anchorRoot, cltypes.PayloadStatusEmpty)
		require.False(t, store.HasImportedDescendantAfter(anchorRoot, 0))
	})
	t.Run("served EMPTY, child on an unavailable FULL", func(t *testing.T) {
		store, anchorRoot := newStore(t, true)
		serve(store, anchorRoot, cltypes.PayloadStatusEmpty)
		require.True(t, store.HasImportedDescendantAfter(anchorRoot, 0))
	})
	t.Run("served FULL, child on FULL", func(t *testing.T) {
		store, anchorRoot := newStore(t, true)
		serve(store, anchorRoot, cltypes.PayloadStatusFull)
		require.True(t, store.HasImportedDescendantAfter(anchorRoot, 0))
	})
	t.Run("served FULL, child on EMPTY", func(t *testing.T) {
		store, anchorRoot := newStore(t, false)
		serve(store, anchorRoot, cltypes.PayloadStatusFull)
		require.False(t, store.HasImportedDescendantAfter(anchorRoot, 0))
	})
	t.Run("another root served", func(t *testing.T) {
		store, anchorRoot := newStore(t, false)
		serve(store, common.Hash{0x99}, cltypes.PayloadStatusFull)
		require.True(t, store.HasImportedDescendantAfter(anchorRoot, 0))
	})
}

// viabilityTree is a Gloas store whose justified checkpoint is at epoch 1 (root at slot 32), with
// one validator, at slot 128. add attaches a block on the parent's FULL or EMPTY variant.
type viabilityTree struct {
	store     *ForkChoiceStore
	graph     *headSnapshotForkGraph
	justified solid.Checkpoint
}

func newViabilityTree(t testing.TB) *viabilityTree {
	t.Helper()
	store, graph := newHeadSnapshotTestStore()
	justified := store.JustifiedCheckpoint()
	justified.Epoch = 1
	graph.blocks[justified.Root].Block.Slot = 32
	graph.headers[justified.Root].Slot = 32
	store.justifiedCheckpoint.Store(justified)
	store.timeMs.Store(128 * store.beaconCfg.SecondsPerSlot * 1000)
	store.checkpointStates.Store(justified, &checkpointState{
		beaconConfig: store.beaconCfg, validatorSetSize: 1,
		balances: []uint64{32}, actives: []byte{1}, slasheds: []byte{0},
	})
	return &viabilityTree{store: store, graph: graph, justified: justified}
}

// add attaches root at slot on parent's FULL or EMPTY variant. viable sets whether its voting
// source matches the justified checkpoint.
func (v *viabilityTree) add(root, parent common.Hash, slot uint64, full, viable bool) {
	block := cltypes.NewSignedBeaconBlock(v.store.beaconCfg, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = parent
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.BlockHash = root
	parentBid := v.graph.blocks[parent].Block.Body.GetSignedExecutionPayloadBid().Message
	bid.ParentBlockHash = parentBid.ParentBlockHash
	if full {
		bid.ParentBlockHash = parentBid.BlockHash
	}
	v.graph.blocks[root] = block
	v.graph.headers[root] = block.SignedBeaconBlockHeader().Header
	v.store.updateChildren(v.graph.headers[parent].Slot, parent, root)
	source := solid.Checkpoint{}
	if viable {
		source = v.justified
	}
	v.store.unrealizedJustifications.Store(root, source)
}

// A descendant on a branch get_head filters out for its justification does not make the head
// lag; the same descendant with a viable justification does. The served head H/EMPTY at slot 64
// has a viable FULL child at slot 65 and an EMPTY child at slot 98.
func TestHasImportedDescendantAfterSkipsNonViableBranches(t *testing.T) {
	for _, tc := range []struct {
		name            string
		viable, lagging bool
	}{
		{name: "non-viable justification", viable: false, lagging: false},
		{name: "viable justification", viable: true, lagging: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newViabilityTree(t)
			store := v.store
			headRoot, fullChild, emptyChild := common.Hash{0xbb}, common.Hash{0xcc}, common.Hash{0xdd}
			v.add(headRoot, v.justified.Root, 64, true, true)
			v.add(fullChild, headRoot, 65, true, true)
			v.add(emptyChild, headRoot, 98, false, tc.viable)
			store.setLatestMessage(0, LatestMessage{Slot: 100, Root: headRoot})

			filtered := store.getFilteredBlockTree(v.justified.Root, v.justified, store.Slot())
			require.Contains(t, filtered, fullChild)
			require.Equal(t, tc.viable, filtered[emptyChild] != nil)
			store.selectedHead.Store(&ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusEmpty})
			require.Equal(t, tc.lagging, store.HasImportedDescendantAfter(headRoot, 64+store.beaconCfg.SlotsPerEpoch))
		})
	}
}

// A block whose only viable descendant sits on its invalidated payload is not reachable: get_head
// drops its FULL variant, and its EMPTY variant fails justification as a leaf. The served head
// H/EMPTY at slot 64 has an EMPTY child C at slot 98 with a FULL child D at slot 99.
func TestHasImportedDescendantAfterSkipsABlockViableOnlyThroughAnInvalidatedPayload(t *testing.T) {
	for _, tc := range []struct {
		name               string
		invalid, emptyLeaf bool
		lagging            bool
	}{
		{name: "invalidated payload", invalid: true, lagging: false},
		{name: "invalidated payload, viable EMPTY leaf", invalid: true, emptyLeaf: true, lagging: true},
		{name: "payload not invalidated", lagging: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newViabilityTree(t)
			headRoot, c, d := common.Hash{0xbb}, common.Hash{0xc1}, common.Hash{0xd1}
			v.add(headRoot, v.justified.Root, 64, true, true)
			v.add(c, headRoot, 98, false, tc.emptyLeaf)
			v.add(d, c, 99, true, true)
			if tc.invalid {
				v.graph.invalid = map[common.Hash]bool{c: true}
			}
			v.store.selectedHead.Store(&ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusEmpty})

			require.Equal(t, tc.lagging, v.store.HasImportedDescendantAfter(headRoot, 64+v.store.beaconCfg.SlotsPerEpoch))
		})
	}
}

// When get_head finds no viable node, it falls back to the justified root on EMPTY without having
// chosen EMPTY over FULL. The gate then counts the FULL branch too: here an available FULL child
// with an unavailable payload hides a viable grandchild past the tolerance.
func TestHasImportedDescendantAfterCountsBothVariantsOfAFallbackHead(t *testing.T) {
	v := newViabilityTree(t)
	j := v.justified.Root
	c, g := common.Hash{0xc2}, common.Hash{0xd2}
	v.add(c, j, 33, true, false)
	v.add(g, c, 98, true, true)
	v.graph.noEnvelope = map[common.Hash]bool{c: true}
	require.True(t, v.store.isPayloadAvailable(j))
	require.False(t, v.store.isPayloadAvailable(c))

	head, _, err := v.store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: j, PayloadStatus: cltypes.PayloadStatusEmpty}, head, "no viable node: the fallback")

	require.True(t, v.store.HasImportedDescendantAfter(j, 32+v.store.beaconCfg.SlotsPerEpoch))
}

// A changed payload decision can rule out the served variant before get_head runs again; until it
// does, the gate counts the children of both variants.
func TestHasImportedDescendantAfterCountsBothVariantsAfterAPayloadDecisionChanges(t *testing.T) {
	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)
	anchorBlock, ok := store.forkGraph.GetBlock(anchorRoot)
	require.True(t, ok)
	anchorBlock.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{0xe1}
	require.Equal(t, cltypes.PayloadStatusEmpty, store.getParentPayloadStatus(child.Block))
	store.selectedHead.Store(&ForkChoiceNode{Root: anchorRoot, PayloadStatus: cltypes.PayloadStatusFull})
	require.False(t, store.HasImportedDescendantAfter(anchorRoot, 0))

	store.MarkPayloadStatus(anchorRoot, common.Hash{0xe1}, execution_client.PayloadStatusInvalidated)

	require.True(t, store.HasImportedDescendantAfter(anchorRoot, 0), "the FULL head is ruled out, so its EMPTY child counts")
}

func TestGetHeadPublishesTheSelectedNode(t *testing.T) {
	store, _, _ := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)
	head, _, err := store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, &head, store.selectedHead.Load())
}

// The case behind the stuck head: a child imported on the FULL variant of a parent whose payload
// has no recorded status. get_head stays on the parent, and the gate must see the child.
func TestHasImportedDescendantAfterSeesAChildGetHeadCannotReach(t *testing.T) {
	store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)
	require.Equal(t, cltypes.PayloadStatusFull, store.getParentPayloadStatus(child.Block))

	head, headSlot, err := store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, anchorRoot, head.Root)
	require.Less(t, headSlot, child.Block.Slot)
	require.True(t, store.HasImportedDescendantAfter(head.Root, child.Block.Slot-1))
}

// A head get_head selects has no imported descendant past it, on either payload variant.
func TestHasImportedDescendantAfterIsFalseForTheSelectedHead(t *testing.T) {
	for _, onFull := range []bool{true, false} {
		t.Run(fmt.Sprintf("child on FULL=%t", onFull), func(t *testing.T) {
			store, anchorRoot, child := runEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false)
			if onFull {
				store.MarkPayloadStatus(anchorRoot, common.Hash{0xe1}, execution_client.PayloadStatusValidated)
			} else {
				anchorBlock, ok := store.forkGraph.GetBlock(anchorRoot)
				require.True(t, ok)
				anchorBlock.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{0xe1}
			}
			childRoot, err := child.Block.HashSSZ()
			require.NoError(t, err)

			head, headSlot, err := store.GetHeadNode()
			require.NoError(t, err)
			require.Equal(t, common.Hash(childRoot), head.Root)
			require.False(t, store.HasImportedDescendantAfter(head.Root, headSlot))
			require.False(t, store.HasImportedDescendantAfter(head.Root, 0))
		})
	}
}
