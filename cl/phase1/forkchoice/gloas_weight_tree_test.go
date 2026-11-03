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
	"encoding/binary"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

func newGloasWeightTreeTestStore() *ForkChoiceStore {
	f := &ForkChoiceStore{
		beaconCfg:      &clparams.MainnetBeaconConfig,
		latestMessages: newLatestMessagesStore(16),
	}
	f.justifiedCheckpoint.Store(solid.Checkpoint{})
	f.gloasWeightTree = newGloasWeightTree(f)
	return f
}

func TestPreGloasDoesNotDirtyGloasWeightTree(t *testing.T) {
	f := newGloasWeightTreeTestStore()

	att := &solid.Attestation{
		Data: &solid.AttestationData{
			Slot:            64,
			BeaconBlockRoot: common.HexToHash("0xbeef"),
			Target:          solid.Checkpoint{Epoch: 2, Root: common.HexToHash("0xaaaa")},
		},
	}
	f.updateLatestMessagesPreGloas(att, []uint64{1, 2, 3})

	msg, has := f.getLatestMessage(2)
	require.True(t, has)
	require.Equal(t, att.Data.BeaconBlockRoot, msg.Root)
	require.Empty(t, f.gloasWeightTree.applied)
}

func TestPreGloasEquivocationDoesNotDirtyGloasWeightTree(t *testing.T) {
	f := newGloasWeightTreeTestStore()

	f.setUnequivocating(4)

	require.True(t, f.isUnequivocating(4))
	require.Empty(t, f.gloasWeightTree.applied)
}

func TestSetUnequivocatingInvalidatesHeadCache(t *testing.T) {
	f := newGloasWeightTreeTestStore()
	f.headHash = common.HexToHash("0xbeef")
	f.headPayloadStatus = cltypes.PayloadStatusFull

	f.setUnequivocating(4)

	require.Equal(t, common.Hash{}, f.headHash)
	require.Equal(t, cltypes.PayloadStatusPending, f.headPayloadStatus)
}

func TestProcessAttestingIndiciesInvalidatesGloasHeadCacheOnlyForNewMessages(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	f := newGloasWeightTreeTestStore()
	f.beaconCfg = &cfg
	cachedHead := common.HexToHash("0xbeef")
	f.headHash = cachedHead
	f.headPayloadStatus = cltypes.PayloadStatusFull
	attestation := &solid.Attestation{Data: &solid.AttestationData{
		Slot:            1,
		BeaconBlockRoot: common.HexToHash("0xaaaa"),
		Target:          solid.Checkpoint{Epoch: 0},
	}}

	f.ProcessAttestingIndicies(attestation, []uint64{1})

	require.Equal(t, common.Hash{}, f.headHash, "a new latest message must invalidate the cached head")
	require.Equal(t, cltypes.PayloadStatusPending, f.headPayloadStatus)
	f.headHash = cachedHead
	f.headPayloadStatus = cltypes.PayloadStatusFull

	f.ProcessAttestingIndicies(attestation, []uint64{1})

	require.Equal(t, cachedHead, f.headHash, "an unchanged latest message must preserve the cached head")
	require.Equal(t, cltypes.PayloadStatusFull, f.headPayloadStatus)
}

func TestProcessAttestingIndiciesKeepsPreGloasHeadCache(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	f := newGloasWeightTreeTestStore()
	f.beaconCfg = &cfg
	f.headHash = common.HexToHash("0xbeef")
	f.headPayloadStatus = cltypes.PayloadStatusFull
	attestation := &solid.Attestation{Data: &solid.AttestationData{
		Slot:            1,
		BeaconBlockRoot: common.HexToHash("0xaaaa"),
		Target:          solid.Checkpoint{Epoch: 0},
	}}

	f.ProcessAttestingIndicies(attestation, []uint64{1})

	require.Equal(t, common.HexToHash("0xbeef"), f.headHash)
	require.Equal(t, cltypes.PayloadStatusFull, f.headPayloadStatus)
	latestMessage, found := f.getLatestMessage(1)
	require.True(t, found)
	require.Equal(t, attestation.Data.BeaconBlockRoot, latestMessage.Root)
}

func TestGetHeadPublishesCachedSelection(t *testing.T) {
	manager := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	manager.OnSelectedHead(common.Hash{0xaa}, 100)
	store := &ForkChoiceStore{
		headHash:          common.Hash{0xbb},
		headSlot:          99,
		syncedDataManager: manager,
	}

	root, slot, err := store.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, common.Hash{0xbb}, root)
	require.Equal(t, uint64(99), slot)
	selectedRoot, selectedSlot, ok := manager.SelectedHead()
	require.True(t, ok)
	require.Equal(t, root, selectedRoot)
	require.Equal(t, slot, selectedSlot)
}

func TestGetHeadPublishesAnchorFallback(t *testing.T) {
	manager := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	anchorRoot := common.Hash{0xaa}
	store := &ForkChoiceStore{
		beaconCfg:         &clparams.MainnetBeaconConfig,
		forkGraph:         &getFinalizedExecutionHashForkGraph{anchorRoot: anchorRoot, anchorSlot: 10},
		syncedDataManager: manager,
	}
	store.justifiedCheckpoint.Store(solid.Checkpoint{Root: common.Hash{0xbb}})

	root, slot, err := store.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, anchorRoot, root)
	require.Equal(t, uint64(10), slot)
	selectedRoot, selectedSlot, ok := manager.SelectedHead()
	require.True(t, ok)
	require.Equal(t, root, selectedRoot)
	require.Equal(t, slot, selectedSlot)
}

func TestGetHeadNodeRefreshesGloasSelection(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	manager := synced_data.NewSyncedDataManager(&cfg, true)
	root := common.Hash{0xaa}
	checkpoint := solid.Checkpoint{Root: root}
	graph := &getFinalizedExecutionHashForkGraph{
		headers: map[common.Hash]*cltypes.BeaconBlockHeader{
			root: {Slot: 10},
		},
		blocks:           map[common.Hash]*cltypes.SignedBeaconBlock{},
		currentJustified: checkpoint,
	}
	store := newGloasWeightTreeTestStore()
	store.beaconCfg = &cfg
	store.forkGraph = graph
	store.syncedDataManager = manager
	store.justifiedCheckpoint.Store(checkpoint)
	store.finalizedCheckpoint.Store(solid.Checkpoint{})
	store.proposerBoostRoot.Store(common.Hash{})
	store.checkpointStates.Store(checkpoint, &checkpointState{beaconConfig: &cfg})

	selectedRoot, selectedSlot, err := store.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, root, selectedRoot)
	require.Equal(t, uint64(10), selectedSlot)
	publishedRoot, publishedSlot, ok := manager.SelectedHead()
	require.True(t, ok)
	require.Equal(t, selectedRoot, publishedRoot)
	require.Equal(t, selectedSlot, publishedSlot)
	head, _, err := store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusEmpty}, head)

	store.mu.Lock()
	store.headHash = common.Hash{}
	store.headPayloadStatus = cltypes.PayloadStatusPending
	store.mu.Unlock()

	head, _, err = store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusEmpty}, head)

	// A recomputation may select a different root. The old root must not inherit
	// the new head's status, and the published selection must advance with it.
	childRoot := common.Hash{0xbb}
	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	child.Block.Slot = 11
	child.Block.ParentRoot = root
	graph.headers[childRoot] = child.SignedBeaconBlockHeader().Header
	graph.blocks[childRoot] = child
	store.updateChildren(10, root, childRoot)
	store.OnTick(12 * cfg.SecondsPerSlot * 1000)

	head, selectedSlot, err = store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: childRoot, PayloadStatus: cltypes.PayloadStatusEmpty}, head)
	require.Equal(t, uint64(11), selectedSlot)
	publishedRoot, publishedSlot, ok = manager.SelectedHead()
	require.True(t, ok)
	require.Equal(t, childRoot, publishedRoot)
	require.Equal(t, uint64(11), publishedSlot)
}

type countingHeadForkGraph struct {
	*getFinalizedExecutionHashForkGraph
	leafVisits atomic.Int32
}

func (g *countingHeadForkGraph) GetCurrentJustifiedCheckpoint(root common.Hash) (solid.Checkpoint, bool) {
	g.leafVisits.Add(1)
	return g.getFinalizedExecutionHashForkGraph.GetCurrentJustifiedCheckpoint(root)
}

type headSnapshotForkGraph struct {
	*getFinalizedExecutionHashForkGraph
	onGetBlock func()
	invalid    map[common.Hash]bool
	noEnvelope map[common.Hash]bool
}

func (g *headSnapshotForkGraph) IsBlockInvalid(root common.Hash) bool {
	return g.invalid[root]
}

func (g *headSnapshotForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if g.onGetBlock != nil {
		g.onGetBlock()
	}
	return g.getFinalizedExecutionHashForkGraph.GetBlock(root)
}

func (g *headSnapshotForkGraph) HasEnvelope(root common.Hash) bool {
	return g.blocks[root] != nil && !g.noEnvelope[root]
}

func (g *headSnapshotForkGraph) PayloadAccepted(root common.Hash) (bool, bool) {
	return true, g.blocks[root] != nil
}

func newHeadSnapshotTestStore() (*ForkChoiceStore, *headSnapshotForkGraph) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	root := common.Hash{0xaa}
	checkpoint := solid.Checkpoint{Root: root}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = 11
	graph := &headSnapshotForkGraph{getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{
		headers:          map[common.Hash]*cltypes.BeaconBlockHeader{root: block.SignedBeaconBlockHeader().Header},
		blocks:           map[common.Hash]*cltypes.SignedBeaconBlock{root: block},
		currentJustified: checkpoint,
	}}
	store := newGloasWeightTreeTestStore()
	store.beaconCfg = &cfg
	store.forkGraph = graph
	store.justifiedCheckpoint.Store(checkpoint)
	store.finalizedCheckpoint.Store(solid.Checkpoint{})
	store.proposerBoostRoot.Store(common.Hash{})
	store.checkpointStates.Store(checkpoint, &checkpointState{beaconConfig: &cfg})
	store.timeMs.Store(12 * cfg.SecondsPerSlot * 1000)
	return store, graph
}

func TestGloasHeadUsesOneSlotForPayloadDecisions(t *testing.T) {
	for _, test := range []struct {
		name          string
		advanceAtRead int
		weightOnEmpty bool
	}{
		{name: "clock advances during EMPTY tiebreaker", advanceAtRead: 3},
		{name: "clock advances before weights", advanceAtRead: 1, weightOnEmpty: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, graph := newHeadSnapshotTestStore()
			checkpoint := store.justifiedCheckpoint.Load().(solid.Checkpoint)
			if test.weightOnEmpty {
				store.checkpointStates.Store(checkpoint, &checkpointState{
					beaconConfig: store.beaconCfg, validatorSetSize: 1,
					balances: []uint64{32}, actives: []byte{1}, slasheds: []byte{0},
				})
				store.setLatestMessage(0, LatestMessage{Slot: 12, Root: checkpoint.Root})
			}
			reads := 0
			graph.onGetBlock = func() {
				reads++
				if reads == test.advanceAtRead {
					// OnTick can advance time while the head walk holds f.mu. Both
					// variants must use the slot captured before that clock update.
					store.timeMs.Store(13 * store.beaconCfg.SecondsPerSlot * 1000)
				}
			}

			head, slot, err := store.GetHeadNode()

			require.NoError(t, err)
			require.Equal(t, uint64(13), store.Slot(), "the clock must advance during the head walk")
			require.Equal(t, uint64(11), slot)
			require.Equal(t, ForkChoiceNode{Root: checkpoint.Root, PayloadStatus: cltypes.PayloadStatusFull}, head)
		})
	}
}

func TestPreGloasHeadClearsPayloadStatus(t *testing.T) {
	store, _ := newHeadSnapshotTestStore()
	store.beaconCfg.GloasForkEpoch = 1
	store.beaconCfg.InitializeForkSchedule()
	store.headPayloadStatus = cltypes.PayloadStatusFull

	head, _, err := store.GetHeadNode()

	require.NoError(t, err)
	require.Equal(t, cltypes.PayloadStatusPending, head.PayloadStatus)
	require.Equal(t, cltypes.PayloadStatusPending, store.headPayloadStatus)
}

func TestPreGloasHeadReadCrossesGloasBoundary(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		name := "without another head reader"
		if refresh {
			name = "with a newer cached Gloas head"
		}
		t.Run(name, func(t *testing.T) {
			store, graph := newHeadSnapshotTestStore()
			store.beaconCfg.GloasForkEpoch = 1
			store.beaconCfg.InitializeForkSchedule()
			store.publicKeysRegistry = public_keys_registry.NewInMemoryPublicKeysRegistry()
			checkpoint := store.justifiedCheckpoint.Load().(solid.Checkpoint)
			cs, _ := store.checkpointStates.LoadAndDelete(checkpoint)
			store.unrealizedJustifiedCheckpoint.Store(checkpoint)
			store.unrealizedFinalizedCheckpoint.Store(solid.Checkpoint{})
			store.unrealizedJustifications.Store(checkpoint.Root, checkpoint)
			graph.states = map[common.Hash]*state2.CachingBeaconState{checkpoint.Root: state2.New(store.beaconCfg)}
			graph.getStateStarted = make(chan common.Hash, 1)
			graph.getStateRelease = make(chan struct{})
			release := sync.OnceFunc(func() { close(graph.getStateRelease) })
			type result struct {
				head ForkChoiceNode
				err  error
			}
			results := make(chan result, 1)
			var wg sync.WaitGroup
			t.Cleanup(func() {
				release()
				wg.Wait()
			})
			wg.Go(func() {
				head, _, err := store.GetHeadNode()
				results <- result{head: head, err: err}
			})
			select {
			case <-graph.getStateStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("pre-Gloas head read did not reach checkpoint lookup")
			}

			// Cross the fork while the pre-fork reader is outside f.mu for disk I/O.
			store.checkpointStates.Store(checkpoint, cs)
			store.OnTick(store.beaconCfg.SlotsPerEpoch * store.beaconCfg.SecondsPerSlot * 1000)
			want := ForkChoiceNode{Root: checkpoint.Root, PayloadStatus: cltypes.PayloadStatusFull}
			if refresh {
				head, _, err := store.GetHeadNode()
				require.NoError(t, err)
				require.Equal(t, want, head)
			}
			release()
			select {
			case got := <-results:
				require.NoError(t, got.err)
				require.Equal(t, want, got.head)
			case <-time.After(5 * time.Second):
				t.Fatal("head read did not finish")
			}
			wg.Wait()
			require.Equal(t, want.Root, store.headHash)
			require.Equal(t, want.PayloadStatus, store.headPayloadStatus)
		})
	}
}

func TestGloasConcurrentHeadReadsShareRecomputation(t *testing.T) {
	const readers = 8
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	root := common.Hash{0xaa}
	checkpoint := solid.Checkpoint{Root: root}
	graph := &countingHeadForkGraph{getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{
		headers:          map[common.Hash]*cltypes.BeaconBlockHeader{root: {Slot: 10}},
		states:           map[common.Hash]*state2.CachingBeaconState{root: state2.New(&cfg)},
		currentJustified: checkpoint,
		getStateStarted:  make(chan common.Hash, readers),
		getStateRelease:  make(chan struct{}),
	}}
	store := newGloasWeightTreeTestStore()
	store.beaconCfg = &cfg
	store.forkGraph = graph
	store.publicKeysRegistry = public_keys_registry.NewInMemoryPublicKeysRegistry()
	store.justifiedCheckpoint.Store(checkpoint)
	store.finalizedCheckpoint.Store(solid.Checkpoint{})
	store.proposerBoostRoot.Store(common.Hash{})

	var wg sync.WaitGroup
	release := sync.OnceFunc(func() { close(graph.getStateRelease) })
	t.Cleanup(func() {
		release()
		wg.Wait()
	})
	results := make(chan ForkChoiceNode, readers)
	for range readers {
		wg.Go(func() {
			head, _, _ := store.GetHeadNode()
			results <- head
		})
	}
	// Park every reader after its cache miss. Once released, only the first
	// reader needs to walk the tree; the others must reuse that result.
	select {
	case <-graph.getStateStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("head readers did not reach checkpoint lookup")
	}
	require.Never(t, func() bool { return len(graph.getStateStarted) > 0 }, 300*time.Millisecond, 5*time.Millisecond)
	release()
	wg.Wait()
	for range readers {
		require.Equal(t, ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusEmpty}, <-results)
	}
	require.Equal(t, int32(1), graph.leafVisits.Load(), "one cache invalidation must cause only one tree walk")
}

func TestGetHeadNodeUsesPreGloasForkChoice(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	manager := synced_data.NewSyncedDataManager(&cfg, true)
	root := common.Hash{0xaa}
	checkpoint := solid.Checkpoint{Root: root}
	graph := &getFinalizedExecutionHashForkGraph{
		headers: map[common.Hash]*cltypes.BeaconBlockHeader{
			root: {Slot: 10},
		},
		blocks:           map[common.Hash]*cltypes.SignedBeaconBlock{},
		currentJustified: checkpoint,
	}
	store := newGloasWeightTreeTestStore()
	store.beaconCfg = &cfg
	store.forkGraph = graph
	store.syncedDataManager = manager
	store.justifiedCheckpoint.Store(checkpoint)
	store.finalizedCheckpoint.Store(solid.Checkpoint{})
	store.proposerBoostRoot.Store(common.Hash{})
	store.checkpointStates.Store(checkpoint, &checkpointState{beaconConfig: &cfg})

	head, slot, err := store.GetHeadNode()

	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending}, head)
	require.Equal(t, uint64(10), slot)
	publishedRoot, publishedSlot, selected := manager.SelectedHead()
	require.True(t, selected)
	require.Equal(t, head.Root, publishedRoot)
	require.Equal(t, slot, publishedSlot)
}

func TestSetUnequivocatingGrowsAmortized(t *testing.T) {
	f := newGloasWeightTreeTestStore()

	f.setUnequivocating(16)

	require.True(t, f.isUnequivocating(16))
	require.Len(t, f.equivocatingIndicies, 3)
	require.Greater(t, cap(f.equivocatingIndicies), len(f.equivocatingIndicies))
}

func TestGrowGloasContributionsGrowsAmortized(t *testing.T) {
	applied := growGloasContributions(nil, 1)
	require.Len(t, applied, 1)
	require.Equal(t, 1, cap(applied))
	applied[0] = gloasVoteContribution{contribution: 42, set: true}

	applied = growGloasContributions(applied, 2)
	require.Len(t, applied, 2)
	require.Equal(t, 2, cap(applied))
	require.Equal(t, gloasVoteContribution{contribution: 42, set: true}, applied[0])
	require.Equal(t, gloasVoteContribution{}, applied[1])

	applied = growGloasContributions(applied, 3)
	require.Len(t, applied, 3)
	require.Greater(t, cap(applied), len(applied))
	require.Equal(t, gloasVoteContribution{contribution: 42, set: true}, applied[0])
	require.Equal(t, gloasVoteContribution{}, applied[2])
}

func TestGrowGloasContributionsReusesCapacity(t *testing.T) {
	applied := make([]gloasVoteContribution, 1, 3)
	applied[0] = gloasVoteContribution{contribution: 42, set: true}
	first := &applied[0]

	applied = growGloasContributions(applied, 3)

	require.Len(t, applied, 3)
	require.Equal(t, 3, cap(applied))
	require.Same(t, first, &applied[0])
	require.Equal(t, gloasVoteContribution{contribution: 42, set: true}, applied[0])
	require.Equal(t, gloasVoteContribution{}, applied[1])
	require.Equal(t, gloasVoteContribution{}, applied[2])
}

func TestGrowGloasContributionsClearsReexposedCapacity(t *testing.T) {
	backing := []gloasVoteContribution{
		{contribution: 42, set: true},
		{contribution: 64, set: true},
		{contribution: 128, set: true},
	}
	applied := backing[:1]
	first := &applied[0]

	applied = growGloasContributions(applied, len(backing))

	require.Same(t, first, &applied[0])
	require.Equal(t, gloasVoteContribution{contribution: 42, set: true}, applied[0])
	require.Equal(t, gloasVoteContribution{}, applied[1])
	require.Equal(t, gloasVoteContribution{}, applied[2])
}

func TestGrowGloasContributionsNoGrowth(t *testing.T) {
	require.Nil(t, growGloasContributions(nil, 0))

	applied := []gloasVoteContribution{{contribution: 42, set: true}, {contribution: 64}}
	first := &applied[0]

	unchanged := growGloasContributions(applied, len(applied))
	require.Len(t, unchanged, len(applied))
	require.Same(t, first, &unchanged[0])
	require.Equal(t, applied, unchanged)

	notShrunk := growGloasContributions(applied, 1)
	require.Len(t, notShrunk, len(applied))
	require.Same(t, first, &notShrunk[0])
	require.Equal(t, applied, notShrunk)
}

// Before the first prepare there is no applied baseline to delta against, so
// updates are dropped; the first prepare's full rebuild covers every validator.
func TestGloasUpdatesBeforeBaselineAreDropped(t *testing.T) {
	f := newGloasWeightTreeTestStore()

	att := &solid.Attestation{
		Data: &solid.AttestationData{
			Slot:            64,
			BeaconBlockRoot: common.HexToHash("0xbeef"),
			Target:          solid.Checkpoint{Epoch: 2, Root: common.HexToHash("0xaaaa")},
		},
	}
	f.updateLatestMessagesGloas(att, []uint64{1, 2, 3})

	require.Empty(t, f.gloasWeightTree.applied)
}

// Latest messages can outgrow the justified checkpoint's validator registry;
// vote counting must skip those indices instead of panicking.
func TestComputeVotesSkipsMessagesBeyondJustifiedRegistry(t *testing.T) {
	f := newGloasWeightTreeTestStore()
	f.proposerBoostRoot.Store(common.Hash{})
	f.latestMessages.set(20, LatestMessage{Root: common.HexToHash("0x01"), Slot: 5})
	cs := &checkpointState{
		validatorSetSize: 1,
		actives:          make([]byte, 1),
		slasheds:         make([]byte, 1),
		balances:         []uint64{32_000_000_000},
	}

	votes := f.computeVotes(solid.Checkpoint{}, cs, nil)

	require.Empty(t, votes)
}

// Same guarantee for the auxiliary-state branch: ValidatorSet.Get panics on
// out-of-bounds indices, so vote counting must not walk past the registry.
func TestComputeVotesAuxStateSkipsMessagesBeyondValidatorSet(t *testing.T) {
	f := buildExAnteStore(t)
	s, err := f.GetStateAtBlockRoot(f.ProposerBoostRoot())
	require.NoError(t, err)
	require.NotNil(t, s)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)

	f.latestMessages.set(s.ValidatorLength(), LatestMessage{Root: common.HexToHash("0x01"), Slot: 5})

	votes := f.computeVotes(justified, cs, s)

	require.NotNil(t, votes)
}

func TestPreGloasHeadWithAuxiliaryStateDoesNotRequireCheckpointState(t *testing.T) {
	store, _ := newHeadSnapshotTestStore()
	store.beaconCfg.GloasForkEpoch = 100
	store.beaconCfg.InitializeForkSchedule()
	checkpoint := store.justifiedCheckpoint.Load().(solid.Checkpoint)
	store.checkpointStates.Delete(checkpoint)

	head, slot, err := store.GetHead(state2.New(store.beaconCfg))

	require.NoError(t, err)
	require.Equal(t, checkpoint.Root, head)
	require.Equal(t, uint64(11), slot)
}

func TestPreGloasHeadWithAuxiliaryStateDoesNotDependOnCheckpointStateCache(t *testing.T) {
	store, graph := newHeadSnapshotTestStore()
	store.beaconCfg.GloasForkEpoch = 100
	store.beaconCfg.InitializeForkSchedule()
	store.publicKeysRegistry = public_keys_registry.NewInMemoryPublicKeysRegistry()
	checkpoint := solid.Checkpoint{Epoch: 1, Root: common.Hash{0xaa}}
	store.justifiedCheckpoint.Store(checkpoint)
	graph.currentJustified = checkpoint

	checkpointState := state2.New(store.beaconCfg)
	require.NoError(t, checkpointState.SetSlot(store.beaconCfg.SlotsPerEpoch))
	auxiliaryState := state2.New(store.beaconCfg)
	require.NoError(t, auxiliaryState.SetSlot(2*store.beaconCfg.SlotsPerEpoch))
	const effectiveBalance = uint64(32_000_000_000)
	for i := range 80 {
		validator := solid.NewValidator()
		validator.SetPublicKey([48]byte{byte(i + 1)})
		validator.SetActivationEpoch(0)
		validator.SetExitEpoch(store.beaconCfg.FarFutureEpoch)
		validator.SetEffectiveBalance(effectiveBalance)
		require.NoError(t, checkpointState.AddValidator(validator, effectiveBalance))
		require.NoError(t, auxiliaryState.AddValidator(validator, effectiveBalance))
	}
	newlyActive := solid.NewValidator()
	newlyActive.SetPublicKey([48]byte{81})
	newlyActive.SetActivationEpoch(2)
	newlyActive.SetExitEpoch(store.beaconCfg.FarFutureEpoch)
	newlyActive.SetEffectiveBalance(effectiveBalance)
	require.NoError(t, auxiliaryState.AddValidator(newlyActive, effectiveBalance))
	graph.states = map[common.Hash]*state2.CachingBeaconState{checkpoint.Root: checkpointState}

	boostedRoot := common.Hash{0x01}
	nonBoostedRoot := common.Hash{0x02}
	for _, root := range []common.Hash{boostedRoot, nonBoostedRoot} {
		block := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.DenebVersion)
		block.Block.Slot = 2 * store.beaconCfg.SlotsPerEpoch
		block.Block.ParentRoot = checkpoint.Root
		graph.headers[root] = block.SignedBeaconBlockHeader().Header
		graph.blocks[root] = block
		store.updateChildren(11, checkpoint.Root, root)
	}
	store.headSet = map[common.Hash]struct{}{boostedRoot: {}, nonBoostedRoot: {}}
	store.proposerBoostRoot.Store(boostedRoot)
	store.setLatestMessage(0, LatestMessage{Slot: 2 * store.beaconCfg.SlotsPerEpoch, Root: nonBoostedRoot})
	store.timeMs.Store(2 * store.beaconCfg.SlotsPerEpoch * store.beaconCfg.SecondsPerSlot * 1000)
	store.checkpointStates.Delete(checkpoint)

	coldHead, _, err := store.GetHead(auxiliaryState)
	require.NoError(t, err)
	store.headHash = common.Hash{}
	_, err = store.getCheckpointState(checkpoint)
	require.NoError(t, err)
	warmHead, _, err := store.GetHead(auxiliaryState)
	require.NoError(t, err)

	require.Equal(t, nonBoostedRoot, warmHead)
	require.Equal(t, warmHead, coldHead)
}

func TestComputeVotesAuxStateUsesCachedJustifiedBalanceForProposerBoost(t *testing.T) {
	f := newGloasWeightTreeTestStore()
	root := common.HexToHash("0x01")
	checkpoint := solid.Checkpoint{Epoch: 1, Root: common.HexToHash("0x02")}
	f.proposerBoostRoot.Store(root)
	f.checkpointStates.Store(checkpoint, &checkpointState{
		unslashedActiveBalance: 64_000_000_000,
		beaconConfig:           &clparams.MainnetBeaconConfig,
	})
	auxiliaryState := state2.New(&clparams.MainnetBeaconConfig)
	active := solid.NewValidator()
	active.SetActivationEpoch(0)
	active.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
	active.SetEffectiveBalance(32_000_000_000)
	require.NoError(t, auxiliaryState.AddValidator(active, 32_000_000_000))

	votes := f.computeVotes(checkpoint, nil, auxiliaryState)

	require.Equal(t, uint64(800_000_000), votes[root])
}

func TestComputeVotesAuxStateExcludesSlashedBalanceFromProposerBoost(t *testing.T) {
	f := newGloasWeightTreeTestStore()
	root := common.HexToHash("0x01")
	f.proposerBoostRoot.Store(root)
	checkpointState := &checkpointState{
		unslashedActiveBalance: 96_000_000_000,
		beaconConfig:           &clparams.MainnetBeaconConfig,
	}
	auxiliaryState := state2.New(&clparams.MainnetBeaconConfig)
	active := solid.NewValidator()
	active.SetActivationEpoch(0)
	active.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
	active.SetEffectiveBalance(32_000_000_000)
	require.NoError(t, auxiliaryState.AddValidator(active, 32_000_000_000))
	slashed := solid.NewValidator()
	slashed.SetActivationEpoch(0)
	slashed.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
	slashed.SetEffectiveBalance(64_000_000_000)
	slashed.SetSlashed(true)
	require.NoError(t, auxiliaryState.AddValidator(slashed, 64_000_000_000))

	votes := f.computeVotes(solid.Checkpoint{}, checkpointState, auxiliaryState)

	require.Equal(t, uint64(400_000_000), votes[root])
}

func TestVoteSampleBoundsUsesClampedCount(t *testing.T) {
	startIdx, step := voteSampleBounds(0, true, rand.New(rand.NewSource(0)))
	require.Equal(t, 0, startIdx)
	require.Equal(t, 1, step)

	for count := 1; count < sampleBasis; count++ {
		startIdx, step = voteSampleBounds(count, true, rand.New(rand.NewSource(int64(count))))

		require.GreaterOrEqual(t, startIdx, 0)
		require.Less(t, startIdx, count)
		require.GreaterOrEqual(t, step, sampleBasis)
	}

	for count := sampleBasis; count < sampleBasis*3; count++ {
		startIdx, step = voteSampleBounds(count, true, rand.New(rand.NewSource(int64(count))))

		require.GreaterOrEqual(t, startIdx, 0)
		require.Less(t, startIdx, sampleBasis)
		require.GreaterOrEqual(t, step, sampleBasis)
	}
}

func TestComputeVotesProbabilisticAuxStateUsesClampedCount(t *testing.T) {
	f := newGloasWeightTreeTestStore()
	f.probabilisticHeadGetter = true
	f.proposerBoostRoot.Store(common.Hash{})
	root := common.HexToHash("0x01")
	f.latestMessages.set(0, LatestMessage{Root: root, Slot: 5})

	s := state2.New(&clparams.MainnetBeaconConfig)
	v := solid.NewValidator()
	v.SetActivationEpoch(0)
	v.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
	v.SetEffectiveBalance(32_000_000_000)
	require.NoError(t, s.AddValidator(v, 32_000_000_000))

	votes := f.computeVotes(solid.Checkpoint{}, &checkpointState{}, s)

	require.Equal(t, uint64(32_000_000_000), votes[root])
}

func decodeDiffBlock(t *testing.T, enc []byte) (*cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	b := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(b, enc, int(clparams.AltairVersion)))
	root, err := b.Block.HashSSZ()
	require.NoError(t, err)
	return b, root
}

func TestGloasWeightTreePrepareIsIdempotent(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	tree := f.gloasWeightTree.prepare(justified, cs)
	root := justified.Root
	first := tree.GetAttestationScore(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending})
	tree = f.gloasWeightTree.prepare(justified, cs)
	second := tree.GetAttestationScore(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending})

	require.Equal(t, first, second)
	require.NotZero(t, first)
}

func TestGloasWeightTreePayloadStatusChangeSkipsDirectRebuild(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.gloasWeightTree.prepare(justified, cs)
	children := f.gloasWeightTree.nodes[justified.Root].children
	require.NotEmpty(t, children)

	childNode := f.gloasWeightTree.nodes[children[0]]
	actualStatus := childNode.parentPayloadStatus
	childNode.parentPayloadStatus = cltypes.PayloadStatusPending
	if actualStatus == cltypes.PayloadStatusPending {
		childNode.parentPayloadStatus = cltypes.PayloadStatusEmpty
	}

	f.gloasWeightTree.ensureTopology(justified.Root)
	require.Equal(t, actualStatus, childNode.parentPayloadStatus)
	require.False(t, f.gloasWeightTree.allDirty)
}

func TestGloasWeightTreeEquivocationDeltaMatchesFullScan(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	tree := f.gloasWeightTree.prepare(justified, cs)
	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}
	require.Equal(t, newWeightStoreFromCheckpointState(f, cs).GetAttestationScore(node), tree.GetAttestationScore(node))

	var validatorIndex uint64
	found := false
	for i, applied := range f.gloasWeightTree.applied {
		if applied.set && f.isAncestor(f.getSupportedNode(applied.message), node) {
			validatorIndex = uint64(i)
			found = true
			break
		}
	}
	require.True(t, found)

	f.setUnequivocating(validatorIndex)
	tree = f.gloasWeightTree.prepare(justified, cs)

	require.Equal(t, newWeightStoreFromCheckpointState(f, cs).GetAttestationScore(node), tree.GetAttestationScore(node))
}

func gloasDirectWeights(t *gloasWeightTree) map[common.Hash][3]uint64 {
	weights := make(map[common.Hash][3]uint64, len(t.nodes))
	for root, node := range t.nodes {
		weights[root] = [3]uint64{node.directPending, node.directEmpty, node.directFull}
	}
	return weights
}

// Once the tree has a baseline, every recorded vote or equivocation must already be counted,
// matching a full rebuild, so get_head has no per-validator work left.
func TestGloasWeightTreeAppliesVotesWhenRecorded(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.gloasWeightTree.prepare(justified, cs)
	roots := []common.Hash{common.HexToHash("0xdeadbeef")}
	for root := range f.gloasWeightTree.nodes {
		roots = append(roots, root)
	}
	require.Greater(t, len(roots), 2)
	rng := rand.New(rand.NewSource(1))
	for step := range 500 {
		vi := uint64(rng.Intn(cs.validatorSetSize + 8))
		if rng.Intn(10) == 0 {
			f.setUnequivocating(vi)
		} else {
			f.setLatestMessage(vi, LatestMessage{
				Slot:           uint64(rng.Intn(64)),
				Root:           roots[rng.Intn(len(roots))],
				PayloadPresent: rng.Intn(2) == 0,
			})
		}
		want := newGloasWeightTree(f)
		want.prepare(justified, cs)
		require.Equal(t, gloasDirectWeights(want), gloasDirectWeights(f.gloasWeightTree), "step %d", step)
	}

	tree := f.gloasWeightTree.prepare(justified, cs)
	fullScan := newWeightStoreFromCheckpointState(f, cs)
	for root := range f.gloasWeightTree.topologySeen {
		for _, status := range []cltypes.PayloadStatus{cltypes.PayloadStatusPending, cltypes.PayloadStatusEmpty, cltypes.PayloadStatusFull} {
			node := ForkChoiceNode{Root: root, PayloadStatus: status}
			require.Equal(t, fullScan.GetAttestationScore(node), tree.GetAttestationScore(node), "%v", node)
		}
	}
}

// Votes recorded between prepares must match a full rebuild across finalization pruning,
// checkpoint-state switches and a voted block that joins the tree later.
func TestGloasWeightTreeMatchesRebuildAcrossPrepares(t *testing.T) {
	f, pending := buildExAnteStorePendingLast(t, nil)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)
	pendingRoot, err := pending.Block.HashSSZ()
	require.NoError(t, err)
	next := *cs
	next.balances = append([]uint64(nil), cs.balances...)
	for i := range next.balances {
		next.balances[i] += uint64(i%3) * 1_000_000_000
	}
	states := []*checkpointState{cs, &next}
	current := cs

	f.mu.Lock()
	f.gloasWeightTree.prepare(justified, current)
	roots := []common.Hash{pendingRoot}
	for root := range f.gloasWeightTree.nodes {
		roots = append(roots, root)
	}
	require.NotContains(t, f.gloasWeightTree.nodes, common.Hash(pendingRoot))
	check := func(step int) {
		f.gloasWeightTree.prepare(justified, current)
		want := newGloasWeightTree(f)
		want.prepare(justified, current)
		require.Equal(t, gloasDirectWeights(want), gloasDirectWeights(f.gloasWeightTree), "step %d", step)
	}
	rng := rand.New(rand.NewSource(3))
	for step := range 400 {
		if step == 200 {
			check(step)
			for range 20 {
				f.setLatestMessage(uint64(rng.Intn(cs.validatorSetSize)), LatestMessage{Slot: 60, Root: pendingRoot})
			}
			f.mu.Unlock()
			require.NoError(t, f.OnBlock(t.Context(), pending, false, true, false))
			f.mu.Lock()
			check(step)
		}
		vi := uint64(rng.Intn(cs.validatorSetSize + 8))
		switch r := rng.Intn(20); {
		case r == 0:
			f.gloasWeightTree.pruneFinalized(1)
		case r == 1:
			current = states[rng.Intn(len(states))]
		case r < 5:
			check(step)
		case r < 7:
			f.setUnequivocating(vi)
		default:
			f.setLatestMessage(vi, LatestMessage{
				Slot:           uint64(rng.Intn(64)),
				Root:           roots[rng.Intn(len(roots))],
				PayloadPresent: rng.Intn(2) == 0,
			})
		}
	}
	check(400)
	require.Contains(t, f.gloasWeightTree.nodes, common.Hash(pendingRoot))
	f.mu.Unlock()
}

func TestGloasWeightTreeFirstPrepareIncludesPreReadyEquivocation(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}
	var validatorIndex uint64
	found := false
	for i := 0; i < f.latestMessages.latestMessagesCount(); i++ {
		message, has := f.latestMessages.get(i)
		if has && message != (LatestMessage{}) && f.isAncestor(f.getSupportedNode(message), node) {
			validatorIndex = uint64(i)
			found = true
			break
		}
	}
	require.True(t, found)

	f.setUnequivocating(validatorIndex)
	require.Empty(t, f.gloasWeightTree.applied)

	tree := f.gloasWeightTree.prepare(justified, cs)

	require.Equal(t, newWeightStoreFromCheckpointState(f, cs).GetAttestationScore(node), tree.GetAttestationScore(node))
}

func TestGloasWeightTreeRebuildsOnCheckpointStateChange(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	tree := f.gloasWeightTree.prepare(justified, cs)
	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}
	first := tree.GetAttestationScore(node)
	require.NotZero(t, first)

	next := *cs
	next.balances = append([]uint64(nil), cs.balances...)
	for i, applied := range f.gloasWeightTree.applied {
		if applied.set {
			next.balances[i] += 1
			break
		}
	}
	tree = f.gloasWeightTree.prepare(justified, &next)

	require.Greater(t, tree.GetAttestationScore(node), first)
}

func TestGloasWeightTreeNilCheckpointStateReturnsZero(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}
	tree := f.gloasWeightTree.prepare(justified, cs)
	require.NotZero(t, tree.GetAttestationScore(node))

	tree = f.gloasWeightTree.prepare(justified, nil)

	require.Zero(t, tree.GetAttestationScore(node))
	require.Zero(t, tree.GetWeight(node, f.Slot()))
}

func TestGloasWeightTreeRecomputeDeepChain(t *testing.T) {
	f := newGloasWeightTreeTestStore()
	tree := f.gloasWeightTree
	const depth = 10_000

	root := testRoot(1)
	current := root
	for i := uint64(1); i <= depth; i++ {
		next := testRoot(i + 1)
		node := tree.nodes[current]
		if node == nil {
			node = &gloasWeightNode{}
			tree.nodes[current] = node
		}
		node.children = []common.Hash{next}
		node.directPending = 1
		child := tree.nodes[next]
		if child == nil {
			child = &gloasWeightNode{}
			tree.nodes[next] = child
		}
		child.parentPayloadStatus = cltypes.PayloadStatusFull
		child.directPending = 1
		current = next
	}

	tree.recompute(root)

	require.Equal(t, uint64(depth+1), tree.nodes[root].pendingWeight)
	require.Equal(t, uint64(depth), tree.nodes[root].fullWeight)
}

func testRoot(i uint64) common.Hash {
	var root common.Hash
	binary.BigEndian.PutUint64(root[24:], i)
	return root
}

func TestGloasWeightTreeIgnoresStaleChildEdges(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	staleChild := common.HexToHash("0xdead")
	f.updateChildren(0, justified.Root, staleChild)
	f.gloasWeightTree.prepare(justified, cs)

	require.NotContains(t, f.gloasWeightTree.nodes[justified.Root].children, staleChild)
}

func TestGloasWeightTreeTracksVotesForMissingRoots(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)
	_, missingRoot := decodeDiffBlock(t, diffBlockc2Enc)

	f.mu.Lock()
	defer f.mu.Unlock()

	var validatorIndex uint64
	found := false
	for i := 0; i < cs.validatorSetSize; i++ {
		if readFromBitset(cs.actives, i) && !readFromBitset(cs.slasheds, i) && cs.balances[i] > 0 {
			validatorIndex = uint64(i)
			found = true
			break
		}
	}
	require.True(t, found)

	f.gloasWeightTree.state = cs
	f.gloasWeightTree.allDirty = false
	delete(f.gloasWeightTree.nodes, missingRoot)
	f.setLatestMessage(validatorIndex, LatestMessage{Root: missingRoot})
	require.Contains(t, f.gloasWeightTree.missingRootVotes[missingRoot], validatorIndex)
	require.False(t, f.gloasWeightTree.applied[validatorIndex].direct)

	f.gloasWeightTree.ensureTopology(justified.Root)
	require.NotContains(t, f.gloasWeightTree.missingRootVotes, missingRoot)
	require.Contains(t, f.gloasWeightTree.nodes, missingRoot)
	require.True(t, f.gloasWeightTree.applied[validatorIndex].direct)
	require.Equal(t, cs.balances[validatorIndex], f.gloasWeightTree.nodes[missingRoot].directPending)
	require.False(t, f.gloasWeightTree.allDirty)
}

func TestGloasWeightTreeClearsMissingRootVoteWhenValidatorMovesAway(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	missingRoot := common.HexToHash("0xdeadbeef")
	_, liveRoot := decodeDiffBlock(t, diffBlockc2Enc)
	var validatorIndex uint64
	found := false
	for i := 0; i < cs.validatorSetSize; i++ {
		if readFromBitset(cs.actives, i) && !readFromBitset(cs.slasheds, i) && cs.balances[i] > 0 {
			validatorIndex = uint64(i)
			found = true
			break
		}
	}
	require.True(t, found)

	f.gloasWeightTree.state = cs
	f.gloasWeightTree.allDirty = false
	f.setLatestMessage(validatorIndex, LatestMessage{Root: missingRoot})
	require.Contains(t, f.gloasWeightTree.missingRootVotes[missingRoot], validatorIndex)

	f.gloasWeightTree.nodes[liveRoot] = &gloasWeightNode{}
	f.setLatestMessage(validatorIndex, LatestMessage{Root: liveRoot})

	require.NotContains(t, f.gloasWeightTree.missingRootVotes, missingRoot)
	require.True(t, f.gloasWeightTree.applied[validatorIndex].direct)
	require.Equal(t, cs.balances[validatorIndex], f.gloasWeightTree.nodes[liveRoot].directPending)

	f.gloasWeightTree.updateMissingRootVotes(missingRoot)
	require.Equal(t, cs.balances[validatorIndex], f.gloasWeightTree.nodes[liveRoot].directPending)
}

func TestOnNewFinalizedPrunesGloasWeightTree(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	_, rootC2 := decodeDiffBlock(t, diffBlockc2Enc)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.gloasWeightTree.prepare(justified, cs)
	require.Contains(t, f.gloasWeightTree.nodes, rootC2)

	f.onNewFinalized(solid.Checkpoint{Epoch: 1, Root: rootC2})

	require.NotContains(t, f.gloasWeightTree.nodes, rootC2)
	require.True(t, f.gloasWeightTree.allDirty)
}

func TestGloasWeightTreePruneFinalizedDropsBoundaryAndUnknownRoots(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	_, rootC2 := decodeDiffBlock(t, diffBlockc2Enc)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.gloasWeightTree.prepare(justified, cs)
	require.Contains(t, f.gloasWeightTree.nodes, rootC2)
	header, hasHeader := f.forkGraph.GetHeader(rootC2)
	require.True(t, hasHeader)
	require.NotZero(t, header.Slot)

	f.gloasWeightTree.pruneFinalized(header.Slot - 1)
	require.Contains(t, f.gloasWeightTree.nodes, rootC2)

	unknownRoot := common.HexToHash("0xdeadbeef")
	f.gloasWeightTree.nodes[unknownRoot] = &gloasWeightNode{}

	f.gloasWeightTree.pruneFinalized(header.Slot)

	require.NotContains(t, f.gloasWeightTree.nodes, rootC2)
	require.NotContains(t, f.gloasWeightTree.nodes, unknownRoot)
	require.True(t, f.gloasWeightTree.allDirty)
}

func TestApplyWeightDeltaDoesNotUnderflow(t *testing.T) {
	require.Equal(t, uint64(13), applyWeightDelta(10, 3, true))
	require.Equal(t, uint64(7), applyWeightDelta(10, 3, false))
	require.Zero(t, applyWeightDelta(3, 10, false))
}

func TestGetHeadNodeReturnsCachedSnapshot(t *testing.T) {
	headRoot := common.Hash{0x41}
	store := &ForkChoiceStore{
		headHash:          headRoot,
		headPayloadStatus: cltypes.PayloadStatusFull,
	}

	head, _, err := store.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusFull}, head)
}
