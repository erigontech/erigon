package forkchoice

import (
	"sync/atomic"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type countingGloasHeadForkGraph struct {
	*getFinalizedExecutionHashForkGraph
	getBlocks atomic.Int64
}

type flippingEnvelopeForkGraph struct {
	*getFinalizedExecutionHashForkGraph
	flipRoot common.Hash
	calls    int
}

type nodeFilterForkGraph struct {
	*getFinalizedExecutionHashForkGraph
	votingSources map[common.Hash]solid.Checkpoint
	available     map[common.Hash]bool
}

func (g *nodeFilterForkGraph) GetCurrentJustifiedCheckpoint(root common.Hash) (solid.Checkpoint, bool) {
	checkpoint, ok := g.votingSources[root]
	return checkpoint, ok
}

func (g *nodeFilterForkGraph) HasEnvelope(root common.Hash) bool {
	if g.available != nil {
		return g.available[root]
	}
	return g.blocks[root] != nil
}

func (g *nodeFilterForkGraph) PayloadAccepted(root common.Hash) (bool, bool) {
	return true, g.blocks[root] != nil
}

func (g *flippingEnvelopeForkGraph) HasEnvelope(root common.Hash) bool {
	if root == g.flipRoot {
		g.calls++
		return g.calls == 1
	}
	return g.blocks[root] != nil
}

func (g *flippingEnvelopeForkGraph) PayloadAccepted(root common.Hash) (bool, bool) {
	return true, g.blocks[root] != nil
}

func (g *countingGloasHeadForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	g.getBlocks.Add(1)
	return g.getFinalizedExecutionHashForkGraph.GetBlock(root)
}

func (g *countingGloasHeadForkGraph) HasEnvelope(root common.Hash) bool {
	return g.blocks[root] != nil
}

func (g *countingGloasHeadForkGraph) PayloadAccepted(root common.Hash) (bool, bool) {
	return true, g.blocks[root] != nil
}

func buildGloasHeadChain(t *testing.T, blockCount int) (*ForkChoiceStore, *countingGloasHeadForkGraph, solid.Checkpoint, common.Hash) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	graph := &countingGloasHeadForkGraph{getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{
		headers: make(map[common.Hash]*cltypes.BeaconBlockHeader, blockCount),
		blocks:  make(map[common.Hash]*cltypes.SignedBeaconBlock, blockCount),
	}}
	store := newGloasWeightTreeTestStore()
	store.beaconCfg = &cfg
	store.forkGraph = graph
	startSlot := uint64(2) * cfg.SlotsPerEpoch
	var firstRoot common.Hash
	var parentRoot common.Hash
	var parentBlockHash common.Hash
	var tipRoot common.Hash
	for i := range blockCount {
		block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
		block.Block.Slot = startSlot + uint64(i)
		block.Block.ParentRoot = parentRoot
		blockHash := common.Hash{byte(i), byte(i >> 8), byte(i >> 16), 0xee}
		block.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
			ParentBlockHash: parentBlockHash,
			BlockHash:       blockHash,
		}}
		root := common.Hash{byte(i), byte(i >> 8), byte(i >> 16), 0x01}
		graph.headers[root] = block.SignedBeaconBlockHeader().Header
		graph.blocks[root] = block
		if i == 0 {
			firstRoot = root
		} else {
			store.updateChildren(block.Block.Slot-1, parentRoot, root)
		}
		parentRoot = root
		parentBlockHash = blockHash
		tipRoot = root
	}
	finalized := solid.Checkpoint{Root: firstRoot, Epoch: 2}
	justifiedRoot := common.Hash{byte(blockCount - 64), byte((blockCount - 64) >> 8), byte((blockCount - 64) >> 16), 0x01}
	justifiedHeader := graph.headers[justifiedRoot]
	justified := solid.Checkpoint{Root: justifiedRoot, Epoch: justifiedHeader.Slot / cfg.SlotsPerEpoch}
	graph.currentJustified = justified
	for root := range graph.blocks {
		store.unrealizedJustifications.Store(root, justified)
	}
	store.justifiedCheckpoint.Store(justified)
	store.finalizedCheckpoint.Store(finalized)
	store.proposerBoostRoot.Store(common.Hash{})
	store.checkpointStates.Store(justified, &checkpointState{beaconConfig: &cfg})
	store.timeMs.Store((startSlot + uint64(blockCount) + 1) * cfg.SecondsPerSlot * 1000)
	return store, graph, justified, tipRoot
}

func TestGloasHeadFilterBoundsForkGraphWork(t *testing.T) {
	const blockCount = 1024
	store, graph, justified, tipRoot := buildGloasHeadChain(t, blockCount)
	checkpointState, err := store.getCheckpointState(justified)
	require.NoError(t, err)
	graph.getBlocks.Store(0)

	store.mu.Lock()
	head, slot, err := store.computeHeadGloas(justified, checkpointState)
	store.mu.Unlock()

	require.NoError(t, err)
	require.Equal(t, tipRoot, head.Root)
	require.Equal(t, graph.headers[tipRoot].Slot, slot)
	require.LessOrEqual(t, graph.getBlocks.Load(), int64(8*(blockCount+64)))
}

func TestGloasHeadUsesFilteredChildrenWhenEnvelopeAvailabilityChanges(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	root, childRoot := common.Hash{0x10}, common.Hash{0x11}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = cfg.SlotsPerEpoch
	block.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{BlockHash: common.Hash{0xe0}}}
	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	child.Block.Slot = block.Block.Slot + 1
	child.Block.ParentRoot = root
	child.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		ParentBlockHash: common.Hash{0xe0},
		BlockHash:       common.Hash{0xe1},
	}}
	justified := solid.Checkpoint{Root: root, Epoch: 1}
	graph := &flippingEnvelopeForkGraph{
		getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{
			headers: map[common.Hash]*cltypes.BeaconBlockHeader{
				root:      block.SignedBeaconBlockHeader().Header,
				childRoot: child.SignedBeaconBlockHeader().Header,
			},
			blocks: map[common.Hash]*cltypes.SignedBeaconBlock{
				root:      block,
				childRoot: child,
			},
			currentJustified: justified,
		},
		flipRoot: root,
	}
	store := newGloasWeightTreeTestStore()
	store.beaconCfg = &cfg
	store.forkGraph = graph
	store.updateChildren(block.Block.Slot, root, childRoot)
	store.unrealizedJustifications.Store(childRoot, justified)
	store.justifiedCheckpoint.Store(justified)
	store.finalizedCheckpoint.Store(justified)
	store.proposerBoostRoot.Store(common.Hash{})
	store.checkpointStates.Store(justified, &checkpointState{beaconConfig: &cfg})
	store.timeMs.Store(100 * cfg.SecondsPerSlot * 1000)

	head, slot, err := store.GetHeadNode()

	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: childRoot, PayloadStatus: cltypes.PayloadStatusFull}, head)
	require.Equal(t, child.Block.Slot, slot)
}

func TestGloasHeadReturnsEmptyJustifiedNodeWhenNoNodeIsViable(t *testing.T) {
	store, root, _ := newNodeFilterTestStore(t, cltypes.PayloadStatusPending, 2)

	head, slot, err := store.GetHeadNode()

	require.NoError(t, err)
	require.Equal(t, ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusEmpty}, head)
	require.Equal(t, uint64(128), slot)
}

func TestGloasHeadFiltersNodeVariantsIndependently(t *testing.T) {
	for _, test := range []struct {
		name              string
		childParentStatus cltypes.PayloadStatus
		currentSlotDelta  uint64
		want              cltypes.PayloadStatus
	}{
		{
			name:              "non-viable FULL subtree leaves EMPTY viable",
			childParentStatus: cltypes.PayloadStatusFull,
			currentSlotDelta:  2,
			want:              cltypes.PayloadStatusEmpty,
		},
		{
			name:              "non-viable EMPTY subtree leaves FULL viable",
			childParentStatus: cltypes.PayloadStatusEmpty,
			currentSlotDelta:  1,
			want:              cltypes.PayloadStatusFull,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, root, _ := newNodeFilterTestStore(t, test.childParentStatus, test.currentSlotDelta)

			head, slot, err := store.GetHeadNode()

			require.NoError(t, err)
			require.Equal(t, ForkChoiceNode{Root: root, PayloadStatus: test.want}, head)
			require.Equal(t, uint64(128), slot)
		})
	}
}

func TestGloasHeadFinalizedCheckpointWhenJustifiedRootPredatesEpochStart(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	baseRoot := common.Hash{0x30}
	directRoot := common.Hash{0x31}
	intermediateRoot := common.Hash{0x32}
	descendantRoot := common.Hash{0x33}
	newBlock := func(slot uint64, parentRoot common.Hash, blockHash byte) *cltypes.SignedBeaconBlock {
		block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
		block.Block.Slot = slot
		block.Block.ParentRoot = parentRoot
		block.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
			ParentBlockHash: common.Hash{0xff},
			BlockHash:       common.Hash{blockHash},
		}}
		return block
	}
	base := newBlock(100, common.Hash{}, 0xa0)
	direct := newBlock(130, baseRoot, 0xa1)
	intermediate := newBlock(120, baseRoot, 0xa2)
	descendant := newBlock(130, intermediateRoot, 0xa3)
	graph := &nodeFilterForkGraph{
		getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{
			headers: map[common.Hash]*cltypes.BeaconBlockHeader{
				baseRoot:         base.SignedBeaconBlockHeader().Header,
				directRoot:       direct.SignedBeaconBlockHeader().Header,
				intermediateRoot: intermediate.SignedBeaconBlockHeader().Header,
				descendantRoot:   descendant.SignedBeaconBlockHeader().Header,
			},
			blocks: map[common.Hash]*cltypes.SignedBeaconBlock{
				baseRoot:         base,
				directRoot:       direct,
				intermediateRoot: intermediate,
				descendantRoot:   descendant,
			},
		},
		votingSources: make(map[common.Hash]solid.Checkpoint),
		available: map[common.Hash]bool{
			directRoot:       true,
			intermediateRoot: true,
			descendantRoot:   true,
		},
	}
	store := newGloasWeightTreeTestStore()
	store.beaconCfg = &cfg
	store.forkGraph = graph
	store.updateChildren(base.Block.Slot, baseRoot, directRoot)
	store.updateChildren(base.Block.Slot, baseRoot, intermediateRoot)
	store.updateChildren(intermediate.Block.Slot, intermediateRoot, descendantRoot)
	justified := solid.Checkpoint{Root: baseRoot, Epoch: 5}
	store.justifiedCheckpoint.Store(justified)
	store.finalizedCheckpoint.Store(solid.Checkpoint{Root: baseRoot, Epoch: 4})
	store.proposerBoostRoot.Store(common.Hash{})
	store.checkpointStates.Store(justified, &checkpointState{beaconConfig: &cfg})
	store.timeMs.Store(6 * cfg.SlotsPerEpoch * cfg.SecondsPerSlot * 1000)
	for root := range graph.blocks {
		store.unrealizedJustifications.Store(root, justified)
	}

	head, slot, err := store.GetHeadNode()

	require.NoError(t, err)
	require.Equal(t, directRoot, head.Root)
	require.Equal(t, direct.Block.Slot, slot)
}

func newNodeFilterTestStore(t *testing.T, childParentStatus cltypes.PayloadStatus, currentSlotDelta uint64) (*ForkChoiceStore, common.Hash, common.Hash) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	root, childRoot := common.Hash{0x20}, common.Hash{0x21}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = 4 * cfg.SlotsPerEpoch
	blockHash := common.Hash{0xe0}
	block.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{BlockHash: blockHash}}
	graph := &nodeFilterForkGraph{
		getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{
			headers: map[common.Hash]*cltypes.BeaconBlockHeader{root: block.SignedBeaconBlockHeader().Header},
			blocks:  map[common.Hash]*cltypes.SignedBeaconBlock{root: block},
		},
		votingSources: make(map[common.Hash]solid.Checkpoint),
	}
	store := newGloasWeightTreeTestStore()
	store.beaconCfg = &cfg
	store.forkGraph = graph
	justified := solid.Checkpoint{Root: root, Epoch: 4}
	graph.currentJustified = justified
	store.justifiedCheckpoint.Store(justified)
	store.finalizedCheckpoint.Store(justified)
	store.proposerBoostRoot.Store(common.Hash{})
	store.checkpointStates.Store(justified, &checkpointState{beaconConfig: &cfg})
	store.timeMs.Store((block.Block.Slot + currentSlotDelta) * cfg.SecondsPerSlot * 1000)
	if childParentStatus == cltypes.PayloadStatusPending {
		return store, root, common.Hash{}
	}
	graph.votingSources[root] = justified
	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	child.Block.Slot = block.Block.Slot + 1
	child.Block.ParentRoot = root
	parentBlockHash := common.Hash{0xff}
	if childParentStatus == cltypes.PayloadStatusFull {
		parentBlockHash = blockHash
	}
	child.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		ParentBlockHash: parentBlockHash,
		BlockHash:       common.Hash{0xe1},
	}}
	graph.headers[childRoot] = child.SignedBeaconBlockHeader().Header
	graph.blocks[childRoot] = child
	store.updateChildren(block.Block.Slot, root, childRoot)
	if childParentStatus == cltypes.PayloadStatusEmpty {
		store.proposerBoostRoot.Store(childRoot)
	}
	return store, root, childRoot
}
