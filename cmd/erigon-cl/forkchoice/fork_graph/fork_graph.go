package fork_graph

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/beacon_changeset"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/ledgerwatch/log/v3"
)

type ChainSegmentInsertionResult uint

const (
	Success        ChainSegmentInsertionResult = 0
	InvalidBlock   ChainSegmentInsertionResult = 1
	MissingSegment ChainSegmentInsertionResult = 2
	BelowAnchor    ChainSegmentInsertionResult = 3
	LogisticError  ChainSegmentInsertionResult = 4
)

const maxGraphExtension = 256

/*
* The state store process is related to graph theory in the sense that the Ethereum blockchain can be thought of as a directed graph,
* where each block represents a node and the links between blocks represent directed edges.
* In this context, rolling back the state of Ethereum to a previous state can be thought of as traversing the graph in reverse,
* from the current state to a previous state.
* The process of reverting the state involves undoing the changes made in the blocks that have been added to the blockchain since the previous state.
* This can be thought of as "reversing the edges" in the graph, effectively undoing the changes made to the state of Ethereum.
* By thinking of the Ethereum blockchain as a graph, we can use graph theory concepts, such as traversal algorithms,
* to analyze and manipulate the state of the blockchain.
 */

// ForkGraph is our graph for ETH 2.0 consensus forkchoice. Each node is a (block root, changes) pair and
// each edge is the path described as (prevBlockRoot, currBlockRoot). if we want to go forward we use blocks.
type ForkGraph struct {
	inverseEdges          *lru.Cache[inverseForkStoreEdge, *beacon_changeset.ReverseBeaconStateChangeSet]
	forwardEdges          *lru.Cache[libcommon.Hash, *cltypes.SignedBeaconBlock]
	farthestExtendingPath *lru.Cache[libcommon.Hash, bool] // The longest path is used as the "canonical"
	badBlocks             *lru.Cache[libcommon.Hash, bool] // blocks that are invalid and that leads to automatic fail of extension.
	lastState             *state.BeaconState
	// Cap for how farther we can reorg (initial state slot)
	anchorSlot uint64
}

// StateStoreEdge is the path beetwen 2 nodes unidirectionally.
type inverseForkStoreEdge struct {
	destinationStateBlockRoot libcommon.Hash
	sourceStateBlockRoot      libcommon.Hash
}

// Initialize fork graph with a new state
func New(anchorState *state.BeaconState) *ForkGraph {
	inverseEdges, err := lru.New[inverseForkStoreEdge, *beacon_changeset.ReverseBeaconStateChangeSet](maxGraphExtension)
	if err != nil {
		panic(err)
	}
	forwardEdges, err := lru.New[libcommon.Hash, *cltypes.SignedBeaconBlock](maxGraphExtension)
	if err != nil {
		panic(err)
	}
	farthestExtendingPath, err := lru.New[libcommon.Hash, bool](maxGraphExtension)
	if err != nil {
		panic(err)
	}
	badBlocks, err := lru.New[libcommon.Hash, bool](maxGraphExtension)
	if err != nil {
		panic(err)
	}
	return &ForkGraph{
		// Bidirectional edges
		inverseEdges:          inverseEdges,
		forwardEdges:          forwardEdges,
		farthestExtendingPath: farthestExtendingPath,
		badBlocks:             badBlocks,
		lastState:             anchorState,
		// Slots configuration
		anchorSlot: anchorState.Slot(),
	}
}

// Add a new node and edge to the graph
func (f *ForkGraph) AddChainSegment(signedBlock *cltypes.SignedBeaconBlock) (ChainSegmentInsertionResult, error) {
	f.cleanOldNodesAndEdges()
	block := signedBlock.Block
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return LogisticError, err
	}
	fmt.Println(block)

	if _, ok := f.forwardEdges.Get(blockRoot); ok {
		return Success, nil
	}
	// Blocks below anchors are invalid.
	if block.Slot <= f.anchorSlot {
		log.Debug("block below anchor slot", "slot", block.Slot, "hash", libcommon.Hash(blockRoot))
		f.badBlocks.Add(blockRoot, true)
		return BelowAnchor, nil
	}
	// Check if block being process right now was marked as invalid.
	if invalid, ok := f.badBlocks.Get(blockRoot); ok && invalid {
		log.Debug("block has invalid parent", "slot", block.Slot, "hash", libcommon.Hash(blockRoot))
		f.badBlocks.Add(blockRoot, true)
		return InvalidBlock, nil
	}
	currentBlockRoot, err := f.lastState.BlockRoot()
	if err != nil {
		return LogisticError, err
	}
	// Check if it extend the farthest state.
	if currentBlockRoot == block.ParentRoot {
		f.lastState.StartCollectingReverseChangeSet()
		// Execute the state
		if err := transition.TransitionState(f.lastState, signedBlock /*fullValidation=*/, true); err != nil {
			changeset := f.lastState.StopCollectingReverseChangeSet()
			// Revert bad block changes
			f.lastState.RevertWithChangeset(changeset)
			// Add block to list of invalid blocks
			log.Debug("Invalid beacon block", "reason", err)
			f.badBlocks.Add(blockRoot, true)
			return InvalidBlock, nil
		}
		// if it is finished then update the graph
		f.inverseEdges.Add(inverseForkStoreEdge{
			destinationStateBlockRoot: block.ParentRoot,
			sourceStateBlockRoot:      blockRoot,
		}, f.lastState.StopCollectingReverseChangeSet())
		f.forwardEdges.Add(blockRoot, signedBlock)
		f.farthestExtendingPath.Add(blockRoot, true)
		return Success, nil
	}
	// collect all blocks beetwen greatest extending node path and block.
	blockRootsFromFarthestExtendingPath := []libcommon.Hash{}
	// Use the parent root as a reverse iterator.
	currentIteratorRoot := block.ParentRoot
	// try and find the point of recconection
	for reconnect, ok := f.farthestExtendingPath.Get(currentIteratorRoot); !ok || !reconnect; {
		currentBlock, isSegmentPresent := f.forwardEdges.Get(currentIteratorRoot)
		if !isSegmentPresent {
			return MissingSegment, nil
		}
		currentRoot, err := currentBlock.Block.HashSSZ()
		if err != nil {
			return LogisticError, err
		}
		blockRootsFromFarthestExtendingPath = append(blockRootsFromFarthestExtendingPath, currentRoot)
	}
	// Initalize edge.
	edge := inverseForkStoreEdge{
		sourceStateBlockRoot:      currentBlockRoot,
		destinationStateBlockRoot: f.lastState.LatestBlockHeader().ParentRoot,
	}
	inverselyTraversedRoots := []libcommon.Hash{currentBlockRoot}

	// Unwind to the recconection root.
	for edge.sourceStateBlockRoot != currentIteratorRoot {
		changeset, isChangesetPreset := f.inverseEdges.Get(edge)
		if !isChangesetPreset {
			return MissingSegment, nil
		}
		f.lastState.RevertWithChangeset(changeset)
		// Recompute currentBlockRoot
		currentBlockRoot, err := f.lastState.BlockRoot()
		if err != nil {
			return LogisticError, err
		}
		inverselyTraversedRoots = append(inverselyTraversedRoots, currentBlockRoot)
		// go on.
		edge = inverseForkStoreEdge{
			sourceStateBlockRoot:      currentBlockRoot,
			destinationStateBlockRoot: f.lastState.LatestBlockHeader().ParentRoot,
		}
	}
	// Traverse the graph forward now (the nodes are in reverse order).
	for i := len(blockRootsFromFarthestExtendingPath) - 1; i >= 0; i-- {
		currentBlock, _ := f.forwardEdges.Get(blockRootsFromFarthestExtendingPath[i])
		if err := transition.TransitionState(f.lastState, currentBlock, false); err != nil {
			log.Debug("Invalid beacon block", "reason", err)
			f.badBlocks.Add(blockRoot, true)
			return InvalidBlock, nil
		}
	}

	// If we have a new farthest extended path, update it accordingly.
	for _, root := range inverselyTraversedRoots {
		f.farthestExtendingPath.Add(root, false)
	}
	for _, root := range blockRootsFromFarthestExtendingPath {
		f.farthestExtendingPath.Add(root, true)
	}
	f.lastState.StartCollectingReverseChangeSet()
	// Execute the state
	if err := transition.TransitionState(f.lastState, signedBlock /*fullValidation=*/, true); err != nil {
		// Revert bad block changes
		f.lastState.RevertWithChangeset(f.lastState.StopCollectingReverseChangeSet())
		// Add block to list of invalid blocks
		log.Debug("Invalid beacon block", "reason", err)
		f.badBlocks.Add(blockRoot, true)
		return InvalidBlock, nil
	}

	// if it is finished then update the graph
	f.inverseEdges.Add(inverseForkStoreEdge{
		destinationStateBlockRoot: block.ParentRoot,
		sourceStateBlockRoot:      blockRoot,
	}, f.lastState.StopCollectingReverseChangeSet())

	f.forwardEdges.Add(blockRoot, signedBlock)
	f.farthestExtendingPath.Add(blockRoot, true)

	return Success, nil
}

// Graph needs to be constant in extension so clean old nodes and edges periodically.
func (f *ForkGraph) cleanOldNodesAndEdges() {
	/*for edge := range f.inverseEdges {
		if edge.sourceSlot+maxGraphExtension <= f.lastState.Slot() {
			delete(f.forwardEdges, edge.destinationStateBlockRoot)
			delete(f.forwardEdges, edge.sourceStateBlockRoot)
			delete(f.farthestExtendingPath, edge.destinationStateBlockRoot)
			delete(f.farthestExtendingPath, edge.sourceStateBlockRoot)
		}
	}*/
}

// LastState returns the last state.
func (f *ForkGraph) LastState() *state.BeaconState {
	return f.lastState
}

func (f *ForkGraph) GenesisTime() uint64 {
	return f.lastState.GenesisTime()
}

func (f *ForkGraph) GetBlock(blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return f.forwardEdges.Get(blockRoot)
}
