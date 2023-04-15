package fork_graph

import (
	lru "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/beacon_changeset"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

type ChainSegmentInsertionResult uint

const (
	Success        ChainSegmentInsertionResult = 0
	InvalidBlock   ChainSegmentInsertionResult = 1
	MissingSegment ChainSegmentInsertionResult = 2
	BelowAnchor    ChainSegmentInsertionResult = 3
	LogisticError  ChainSegmentInsertionResult = 4
	PreValidated   ChainSegmentInsertionResult = 5
)

const maxGraphExtension = 1024

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
	// I am using lrus caches because of the auto cleanup feature.
	inverseEdges          *lru.Cache[libcommon.Hash, *beacon_changeset.ChangeSet]
	forwardEdges          *lru.Cache[libcommon.Hash, *beacon_changeset.ChangeSet]
	headers               *lru.Cache[libcommon.Hash, *cltypes.BeaconBlockHeader] // Could be either a checkpoint or state
	farthestExtendingPath *lru.Cache[libcommon.Hash, bool]                       // The longest path is used as the "canonical"
	badBlocks             *lru.Cache[libcommon.Hash, bool]                       // blocks that are invalid and that leads to automatic fail of extension.
	lastState             *state.BeaconState
	// Cap for how farther we can reorg (initial state slot)
	anchorSlot uint64
	// childrens maps each block roots to its children block roots
	childrens *lru.Cache[libcommon.Hash, []libcommon.Hash]
	// for each block root we also keep track of te equivalent current justified and finalized checkpoints for faster head retrieval.
	currentJustifiedCheckpoints *lru.Cache[libcommon.Hash, *cltypes.Checkpoint]
	finalizedCheckpoints        *lru.Cache[libcommon.Hash, *cltypes.Checkpoint]
}

// Initialize fork graph with a new state
func New(anchorState *state.BeaconState) *ForkGraph {
	inverseEdges, err := lru.New[libcommon.Hash, *beacon_changeset.ChangeSet](maxGraphExtension)
	if err != nil {
		panic(err)
	}
	forwardEdges, err := lru.New[libcommon.Hash, *beacon_changeset.ChangeSet](maxGraphExtension)
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
	headers, err := lru.New[libcommon.Hash, *cltypes.BeaconBlockHeader](maxGraphExtension)
	if err != nil {
		panic(err)
	}
	childrens, err := lru.New[libcommon.Hash, []libcommon.Hash](maxGraphExtension)
	if err != nil {
		panic(err)
	}
	currentJustifiedCheckpoints, err := lru.New[libcommon.Hash, *cltypes.Checkpoint](maxGraphExtension)
	if err != nil {
		panic(err)
	}
	finalizedCheckpoints, err := lru.New[libcommon.Hash, *cltypes.Checkpoint](maxGraphExtension)
	if err != nil {
		panic(err)
	}
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		panic(err)
	}
	anchorHeader := anchorState.LatestBlockHeader()
	headers.Add(anchorRoot, &anchorHeader)
	farthestExtendingPath.Add(anchorRoot, true)
	return &ForkGraph{
		// Bidirectional edges
		inverseEdges: inverseEdges,
		forwardEdges: forwardEdges,
		// storage
		headers:               headers,
		farthestExtendingPath: farthestExtendingPath,
		badBlocks:             badBlocks,
		lastState:             anchorState,
		// Slots configuration
		anchorSlot: anchorState.Slot(),
		// childrens
		childrens: childrens,
		// checkpoints trackers
		currentJustifiedCheckpoints: currentJustifiedCheckpoints,
		finalizedCheckpoints:        finalizedCheckpoints,
	}
}

// Add a new node and edge to the graph
func (f *ForkGraph) AddChainSegment(signedBlock *cltypes.SignedBeaconBlock) (ChainSegmentInsertionResult, error) {
	block := signedBlock.Block
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return LogisticError, err
	}

	if _, ok := f.forwardEdges.Get(blockRoot); ok {
		return PreValidated, nil
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

	f.lastState, err = f.GetState(block.ParentRoot)
	if err != nil {
		return InvalidBlock, err
	}
	if f.lastState == nil {
		return MissingSegment, nil
	}

	f.lastState.StartCollectingReverseChangeSet()
	f.lastState.StartCollectingForwardChangeSet()
	// Execute the state
	if err := transition.TransitionState(f.lastState, signedBlock /*fullValidation=*/, true); err != nil {
		// Revert bad block changes
		f.lastState.RevertWithChangeset(f.lastState.StopCollectingReverseChangeSet())
		f.lastState.StopCollectingForwardChangeSet()
		// Add block to list of invalid blocks
		log.Debug("Invalid beacon block", "reason", err)
		f.badBlocks.Add(blockRoot, true)
		return InvalidBlock, nil
	}

	// if it is finished then update the graph
	f.inverseEdges.Add(blockRoot, f.lastState.StopCollectingReverseChangeSet())
	f.forwardEdges.Add(blockRoot, f.lastState.StopCollectingForwardChangeSet())
	bodyRoot, err := block.Body.HashSSZ()
	if err != nil {
		return LogisticError, err
	}
	f.headers.Add(blockRoot, &cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		Root:          block.StateRoot,
		BodyRoot:      bodyRoot,
	})
	f.farthestExtendingPath.Add(blockRoot, true)
	// Update the children of the parent
	f.updateChildren(block.ParentRoot, blockRoot)
	// Lastly add checkpoints to caches as well.
	f.currentJustifiedCheckpoints.Add(blockRoot, f.lastState.CurrentJustifiedCheckpoint().Copy())
	f.finalizedCheckpoints.Add(blockRoot, f.lastState.FinalizedCheckpoint().Copy())
	return Success, nil
}

// LastState returns the last state.
func (f *ForkGraph) LastState() *state.BeaconState {
	return f.lastState
}

func (f *ForkGraph) GenesisTime() uint64 {
	return f.lastState.GenesisTime()
}

func (f *ForkGraph) Config() *clparams.BeaconChainConfig {
	return f.lastState.BeaconConfig()
}

func (f *ForkGraph) GetHeader(blockRoot libcommon.Hash) (*cltypes.BeaconBlockHeader, bool) {
	return f.headers.Get(blockRoot)
}

func (f *ForkGraph) GetState(blockRoot libcommon.Hash) (*state.BeaconState, error) {
	currentStateBlockRoot, err := f.lastState.BlockRoot()
	if err != nil {
		return nil, err
	}
	if currentStateBlockRoot == blockRoot {
		return f.lastState, nil
	}
	// collect all blocks beetwen greatest extending node path and block.
	blockRootsFromFarthestExtendingPath := []libcommon.Hash{}
	// Use the parent root as a reverse iterator.
	currentIteratorRoot := blockRoot
	// try and find the point of recconection
	for reconnect, ok := f.farthestExtendingPath.Get(currentIteratorRoot); !ok || !reconnect; reconnect, ok = f.farthestExtendingPath.Get(currentIteratorRoot) {
		parent, isSegmentPresent := f.GetHeader(currentIteratorRoot)
		if !isSegmentPresent {
			return nil, nil
		}
		blockRootsFromFarthestExtendingPath = append(blockRootsFromFarthestExtendingPath, currentIteratorRoot)
		currentIteratorRoot = parent.ParentRoot
	}
	// Initalize edge.
	edge := currentStateBlockRoot
	inverselyTraversedRoots := []libcommon.Hash{}

	// Unwind to the recconection root.
	for edge != currentIteratorRoot {
		changeset, isChangesetPreset := f.inverseEdges.Get(edge)
		if !isChangesetPreset {
			return nil, nil
		}
		// Recompute currentBlockRoot
		currentBlockRoot, err := f.lastState.BlockRoot()
		if err != nil {
			return nil, err
		}
		inverselyTraversedRoots = append(inverselyTraversedRoots, currentBlockRoot)
		f.lastState.RevertWithChangeset(changeset)
		// go on.
		edge = currentBlockRoot
	}
	// Traverse the graph forward now (the nodes are in reverse order).
	for i := len(blockRootsFromFarthestExtendingPath) - 1; i >= 0; i-- {
		changeset, _ := f.forwardEdges.Get(blockRootsFromFarthestExtendingPath[i])
		f.lastState.RevertWithChangeset(changeset)
	}
	// If we have a new farthest extended path, update it accordingly.
	for _, root := range inverselyTraversedRoots {
		if root == edge {
			continue
		}
		f.farthestExtendingPath.Add(root, false)
	}
	for _, root := range blockRootsFromFarthestExtendingPath {
		f.farthestExtendingPath.Add(root, true)
	}
	return f.lastState, nil
}

// updateChildren adds a new child to the parent node hash.
func (f *ForkGraph) updateChildren(parent, child libcommon.Hash) {
	childrens, _ := f.childrens.Get(parent)
	if slices.Contains(childrens, child) {
		return
	}
	childrens = append(childrens, child)
	f.childrens.Add(parent, childrens)
}

// GetChildren retrieves the children block root of the given block root.
func (f *ForkGraph) GetChildren(parent libcommon.Hash) []libcommon.Hash {
	childrens, _ := f.childrens.Get(parent)
	return childrens
}

func (f *ForkGraph) GetCurrentJustifiedCheckpoint(blockRoot libcommon.Hash) (*cltypes.Checkpoint, bool) {
	return f.currentJustifiedCheckpoints.Get(blockRoot)
}

func (f *ForkGraph) GetFinalizedCheckpoint(blockRoot libcommon.Hash) (*cltypes.Checkpoint, bool) {
	return f.finalizedCheckpoints.Get(blockRoot)
}
