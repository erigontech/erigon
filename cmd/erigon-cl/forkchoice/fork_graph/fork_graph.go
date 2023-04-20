package fork_graph

import (
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
	inverseEdges          map[libcommon.Hash]*beacon_changeset.ChangeSet
	forwardEdges          map[libcommon.Hash]*beacon_changeset.ChangeSet
	headers               map[libcommon.Hash]*cltypes.BeaconBlockHeader // Could be either a checkpoint or state
	farthestExtendingPath map[libcommon.Hash]bool                       // The longest path is used as the "canonical"
	badBlocks             map[libcommon.Hash]bool                       // blocks that are invalid and that leads to automatic fail of extension.
	lastState             *state.BeaconState
	// Cap for how farther we can reorg (initial state slot)
	anchorSlot uint64
	// childrens maps each block roots to its children block roots
	childrens map[libcommon.Hash][]libcommon.Hash
	// for each block root we also keep track of te equivalent current justified and finalized checkpoints for faster head retrieval.
	currentJustifiedCheckpoints map[libcommon.Hash]*cltypes.Checkpoint
	finalizedCheckpoints        map[libcommon.Hash]*cltypes.Checkpoint
}

// Initialize fork graph with a new state
func New(anchorState *state.BeaconState) *ForkGraph {
	farthestExtendingPath := make(map[libcommon.Hash]bool)
	headers := make(map[libcommon.Hash]*cltypes.BeaconBlockHeader)
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		panic(err)
	}
	anchorHeader := anchorState.LatestBlockHeader()
	headers[anchorRoot] = &anchorHeader
	farthestExtendingPath[anchorRoot] = true
	return &ForkGraph{
		// Bidirectional edges
		inverseEdges: make(map[libcommon.Hash]*beacon_changeset.ChangeSet),
		forwardEdges: make(map[libcommon.Hash]*beacon_changeset.ChangeSet),
		// storage
		headers:               headers,
		farthestExtendingPath: farthestExtendingPath,
		badBlocks:             make(map[libcommon.Hash]bool),
		lastState:             anchorState,
		// Slots configuration
		anchorSlot: anchorState.Slot(),
		// childrens
		childrens: make(map[libcommon.Hash][]libcommon.Hash),
		// checkpoints trackers
		currentJustifiedCheckpoints: make(map[libcommon.Hash]*cltypes.Checkpoint),
		finalizedCheckpoints:        make(map[libcommon.Hash]*cltypes.Checkpoint),
	}
}

// Add a new node and edge to the graph
func (f *ForkGraph) AddChainSegment(signedBlock *cltypes.SignedBeaconBlock, fullValidation bool) (ChainSegmentInsertionResult, error) {
	block := signedBlock.Block
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return LogisticError, err
	}

	if _, ok := f.forwardEdges[blockRoot]; ok {
		return PreValidated, nil
	}
	// Blocks below anchors are invalid.
	if block.Slot <= f.anchorSlot {
		log.Debug("block below anchor slot", "slot", block.Slot, "hash", libcommon.Hash(blockRoot))
		f.badBlocks[blockRoot] = true
		return BelowAnchor, nil
	}
	// Check if block being process right now was marked as invalid.
	if invalid, ok := f.badBlocks[blockRoot]; ok && invalid {
		log.Debug("block has invalid parent", "slot", block.Slot, "hash", libcommon.Hash(blockRoot))
		f.badBlocks[blockRoot] = true
		return InvalidBlock, nil
	}

	baseState, err := f.GetState(block.ParentRoot)
	if err != nil {
		return InvalidBlock, err
	}
	if baseState == nil {
		return MissingSegment, nil
	}
	f.lastState = baseState

	f.lastState.StartCollectingReverseChangeSet()
	f.lastState.StartCollectingForwardChangeSet()
	// Execute the state
	if err := transition.TransitionState(f.lastState, signedBlock, fullValidation); err != nil {
		// Revert bad block changes
		f.lastState.RevertWithChangeset(f.lastState.StopCollectingReverseChangeSet())
		f.lastState.StopCollectingForwardChangeSet()
		// Add block to list of invalid blocks
		log.Debug("Invalid beacon block", "reason", err)
		f.badBlocks[blockRoot] = true
		return InvalidBlock, err
	}

	// if it is finished then update the graph
	f.inverseEdges[blockRoot] = f.lastState.StopCollectingReverseChangeSet()
	f.forwardEdges[blockRoot] = f.lastState.StopCollectingForwardChangeSet()
	bodyRoot, err := block.Body.HashSSZ()
	if err != nil {
		return LogisticError, err
	}
	f.headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		Root:          block.StateRoot,
		BodyRoot:      bodyRoot,
	}
	f.farthestExtendingPath[blockRoot] = true
	// Update the children of the parent
	f.updateChildren(block.ParentRoot, blockRoot)
	// Lastly add checkpoints to caches as well.
	f.currentJustifiedCheckpoints[blockRoot] = f.lastState.CurrentJustifiedCheckpoint().Copy()
	f.finalizedCheckpoints[blockRoot] = f.lastState.FinalizedCheckpoint().Copy()
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
	obj, has := f.headers[blockRoot]
	return obj, has
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
	for reconnect, ok := f.farthestExtendingPath[currentIteratorRoot]; !ok || !reconnect; reconnect, ok = f.farthestExtendingPath[currentIteratorRoot] {
		parent, isSegmentPresent := f.GetHeader(currentIteratorRoot)
		if !isSegmentPresent {
			log.Debug("Could not retrieve state: Missing header", "missing", currentIteratorRoot)
			return nil, nil
		}
		blockRootsFromFarthestExtendingPath = append(blockRootsFromFarthestExtendingPath, currentIteratorRoot)
		currentIteratorRoot = parent.ParentRoot
	}
	// Initalize edge.
	edge := libcommon.Hash(currentStateBlockRoot)
	inverselyTraversedRoots := []libcommon.Hash{}
	inverseChangesets := make([]*beacon_changeset.ChangeSet, 0, len(blockRootsFromFarthestExtendingPath))
	// Unwind to the recconection root.
	for edge != currentIteratorRoot {
		changeset, isChangesetPreset := f.inverseEdges[edge]
		if !isChangesetPreset {
			log.Debug("Could not retrieve state: Missing changeset", "missing", edge)
			return nil, nil
		}
		// you need the parent for the root
		parent, isSegmentPresent := f.GetHeader(edge)
		if !isSegmentPresent {
			log.Debug("Could not retrieve state: Missing header in history reconstruction", "missing", currentIteratorRoot)
			return nil, nil
		}
		inverselyTraversedRoots = append(inverselyTraversedRoots, edge)
		inverseChangesets = append(inverseChangesets, changeset)
		// go on.
		edge = parent.ParentRoot
	}
	// Reverse changeset ONLY if we can
	for _, changeset := range inverseChangesets {
		f.lastState.RevertWithChangeset(changeset)
	}

	// Traverse the graph forward now (the nodes are in reverse order).
	for i := len(blockRootsFromFarthestExtendingPath) - 1; i >= 0; i-- {
		changeset := f.forwardEdges[blockRootsFromFarthestExtendingPath[i]]
		f.lastState.RevertWithChangeset(changeset)
	}
	// If we have a new farthest extended path, update it accordingly.
	for _, root := range inverselyTraversedRoots {
		if root == edge {
			continue
		}
		f.farthestExtendingPath[root] = false
	}
	for _, root := range blockRootsFromFarthestExtendingPath {
		f.farthestExtendingPath[root] = true
	}
	return f.lastState, nil
}

func (f *ForkGraph) GetStateCopy(blockRoot libcommon.Hash) (*state.BeaconState, error) {
	state, err := f.lastState.Copy()
	if err != nil {
		return nil, err
	}

	currentStateBlockRoot, err := state.BlockRoot()
	if err != nil {
		return nil, err
	}
	if currentStateBlockRoot == blockRoot {
		return state, nil
	}
	// collect all blocks beetwen greatest extending node path and block.
	blockRootsFromFarthestExtendingPath := []libcommon.Hash{}
	// Use the parent root as a reverse iterator.
	currentIteratorRoot := blockRoot
	// try and find the point of recconection
	for reconnect, ok := f.farthestExtendingPath[currentIteratorRoot]; !ok || !reconnect; reconnect, ok = f.farthestExtendingPath[currentIteratorRoot] {
		parent, isSegmentPresent := f.GetHeader(currentIteratorRoot)
		if !isSegmentPresent {
			log.Debug("Could not retrieve state: Missing header", "missing", currentIteratorRoot)
			return nil, nil
		}
		blockRootsFromFarthestExtendingPath = append(blockRootsFromFarthestExtendingPath, currentIteratorRoot)
		currentIteratorRoot = parent.ParentRoot
	}
	// Initalize edge.
	edge := libcommon.Hash(currentStateBlockRoot)
	inverseChangesets := make([]*beacon_changeset.ChangeSet, 0, len(blockRootsFromFarthestExtendingPath))
	// Unwind to the recconection root.
	for edge != currentIteratorRoot {
		changeset, isChangesetPreset := f.inverseEdges[edge]
		if !isChangesetPreset {
			log.Debug("Could not retrieve state: Missing changeset", "missing", edge)
			return nil, nil
		}
		// you need the parent for the root
		parent, isSegmentPresent := f.GetHeader(edge)
		if !isSegmentPresent {
			log.Debug("Could not retrieve state: Missing header in history reconstruction", "missing", currentIteratorRoot)
			return nil, nil
		}
		inverseChangesets = append(inverseChangesets, changeset)
		// go on.
		edge = parent.ParentRoot
	}
	// Reverse changeset ONLY if we can
	for _, changeset := range inverseChangesets {
		state.RevertWithChangeset(changeset)
	}

	// Traverse the graph forward now (the nodes are in reverse order).
	for i := len(blockRootsFromFarthestExtendingPath) - 1; i >= 0; i-- {
		changeset := f.forwardEdges[blockRootsFromFarthestExtendingPath[i]]
		state.RevertWithChangeset(changeset)
	}
	return state, nil
}

// updateChildren adds a new child to the parent node hash.
func (f *ForkGraph) updateChildren(parent, child libcommon.Hash) {
	childrens := f.childrens[parent]
	if slices.Contains(childrens, child) {
		return
	}
	childrens = append(childrens, child)
	f.childrens[parent] = childrens
}

// GetChildren retrieves the children block root of the given block root.
func (f *ForkGraph) GetChildren(parent libcommon.Hash) []libcommon.Hash {
	return f.childrens[parent]
}

func (f *ForkGraph) GetCurrentJustifiedCheckpoint(blockRoot libcommon.Hash) (*cltypes.Checkpoint, bool) {
	obj, has := f.currentJustifiedCheckpoints[blockRoot]
	return obj, has
}

func (f *ForkGraph) GetFinalizedCheckpoint(blockRoot libcommon.Hash) (*cltypes.Checkpoint, bool) {
	obj, has := f.finalizedCheckpoints[blockRoot]
	return obj, has
}

func (f *ForkGraph) RemoveOldBlocks(pruneSlot uint64) {
	oldRoots := make([]libcommon.Hash, 0, len(f.headers))
	for hash, header := range f.headers {
		if header.Slot >= pruneSlot {
			continue
		}
		oldRoots = append(oldRoots, hash)
	}
	for _, root := range oldRoots {
		delete(f.inverseEdges, root)
		delete(f.headers, root)
		delete(f.forwardEdges, root)
		delete(f.farthestExtendingPath, root)
		delete(f.badBlocks, root)
		delete(f.childrens, root)
		delete(f.currentJustifiedCheckpoints, root)
		delete(f.finalizedCheckpoints, root)
	}
}
