package fork_graph

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
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

const snapshotStateEverySlot = 64

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
	// Alternate beacon states
	currentReferenceState *state.BeaconState
	nextReferenceState    *state.BeaconState
	blocks                map[libcommon.Hash]*cltypes.SignedBeaconBlock // set of blocks
	headers               map[libcommon.Hash]*cltypes.BeaconBlockHeader // set of headers
	badBlocks             map[libcommon.Hash]struct{}                   // blocks that are invalid and that leads to automatic fail of extension.
	// current state data
	currentState          *state.BeaconState
	currentStateBlockRoot libcommon.Hash
	// childrens maps each block roots to its children block roots
	childrens map[libcommon.Hash][]libcommon.Hash
	// for each block root we also keep track of te equivalent current justified and finalized checkpoints for faster head retrieval.
	currentJustifiedCheckpoints map[libcommon.Hash]*cltypes.Checkpoint
	finalizedCheckpoints        map[libcommon.Hash]*cltypes.Checkpoint
	// Disable for tests
	enabledPruning bool
	// configurations
	beaconCfg   *clparams.BeaconChainConfig
	genesisTime uint64
}

func (f *ForkGraph) AnchorSlot() uint64 {
	return f.currentReferenceState.Slot()
}

// Initialize fork graph with a new state
func New(anchorState *state.BeaconState, enabledPruning bool) *ForkGraph {
	farthestExtendingPath := make(map[libcommon.Hash]bool)
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		panic(err)
	}
	headers := make(map[libcommon.Hash]*cltypes.BeaconBlockHeader)
	anchorHeader := anchorState.LatestBlockHeader()
	if anchorHeader.Root, err = anchorState.HashSSZ(); err != nil {
		panic(err)
	}
	headers[anchorRoot] = &anchorHeader

	farthestExtendingPath[anchorRoot] = true
	currentStateReference, err := anchorState.Copy()
	if err != nil {
		panic(err)
	}
	return &ForkGraph{
		currentReferenceState: currentStateReference,
		nextReferenceState:    currentStateReference,
		// storage
		blocks:    make(map[libcommon.Hash]*cltypes.SignedBeaconBlock),
		headers:   headers,
		badBlocks: make(map[libcommon.Hash]struct{}),
		// current state data
		currentState:          anchorState,
		currentStateBlockRoot: anchorRoot,
		// childrens
		childrens: make(map[libcommon.Hash][]libcommon.Hash),
		// checkpoints trackers
		currentJustifiedCheckpoints: make(map[libcommon.Hash]*cltypes.Checkpoint),
		finalizedCheckpoints:        make(map[libcommon.Hash]*cltypes.Checkpoint),
		enabledPruning:              enabledPruning,
		// configuration
		beaconCfg:   anchorState.BeaconConfig(),
		genesisTime: anchorState.GenesisTime(),
	}
}

// Add a new node and edge to the graph
func (f *ForkGraph) AddChainSegment(signedBlock *cltypes.SignedBeaconBlock, fullValidation bool) (*state.BeaconState, ChainSegmentInsertionResult, error) {
	block := signedBlock.Block
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return nil, LogisticError, err
	}

	if _, ok := f.headers[blockRoot]; ok {
		return nil, PreValidated, nil
	}
	// Blocks below anchors are invalid.
	if block.Slot <= f.currentReferenceState.Slot() {
		log.Debug("block below anchor slot", "slot", block.Slot, "hash", libcommon.Hash(blockRoot))
		f.badBlocks[blockRoot] = struct{}{}
		return nil, BelowAnchor, nil
	}
	// Check if block being process right now was marked as invalid.
	if _, ok := f.badBlocks[blockRoot]; ok {
		log.Debug("block has invalid parent", "slot", block.Slot, "hash", libcommon.Hash(blockRoot))
		f.badBlocks[blockRoot] = struct{}{}
		return nil, InvalidBlock, nil
	}

	newState, err := f.GetState(block.ParentRoot, false)
	if err != nil {
		return nil, InvalidBlock, err
	}
	if newState == nil {
		log.Debug("AddChainSegment: missing segment", "block", libcommon.Hash(blockRoot))
		return nil, MissingSegment, nil
	}
	// We may just use the current beacon state
	prevCurrentStateSlot := f.currentState.Slot()

	// Execute the state
	if err := transition.TransitionState(newState, signedBlock, fullValidation); err != nil {
		// Add block to list of invalid blocks
		log.Debug("Invalid beacon block", "reason", err)
		f.badBlocks[blockRoot] = struct{}{}
		f.currentState = nil
		return nil, InvalidBlock, err
	}

	f.blocks[blockRoot] = signedBlock
	bodyRoot, err := signedBlock.Block.Body.HashSSZ()
	if err != nil {
		return nil, LogisticError, err
	}
	f.headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		Root:          block.StateRoot,
		BodyRoot:      bodyRoot,
	}
	// Update the children of the parent
	f.updateChildren(block.ParentRoot, blockRoot)
	// Lastly add checkpoints to caches as well.
	f.currentJustifiedCheckpoints[blockRoot] = newState.CurrentJustifiedCheckpoint().Copy()
	f.finalizedCheckpoints[blockRoot] = newState.FinalizedCheckpoint().Copy()
	if newState.Slot() > prevCurrentStateSlot {
		f.currentState = newState
		f.currentStateBlockRoot = blockRoot
		if newState.Slot()%snapshotStateEverySlot == 0 && f.enabledPruning {
			if err := f.removeOldData(); err != nil {
				return nil, LogisticError, err
			}
		}
	}
	return newState, Success, nil
}

func (f *ForkGraph) GenesisTime() uint64 {
	return f.genesisTime
}

func (f *ForkGraph) Config() *clparams.BeaconChainConfig {
	return f.beaconCfg
}

func (f *ForkGraph) GetHeader(blockRoot libcommon.Hash) (*cltypes.BeaconBlockHeader, bool) {
	obj, has := f.headers[blockRoot]
	return obj, has
}

func (f *ForkGraph) getBlock(blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlock, bool) {
	obj, has := f.blocks[blockRoot]
	return obj, has
}

func (f *ForkGraph) GetState(blockRoot libcommon.Hash, alwaysCopy bool) (*state.BeaconState, error) {
	if f.currentStateBlockRoot == blockRoot {
		if alwaysCopy {
			return f.currentState.Copy()
		}
		return f.currentState, nil
	}
	// collect all blocks beetwen greatest extending node path and block.
	blocksInTheWay := []*cltypes.SignedBeaconBlock{}
	// Use the parent root as a reverse iterator.
	currentIteratorRoot := blockRoot
	// use the current reference state root as reconnectio
	reconnectionRootLong, err := f.currentReferenceState.BlockRoot()
	if err != nil {
		return nil, err
	}
	reconnectionRootShort, err := f.nextReferenceState.BlockRoot()
	if err != nil {
		return nil, err
	}
	// try and find the point of recconection
	for currentIteratorRoot != reconnectionRootLong && currentIteratorRoot != reconnectionRootShort {
		block, isSegmentPresent := f.getBlock(currentIteratorRoot)
		if !isSegmentPresent {
			log.Debug("Could not retrieve state: Missing header", "missing", currentIteratorRoot,
				"longRecconection", libcommon.Hash(reconnectionRootLong), "shortRecconection", libcommon.Hash(reconnectionRootShort))
			return nil, nil
		}
		blocksInTheWay = append(blocksInTheWay, block)
		currentIteratorRoot = block.Block.ParentRoot
	}
	var copyReferencedState *state.BeaconState
	// Take a copy to the reference state.
	if currentIteratorRoot == reconnectionRootLong {
		copyReferencedState, err = f.currentReferenceState.Copy()
		if err != nil {
			return nil, err
		}
	} else {
		copyReferencedState, err = f.nextReferenceState.Copy()
		if err != nil {
			return nil, err
		}
	}

	// Traverse the blocks from top to bottom.
	for i := len(blocksInTheWay) - 1; i >= 0; i-- {
		if err := transition.TransitionState(copyReferencedState, blocksInTheWay[i], false); err != nil {
			return nil, err
		}
	}
	return copyReferencedState, nil
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

func (f *ForkGraph) removeOldData() (err error) {
	pruneSlot := f.nextReferenceState.Slot()
	log.Debug("Pruning old blocks", "pruneSlot", pruneSlot)
	oldRoots := make([]libcommon.Hash, 0, len(f.blocks))
	for hash, signedBlock := range f.blocks {
		if signedBlock.Block.Slot >= pruneSlot {
			continue
		}
		oldRoots = append(oldRoots, hash)
	}
	for _, root := range oldRoots {
		delete(f.badBlocks, root)
		delete(f.blocks, root)
		delete(f.childrens, root)
		delete(f.currentJustifiedCheckpoints, root)
		delete(f.finalizedCheckpoints, root)
		delete(f.headers, root)
	}
	// Lastly snapshot the state
	f.currentReferenceState = f.nextReferenceState
	err = f.currentState.CopyInto(f.nextReferenceState)
	if err != nil {
		panic(err) // dead at this point
	}
	return
}
