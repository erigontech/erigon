package fork_graph

import (
	"bytes"
	"errors"
	"sync"

	"github.com/klauspost/compress/zstd"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/transition"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
	"golang.org/x/exp/slices"
)

type syncCommittees struct {
	currentSyncCommittee *solid.SyncCommittee
	nextSyncCommittee    *solid.SyncCommittee
}

var compressorPool = sync.Pool{
	New: func() interface{} {
		w, err := zstd.NewWriter(nil)
		if err != nil {
			panic(err)
		}
		return w
	},
}

var decompressPool = sync.Pool{
	New: func() interface{} {
		r, err := zstd.NewReader(nil)
		if err != nil {
			panic(err)
		}
		return r
	},
}

var ErrStateNotFound = errors.New("state not found")

type ChainSegmentInsertionResult uint

const (
	Success        ChainSegmentInsertionResult = 0
	InvalidBlock   ChainSegmentInsertionResult = 1
	MissingSegment ChainSegmentInsertionResult = 2
	BelowAnchor    ChainSegmentInsertionResult = 3
	LogisticError  ChainSegmentInsertionResult = 4
	PreValidated   ChainSegmentInsertionResult = 5
)

type savedStateRecord struct {
	slot uint64
}

// ForkGraph is our graph for ETH 2.0 consensus forkchoice. Each node is a (block root, changes) pair and
// each edge is the path described as (prevBlockRoot, currBlockRoot). if we want to go forward we use blocks.
type forkGraphDisk struct {
	// Alternate beacon states
	fs        afero.Fs
	blocks    sync.Map                    // set of blocks (block root -> block)
	headers   sync.Map                    // set of headers
	badBlocks map[libcommon.Hash]struct{} // blocks that are invalid and that leads to automatic fail of extension.

	// TODO: this leaks, but it isn't a big deal since it's only ~24 bytes per block.
	// the dirty solution is to just make it an LRU with max size of like 128 epochs or something probably?
	stateRoots map[libcommon.Hash]libcommon.Hash // set of stateHash -> blockHash

	// current state data
	currentState          *state.CachingBeaconState
	currentStateBlockRoot libcommon.Hash

	// saveStates are indexed by block index
	saveStates map[libcommon.Hash]savedStateRecord

	// for each block root we also keep track of te equivalent current justified and finalized checkpoints for faster head retrieval.
	currentJustifiedCheckpoints map[libcommon.Hash]solid.Checkpoint
	finalizedCheckpoints        map[libcommon.Hash]solid.Checkpoint
	// keep track of rewards too
	blockRewards map[libcommon.Hash]*eth2.BlockRewardsCollector
	// for each block root we keep track of the sync committees for head retrieval.
	syncCommittees map[libcommon.Hash]syncCommittees

	// configurations
	beaconCfg   *clparams.BeaconChainConfig
	genesisTime uint64
	// highest block seen
	highestSeen, lowestAvaiableSlot, anchorSlot uint64

	// reusable buffers
	sszBuffer       bytes.Buffer
	sszSnappyBuffer bytes.Buffer
}

// Initialize fork graph with a new state
func NewForkGraphDisk(anchorState *state.CachingBeaconState, aferoFs afero.Fs) ForkGraph {
	farthestExtendingPath := make(map[libcommon.Hash]bool)
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		panic(err)
	}
	anchorHeader := anchorState.LatestBlockHeader()
	if anchorHeader.Root, err = anchorState.HashSSZ(); err != nil {
		panic(err)
	}

	farthestExtendingPath[anchorRoot] = true

	f := &forkGraphDisk{
		fs: aferoFs,
		// storage
		badBlocks:  make(map[libcommon.Hash]struct{}),
		stateRoots: make(map[libcommon.Hash]libcommon.Hash),
		// current state data
		currentState:          anchorState,
		currentStateBlockRoot: anchorRoot,
		saveStates:            make(map[libcommon.Hash]savedStateRecord),
		syncCommittees:        make(map[libcommon.Hash]syncCommittees),
		// checkpoints trackers
		currentJustifiedCheckpoints: make(map[libcommon.Hash]solid.Checkpoint),
		finalizedCheckpoints:        make(map[libcommon.Hash]solid.Checkpoint),
		blockRewards:                make(map[libcommon.Hash]*eth2.BlockRewardsCollector),
		// configuration
		beaconCfg:          anchorState.BeaconConfig(),
		genesisTime:        anchorState.GenesisTime(),
		anchorSlot:         anchorState.Slot(),
		lowestAvaiableSlot: anchorState.Slot(),
	}
	f.headers.Store(libcommon.Hash(anchorRoot), &anchorHeader)

	f.dumpBeaconStateOnDisk(anchorState, anchorRoot)
	return f
}

func (f *forkGraphDisk) AnchorSlot() uint64 {
	return f.anchorSlot
}

// Add a new node and edge to the graph
func (f *forkGraphDisk) AddChainSegment(signedBlock *cltypes.SignedBeaconBlock, fullValidation bool) (*state.CachingBeaconState, ChainSegmentInsertionResult, error) {
	block := signedBlock.Block
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return nil, LogisticError, err
	}

	if _, ok := f.GetHeader(libcommon.Hash(blockRoot)); ok {
		return nil, PreValidated, nil
	}
	// Blocks below anchors are invalid.
	if block.Slot <= f.anchorSlot {
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

	blockRewardsCollector := &eth2.BlockRewardsCollector{}
	// Execute the state
	if invalidBlockErr := transition.TransitionState(newState, signedBlock, blockRewardsCollector, fullValidation); invalidBlockErr != nil {
		// Add block to list of invalid blocks
		log.Debug("Invalid beacon block", "reason", invalidBlockErr)
		f.badBlocks[blockRoot] = struct{}{}
		f.currentStateBlockRoot = libcommon.Hash{}
		f.currentState, err = f.GetState(block.ParentRoot, true)
		if err != nil {
			log.Error("[Caplin] Could not recover from invalid block", "err", err)
		} else {
			f.currentStateBlockRoot = block.ParentRoot
		}

		return nil, InvalidBlock, invalidBlockErr
	}

	f.blockRewards[blockRoot] = blockRewardsCollector
	f.syncCommittees[blockRoot] = syncCommittees{
		currentSyncCommittee: newState.CurrentSyncCommittee().Copy(),
		nextSyncCommittee:    newState.NextSyncCommittee().Copy(),
	}

	f.blocks.Store(libcommon.Hash(blockRoot), signedBlock)
	bodyRoot, err := signedBlock.Block.Body.HashSSZ()
	if err != nil {
		return nil, LogisticError, err
	}

	f.headers.Store(libcommon.Hash(blockRoot), &cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		Root:          block.StateRoot,
		BodyRoot:      bodyRoot,
	})

	// add the state root
	stateRoot, err := newState.HashSSZ()
	if err != nil {
		return nil, LogisticError, err
	}
	f.stateRoots[stateRoot] = blockRoot

	if newState.Slot()%f.beaconCfg.SlotsPerEpoch == 0 {
		if err := f.dumpBeaconStateOnDisk(newState, blockRoot); err != nil {
			return nil, LogisticError, err
		}
		f.saveStates[blockRoot] = savedStateRecord{slot: newState.Slot()}
	}

	// Lastly add checkpoints to caches as well.
	f.currentJustifiedCheckpoints[blockRoot] = newState.CurrentJustifiedCheckpoint().Copy()
	f.finalizedCheckpoints[blockRoot] = newState.FinalizedCheckpoint().Copy()
	if newState.Slot() > f.highestSeen {
		f.highestSeen = newState.Slot()
		f.currentState = newState
		f.currentStateBlockRoot = blockRoot
	}
	return newState, Success, nil
}

func (f *forkGraphDisk) GetHeader(blockRoot libcommon.Hash) (*cltypes.BeaconBlockHeader, bool) {
	obj, has := f.headers.Load(blockRoot)
	if !has {
		return nil, false
	}
	return obj.(*cltypes.BeaconBlockHeader), true
}

func (f *forkGraphDisk) getBlock(blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlock, bool) {
	obj, has := f.blocks.Load(blockRoot)
	if !has {
		return nil, false
	}

	return obj.(*cltypes.SignedBeaconBlock), true
}

// GetStateAtSlot is for getting a state based off the slot number
// NOTE: all this does is call GetStateAtSlot using the stateRoots index and existing blocks.
func (f *forkGraphDisk) GetStateAtStateRoot(root libcommon.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
	blockRoot, ok := f.stateRoots[root]
	if !ok {
		return nil, ErrStateNotFound
	}
	blockSlot, ok := f.getBlock(blockRoot)
	if !ok {
		return nil, ErrStateNotFound
	}
	return f.GetStateAtSlot(blockSlot.Block.Slot, alwaysCopy)

}

// GetStateAtSlot is for getting a state based off the slot number
// TODO: this is rather inefficient. we could create indices that make it faster
func (f *forkGraphDisk) GetStateAtSlot(slot uint64, alwaysCopy bool) (*state.CachingBeaconState, error) {
	// fast path for if the slot is the current slot
	if f.currentState.Slot() == slot {
		// always copy.
		if alwaysCopy {
			ret, err := f.currentState.Copy()
			return ret, err
		}
		return f.currentState, nil
	}
	// if the slot requested is larger than the current slot, we know it is not found, so another fast path
	if slot > f.currentState.Slot() {
		return nil, ErrStateNotFound
	}
	if len(f.saveStates) == 0 {
		return nil, ErrStateNotFound
	}
	bestSlot := uint64(0)
	startHash := libcommon.Hash{}
	// iterate over all savestates. there should be less than 10 of these, so this should be safe.
	for blockHash, v := range f.saveStates {
		// make sure the slot is smaller than the target slot
		// (equality case caught by short circuit)
		// and that the slot is larger than the current best found starting slot
		if v.slot < slot && v.slot > bestSlot {
			bestSlot = v.slot
			startHash = blockHash
		}
	}
	// no snapshot old enough to honor this request :(
	if bestSlot == 0 {
		return nil, ErrStateNotFound
	}
	copyReferencedState, err := f.readBeaconStateFromDisk(startHash)
	if err != nil {
		return nil, err
	}
	// cache lied? return state not found
	if copyReferencedState == nil {
		return nil, ErrStateNotFound
	}

	// what we need to do is grab every block in our block store that is between the target slot and the current slot
	// this is linear time from the distance to our last snapshot.
	blocksInTheWay := []*cltypes.SignedBeaconBlock{}
	f.blocks.Range(func(key, value interface{}) bool {
		block := value.(*cltypes.SignedBeaconBlock)
		if block.Block.Slot <= f.currentState.Slot() && block.Block.Slot >= slot {
			blocksInTheWay = append(blocksInTheWay, block)
		}
		return true
	})

	// sort the slots from low to high
	slices.SortStableFunc(blocksInTheWay, func(a, b *cltypes.SignedBeaconBlock) int {
		return int(a.Block.Slot) - int(b.Block.Slot)
	})

	// Traverse the blocks from top to bottom.
	for _, block := range blocksInTheWay {
		if err := transition.TransitionState(copyReferencedState, block, nil, false); err != nil {
			return nil, err
		}
	}
	return copyReferencedState, nil
}

func (f *forkGraphDisk) GetState(blockRoot libcommon.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
	if f.currentStateBlockRoot == blockRoot {
		if alwaysCopy {
			ret, err := f.currentState.Copy()
			return ret, err
		}
		return f.currentState, nil
	}

	// collect all blocks beetwen greatest extending node path and block.
	blocksInTheWay := []*cltypes.SignedBeaconBlock{}
	// Use the parent root as a reverse iterator.
	currentIteratorRoot := blockRoot
	// try and find the point of recconection
	for {
		block, isSegmentPresent := f.getBlock(currentIteratorRoot)
		if !isSegmentPresent {
			// check if it is in the header
			bHeader, ok := f.GetHeader(currentIteratorRoot)
			if ok && bHeader.Slot%f.beaconCfg.SlotsPerEpoch == 0 {
				break
			}
			log.Debug("Could not retrieve state: Missing header", "missing", currentIteratorRoot)
			return nil, nil
		}
		if block.Block.Slot%f.beaconCfg.SlotsPerEpoch == 0 {
			break
		}
		blocksInTheWay = append(blocksInTheWay, block)
		currentIteratorRoot = block.Block.ParentRoot
	}
	copyReferencedState, err := f.readBeaconStateFromDisk(currentIteratorRoot)
	if err != nil {
		return nil, err
	}
	if copyReferencedState == nil {
		return nil, ErrStateNotFound
	}

	// Traverse the blocks from top to bottom.
	for i := len(blocksInTheWay) - 1; i >= 0; i-- {
		if err := transition.TransitionState(copyReferencedState, blocksInTheWay[i], nil, false); err != nil {
			return nil, err
		}
	}
	return copyReferencedState, nil
}

func (f *forkGraphDisk) GetCurrentJustifiedCheckpoint(blockRoot libcommon.Hash) (solid.Checkpoint, bool) {
	obj, has := f.currentJustifiedCheckpoints[blockRoot]
	return obj, has
}

func (f *forkGraphDisk) GetFinalizedCheckpoint(blockRoot libcommon.Hash) (solid.Checkpoint, bool) {
	obj, has := f.finalizedCheckpoints[blockRoot]
	return obj, has
}

func (f *forkGraphDisk) MarkHeaderAsInvalid(blockRoot libcommon.Hash) {
	f.badBlocks[blockRoot] = struct{}{}
}

func (f *forkGraphDisk) Prune(pruneSlot uint64) (err error) {
	pruneSlot -= f.beaconCfg.SlotsPerEpoch * 2
	oldRoots := make([]libcommon.Hash, 0, f.beaconCfg.SlotsPerEpoch)
	f.blocks.Range(func(key, value interface{}) bool {
		hash := key.(libcommon.Hash)
		signedBlock := value.(*cltypes.SignedBeaconBlock)
		if signedBlock.Block.Slot >= pruneSlot {
			return true
		}
		oldRoots = append(oldRoots, hash)
		return true
	})

	f.lowestAvaiableSlot = pruneSlot + 1
	for _, root := range oldRoots {
		delete(f.badBlocks, root)
		f.blocks.Delete(root)
		delete(f.currentJustifiedCheckpoints, root)
		delete(f.finalizedCheckpoints, root)
		f.headers.Delete(root)
		delete(f.saveStates, root)
		delete(f.syncCommittees, root)
		delete(f.blockRewards, root)
		f.fs.Remove(getBeaconStateFilename(root))
		f.fs.Remove(getBeaconStateCacheFilename(root))
	}
	log.Debug("Pruned old blocks", "pruneSlot", pruneSlot)
	return
}

func (f *forkGraphDisk) GetSyncCommittees(blockRoot libcommon.Hash) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	obj, has := f.syncCommittees[blockRoot]
	if !has {
		return nil, nil, false
	}
	return obj.currentSyncCommittee, obj.nextSyncCommittee, true
}

func (f *forkGraphDisk) GetBlockRewards(blockRoot libcommon.Hash) (*eth2.BlockRewardsCollector, bool) {
	obj, has := f.blockRewards[blockRoot]
	return obj, has
}

func (f *forkGraphDisk) LowestAvaiableSlot() uint64 {
	return f.lowestAvaiableSlot
}
