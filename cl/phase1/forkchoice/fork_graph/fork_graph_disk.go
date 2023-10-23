package fork_graph

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/golang/snappy"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/transition"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
)

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

// ForkGraph is our graph for ETH 2.0 consensus forkchoice. Each node is a (block root, changes) pair and
// each edge is the path described as (prevBlockRoot, currBlockRoot). if we want to go forward we use blocks.
type forkGraphDisk struct {
	// Alternate beacon states
	fs        afero.Fs
	blocks    map[libcommon.Hash]*cltypes.SignedBeaconBlock // set of blocks
	headers   map[libcommon.Hash]*cltypes.BeaconBlockHeader // set of headers
	badBlocks map[libcommon.Hash]struct{}                   // blocks that are invalid and that leads to automatic fail of extension.
	// current state data
	currentState          *state.CachingBeaconState
	currentStateBlockRoot libcommon.Hash
	// for each block root we also keep track of te equivalent current justified and finalized checkpoints for faster head retrieval.
	currentJustifiedCheckpoints map[libcommon.Hash]solid.Checkpoint
	finalizedCheckpoints        map[libcommon.Hash]solid.Checkpoint

	// configurations
	beaconCfg   *clparams.BeaconChainConfig
	genesisTime uint64
	// highest block seen
	highestSeen, anchorSlot uint64

	// reusable buffers
	sszBuffer       bytes.Buffer
	sszSnappyBuffer bytes.Buffer
}

func getBeaconStateFilename(blockRoot libcommon.Hash) string {
	return fmt.Sprintf("%x.snappy_ssz", blockRoot)
}

func (f *forkGraphDisk) readBeaconStateFromDisk(blockRoot libcommon.Hash) (bs *state.CachingBeaconState, err error) {
	var file afero.File
	file, err = f.fs.Open(getBeaconStateFilename(blockRoot))

	if err != nil {
		return
	}
	defer file.Close()
	// Read the version
	v := []byte{0}
	if _, err := file.Read(v); err != nil {
		return nil, err
	}
	// Read the length
	lengthBytes := make([]byte, 8)
	_, err = file.Read(lengthBytes)
	if err != nil {
		return
	}
	// Grow the snappy buffer
	f.sszSnappyBuffer.Grow(int(binary.BigEndian.Uint64(lengthBytes)))
	// Read the snappy buffer
	sszSnappyBuffer := f.sszSnappyBuffer.Bytes()
	sszSnappyBuffer = sszSnappyBuffer[:cap(sszSnappyBuffer)]
	var n int
	n, err = file.Read(sszSnappyBuffer)
	if err != nil {
		return
	}

	decLen, err := snappy.DecodedLen(sszSnappyBuffer[:n])
	if err != nil {
		return
	}
	// Grow the plain ssz buffer
	f.sszBuffer.Grow(decLen)
	sszBuffer := f.sszBuffer.Bytes()
	sszBuffer, err = snappy.Decode(sszBuffer, sszSnappyBuffer[:n])
	if err != nil {
		return
	}
	bs = state.New(f.beaconCfg)
	err = bs.DecodeSSZ(sszBuffer, int(v[0]))
	return
}

// dumpBeaconStateOnDisk dumps a beacon state on disk in ssz snappy format
func (f *forkGraphDisk) dumpBeaconStateOnDisk(bs *state.CachingBeaconState, blockRoot libcommon.Hash) (err error) {
	// Truncate and then grow the buffer to the size of the state.
	encodingSizeSSZ := bs.EncodingSizeSSZ()
	f.sszBuffer.Grow(encodingSizeSSZ)
	f.sszBuffer.Reset()

	sszBuffer := f.sszBuffer.Bytes()
	sszBuffer, err = bs.EncodeSSZ(sszBuffer)
	if err != nil {
		return
	}
	// Grow the snappy buffer
	f.sszSnappyBuffer.Grow(snappy.MaxEncodedLen(len(sszBuffer)))
	// Compress the ssz buffer
	sszSnappyBuffer := f.sszSnappyBuffer.Bytes()
	sszSnappyBuffer = sszSnappyBuffer[:cap(sszSnappyBuffer)]
	sszSnappyBuffer = snappy.Encode(sszSnappyBuffer, sszBuffer)
	var dumpedFile afero.File
	dumpedFile, err = f.fs.OpenFile(getBeaconStateFilename(blockRoot), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return
	}
	// First write the hard fork version
	_, err = dumpedFile.Write([]byte{byte(bs.Version())})
	if err != nil {
		return
	}
	// Second write the length
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(sszSnappyBuffer)))
	_, err = dumpedFile.Write(length)
	if err != nil {
		return
	}
	// Lastly dump the state
	_, err = dumpedFile.Write(sszSnappyBuffer)
	if err != nil {
		return
	}

	err = dumpedFile.Sync()
	return
}

// Initialize fork graph with a new state
func NewForkGraphDisk(anchorState *state.CachingBeaconState, aferoFs afero.Fs) ForkGraph {
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

	f := &forkGraphDisk{
		fs: aferoFs,
		// storage
		blocks:    make(map[libcommon.Hash]*cltypes.SignedBeaconBlock),
		headers:   headers,
		badBlocks: make(map[libcommon.Hash]struct{}),
		// current state data
		currentState:          anchorState,
		currentStateBlockRoot: anchorRoot,
		// checkpoints trackers
		currentJustifiedCheckpoints: make(map[libcommon.Hash]solid.Checkpoint),
		finalizedCheckpoints:        make(map[libcommon.Hash]solid.Checkpoint),
		// configuration
		beaconCfg:   anchorState.BeaconConfig(),
		genesisTime: anchorState.GenesisTime(),
		anchorSlot:  anchorState.Slot(),
	}
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

	if _, ok := f.headers[blockRoot]; ok {
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

	// Execute the state
	if invalidBlockErr := transition.TransitionState(newState, signedBlock, fullValidation); invalidBlockErr != nil {
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

	if newState.Slot()%f.beaconCfg.SlotsPerEpoch == 0 {
		if err := f.dumpBeaconStateOnDisk(newState, blockRoot); err != nil {
			return nil, LogisticError, err
		}
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
	obj, has := f.headers[blockRoot]
	return obj, has
}

func (f *forkGraphDisk) getBlock(blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlock, bool) {
	obj, has := f.blocks[blockRoot]
	return obj, has
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
		if err := transition.TransitionState(copyReferencedState, blocksInTheWay[i], false); err != nil {
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
		delete(f.currentJustifiedCheckpoints, root)
		delete(f.finalizedCheckpoints, root)
		delete(f.headers, root)
		f.fs.Remove(getBeaconStateFilename(root))
	}
	log.Debug("Pruned old blocks", "pruneSlot", pruneSlot)
	return
}
