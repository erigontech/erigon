package fork_graph

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beacon_router_configuration"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/lightclient_utils"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	diffstorage "github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph/diff_storage"
	"github.com/ledgerwatch/erigon/cl/transition"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
)

const dumpSlotFrequency = 17

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

func convertHashSliceToHashList(in [][32]byte) solid.HashVectorSSZ {
	out := solid.NewHashVector(len(in))
	for i, v := range in {
		out.Set(i, libcommon.Hash(v))
	}
	return out
}

// ForkGraph is our graph for ETH 2.0 consensus forkchoice. Each node is a (block root, changes) pair and
// each edge is the path described as (prevBlockRoot, currBlockRoot). if we want to go forward we use blocks.
type forkGraphDisk struct {
	// Alternate beacon states
	fs        afero.Fs
	blocks    sync.Map // set of blocks (block root -> block)
	headers   sync.Map // set of headers
	badBlocks sync.Map // blocks that are invalid and that leads to automatic fail of extension.

	// current state data
	currentState *state.CachingBeaconState

	// for each block root we also keep track of te equivalent current justified and finalized checkpoints for faster head retrieval.
	currentJustifiedCheckpoints sync.Map
	finalizedCheckpoints        sync.Map
	// keep track of rewards too
	blockRewards sync.Map
	// for each block root we keep track of the sync committees for head retrieval.
	syncCommittees        sync.Map
	lightclientBootstraps sync.Map
	// diffs storage
	balancesStorage         *diffstorage.ChainDiffStorage
	validatorSetStorage     *diffstorage.ChainDiffStorage
	inactivityScoresStorage *diffstorage.ChainDiffStorage
	previousIndicies        sync.Map
	currentIndicies         sync.Map

	// configurations
	beaconCfg   *clparams.BeaconChainConfig
	genesisTime uint64
	// highest block seen
	highestSeen, anchorSlot uint64
	lowestAvaiableBlock     atomic.Uint64

	newestLightClientUpdate atomic.Value
	// the lightclientUpdates leaks memory, but it's not a big deal since new data is added every 27 hours.
	lightClientUpdates sync.Map // period -> lightclientupdate

	// reusable buffers
	sszBuffer       bytes.Buffer
	sszSnappyBuffer bytes.Buffer

	rcfg beacon_router_configuration.RouterConfiguration
}

// Initialize fork graph with a new state
func NewForkGraphDisk(anchorState *state.CachingBeaconState, aferoFs afero.Fs, rcfg beacon_router_configuration.RouterConfiguration) ForkGraph {
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

	balancesStorage := diffstorage.NewChainDiffStorage(base_encoding.ComputeCompressedSerializedUint64ListDiff, base_encoding.ApplyCompressedSerializedUint64ListDiff)
	validatorSetStorage := diffstorage.NewChainDiffStorage(base_encoding.ComputeCompressedSerializedValidatorSetListDiff, base_encoding.ApplyCompressedSerializedValidatorListDiff)
	inactivityScoresStorage := diffstorage.NewChainDiffStorage(base_encoding.ComputeCompressedSerializedUint64ListDiff, base_encoding.ApplyCompressedSerializedUint64ListDiff)

	f := &forkGraphDisk{
		fs: aferoFs,
		// current state data
		currentState: anchorState,
		// configuration
		beaconCfg:               anchorState.BeaconConfig(),
		genesisTime:             anchorState.GenesisTime(),
		anchorSlot:              anchorState.Slot(),
		balancesStorage:         balancesStorage,
		validatorSetStorage:     validatorSetStorage,
		inactivityScoresStorage: inactivityScoresStorage,
		rcfg:                    rcfg,
	}
	f.lowestAvaiableBlock.Store(anchorState.Slot())
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
		f.badBlocks.Store(libcommon.Hash(blockRoot), struct{}{})
		return nil, BelowAnchor, nil
	}
	// Check if block being process right now was marked as invalid.
	if _, ok := f.badBlocks.Load(libcommon.Hash(blockRoot)); ok {
		log.Debug("block has invalid parent", "slot", block.Slot, "hash", libcommon.Hash(blockRoot))
		return nil, InvalidBlock, nil
	}

	newState, err := f.GetState(block.ParentRoot, false)
	if err != nil {
		return nil, LogisticError, fmt.Errorf("AddChainSegment: %w, parentRoot; %x", err, block.ParentRoot)
	}
	if newState == nil {
		log.Trace("AddChainSegment: missing segment", "block", libcommon.Hash(blockRoot))
		return nil, MissingSegment, nil
	}
	finalizedBlock, hasFinalized := f.getBlock(newState.FinalizedCheckpoint().BlockRoot())
	parentBlock, hasParentBlock := f.getBlock(block.ParentRoot)

	// Before processing the state: update the newest lightclient update.
	if block.Version() >= clparams.AltairVersion && hasParentBlock && fullValidation && hasFinalized && f.rcfg.Beacon {
		nextSyncCommitteeBranch, err := newState.NextSyncCommitteeBranch()
		if err != nil {
			return nil, LogisticError, err
		}
		finalityBranch, err := newState.FinalityRootBranch()
		if err != nil {
			return nil, LogisticError, err
		}
		lightclientUpdate, err := lightclient_utils.CreateLightClientUpdate(f.beaconCfg, signedBlock, finalizedBlock, parentBlock, newState.Slot(),
			newState.NextSyncCommittee(), newState.FinalizedCheckpoint(), convertHashSliceToHashList(nextSyncCommitteeBranch), convertHashSliceToHashList(finalityBranch))
		if err != nil {
			log.Debug("Could not create light client update", "err", err)
		} else {
			f.newestLightClientUpdate.Store(lightclientUpdate)
			period := f.beaconCfg.SyncCommitteePeriod(newState.Slot())
			_, hasPeriod := f.lightClientUpdates.Load(period)
			if !hasPeriod {
				log.Info("Adding light client update", "period", period)
				f.lightClientUpdates.Store(period, lightclientUpdate)
			}
		}
	}

	blockRewardsCollector := &eth2.BlockRewardsCollector{}
	var prevDumpBalances, prevValidatorSetDump, prevInactivityScores []byte
	epochCross := newState.Slot()/f.beaconCfg.SlotsPerEpoch != block.Slot/f.beaconCfg.SlotsPerEpoch
	if (f.rcfg.Beacon || f.rcfg.Validator || f.rcfg.Lighthouse) && !epochCross {
		prevDumpBalances = libcommon.Copy(newState.RawBalances())
		prevValidatorSetDump = libcommon.Copy(newState.RawValidatorSet())
		prevInactivityScores = libcommon.Copy(newState.RawInactivityScores())
	}
	// Execute the state
	if invalidBlockErr := transition.TransitionState(newState, signedBlock, blockRewardsCollector, fullValidation); invalidBlockErr != nil {
		// Add block to list of invalid blocks
		log.Warn("Invalid beacon block", "slot", block.Slot, "blockRoot", blockRoot, "reason", invalidBlockErr)
		f.badBlocks.Store(libcommon.Hash(blockRoot), struct{}{})
		f.currentState = nil

		return nil, InvalidBlock, invalidBlockErr
	}

	// update diff storages.
	if f.rcfg.Beacon || f.rcfg.Validator || f.rcfg.Lighthouse {
		if block.Version() != clparams.Phase0Version {
			f.currentIndicies.Store(libcommon.Hash(blockRoot), libcommon.Copy(newState.RawCurrentEpochParticipation()))
			f.previousIndicies.Store(libcommon.Hash(blockRoot), libcommon.Copy(newState.RawPreviousEpochParticipation()))
			f.inactivityScoresStorage.Insert(libcommon.Hash(blockRoot), block.ParentRoot, prevInactivityScores, newState.RawInactivityScores(), epochCross)
		}
		f.blockRewards.Store(libcommon.Hash(blockRoot), blockRewardsCollector)
		f.balancesStorage.Insert(libcommon.Hash(blockRoot), block.ParentRoot, prevDumpBalances, newState.RawBalances(), epochCross)
		f.validatorSetStorage.Insert(libcommon.Hash(blockRoot), block.ParentRoot, prevValidatorSetDump, newState.RawValidatorSet(), epochCross)
		period := f.beaconCfg.SyncCommitteePeriod(newState.Slot())
		f.syncCommittees.Store(period, syncCommittees{
			currentSyncCommittee: newState.CurrentSyncCommittee().Copy(),
			nextSyncCommittee:    newState.NextSyncCommittee().Copy(),
		})

		if block.Version() >= clparams.AltairVersion {
			lightclientBootstrap, err := lightclient_utils.CreateLightClientBootstrap(newState, signedBlock)
			if err != nil {
				return nil, LogisticError, err
			}
			f.lightclientBootstraps.Store(libcommon.Hash(blockRoot), lightclientBootstrap)
		}
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

	if newState.Slot()%dumpSlotFrequency == 0 {
		if err := f.dumpBeaconStateOnDisk(newState, blockRoot); err != nil {
			return nil, LogisticError, err
		}
	}

	// Lastly add checkpoints to caches as well.
	f.currentJustifiedCheckpoints.Store(libcommon.Hash(blockRoot), newState.CurrentJustifiedCheckpoint().Copy())
	f.finalizedCheckpoints.Store(libcommon.Hash(blockRoot), newState.FinalizedCheckpoint().Copy())
	if newState.Slot() > f.highestSeen {
		f.highestSeen = newState.Slot()
		f.currentState = newState
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

func (f *forkGraphDisk) GetState(blockRoot libcommon.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
	if f.currentState != nil && !alwaysCopy {
		currentStateBlockRoot, err := f.currentState.BlockRoot()
		if err != nil {
			return nil, err
		}
		if currentStateBlockRoot == blockRoot {
			return f.currentState, nil
		}
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
			if ok && bHeader.Slot%dumpSlotFrequency == 0 {
				break
			}
			log.Trace("Could not retrieve state: Missing header", "missing", currentIteratorRoot)
			return nil, nil
		}
		if block.Block.Slot%dumpSlotFrequency == 0 {
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
	obj, has := f.currentJustifiedCheckpoints.Load(blockRoot)
	if !has {
		return solid.Checkpoint{}, false
	}
	return obj.(solid.Checkpoint), has
}

func (f *forkGraphDisk) GetFinalizedCheckpoint(blockRoot libcommon.Hash) (solid.Checkpoint, bool) {
	obj, has := f.finalizedCheckpoints.Load(blockRoot)
	if !has {
		return solid.Checkpoint{}, false
	}
	return obj.(solid.Checkpoint), has
}

func (f *forkGraphDisk) MarkHeaderAsInvalid(blockRoot libcommon.Hash) {
	f.badBlocks.Store(blockRoot, struct{}{})
}

func (f *forkGraphDisk) Prune(pruneSlot uint64) (err error) {
	pruneSlot -= f.beaconCfg.SlotsPerEpoch * 2
	oldRoots := make([]libcommon.Hash, 0, f.beaconCfg.SlotsPerEpoch)
	highestCrossedEpochSlot := uint64(0)
	f.blocks.Range(func(key, value interface{}) bool {
		hash := key.(libcommon.Hash)
		signedBlock := value.(*cltypes.SignedBeaconBlock)
		if signedBlock.Block.Slot%f.beaconCfg.SlotsPerEpoch == 0 && highestCrossedEpochSlot < signedBlock.Block.Slot {
			highestCrossedEpochSlot = signedBlock.Block.Slot
		}
		if signedBlock.Block.Slot >= pruneSlot {
			return true
		}
		oldRoots = append(oldRoots, hash)

		return true
	})
	if pruneSlot >= highestCrossedEpochSlot {
		return
	}

	f.lowestAvaiableBlock.Store(pruneSlot + 1)
	for _, root := range oldRoots {
		f.badBlocks.Delete(root)
		f.blocks.Delete(root)
		f.lightclientBootstraps.Delete(root)
		f.currentJustifiedCheckpoints.Delete(root)
		f.finalizedCheckpoints.Delete(root)
		f.headers.Delete(root)
		f.blockRewards.Delete(root)
		f.fs.Remove(getBeaconStateFilename(root))
		f.fs.Remove(getBeaconStateCacheFilename(root))
		f.balancesStorage.Delete(root)
		f.validatorSetStorage.Delete(root)
		f.inactivityScoresStorage.Delete(root)
		f.previousIndicies.Delete(root)
		f.currentIndicies.Delete(root)
	}
	log.Debug("Pruned old blocks", "pruneSlot", pruneSlot)
	return
}

func (f *forkGraphDisk) GetSyncCommittees(period uint64) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	obj, has := f.syncCommittees.Load(period)
	if !has {
		return nil, nil, false
	}
	ret := obj.(syncCommittees)
	return ret.currentSyncCommittee, ret.nextSyncCommittee, true
}

func (f *forkGraphDisk) GetBlockRewards(blockRoot libcommon.Hash) (*eth2.BlockRewardsCollector, bool) {
	obj, has := f.blockRewards.Load(blockRoot)
	if !has {
		return nil, false
	}
	return obj.(*eth2.BlockRewardsCollector), true
}

func (f *forkGraphDisk) LowestAvaiableSlot() uint64 {
	return f.lowestAvaiableBlock.Load()
}

func (f *forkGraphDisk) GetLightClientBootstrap(blockRoot libcommon.Hash) (*cltypes.LightClientBootstrap, bool) {
	obj, has := f.lightclientBootstraps.Load(blockRoot)
	if !has {
		return nil, false
	}
	return obj.(*cltypes.LightClientBootstrap), true
}

func (f *forkGraphDisk) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	if f.newestLightClientUpdate.Load() == nil {
		return nil
	}
	return f.newestLightClientUpdate.Load().(*cltypes.LightClientUpdate)
}

func (f *forkGraphDisk) GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool) {
	obj, has := f.lightClientUpdates.Load(period)
	if !has {
		return nil, false
	}
	return obj.(*cltypes.LightClientUpdate), true
}

func (f *forkGraphDisk) GetBalances(blockRoot libcommon.Hash) (solid.Uint64ListSSZ, error) {
	b, err := f.balancesStorage.Get(blockRoot)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	out := solid.NewUint64ListSSZ(int(f.beaconCfg.ValidatorRegistryLimit))
	return out, out.DecodeSSZ(b, 0)
}

func (f *forkGraphDisk) GetInactivitiesScores(blockRoot libcommon.Hash) (solid.Uint64ListSSZ, error) {
	b, err := f.inactivityScoresStorage.Get(blockRoot)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	out := solid.NewUint64ListSSZ(int(f.beaconCfg.ValidatorRegistryLimit))
	return out, out.DecodeSSZ(b, 0)
}

func (f *forkGraphDisk) GetPreviousPartecipationIndicies(blockRoot libcommon.Hash) (*solid.BitList, error) {
	b, ok := f.previousIndicies.Load(blockRoot)
	if !ok {
		return nil, nil
	}
	if len(b.([]byte)) == 0 {
		return nil, nil
	}
	out := solid.NewBitList(0, int(f.beaconCfg.ValidatorRegistryLimit))
	return out, out.DecodeSSZ(b.([]byte), 0)
}

func (f *forkGraphDisk) GetCurrentPartecipationIndicies(blockRoot libcommon.Hash) (*solid.BitList, error) {
	b, ok := f.currentIndicies.Load(blockRoot)
	if !ok {
		return nil, nil
	}
	if len(b.([]byte)) == 0 {
		return nil, nil
	}
	out := solid.NewBitList(0, int(f.beaconCfg.ValidatorRegistryLimit))
	return out, out.DecodeSSZ(b.([]byte), 0)
}

func (f *forkGraphDisk) GetValidatorSet(blockRoot libcommon.Hash) (*solid.ValidatorSet, error) {
	b, err := f.validatorSetStorage.Get(blockRoot)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	out := solid.NewValidatorSet(int(f.beaconCfg.ValidatorRegistryLimit))
	return out, out.DecodeSSZ(b, 0)
}
