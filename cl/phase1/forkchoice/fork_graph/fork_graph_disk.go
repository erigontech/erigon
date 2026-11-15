// Copyright 2024 The Erigon Authors
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

package fork_graph

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/lightclient_utils"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

const (
	dumpSlotFrequency = 4
	pruneBatchSize    = 256
)

type syncCommittees struct {
	currentSyncCommittee *solid.SyncCommittee
	nextSyncCommittee    *solid.SyncCommittee
}

type validatedChildren struct {
	slots   map[common.Hash]uint64
	maxSlot uint64
}

var ErrStateNotFound = errors.New("state not found")

var (
	nextEpochStateOutcomes          = metrics.GetOrCreateCounterVec("caplin_next_epoch_state_outcomes_total", []string{"outcome"})
	replaySyncedHeadFallbackCounter = metrics.GetOrCreateCounter("caplin_forkchoice_replay_synced_head_total")
)

type ChainSegmentInsertionResult uint

const (
	Success                        ChainSegmentInsertionResult = 0
	InvalidBlock                   ChainSegmentInsertionResult = 1
	MissingSegment                 ChainSegmentInsertionResult = 2
	BelowAnchor                    ChainSegmentInsertionResult = 3
	LogisticError                  ChainSegmentInsertionResult = 4
	PreValidated                   ChainSegmentInsertionResult = 5
	Started                        ChainSegmentInsertionResult = 6
	ReplayRequired                 ChainSegmentInsertionResult = 7
	PrecomputedEpochStateAvailable ChainSegmentInsertionResult = 8
)

func (r ChainSegmentInsertionResult) String() string {
	switch r {
	case Success:
		return "Success"
	case InvalidBlock:
		return "block has invalid parent"
	case MissingSegment:
		return "chain missing segment"
	case BelowAnchor:
		return "block below anchor slot"
	case LogisticError:
		return "error occurred"
	case PreValidated:
		return "already validated"
	case Started:
		return "segment started"
	case ReplayRequired:
		return "parent state needs a replay"
	case PrecomputedEpochStateAvailable:
		return "precomputed epoch state available"
	default:
		return fmt.Sprintf("%d <unknown>", r)
	}
}

func convertHashSliceToHashList(in [][32]byte) solid.HashVectorSSZ {
	out := solid.NewHashVector(len(in))
	for i, v := range in {
		out.Set(i, common.Hash(v))
	}
	return out
}

// ForkGraph is our graph for ETH 2.0 consensus forkchoice. Each node is a (block root, changes) pair and
// each edge is the path described as (prevBlockRoot, currBlockRoot). if we want to go forward we use blocks.
type forkGraphDisk struct {
	lifecycleMu       sync.RWMutex
	addPruneMu        sync.Mutex
	pruneMu           sync.Mutex
	pruneBoundaryHook func()
	pruneBatchHook    func()
	pruneChildrenHook func()

	// Alternate beacon states
	fs      afero.Fs
	blocks  sync.Map // set of blocks (block root -> block)
	headers sync.Map // set of headers
	// badBlocks maps an invalid block root to the slot it was seen at, so Prune
	// can drop it: an invalid block is deleted from blocks before its root is
	// recorded here, so the block-keyed pruning below never reaches it. Roots
	// marked with no header to take a slot from are stored as slotUnknown.
	badBlocks           sync.Map // common.Hash -> uint64 slot
	unavailablePayloads sync.Map
	acceptedPayloads    sync.Map
	childrenMu          sync.RWMutex
	children            map[common.Hash]*validatedChildren

	currentState          *state.CachingBeaconState
	currentStateBlockRoot common.Hash // block root of the last processed block (avoids BlockRoot() zeroed-StateRoot issue)
	nextEpochParentRoot   common.Hash
	nextEpochState        *state.CachingBeaconState
	highestTransitionSlot uint64

	// for each block root we also keep track of te equivalent current justified and finalized checkpoints for faster head retrieval.
	currentJustifiedCheckpoints sync.Map
	finalizedCheckpoints        sync.Map
	// keep track of rewards too
	blockRewards sync.Map
	// for each block root we keep track of the sync committees for head retrieval.
	syncCommittees        sync.Map
	lightclientBootstraps sync.Map

	previousIndicies participationIndiciesStore
	currentIndicies  participationIndiciesStore

	// configurations
	beaconCfg   *clparams.BeaconChainConfig
	genesisTime uint64
	// highest block seen
	anchorSlot           uint64
	anchorRoot           common.Hash
	lowestAvailableBlock atomic.Uint64

	newestLightClientUpdate atomic.Value
	// newestLightClientFinalityUpdate is the newest recorded update that has finality (is_finality_update).
	newestLightClientFinalityUpdate atomic.Pointer[cltypes.LightClientUpdate]
	// the lightclientUpdates leaks memory, but it's not a big deal since new data is added every 27 hours.
	lightClientUpdates sync.Map // period -> lightclientupdate
	// lightClientUpdateCandidates holds the updates of blocks that are not finalized yet, by period and signature block
	// root. Finalization moves the canonical ones into lightClientUpdates.
	lightClientUpdateCandidates   map[uint64]map[common.Hash]*cltypes.LightClientUpdate
	lightClientUpdateCandidatesMu sync.RWMutex
	// lightClientFinalized is the block the last KeepFinalizedLightClientUpdates settled, guarded by
	// lightClientUpdateCandidatesMu: a head whose chain does not hold it serves no candidates.
	lightClientFinalized     common.Hash
	lightClientFinalizedSlot uint64

	// Envelope state is tracked in memory by beacon block root.
	// Corrupt files in invalidEnvelopes can be replaced; rejectedEnvelopes blocks redelivery until pruning.
	envelopeExists    sync.Map // common.Hash -> struct{}
	envelopeMissing   sync.Map // common.Hash -> struct{}
	invalidEnvelopes  sync.Map // common.Hash -> struct{}
	rejectedEnvelopes sync.Map // common.Hash -> struct{}

	// stateDumpLock guards the reusable state buffer and the key of the last state dump
	stateDumpLock  sync.Mutex
	sszBuffer      []byte
	lastDumpedRoot common.Hash
	lastDumpedSlot uint64

	// envelopeMu guards the reusable envelope buffer
	envelopeMu        sync.Mutex
	envelopeSSZBuffer []byte

	rcfg       beacon_router_configuration.RouterConfiguration
	syncedData synced_data.SyncedData
}

// Initialize fork graph with a new state.
func NewForkGraphDisk(anchorState *state.CachingBeaconState, syncedData synced_data.SyncedData, aferoFs afero.Fs, rcfg beacon_router_configuration.RouterConfiguration) (ForkGraph, error) {
	farthestExtendingPath := make(map[common.Hash]bool)
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		return nil, err
	}
	anchorHeader := anchorState.LatestBlockHeader()
	if anchorState.Version() >= clparams.GloasVersion && anchorState.Slot() > 0 {
		stateHash := anchorState.PeekPreviousStateRoot()
		if stateHash == (common.Hash{}) {
			if anchorHeader.Slot == anchorState.Slot() && anchorHeader.Root != (common.Hash{}) {
				stateHash = anchorHeader.Root
			} else {
				stateHash, err = anchorState.HashSSZ()
				if err != nil {
					return nil, err
				}
			}
		}
		if anchorHeader.Root == (common.Hash{}) {
			anchorHeader.Root = stateHash
		}
		anchorState.SetPreviousStateRoot(stateHash)
	} else {
		if anchorHeader.Root, err = anchorState.HashSSZ(); err != nil {
			return nil, err
		}
		anchorState.SetPreviousStateRoot(anchorHeader.Root)
	}

	farthestExtendingPath[anchorRoot] = true
	anchorState, err = anchorState.Copy()
	if err != nil {
		return nil, err
	}

	f := &forkGraphDisk{
		fs: aferoFs,
		// current state data
		currentState:          anchorState,
		currentStateBlockRoot: anchorRoot,
		// configuration
		beaconCfg:   anchorState.BeaconConfig(),
		genesisTime: anchorState.GenesisTime(),
		anchorSlot:  anchorState.Slot(),
		anchorRoot:  anchorRoot,
		rcfg:        rcfg,
		syncedData:  syncedData,
		children:    make(map[common.Hash]*validatedChildren),
	}
	f.lowestAvailableBlock.Store(anchorState.Slot())
	f.headers.Store(common.Hash(anchorRoot), &anchorHeader)
	f.sszBuffer = make([]byte, 0, (anchorState.EncodingSizeSSZ()*3)/2)
	if err := f.DumpBeaconStateOnDisk(anchorRoot, anchorState, true); err != nil {
		return nil, err
	}
	// preallocate buffer
	return f, nil
}

func (f *forkGraphDisk) AnchorSlot() uint64 {
	return f.anchorSlot
}

func (f *forkGraphDisk) AnchorRoot() common.Hash {
	return f.anchorRoot
}

func (f *forkGraphDisk) SetNextEpochState(parentRoot common.Hash, state *state.CachingBeaconState) {
	f.addPruneMu.Lock()
	defer f.addPruneMu.Unlock()
	if f.highestTransitionSlot >= state.Slot() {
		recordNextEpochStateOutcome("late", state.Slot())
		return
	}
	f.nextEpochParentRoot = parentRoot
	f.nextEpochState = state
}

func recordNextEpochStateOutcome(outcome string, slot uint64) {
	nextEpochStateOutcomes.WithLabelValues(outcome).Inc()
	log.Debug("Precomputed epoch state", "outcome", outcome, "slot", slot)
}

func (f *forkGraphDisk) takeNextEpochStateLocked(block *cltypes.BeaconBlock, parentSlot uint64, fullValidation bool) *state.CachingBeaconState {
	if f.nextEpochState == nil || block.Slot < f.nextEpochState.Slot() {
		if block.Slot/f.beaconCfg.SlotsPerEpoch > parentSlot/f.beaconCfg.SlotsPerEpoch {
			recordNextEpochStateOutcome("no_entry", block.Slot)
		}
		return nil
	}
	precomputed := f.nextEpochState
	f.nextEpochState = nil
	if !fullValidation {
		recordNextEpochStateOutcome("no_full_validation", block.Slot)
		return nil
	}
	if f.nextEpochParentRoot != block.ParentRoot {
		recordNextEpochStateOutcome("parent_mismatch", block.Slot)
		return nil
	}
	return precomputed
}

func (f *forkGraphDisk) isBlockRootTheCurrentState(blockRoot common.Hash) bool {
	if f.currentState == nil {
		return false
	}
	return blockRoot == f.currentStateBlockRoot
}

func isBelowPrunedBoundary(slot, lowestAvailable uint64) bool {
	return lowestAvailable > 0 && slot < lowestAvailable-1
}

func lastFullyPrunedEpoch(pruneSlot, slotsPerEpoch uint64) (uint64, bool) {
	completedEpochs := pruneSlot / slotsPerEpoch
	if completedEpochs == 0 {
		return 0, false
	}
	return completedEpochs - 1, true
}

func (f *forkGraphDisk) GetHeader(blockRoot common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	obj, has := f.headers.Load(blockRoot)
	if !has {
		return nil, false
	}
	header := obj.(*cltypes.BeaconBlockHeader)
	if blockRoot != f.anchorRoot && isBelowPrunedBoundary(header.Slot, f.lowestAvailableBlock.Load()) {
		return nil, false
	}
	return header, true
}

func (f *forkGraphDisk) GetBlock(blockRoot common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	obj, has := f.blocks.Load(blockRoot)
	if !has {
		return nil, false
	}

	block := obj.(*cltypes.SignedBeaconBlock)
	if isBelowPrunedBoundary(block.Block.Slot, f.lowestAvailableBlock.Load()) {
		return nil, false
	}
	return block, true
}

func (f *forkGraphDisk) HasBlockChildAtOrAfter(blockRoot common.Hash, slot uint64) bool {
	f.childrenMu.RLock()
	defer f.childrenMu.RUnlock()
	children := f.children[blockRoot]
	return children != nil && !isBelowPrunedBoundary(children.maxSlot, f.lowestAvailableBlock.Load()) && children.maxSlot >= slot
}

func (f *forkGraphDisk) HasBlockEquivocation(slot, proposerIndex uint64, exceptRoot common.Hash) bool {
	if isBelowPrunedBoundary(slot, f.lowestAvailableBlock.Load()) {
		return false
	}
	found := false
	f.headers.Range(func(key, value any) bool {
		root, ok := key.(common.Hash)
		if !ok || root == exceptRoot {
			return true
		}
		header, ok := value.(*cltypes.BeaconBlockHeader)
		if ok && header != nil && header.Slot == slot && header.ProposerIndex == proposerIndex {
			found = true
			return false
		}
		return true
	})
	return found
}

func (f *forkGraphDisk) addValidatedChild(parentRoot, childRoot common.Hash, slot uint64) {
	f.childrenMu.Lock()
	defer f.childrenMu.Unlock()
	if f.children == nil {
		f.children = make(map[common.Hash]*validatedChildren)
	}
	if f.children[parentRoot] == nil {
		f.children[parentRoot] = &validatedChildren{slots: make(map[common.Hash]uint64)}
	}
	children := f.children[parentRoot]
	children.slots[childRoot] = slot
	if slot > children.maxSlot {
		children.maxSlot = slot
	}
}

func (f *forkGraphDisk) removeValidatedChildren(rootsByParent map[common.Hash][]common.Hash) {
	f.childrenMu.Lock()
	defer f.childrenMu.Unlock()
	for parentRoot, roots := range rootsByParent {
		children := f.children[parentRoot]
		if children == nil {
			continue
		}
		for _, root := range roots {
			delete(children.slots, root)
		}
		if len(children.slots) == 0 {
			delete(f.children, parentRoot)
			continue
		}
		children.maxSlot = 0
		for _, slot := range children.slots {
			if slot > children.maxSlot {
				children.maxSlot = slot
			}
		}
	}
}

func (f *forkGraphDisk) CurrentStateLocked(blockRoot common.Hash) *state.CachingBeaconState {
	if f.currentState == nil || f.currentStateBlockRoot != blockRoot {
		return nil
	}
	return f.currentState
}

func (f *forkGraphDisk) ReplayState(blockRoot common.Hash) (*state.CachingBeaconState, error) {
	replayed, _, err := f.ReplayStateWithOwnership(blockRoot)
	return replayed, err
}

func (f *forkGraphDisk) ReplayStateWithOwnership(blockRoot common.Hash) (*state.CachingBeaconState, bool, error) {
	if _, ok := f.GetHeader(blockRoot); !ok {
		return nil, false, nil
	}
	if state, ok, err := f.useHeadStateIfPossible(blockRoot, nil); ok || err != nil {
		if ok {
			replaySyncedHeadFallbackCounter.Inc()
		}
		return state, ok, err
	}
	replayed, err := f.replayState(blockRoot, false)
	return replayed, false, err
}

func (f *forkGraphDisk) useHeadStateIfPossible(blockRoot common.Hash, in *state.CachingBeaconState) (out *state.CachingBeaconState, ok bool, err error) {
	if f.syncedData == nil {
		return
	}
	if f.syncedData.HeadRoot() == blockRoot {
		err = f.syncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, headRoot common.Hash, _ uint64) error {
			if headRoot != blockRoot {
				return nil
			}
			// HeadRoot() already matched blockRoot, so we trust the stored root
			// rather than recomputing BlockRoot() which can return incorrect results
			// when the state's incremental hashing cache has been dirtied.
			ok = true
			var err2 error

			if in != nil {
				err2 = headState.CopyInto(in)
				out = in
			} else {
				out, err2 = headState.Copy()
			}
			return err2
		})
		if errors.Is(err, synced_data.ErrNotSynced) {
			err = nil
		}
	}
	return
}

func (f *forkGraphDisk) useCachedStateIfPossible(blockRoot common.Hash, in *state.CachingBeaconState) (out *state.CachingBeaconState, ok bool, err error) {
	if out, ok, err = f.useHeadStateIfPossible(blockRoot, in); ok || err != nil {
		return
	}
	if f.syncedData == nil {
		return
	}

	// check if the state is in the cache
	err = f.syncedData.ViewPreviousHeadState(func(prevHeadState *state.CachingBeaconState) error {
		prevHeadBlockRoot, err := prevHeadState.BlockRoot()
		if err != nil {
			return err
		}

		if prevHeadBlockRoot != blockRoot {
			log.Debug("Not Using a cached beacon state", "blockRoot", blockRoot)
			return nil
		}
		ok = true

		var err2 error
		if in != nil {
			err2 = prevHeadState.CopyInto(in)
			out = in
		} else {
			out, err2 = prevHeadState.Copy()
		}

		return err2
	})
	if errors.Is(err, synced_data.ErrPreviousStateNotAvailable) || errors.Is(err, synced_data.ErrNotSynced) {
		err = nil
	}

	return
}

func (f *forkGraphDisk) replayState(blockRoot common.Hash, addChainSegment bool) (*state.CachingBeaconState, error) {
	if addChainSegment {
		if state, ok, err := f.useCachedStateIfPossible(blockRoot, f.currentState); ok {
			return state, err
		}
	} else {
		if _, ok := f.GetHeader(blockRoot); !ok {
			return nil, nil
		}
	}

	// collect all blocks between greatest extending node path and block.
	blocksInTheWay := []*cltypes.SignedBeaconBlock{}
	// Use the parent root as a reverse iterator.
	currentIteratorRoot := blockRoot
	var copyReferencedState *state.CachingBeaconState
	var err error

	// try and find the point of reconnection
	for copyReferencedState == nil {
		block, isSegmentPresent := f.GetBlock(currentIteratorRoot)
		if !isSegmentPresent {
			// check if it is in the header
			// The anchor state is dumped at startup whatever its slot.
			bHeader, ok := f.GetHeader(currentIteratorRoot)
			if ok && (bHeader.Slot%dumpSlotFrequency == 0 || currentIteratorRoot == f.anchorRoot) {
				copyReferencedState, err = f.readBeaconStateFromDisk(currentIteratorRoot)
				if err != nil {
					if currentIteratorRoot == f.anchorRoot {
						// Once currentState has moved on, this file is the only copy of the anchor state.
						log.Warn("Could not read the anchor state file", "root", currentIteratorRoot, "err", err)
					} else {
						log.Trace("Could not retrieve state", "missing", currentIteratorRoot, "err", err)
					}
					return nil, nil
				}
				continue
			}
			log.Trace("Could not retrieve state: Missing header", "missing", currentIteratorRoot)
			return nil, nil
		}
		if block.Block.Slot%dumpSlotFrequency == 0 {
			copyReferencedState, err = f.readBeaconStateFromDisk(currentIteratorRoot)
			if err != nil {
				log.Trace("Could not retrieve state: Missing header", "missing", currentIteratorRoot, "err", err)
			}
			if copyReferencedState != nil {
				break
			}
		}

		// [Modified in Gloas:EIP7732 defer-payload] No need to track parent envelopes
		// during replay. TransitionState calls ProcessParentExecutionPayload internally,
		// which handles the previous slot's execution effects using data already in the
		// block body (ParentExecutionRequests) and state (LatestExecutionPayloadBid).
		blocksInTheWay = append(blocksInTheWay, block)
		currentIteratorRoot = block.Block.ParentRoot
	}

	// Traverse the blocks from top to bottom.
	for _, b := range slices.Backward(blocksInTheWay) {
		if err := transition.TransitionState(copyReferencedState, b, nil, false); err != nil {
			if addChainSegment {
				f.currentState = nil
				f.currentStateBlockRoot = common.Hash{}
			}
			return nil, err
		}
	}
	return copyReferencedState, nil
}

func (f *forkGraphDisk) GetCurrentJustifiedCheckpoint(blockRoot common.Hash) (solid.Checkpoint, bool) {
	obj, has := f.currentJustifiedCheckpoints.Load(blockRoot)
	if !has {
		return solid.Checkpoint{}, false
	}
	return obj.(solid.Checkpoint), has
}

func (f *forkGraphDisk) GetFinalizedCheckpoint(blockRoot common.Hash) (solid.Checkpoint, bool) {
	obj, has := f.finalizedCheckpoints.Load(blockRoot)
	if !has {
		return solid.Checkpoint{}, false
	}
	return obj.(solid.Checkpoint), has
}

func (f *forkGraphDisk) MarkHeaderAsInvalid(blockRoot common.Hash) {
	slot := uint64(slotUnknown)
	if header, ok := f.GetHeader(blockRoot); ok {
		slot = header.Slot
	}
	f.badBlocks.Store(blockRoot, slot)
}

func (f *forkGraphDisk) IsBlockInvalid(blockRoot common.Hash) bool {
	_, invalid := f.badBlocks.Load(blockRoot)
	return invalid
}

func (f *forkGraphDisk) MarkPayloadUnavailable(blockRoot common.Hash) {
	f.unavailablePayloads.Store(blockRoot, struct{}{})
}

func (f *forkGraphDisk) MarkPayloadAvailable(blockRoot common.Hash) {
	f.unavailablePayloads.Delete(blockRoot)
}

func (f *forkGraphDisk) IsPayloadUnavailable(blockRoot common.Hash) bool {
	_, unavailable := f.unavailablePayloads.Load(blockRoot)
	return unavailable
}

func (f *forkGraphDisk) MarkPayloadAccepted(blockRoot common.Hash, verified bool) {
	f.acceptedPayloads.Store(blockRoot, verified)
}

func (f *forkGraphDisk) ClearPayloadAccepted(blockRoot common.Hash) {
	f.acceptedPayloads.Delete(blockRoot)
}

func (f *forkGraphDisk) PayloadAccepted(blockRoot common.Hash) (bool, bool) {
	verified, ok := f.acceptedPayloads.Load(blockRoot)
	if !ok {
		return false, false
	}
	return verified.(bool), true
}

func (f *forkGraphDisk) retainedBlock(blockRoot common.Hash) bool {
	header, ok := f.headers.Load(blockRoot)
	if !ok {
		return false
	}
	if blockRoot == f.anchorRoot {
		return true
	}
	return !isBelowPrunedBoundary(header.(*cltypes.BeaconBlockHeader).Slot, f.lowestAvailableBlock.Load())
}

func (f *forkGraphDisk) IsBlockRetained(blockRoot common.Hash) bool {
	f.lifecycleMu.RLock()
	defer f.lifecycleMu.RUnlock()
	return f.retainedBlock(blockRoot)
}

func (f *forkGraphDisk) WithRetainedBlock(blockRoot common.Hash, fn func(func(common.Hash) bool)) bool {
	f.lifecycleMu.RLock()
	defer f.lifecycleMu.RUnlock()
	if !f.retainedBlock(blockRoot) {
		return false
	}
	fn(f.retainedBlock)
	return true
}

func (f *forkGraphDisk) hasBeaconState(blockRoot common.Hash) bool {
	exists, err := afero.Exists(f.fs, getBeaconStateFilename(blockRoot))
	return err == nil && exists
}

// slotUnknown marks a bad block with no header to take a slot from, so no prune
// drops it; MarkHeaderAsInvalid records the header's slot when there is one.
const slotUnknown = math.MaxUint64

func (f *forkGraphDisk) Prune(pruneSlot uint64) (err error) {
	f.pruneMu.Lock()
	defer f.pruneMu.Unlock()

	f.badBlocks.Range(func(key, value any) bool {
		if value.(uint64) < pruneSlot {
			f.badBlocks.Delete(key)
		}
		return true
	})
	highestStoredBeaconStateSlot := uint64(0)
	f.blocks.Range(func(key, value any) bool {
		hash := key.(common.Hash)
		signedBlock := value.(*cltypes.SignedBeaconBlock)
		if f.hasBeaconState(hash) && highestStoredBeaconStateSlot < signedBlock.Block.Slot {
			highestStoredBeaconStateSlot = signedBlock.Block.Slot
		}
		return true
	})
	if pruneSlot >= highestStoredBeaconStateSlot {
		return
	}

	f.addPruneMu.Lock()
	f.lifecycleMu.Lock()
	// Prune runs without the fork choice lock, so concurrent (or stale queued)
	// calls may arrive out of order: only ever raise the marker.
	for {
		lowest := f.lowestAvailableBlock.Load()
		if pruneSlot+1 <= lowest || f.lowestAvailableBlock.CompareAndSwap(lowest, pruneSlot+1) {
			break
		}
	}
	f.lifecycleMu.Unlock()
	if lastPrunedEpoch, ok := lastFullyPrunedEpoch(pruneSlot, f.beaconCfg.SlotsPerEpoch); ok {
		currentIndexKeys := f.currentIndicies.keysThrough(lastPrunedEpoch)
		previousIndexKeys := f.previousIndicies.keysThrough(lastPrunedEpoch)
		f.currentIndicies.deleteKeys(currentIndexKeys)
		f.previousIndicies.deleteKeys(previousIndexKeys)
	}
	f.addPruneMu.Unlock()
	if f.pruneBoundaryHook != nil {
		f.pruneBoundaryHook()
	}

	oldRoots := make([]common.Hash, 0, f.beaconCfg.SlotsPerEpoch)
	validatedRootsByParent := make(map[common.Hash][]common.Hash)
	f.blocks.Range(func(key, value any) bool {
		if value.(*cltypes.SignedBeaconBlock).Block.Slot < pruneSlot {
			root := key.(common.Hash)
			oldRoots = append(oldRoots, root)
			if header, ok := f.headers.Load(root); ok {
				h := header.(*cltypes.BeaconBlockHeader)
				validatedRootsByParent[h.ParentRoot] = append(validatedRootsByParent[h.ParentRoot], root)
			}
		}
		return true
	})
	for start := 0; start < len(oldRoots); start += pruneBatchSize {
		end := min(start+pruneBatchSize, len(oldRoots))
		f.lifecycleMu.Lock()
		for _, root := range oldRoots[start:end] {
			f.blocks.Delete(root)
			f.lightclientBootstraps.Delete(root)
			f.currentJustifiedCheckpoints.Delete(root)
			f.finalizedCheckpoints.Delete(root)
			f.headers.Delete(root)
			f.blockRewards.Delete(root)
			f.envelopeExists.Delete(root)
			f.envelopeMissing.Delete(root)
			f.invalidEnvelopes.Delete(root)
			f.rejectedEnvelopes.Delete(root)
			f.unavailablePayloads.Delete(root)
			f.acceptedPayloads.Delete(root)
			f.badBlocks.Delete(root)
		}
		f.lifecycleMu.Unlock()
		if f.pruneBatchHook != nil {
			f.pruneBatchHook()
		}
	}
	if f.pruneChildrenHook != nil {
		f.pruneChildrenHook()
	}
	f.removeValidatedChildren(validatedRootsByParent)

	for _, root := range oldRoots {
		if removeErr := f.fs.Remove(getBeaconStateFilename(root)); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			log.Debug("failed to remove pruned beacon state file", "root", root, "err", removeErr)
		}
		if removeErr := f.fs.Remove(getEnvelopeFilename(root)); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove envelope for root %x: %w", root, removeErr))
		}
		if removeErr := f.fs.Remove(getEnvelopeTempFilename(root)); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove envelope temp for root %x: %w", root, removeErr))
		}
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

func (f *forkGraphDisk) GetBlockRewards(blockRoot common.Hash) (*eth2.BlockRewardsCollector, bool) {
	obj, has := f.blockRewards.Load(blockRoot)
	if !has {
		return nil, false
	}
	return obj.(*eth2.BlockRewardsCollector), true
}

func (f *forkGraphDisk) LowestAvailableSlot() uint64 {
	return f.lowestAvailableBlock.Load()
}

func (f *forkGraphDisk) GetLightClientBootstrap(blockRoot common.Hash) (*cltypes.LightClientBootstrap, bool) {
	obj, has := f.lightclientBootstraps.Load(blockRoot)
	if !has {
		return nil, false
	}
	return obj.(*cltypes.LightClientBootstrap), true
}

func (f *forkGraphDisk) NewestLightClientFinalityUpdate() *cltypes.LightClientUpdate {
	return f.newestLightClientFinalityUpdate.Load()
}

func (f *forkGraphDisk) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	if f.newestLightClientUpdate.Load() == nil {
		return nil
	}
	return f.newestLightClientUpdate.Load().(*cltypes.LightClientUpdate)
}

// recordLightClientUpdateLocked takes the update of a block whose transition succeeded as the newest update, and as a
// candidate of its period if it is signed in the period of its attested header. The caller holds addPruneMu.
func (f *forkGraphDisk) recordLightClientUpdateLocked(signatureBlock common.Hash, update *cltypes.LightClientUpdate) {
	f.newestLightClientUpdate.Store(update)
	if lightclient_utils.IsFinalityUpdate(update) {
		f.newestLightClientFinalityUpdate.Store(update)
	}
	if f.beaconCfg.SyncCommitteePeriod(update.AttestedHeader.Beacon.Slot) != f.beaconCfg.SyncCommitteePeriod(update.SignatureSlot) {
		return
	}
	f.lightClientUpdateCandidatesMu.Lock()
	defer f.lightClientUpdateCandidatesMu.Unlock()
	if f.lightClientUpdateCandidates == nil {
		f.lightClientUpdateCandidates = make(map[uint64]map[common.Hash]*cltypes.LightClientUpdate)
	}
	period := f.beaconCfg.SyncCommitteePeriod(update.SignatureSlot)
	if f.lightClientUpdateCandidates[period] == nil {
		f.lightClientUpdateCandidates[period] = make(map[common.Hash]*cltypes.LightClientUpdate)
	}
	f.lightClientUpdateCandidates[period][signatureBlock] = update
}

// KeepFinalizedLightClientUpdates moves the candidates whose signature block is on the finalized block's chain into
// the period store, and drops the other candidates at or below the finalized slot: they can no longer be canonical.
func (f *forkGraphDisk) KeepFinalizedLightClientUpdates(finalized common.Hash) {
	f.addPruneMu.Lock()
	defer f.addPruneMu.Unlock()
	f.lightClientUpdateCandidatesMu.Lock()
	defer f.lightClientUpdateCandidatesMu.Unlock()
	header, ok := f.GetHeader(finalized)
	if !ok {
		return
	}
	finalizedSlot, lowest := header.Slot, header.Slot
	f.lightClientFinalized, f.lightClientFinalizedSlot = finalized, finalizedSlot
	for _, candidates := range f.lightClientUpdateCandidates {
		lowest = min(lowest, lowestSignatureSlot(candidates))
	}
	for root := finalized; ok && header.Slot >= lowest; header, ok = f.GetHeader(root) {
		if update, isCandidate := f.lightClientUpdateCandidates[f.beaconCfg.SyncCommitteePeriod(header.Slot)][root]; isCandidate {
			f.storeLightClientUpdate(update)
		}
		root = header.ParentRoot
	}
	for period, candidates := range f.lightClientUpdateCandidates {
		for root, update := range candidates {
			if update.SignatureSlot <= finalizedSlot {
				delete(candidates, root)
			}
		}
		if len(candidates) == 0 {
			delete(f.lightClientUpdateCandidates, period)
		}
	}
}

func lowestSignatureSlot(candidates map[common.Hash]*cltypes.LightClientUpdate) uint64 {
	lowest := uint64(math.MaxUint64)
	for _, update := range candidates {
		lowest = min(lowest, update.SignatureSlot)
	}
	return lowest
}

// storeLightClientUpdate keeps the best update of its attested header's sync committee period.
func (f *forkGraphDisk) storeLightClientUpdate(update *cltypes.LightClientUpdate) {
	period := f.beaconCfg.SyncCommitteePeriod(update.AttestedHeader.Beacon.Slot)
	if best, ok := f.lightClientUpdates.Load(period); ok && !lightclient_utils.IsBetterUpdate(f.beaconCfg, update, best.(*cltypes.LightClientUpdate)) {
		return
	}
	f.lightClientUpdates.Store(period, update)
}

// GetLightClientUpdates returns the best update of each sync committee period in [startPeriod, endPeriod), nil for a
// period without one: the update finalization settled, or a better candidate whose signature block is on head's chain.
// A zero head, or a head whose chain does not hold the last finalized block, gives the settled updates only, even when
// that chain holds a better candidate. One walk down head's chain serves every period of the range.
func (f *forkGraphDisk) GetLightClientUpdates(startPeriod, endPeriod uint64, head common.Hash) []*cltypes.LightClientUpdate {
	if endPeriod <= startPeriod {
		return nil
	}
	// Finalization moves candidates into the period store under the write lock, so both are read in one state.
	f.lightClientUpdateCandidatesMu.RLock()
	defer f.lightClientUpdateCandidatesMu.RUnlock()
	updates := make([]*cltypes.LightClientUpdate, endPeriod-startPeriod)
	for i := range updates {
		if obj, ok := f.lightClientUpdates.Load(startPeriod + uint64(i)); ok {
			updates[i] = obj.(*cltypes.LightClientUpdate)
		}
	}
	lowest := uint64(math.MaxUint64)
	for period, candidates := range f.lightClientUpdateCandidates {
		if period >= startPeriod && period < endPeriod {
			lowest = min(lowest, lowestSignatureSlot(candidates))
		}
	}
	if lowest == math.MaxUint64 {
		return updates
	}
	found := make([]*cltypes.LightClientUpdate, len(updates))
	onFinalizedChain := f.lightClientFinalized == (common.Hash{})
	for {
		// The finalized root is recognised before its header is read: pruning can hide that header.
		if !onFinalizedChain && head == f.lightClientFinalized {
			onFinalizedChain = true
		}
		header, ok := f.GetHeader(head)
		if !ok || (onFinalizedChain && header.Slot < lowest) {
			break
		}
		if !onFinalizedChain && header.Slot <= f.lightClientFinalizedSlot {
			return updates // past the finalized slot on another branch
		}
		period := f.beaconCfg.SyncCommitteePeriod(header.Slot)
		if update, isCandidate := f.lightClientUpdateCandidates[period][head]; isCandidate && period >= startPeriod && period < endPeriod {
			if best := found[period-startPeriod]; best == nil || lightclient_utils.IsBetterUpdate(f.beaconCfg, update, best) {
				found[period-startPeriod] = update
			}
		}
		head = header.ParentRoot
	}
	if !onFinalizedChain {
		return updates
	}
	for i, update := range found {
		if update != nil && (updates[i] == nil || lightclient_utils.IsBetterUpdate(f.beaconCfg, update, updates[i])) {
			updates[i] = update
		}
	}
	return updates
}

func (f *forkGraphDisk) GetPreviousParticipationIndicies(epoch uint64) (*solid.ParticipationBitList, error) {
	b, ok := f.previousIndicies.get(epoch)
	if !ok {
		if epoch == 0 {
			return nil, nil
		}
		b, ok = f.previousIndicies.get(epoch - 1)
		if !ok {
			return nil, nil
		}
	}
	if len(b) == 0 {
		return nil, nil
	}
	out := solid.NewParticipationBitList(0, int(f.beaconCfg.ValidatorRegistryLimit))
	err := out.DecodeSSZ(b, 0)
	return out, err
}

func (f *forkGraphDisk) GetCurrentParticipationIndicies(epoch uint64) (*solid.ParticipationBitList, error) {
	b, ok := f.currentIndicies.get(epoch)
	if !ok {
		if epoch == 0 {
			return nil, nil
		}
		b, ok = f.currentIndicies.get(epoch - 1)
		if !ok {
			return nil, nil
		}
	}
	if len(b) == 0 {
		return nil, nil
	}
	out := solid.NewParticipationBitList(0, int(f.beaconCfg.ValidatorRegistryLimit))
	err := out.DecodeSSZ(b, 0)
	return out, err
}
