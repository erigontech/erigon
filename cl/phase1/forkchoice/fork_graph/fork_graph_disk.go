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
	"sync"
	"sync/atomic"

	"github.com/golang/snappy"
	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
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
)

const dumpSlotFrequency = 4

type syncCommittees struct {
	currentSyncCommittee *solid.SyncCommittee
	nextSyncCommittee    *solid.SyncCommittee
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
	// Alternate beacon states
	fs        afero.Fs
	blocks    sync.Map // set of blocks (block root -> block)
	headers   sync.Map // set of headers
	badBlocks sync.Map // blocks that are invalid and that leads to automatic fail of extension.

	// current state data
	currentState          *state.CachingBeaconState
	currentStateBlockRoot common.Hash // block root of the last processed block (avoids BlockRoot() zeroed-StateRoot issue)

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
	lowestAvailableBlock atomic.Uint64

	newestLightClientUpdate atomic.Value
	// the lightclientUpdates leaks memory, but it's not a big deal since new data is added every 27 hours.
	lightClientUpdates sync.Map // period -> lightclientupdate

	// in-memory cache of block roots that have envelopes on disk [Optimization for Gloas:EIP7732]
	envelopeExists sync.Map // common.Hash -> struct{}

	// reusable buffers
	sszBuffer       []byte
	sszSnappyWriter *snappy.Writer
	sszSnappyReader *snappy.Reader

	rcfg       beacon_router_configuration.RouterConfiguration
	emitter    *beaconevents.EventEmitter
	syncedData synced_data.SyncedData

	stateDumpLock sync.Mutex
}

// Initialize fork graph with a new state
func NewForkGraphDisk(anchorState *state.CachingBeaconState, syncedData synced_data.SyncedData, aferoFs afero.Fs, rcfg beacon_router_configuration.RouterConfiguration, emitter *beaconevents.EventEmitter) ForkGraph {
	farthestExtendingPath := make(map[common.Hash]bool)
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		panic(err)
	}
	anchorHeader := anchorState.LatestBlockHeader()
	stateHash, err := anchorState.HashSSZ()
	if err != nil {
		panic(err)
	}
	anchorHeader.Root = stateHash
	// Fix GLOAS checkpoint sync: persist the filled-in Root back to the actual state
	// so that transitionSlot won't re-compute HashSSZ() (which may return a different
	// value after DumpBeaconStateOnDisk mutates the internal merkle cache).
	// Only do this when the original header Root was zero (PENDING state or genesis-like).
	// For FULL states (post-envelope), Root is already correctly set to the pre-envelope
	// state hash — overwriting it with post-envelope HashSSZ() would be wrong.
	if anchorState.LatestBlockHeader().Root == [32]byte{} && anchorState.Slot() > 0 {
		anchorState.SetLatestBlockHeader(&anchorHeader)
	}

	log.Info("[DEBUG] NewForkGraphDisk anchor state",
		"slot", anchorState.Slot(),
		"anchorRoot", common.Hash(anchorRoot),
		"headerSlot", anchorHeader.Slot,
		"headerProposer", anchorHeader.ProposerIndex,
		"headerParentRoot", anchorHeader.ParentRoot,
		"headerBodyRoot", anchorHeader.BodyRoot,
		"headerRoot(stateHash)", stateHash,
		"latestBlockHeaderRoot", anchorState.LatestBlockHeader().Root,
	)

	farthestExtendingPath[anchorRoot] = true

	f := &forkGraphDisk{
		fs: aferoFs,
		// current state data
		currentState: anchorState,
		// configuration
		beaconCfg:   anchorState.BeaconConfig(),
		genesisTime: anchorState.GenesisTime(),
		anchorSlot:  anchorState.Slot(),
		rcfg:        rcfg,
		emitter:     emitter,
		syncedData:  syncedData,
	}
	f.lowestAvailableBlock.Store(anchorState.Slot())
	f.headers.Store(common.Hash(anchorRoot), &anchorHeader)
	f.sszBuffer = make([]byte, 0, (anchorState.EncodingSizeSSZ()*3)/2)

	f.DumpBeaconStateOnDisk(anchorRoot, anchorState, true)
	// preallocate buffer
	return f
}

func (f *forkGraphDisk) AnchorSlot() uint64 {
	return f.anchorSlot
}

func (f *forkGraphDisk) isBlockRootTheCurrentState(blockRoot common.Hash) bool {
	if f.currentState == nil {
		return false
	}
	return blockRoot == f.currentStateBlockRoot
}

// Add a new node and edge to the graph
// parentFullState: if non-nil, use this as the starting state instead of looking up from block_states.
// [Modified in Gloas:EIP7732] Allows passing execution_payload_states when parent is FULL.
func (f *forkGraphDisk) AddChainSegment(signedBlock *cltypes.SignedBeaconBlock, fullValidation bool, parentFullState *state.CachingBeaconState, latestBlockHashOverride common.Hash) (*state.CachingBeaconState, ChainSegmentInsertionResult, error) {
	block := signedBlock.Block
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return nil, LogisticError, err
	}

	if _, ok := f.GetHeader(common.Hash(blockRoot)); ok {
		return nil, PreValidated, nil
	}
	// Blocks below anchors are invalid.
	if block.Slot <= f.anchorSlot {
		log.Debug("block below anchor slot", "slot", block.Slot, "hash", common.Hash(blockRoot))
		f.badBlocks.Store(common.Hash(blockRoot), struct{}{})
		return nil, BelowAnchor, nil
	}

	isBlockRootTheCurrentState := f.isBlockRootTheCurrentState(blockRoot)
	var newState *state.CachingBeaconState
	if parentFullState != nil {
		// [New in Gloas:EIP7732] Use provided parent state (from execution_payload_states)
		newState = parentFullState
	} else if isBlockRootTheCurrentState {
		newState = f.currentState
	} else {
		newState, err = f.getState(block.ParentRoot, false, true)
		if err != nil {
			return nil, LogisticError, fmt.Errorf("AddChainSegment: %w, parentRoot: %x", err, block.ParentRoot)
		}
	}

	// [GLOAS] Patch latestBlockHash if an override is provided (checkpoint sync fallback).
	// Only apply when existing latestBlockHash is zero — FULL states already have the
	// correct value set by ProcessExecutionPayloadEnvelope and must not be overwritten.
	if newState != nil && latestBlockHashOverride != (common.Hash{}) {
		existing := newState.GetLatestBlockHash()
		if existing == (common.Hash{}) {
			log.Info("[DEBUG] AddChainSegment: applying latestBlockHash override (was zero)",
				"slot", block.Slot,
				"override", latestBlockHashOverride,
			)
			newState.SetLatestBlockHash(latestBlockHashOverride)
		} else {
			log.Debug("[DEBUG] AddChainSegment: skipping latestBlockHash override (already set)",
				"slot", block.Slot,
				"existing", existing,
				"override", latestBlockHashOverride,
			)
		}
	}

	if newState == nil {
		var currentStateRoot common.Hash
		var currentStateSlot uint64
		if f.currentState != nil {
			currentStateRoot, _ = f.currentState.BlockRoot()
			currentStateSlot = f.currentState.Slot()
		}
		log.Warn("AddChainSegment: missing segment",
			"slot", block.Slot,
			"blockRoot", common.Hash(blockRoot),
			"parentRoot", block.ParentRoot,
			"currentStateRoot", currentStateRoot,
			"currentStateSlot", currentStateSlot,
			"lowestAvail", f.lowestAvailableBlock.Load(),
			"isCurrentState", isBlockRootTheCurrentState,
			"parentFullState!=nil", parentFullState != nil,
		)
		return nil, MissingSegment, nil
	}
	finalizedBlock, hasFinalized := f.GetBlock(newState.FinalizedCheckpoint().Root)
	parentBlock, hasParentBlock := f.GetBlock(block.ParentRoot)

	// Before processing the state: update the newest lightclient update.
	if block.Version() >= clparams.AltairVersion && hasParentBlock && fullValidation && hasFinalized && f.rcfg.Beacon && !isBlockRootTheCurrentState {
		nextSyncCommitteeBranch, err := newState.NextSyncCommitteeBranch()
		if err != nil {
			return nil, LogisticError, err
		}
		finalityBranch, err := newState.FinalityRootBranch()
		if err != nil {
			return nil, LogisticError, err
		}
		lcUpdate, err := lightclient_utils.CreateLightClientUpdate(f.beaconCfg, signedBlock, finalizedBlock, parentBlock, newState.Slot(),
			newState.NextSyncCommittee(), newState.FinalizedCheckpoint(), convertHashSliceToHashList(nextSyncCommitteeBranch), convertHashSliceToHashList(finalityBranch))
		if err != nil {
			log.Debug("Could not create light client update", "err", err)
		} else {
			f.newestLightClientUpdate.Store(lcUpdate)
			period := f.beaconCfg.SyncCommitteePeriod(newState.Slot())
			_, hasPeriod := f.lightClientUpdates.Load(period)
			if !hasPeriod {
				log.Info("Adding light client update", "period", period)
				f.lightClientUpdates.Store(period, lcUpdate)
			}
			// light client events
			f.emitter.State().SendLightClientFinalityUpdate(&beaconevents.LightClientFinalityUpdateData{
				Version: block.Version().String(),
				Data: cltypes.LightClientFinalityUpdate{
					AttestedHeader:  lcUpdate.AttestedHeader,
					FinalizedHeader: lcUpdate.FinalizedHeader,
					FinalityBranch:  lcUpdate.FinalityBranch,
					SyncAggregate:   lcUpdate.SyncAggregate,
					SignatureSlot:   lcUpdate.SignatureSlot,
				},
			})
			f.emitter.State().SendLightClientOptimisticUpdate(&beaconevents.LightClientOptimisticUpdateData{
				Version: block.Version().String(),
				Data: cltypes.LightClientOptimisticUpdate{
					AttestedHeader: lcUpdate.AttestedHeader,
					SyncAggregate:  lcUpdate.SyncAggregate,
					SignatureSlot:  lcUpdate.SignatureSlot,
				},
			})
		}
	}

	blockRewardsCollector := &eth2.BlockRewardsCollector{}

	if !isBlockRootTheCurrentState {
		// Debug: log pre-transition state details
		{
			preHeader := newState.LatestBlockHeader()
			preHash, _ := newState.HashSSZ()
			log.Info("[DEBUG] AddChainSegment: pre-TransitionState",
				"blockSlot", block.Slot,
				"stateSlot", newState.Slot(),
				"latestBlockHash", newState.GetLatestBlockHash(),
				"headerRoot", preHeader.Root,
				"headerSlot", preHeader.Slot,
				"stateHashSSZ", common.Hash(preHash),
				"previousStateRoot", newState.PreviousStateRoot(),
			)
		}
		// Execute the state
		if invalidBlockErr := transition.TransitionState(newState, signedBlock, blockRewardsCollector, fullValidation); invalidBlockErr != nil {
			// Add block to list of invalid blocks
			log.Warn("Invalid beacon block", "slot", block.Slot, "blockRoot", common.Bytes2Hex(blockRoot[:]), "reason", invalidBlockErr)
			f.badBlocks.Store(common.Hash(blockRoot), struct{}{})
			f.currentState = nil
			f.currentStateBlockRoot = common.Hash{}
			return nil, InvalidBlock, invalidBlockErr
		}
		f.blockRewards.Store(common.Hash(blockRoot), blockRewardsCollector)
	}

	f.currentState = newState
	f.currentStateBlockRoot = common.Hash(blockRoot)

	// Debug: verify currentState.BlockRoot() matches the actual block root.
	// If they diverge, it means the state transition used the wrong pre-state
	// (e.g., block_state instead of execution_payload_state for a FULL parent).
	if computedRoot, cerr := f.currentState.BlockRoot(); cerr == nil && computedRoot != common.Hash(blockRoot) {
		log.Warn("AddChainSegment: BlockRoot MISMATCH after TransitionState",
			"slot", block.Slot,
			"expectedBlockRoot", common.Hash(blockRoot),
			"computedBlockRoot", computedRoot,
			"parentFullState!=nil", parentFullState != nil,
			"parentRoot", block.ParentRoot,
		)
	}

	// update diff storages.
	if f.rcfg.Beacon || f.rcfg.Validator || f.rcfg.Lighthouse {
		if block.Version() != clparams.Phase0Version {
			epoch := state.Epoch(newState)
			f.currentIndicies.add(epoch, newState.RawCurrentEpochParticipation())
			f.previousIndicies.add(epoch, newState.RawPreviousEpochParticipation())
		}

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
			f.lightclientBootstraps.Store(common.Hash(blockRoot), lightclientBootstrap)
		}
	}

	f.blocks.Store(common.Hash(blockRoot), signedBlock)
	bodyRoot, err := signedBlock.Block.Body.HashSSZ()
	if err != nil {
		return nil, LogisticError, err
	}

	f.headers.Store(common.Hash(blockRoot), &cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		Root:          block.StateRoot,
		BodyRoot:      bodyRoot,
	})

	// Lastly add checkpoints to caches as well.
	f.currentJustifiedCheckpoints.Store(common.Hash(blockRoot), newState.CurrentJustifiedCheckpoint())
	f.finalizedCheckpoints.Store(common.Hash(blockRoot), newState.FinalizedCheckpoint())

	return newState, Success, nil
}

func (f *forkGraphDisk) GetHeader(blockRoot common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	obj, has := f.headers.Load(blockRoot)
	if !has {
		return nil, false
	}
	return obj.(*cltypes.BeaconBlockHeader), true
}

func (f *forkGraphDisk) GetBlock(blockRoot common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	obj, has := f.blocks.Load(blockRoot)
	if !has {
		return nil, false
	}

	return obj.(*cltypes.SignedBeaconBlock), true
}
func (f *forkGraphDisk) GetState(blockRoot common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
	return f.getState(blockRoot, alwaysCopy, false)
}

func (f *forkGraphDisk) useCachedStateIfPossible(blockRoot common.Hash, in *state.CachingBeaconState) (out *state.CachingBeaconState, ok bool, err error) {
	if f.syncedData == nil {
		return
	}
	if f.syncedData.HeadRoot() == blockRoot {
		err = f.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
			headBlockRoot, err := headState.BlockRoot()
			if err != nil {
				return err
			}
			if headBlockRoot != blockRoot {
				return nil
			}
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

func (f *forkGraphDisk) getState(blockRoot common.Hash, alwaysCopy bool, addChainSegment bool) (*state.CachingBeaconState, error) {
	if f.currentState != nil {
		currentStateBlockRoot, err := f.currentState.BlockRoot()
		if err != nil {
			return nil, err
		}
		if currentStateBlockRoot == blockRoot {
			if alwaysCopy {
				return f.currentState.Copy()
			}
			return f.currentState, nil
		}
	}
	if addChainSegment && !alwaysCopy {
		if state, ok, err := f.useCachedStateIfPossible(blockRoot, f.currentState); ok {
			return state, err
		}
	}

	// collect all blocks between greatest extending node path and block.
	type replayBlock struct {
		block          *cltypes.SignedBeaconBlock
		parentEnvelope *cltypes.SignedExecutionPayloadEnvelope // non-nil if parent was FULL [New in Gloas:EIP7732]
	}
	blocksInTheWay := []replayBlock{}
	// Use the parent root as a reverse iterator.
	currentIteratorRoot := blockRoot
	var copyReferencedState *state.CachingBeaconState
	var err error

	// try and find the point of recconnection
	for copyReferencedState == nil {
		block, isSegmentPresent := f.GetBlock(currentIteratorRoot)
		if !isSegmentPresent {
			// check if it is in the header
			bHeader, ok := f.GetHeader(currentIteratorRoot)
			if ok && bHeader.Slot%dumpSlotFrequency == 0 {
				copyReferencedState, err = f.readBeaconStateFromDisk(currentIteratorRoot)
				if err != nil {
					log.Trace("Could not retrieve state: Missing header", "missing", currentIteratorRoot, "err", err)
					copyReferencedState = nil
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

		// [New in Gloas:EIP7732] Check if this block was built on parent's FULL path.
		// If so, we need to apply parent's envelope before replaying this block.
		var parentEnvelope *cltypes.SignedExecutionPayloadEnvelope
		if block.Version() >= clparams.GloasVersion {
			parentRoot := block.Block.ParentRoot
			if env, readErr := f.ReadEnvelopeFromDisk(parentRoot); readErr == nil && env != nil {
				bid := block.Block.Body.GetSignedExecutionPayloadBid()
				if bid != nil && bid.Message != nil && env.Message != nil && env.Message.Payload != nil {
					if bid.Message.ParentBlockHash == env.Message.Payload.BlockHash {
						parentEnvelope = env
					}
				}
			}
		}

		blocksInTheWay = append(blocksInTheWay, replayBlock{
			block:          block,
			parentEnvelope: parentEnvelope,
		})
		currentIteratorRoot = block.Block.ParentRoot
	}

	// Traverse the blocks from top to bottom.
	for i := len(blocksInTheWay) - 1; i >= 0; i-- {
		rb := blocksInTheWay[i]
		// [New in Gloas:EIP7732] Apply parent envelope before block if parent was FULL.
		if rb.parentEnvelope != nil {
			if err := transition.DefaultMachine.ProcessExecutionPayloadEnvelope(copyReferencedState, rb.parentEnvelope); err != nil {
				if addChainSegment {
					f.currentState = nil
				}
				return nil, fmt.Errorf("getState: failed to process parent envelope: %w", err)
			}
		}
		if err := transition.TransitionState(copyReferencedState, rb.block, nil, false); err != nil {
			if addChainSegment {
				f.currentState = nil // reset the state if it fails here.
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
	f.badBlocks.Store(blockRoot, struct{}{})
}

func (f *forkGraphDisk) hasBeaconState(blockRoot common.Hash) bool {
	exists, err := afero.Exists(f.fs, getBeaconStateFilename(blockRoot))
	return err == nil && exists
}

// GetExecutionPayloadState reconstructs the post-execution-payload state for a given block.
// It gets the block_state via getState (which handles GLOAS FULL/EMPTY parent paths in replay),
// then applies the target block's envelope to produce the execution_payload_state.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) GetExecutionPayloadState(blockRoot common.Hash) (*state.CachingBeaconState, error) {
	// 1. Read target envelope
	targetEnvelope, err := f.ReadEnvelopeFromDisk(blockRoot)
	if err != nil {
		return nil, fmt.Errorf("GetExecutionPayloadState: failed to read envelope: %w", err)
	}

	// 2. Get block_state (getState now handles GLOAS parent envelopes during replay)
	blockState, err := f.getState(blockRoot, true, false)
	if err != nil {
		return nil, fmt.Errorf("GetExecutionPayloadState: failed to get block state: %w", err)
	}
	if blockState == nil {
		return nil, fmt.Errorf("GetExecutionPayloadState: block state not found for %x", blockRoot)
	}

	// 3. Apply target envelope to get exec_payload_state
	if err := transition.DefaultMachine.ProcessExecutionPayloadEnvelope(blockState, targetEnvelope); err != nil {
		return nil, fmt.Errorf("GetExecutionPayloadState: failed to process envelope: %w", err)
	}

	return blockState, nil
}

func (f *forkGraphDisk) Prune(pruneSlot uint64) (err error) {
	oldRoots := make([]common.Hash, 0, f.beaconCfg.SlotsPerEpoch)
	highestStoredBeaconStateSlot := uint64(0)
	f.blocks.Range(func(key, value any) bool {
		hash := key.(common.Hash)
		signedBlock := value.(*cltypes.SignedBeaconBlock)
		if f.hasBeaconState(hash) && highestStoredBeaconStateSlot < signedBlock.Block.Slot {
			highestStoredBeaconStateSlot = signedBlock.Block.Slot
		}
		if signedBlock.Block.Slot >= pruneSlot {
			return true
		}

		oldRoots = append(oldRoots, hash)
		return true
	})
	if pruneSlot >= highestStoredBeaconStateSlot {
		return
	}

	// prune the indicies for the epoch
	f.currentIndicies.prune(pruneSlot / f.beaconCfg.SlotsPerEpoch)
	f.previousIndicies.prune(pruneSlot / f.beaconCfg.SlotsPerEpoch)

	f.lowestAvailableBlock.Store(pruneSlot + 1)
	for _, root := range oldRoots {
		f.badBlocks.Delete(root)
		f.blocks.Delete(root)
		f.lightclientBootstraps.Delete(root)
		f.currentJustifiedCheckpoints.Delete(root)
		f.finalizedCheckpoints.Delete(root)
		f.headers.Delete(root)
		f.blockRewards.Delete(root)
		f.fs.Remove(getBeaconStateFilename(root))
		// [New in Gloas:EIP7732] Also remove envelope files
		f.envelopeExists.Delete(root)
		f.fs.Remove(getEnvelopeFilename(root))
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

func (f *forkGraphDisk) GetBalances(blockRoot common.Hash) (solid.Uint64ListSSZ, error) {
	st, err := f.GetState(blockRoot, true)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, ErrStateNotFound
	}
	return st.Balances(), nil
}

func (f *forkGraphDisk) GetInactivitiesScores(blockRoot common.Hash) (solid.Uint64ListSSZ, error) {
	st, err := f.GetState(blockRoot, true)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, ErrStateNotFound
	}
	return st.InactivityScores(), nil
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
	return out, out.DecodeSSZ(b, 0)
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
	return out, out.DecodeSSZ(b, 0)
}

func (f *forkGraphDisk) GetValidatorSet(blockRoot common.Hash) (*solid.ValidatorSet, error) {
	st, err := f.GetState(blockRoot, true)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, ErrStateNotFound
	}
	return st.ValidatorSet(), nil
}
