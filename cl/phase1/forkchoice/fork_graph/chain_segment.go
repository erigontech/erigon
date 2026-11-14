// Copyright 2026 The Erigon Authors
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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/lightclient_utils"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// ChainSegment is one block import between BeginChainSegment and CommitChainSegment.
// It owns its beacon states: Begin moves them out of the fork graph, so Transition can
// run without fork-choice or fork-graph locks. To discard a segment, drop it: nothing
// is stored and the moved states are not returned.
type ChainSegment struct {
	signedBlock      *cltypes.SignedBeaconBlock
	blockRoot        common.Hash
	fullValidation   bool
	parentState      *state.CachingBeaconState
	parentSlot       uint64
	precomputedState *state.CachingBeaconState
	runTransition    bool
	storeDiffs       bool
	buildLCUpdate    bool
	finalizedBlock   *cltypes.SignedBeaconBlock
	parentBlock      *cltypes.SignedBeaconBlock

	postState      *state.CachingBeaconState
	rewards        *eth2.BlockRewardsCollector
	lcUpdate       *cltypes.LightClientUpdate
	lcBootstrap    *cltypes.LightClientBootstrap
	syncCommittees syncCommittees
	bodyRoot       common.Hash
	transitionErr  error
	transitionOnce sync.Once
	transitioned   atomic.Bool
	// err is a prepare failure that does not make the block invalid.
	err error
}

// AddChainSegment processes a new block and returns the post-state.
func (f *forkGraphDisk) AddChainSegment(signedBlock *cltypes.SignedBeaconBlock, fullValidation bool) (*state.CachingBeaconState, ChainSegmentInsertionResult, error) {
	f.addPruneMu.Lock()
	defer f.addPruneMu.Unlock()

	blockRoot, err := signedBlock.Block.HashSSZ()
	if err != nil {
		return nil, LogisticError, err
	}
	seg, result, err := f.beginChainSegmentLocked(signedBlock, common.Hash(blockRoot), fullValidation, true)
	if seg == nil {
		return nil, result, err
	}
	// Store the block early so that blocks_by_root can serve it while the state
	// transition runs. Without this, peers asking for a block that's mid-processing
	// get an empty response and penalize us (Lighthouse: -20 per NullBlocksByRoot).
	f.blocks.Store(seg.blockRoot, signedBlock)
	seg.Transition()
	return f.commitChainSegmentLocked(seg)
}

// BeginChainSegment starts the import of a block. It returns a segment only with
// Started; ReplayRequired and PrecomputedEpochStateAvailable require AddChainSegment.
// Other results are final. A started segment leaves the fork graph without a current
// state until it is committed.
func (f *forkGraphDisk) BeginChainSegment(signedBlock *cltypes.SignedBeaconBlock, blockRoot common.Hash, fullValidation bool) (*ChainSegment, ChainSegmentInsertionResult, error) {
	f.addPruneMu.Lock()
	defer f.addPruneMu.Unlock()
	return f.beginChainSegmentLocked(signedBlock, blockRoot, fullValidation, false)
}

// CommitChainSegment publishes a segment. Called before Transition has returned, it
// fails without storing anything.
func (f *forkGraphDisk) CommitChainSegment(s *ChainSegment) (*state.CachingBeaconState, ChainSegmentInsertionResult, error) {
	f.addPruneMu.Lock()
	defer f.addPruneMu.Unlock()
	return f.commitChainSegmentLocked(s)
}

func (f *forkGraphDisk) beginChainSegmentLocked(signedBlock *cltypes.SignedBeaconBlock, blockRoot common.Hash, fullValidation, allowReplay bool) (*ChainSegment, ChainSegmentInsertionResult, error) {
	block := signedBlock.Block

	if _, ok := f.GetHeader(blockRoot); ok {
		return nil, PreValidated, nil
	}
	// Blocks below anchors are invalid.
	if block.Slot <= f.anchorSlot {
		log.Debug("block below anchor slot", "slot", block.Slot, "hash", blockRoot)
		f.badBlocks.Store(blockRoot, block.Slot)
		return nil, BelowAnchor, nil
	}
	if isBelowPrunedBoundary(block.Slot, f.lowestAvailableBlock.Load()) {
		f.badBlocks.Store(blockRoot, block.Slot)
		return nil, BelowAnchor, nil
	}

	isBlockRootTheCurrentState := f.isBlockRootTheCurrentState(blockRoot)
	var parentState *state.CachingBeaconState
	if isBlockRootTheCurrentState {
		parentState = f.currentState
	} else if current := f.CurrentStateLocked(block.ParentRoot); current != nil {
		parentState = current
	} else if !allowReplay {
		return nil, ReplayRequired, nil
	} else {
		replayedState, err := f.replayState(block.ParentRoot, true)
		if err != nil {
			return nil, LogisticError, fmt.Errorf("AddChainSegment: %w, parentRoot: %x", err, block.ParentRoot)
		}
		parentState = replayedState
	}

	if parentState == nil {
		var currentStateRoot common.Hash
		var currentStateSlot uint64
		if f.currentState != nil {
			currentStateRoot, _ = f.currentState.BlockRoot()
			currentStateSlot = f.currentState.Slot()
		}
		log.Debug(
			"AddChainSegment: missing segment",
			"slot", block.Slot,
			"blockRoot", blockRoot,
			"parentRoot", block.ParentRoot,
			"currentStateRoot", currentStateRoot,
			"currentStateSlot", currentStateSlot,
			"lowestAvail", f.lowestAvailableBlock.Load(),
			"isCurrentState", isBlockRootTheCurrentState,
		)
		return nil, MissingSegment, nil
	}
	runTransition := !isBlockRootTheCurrentState
	if !allowReplay && runTransition && f.nextEpochState != nil && block.Slot >= f.nextEpochState.Slot() {
		return nil, PrecomputedEpochStateAvailable, nil
	}
	// The finalized block may be unavailable; the update is then built without finality, as create_light_client_update
	// allows.
	finalizedBlock, _ := f.GetBlock(parentState.FinalizedCheckpoint().Root)
	parentBlock, hasParentBlock := f.GetBlock(block.ParentRoot)
	seg := &ChainSegment{
		signedBlock:    signedBlock,
		blockRoot:      blockRoot,
		fullValidation: fullValidation,
		parentState:    parentState,
		parentSlot:     parentState.Slot(),
		runTransition:  runTransition,
		storeDiffs:     f.rcfg.Beacon || f.rcfg.Validator || f.rcfg.Lighthouse,
		buildLCUpdate:  block.Version() >= clparams.AltairVersion && hasParentBlock && fullValidation && f.rcfg.Beacon && !isBlockRootTheCurrentState,
		finalizedBlock: finalizedBlock,
		parentBlock:    parentBlock,
	}
	if seg.runTransition {
		f.highestTransitionSlot = max(f.highestTransitionSlot, block.Slot)
		seg.precomputedState = f.takeNextEpochStateLocked(block, seg.parentSlot, fullValidation)
	}
	// The segment now owns the parent state; no fork-graph reader may reach it.
	f.currentState = nil
	f.currentStateBlockRoot = common.Hash{}
	return seg, Started, nil
}

// Transition runs the state transition and prepares every artifact Commit publishes.
// It takes no locks, may run on another goroutine and runs only once. It reads the
// segment's states and the blocks taken at Begin, which must not be mutated meanwhile.
func (s *ChainSegment) Transition() {
	s.transitionOnce.Do(func() {
		s.err = s.transition()
		s.transitioned.Store(true)
	})
}

// PostState returns the completed valid transition state.
func (s *ChainSegment) PostState() *state.CachingBeaconState {
	if !s.transitioned.Load() || s.err != nil || s.transitionErr != nil {
		return nil
	}
	return s.postState
}

func (s *ChainSegment) transition() error {
	block := s.signedBlock.Block
	newState := s.parentState
	// The light-client update must be built from the parent state before the transition.
	if s.buildLCUpdate {
		nextSyncCommitteeBranch, err := newState.NextSyncCommitteeBranch()
		if err != nil {
			return err
		}
		finalityBranch, err := newState.FinalityRootBranch()
		if err != nil {
			return err
		}
		lcUpdate, err := lightclient_utils.CreateLightClientUpdate(newState.BeaconConfig(), s.signedBlock, s.finalizedBlock, s.parentBlock, newState.Slot(),
			newState.NextSyncCommittee(), newState.FinalizedCheckpoint(), convertHashSliceToHashList(nextSyncCommitteeBranch), convertHashSliceToHashList(finalityBranch))
		if err != nil {
			log.Debug("Could not create light client update", "err", err)
		} else {
			s.lcUpdate = lcUpdate
		}
	}

	if s.runTransition {
		blockRewardsCollector := &eth2.BlockRewardsCollector{}
		var precomputedErr error
		if s.precomputedState != nil {
			precomputedErr = transition.TransitionStateFromPrecomputedEpochState(s.precomputedState, s.signedBlock, blockRewardsCollector, s.fullValidation)
			if precomputedErr == nil {
				newState = s.precomputedState
				recordNextEpochStateOutcome("hit", block.Slot)
			} else {
				blockRewardsCollector = &eth2.BlockRewardsCollector{}
			}
		}
		if s.precomputedState == nil || precomputedErr != nil {
			if invalidBlockErr := transition.TransitionState(newState, s.signedBlock, blockRewardsCollector, s.fullValidation); invalidBlockErr != nil {
				if precomputedErr != nil {
					recordNextEpochStateOutcome("invalid", block.Slot)
				}
				log.Warn("Invalid beacon block", "slot", block.Slot, "blockRoot", common.Bytes2Hex(s.blockRoot[:]), "reason", invalidBlockErr)
				s.transitionErr = invalidBlockErr
				return nil
			}
			if precomputedErr != nil {
				nextEpochStateOutcomes.WithLabelValues("diverged").Inc()
				log.Warn("Precomputed epoch state diverged", "slot", block.Slot, "blockRoot", common.Bytes2Hex(s.blockRoot[:]), "reason", precomputedErr)
			}
		}
		s.rewards = blockRewardsCollector
	}
	s.postState = newState

	// Verify the post-state's BlockRoot() matches the actual block root.
	if computedRoot, cerr := newState.BlockRoot(); cerr == nil && computedRoot != s.blockRoot {
		// Detailed diagnostics: compare state header fields with block fields.
		hdr := newState.LatestBlockHeader()
		stateHashSSZ, _ := newState.HashSSZ()
		log.Warn(
			"AddChainSegment: BlockRoot MISMATCH after TransitionState",
			"slot", block.Slot,
			"expectedBlockRoot", s.blockRoot,
			"computedBlockRoot", computedRoot,
			"parentRoot", block.ParentRoot,
			"blockStateRoot", block.StateRoot,
			"stateHashSSZ", stateHashSSZ,
			"fullValidation", s.fullValidation,
			"hdrSlot", hdr.Slot,
			"hdrProposer", hdr.ProposerIndex,
			"hdrParentRoot", hdr.ParentRoot,
			"hdrBodyRoot", hdr.BodyRoot,
			"hdrRoot", hdr.Root,
		)
	}

	if s.storeDiffs {
		s.syncCommittees = syncCommittees{
			currentSyncCommittee: newState.CurrentSyncCommittee().Copy(),
			nextSyncCommittee:    newState.NextSyncCommittee().Copy(),
		}
		if block.Version() >= clparams.AltairVersion {
			lightclientBootstrap, err := lightclient_utils.CreateLightClientBootstrap(newState, s.signedBlock)
			if err != nil {
				return err
			}
			s.lcBootstrap = lightclientBootstrap
		}
	}

	bodyRoot, err := block.Body.HashSSZ()
	if err != nil {
		return err
	}
	s.bodyRoot = bodyRoot
	return nil
}

// commitChainSegmentLocked re-checks what may have changed since Begin and then
// publishes what Transition prepared. The light-client update is kept only after a
// successful transition, which verified the block's sync aggregate.
func (f *forkGraphDisk) commitChainSegmentLocked(s *ChainSegment) (*state.CachingBeaconState, ChainSegmentInsertionResult, error) {
	if !s.transitioned.Load() {
		return nil, LogisticError, errors.New("chain segment committed before its transition")
	}
	block := s.signedBlock.Block
	if _, ok := f.GetHeader(s.blockRoot); ok {
		return nil, PreValidated, nil
	}
	if isBelowPrunedBoundary(block.Slot, f.lowestAvailableBlock.Load()) {
		f.badBlocks.Store(s.blockRoot, block.Slot)
		return nil, BelowAnchor, nil
	}

	if s.err != nil {
		f.blocks.Delete(s.blockRoot)
		return nil, LogisticError, s.err
	}
	if s.transitionErr != nil {
		f.blocks.Delete(s.blockRoot)
		if !errors.Is(s.transitionErr, eth2.ErrInvalidBlockSignature) {
			f.badBlocks.Store(s.blockRoot, block.Slot)
		}
		return nil, InvalidBlock, s.transitionErr
	}
	if s.lcUpdate != nil {
		f.recordLightClientUpdateLocked(s.blockRoot, s.lcUpdate)
	}

	newState := s.postState
	f.blocks.Store(s.blockRoot, s.signedBlock)
	if s.rewards != nil {
		f.blockRewards.Store(s.blockRoot, s.rewards)
	}
	f.currentState = newState
	f.currentStateBlockRoot = s.blockRoot

	if s.storeDiffs {
		if block.Version() != clparams.Phase0Version {
			epoch := state.Epoch(newState)
			f.currentIndicies.add(epoch, newState.RawCurrentEpochParticipation())
			f.previousIndicies.add(epoch, newState.RawPreviousEpochParticipation())
		}
		f.syncCommittees.Store(f.beaconCfg.SyncCommitteePeriod(newState.Slot()), s.syncCommittees)
		if s.lcBootstrap != nil {
			f.lightclientBootstraps.Store(s.blockRoot, s.lcBootstrap)
		}
	}

	header := &cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		Root:          block.StateRoot,
		BodyRoot:      s.bodyRoot,
	}
	currentJustified := newState.CurrentJustifiedCheckpoint()
	finalized := newState.FinalizedCheckpoint()

	f.lifecycleMu.Lock()
	f.headers.Store(s.blockRoot, header)
	f.addValidatedChild(block.ParentRoot, s.blockRoot, block.Slot)
	f.currentJustifiedCheckpoints.Store(s.blockRoot, currentJustified)
	f.finalizedCheckpoints.Store(s.blockRoot, finalized)
	f.lifecycleMu.Unlock()

	return newState, Success, nil
}
