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

package forkchoice

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

const foreseenProposers = 16

var (
	ErrEIP4844DataNotAvailable       = errors.New("EIP-4844 blob data is not available")
	ErrEIP7594ColumnDataNotAvailable = errors.New("EIP-7594 column data is not available")
	ErrNewPayloadNoStatus            = errors.New("newPayload returned no status")
	ErrMissingSegment                = errors.New("missing segment: parent state not available")
	ErrParentEnvelopePending         = errors.New("parent execution payload envelope not yet available")
)

func verifyKzgCommitmentsAgainstTransactions(cfg *clparams.BeaconChainConfig, block *cltypes.BeaconBlock) error {
	expectedBlobHashes := []common.Hash{}
	transactions, err := types.DecodeTransactions(block.Body.ExecutionPayload.Transactions.UnderlyngReference())
	if err != nil {
		return fmt.Errorf("unable to decode transactions: %v", err)
	}
	block.Body.BlobKzgCommitments.Range(func(index int, value *cltypes.KZGCommitment, length int) bool {
		var kzg common.Hash
		kzg, err = utils.KzgCommitmentToVersionedHash(common.Bytes48(*value))
		if err != nil {
			return false
		}
		expectedBlobHashes = append(expectedBlobHashes, kzg)
		return true
	})
	if err != nil {
		return err
	}

	maxBlobsPerBlock := cfg.MaxBlobsPerBlockByVersion(block.Version())
	if block.Version() >= clparams.FuluVersion {
		maxBlobsPerBlock = cfg.GetBlobParameters(block.Slot / cfg.SlotsPerEpoch).MaxBlobsPerBlock
	}
	return misc.ValidateBlobs(block.Body.ExecutionPayload.BlobGasUsed, cfg.MaxBlobGasPerBlock, maxBlobsPerBlock, expectedBlobHashes, &transactions)
}

func collectOnBlockLatencyToUnixTime(ethClock eth_clock.EthereumClock, slot uint64) {
	currSlot := ethClock.GetCurrentSlot()
	if slot != currSlot {
		return
	}
	initialSlotTime := ethClock.GetSlotTime(slot)
	monitor.ObserveBlockImportingLatency(initialSlotTime)
}

func (f *ForkChoiceStore) OnBlock(ctx context.Context, block *cltypes.SignedBeaconBlock, newPayload, fullValidation, checkDataAvaiability bool) error {
	f.mu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			f.mu.Unlock()
		}
	}()
	f.headHash = common.Hash{}
	start := time.Now()
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	// Use wall-clock time with MAXIMUM_GOSSIP_CLOCK_DISPARITY (500ms) tolerance
	// to avoid rejecting blocks that arrive slightly before the slot boundary.
	// f.Slot() uses second-precision time from OnTick and can lag up to ~1s behind.
	wallNow := uint64(time.Now().Add(500 * time.Millisecond).Unix())
	wallSlot := f.beaconCfg.GenesisSlot + ((wallNow - f.genesisTime) / f.beaconCfg.SecondsPerSlot)
	if wallSlot < block.Block.Slot {
		return errors.New("block is too early compared to current_slot")
	}

	// Check that block is later than the finalized epoch slot (optimization to reduce calls to get_ancestor)
	finalizedCheckpoint := f.finalizedCheckpoint.Load().(solid.Checkpoint)
	finalizedSlot := f.computeStartSlotAtEpoch(finalizedCheckpoint.Epoch)
	if block.Block.Slot <= finalizedSlot {
		return nil
	}
	// Check block is a descendant of the finalized block at the checkpoint finalized slot
	if ancestorNode := f.Ancestor(block.Block.ParentRoot, finalizedSlot); ancestorNode.Root != finalizedCheckpoint.Root {
		return fmt.Errorf("block is not a descendant of the finalized checkpoint")
	}

	// Validate parent payload status path early (before expensive operations)
	blockEpoch := f.computeEpochAtSlot(block.Block.Slot)
	blockVersion := f.beaconCfg.GetCurrentStateVersion(blockEpoch)
	isGloas := blockVersion >= clparams.GloasVersion
	if isGloas {
		if err := f.validateParentPayloadPath(block.Block); err != nil {
			return err
		}
	}

	// Pre-GLOAS execution payload processing.
	// In GLOAS, ExecutionPayload and BlobKzgCommitments are nil in BeaconBlock.
	// These fields are handled separately in OnExecutionPayload when the envelope arrives.
	startEngine := time.Now()
	isVerifiedExecutionPayload := f.verifiedExecutionPayload.Contains(blockRoot)
	if blockVersion < clparams.GloasVersion {
		// Find the versioned hashes from blob commitments
		var versionedHashes []common.Hash
		if newPayload && f.engine != nil && block.Version() >= clparams.DenebVersion {
			versionedHashes = []common.Hash{}
			solid.RangeErr[*cltypes.KZGCommitment](block.Block.Body.BlobKzgCommitments, func(i1 int, k *cltypes.KZGCommitment, i2 int) error {
				versionedHash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(*k))
				if err != nil {
					return err
				}
				versionedHashes = append(versionedHashes, versionedHash)
				return nil
			})
		}

		// Check if EL has blobs
		elHasBlobs := false
		if f.engine != nil && checkDataAvaiability && block.Block.Body.BlobKzgCommitments.Len() > 0 && !f.peerDas.IsArchivedMode() {
			blobsWithProof, proofs, err := f.engine.GetBlobs(ctx, versionedHashes, block.Version())
			if err != nil {
				log.Warn("OnBlock: GetBlobs failed", "blockRoot", common.Hash(blockRoot), "err", err)
			}
			elHasBlobs = err == nil && len(blobsWithProof) == len(versionedHashes) && len(proofs) == len(versionedHashes)
			log.Debug("OnBlock: EL blob data availability", "blockRoot", common.Hash(blockRoot), "elHasBlobs", elHasBlobs)
		}

		// Check if blob data is available (skip if blobs are in txpool)
		if checkDataAvaiability && block.Block.Body.BlobKzgCommitments.Len() > 0 && !elHasBlobs {
			if block.Version() >= clparams.FuluVersion {
				available, err := f.peerDas.IsDataAvailable(block.Block.Slot, blockRoot)
				if err != nil {
					return err
				}
				if !available {
					if f.syncedDataManager.Syncing() {
						return ErrEIP7594ColumnDataNotAvailable
					} else {
						if err := f.peerDas.SyncColumnDataLater(block); err != nil {
							log.Warn("failed to schedule deferred column data sync", "slot", block.Block.Slot, "blockRoot", blockRoot, "err", err)
						}
					}
				}
			} else if block.Version() >= clparams.DenebVersion {
				if err := f.isDataAvailable(ctx, block.Block.Slot, blockRoot, block.Block.Body.BlobKzgCommitments); err != nil {
					if errors.Is(err, ErrEIP4844DataNotAvailable) {
						return err
					}
					return fmt.Errorf("OnBlock: data is not available for block %x: %v", common.Hash(blockRoot), err)
				}
				if f.highestSeen.Load() < block.Block.Slot {
					collectOnBlockLatencyToUnixTime(f.ethClock, block.Block.Slot)
				}
			}
		}

		// Get execution requests list for Electra+
		var executionRequestsList []hexutil.Bytes
		if block.Version() >= clparams.ElectraVersion {
			executionRequestsList = block.Block.Body.GetExecutionRequestsList()
			if executionRequestsList == nil {
				executionRequestsList = []hexutil.Bytes{}
			}
		}

		// Call NewPayload to validate execution payload
		if newPayload && f.engine != nil && !isVerifiedExecutionPayload {
			if block.Version() >= clparams.DenebVersion {
				if err := verifyKzgCommitmentsAgainstTransactions(f.beaconCfg, block.Block); err != nil {
					return fmt.Errorf("OnBlock: failed to process kzg commitments: %v", err)
				}
			}
			timeStartExec := time.Now()
			payloadStatus, err := f.engine.NewPayload(ctx, block.Block.Body.ExecutionPayload, &block.Block.ParentRoot, versionedHashes, executionRequestsList)
			monitor.ObserveNewPayloadTime(timeStartExec)
			log.Debug("[OnBlock] NewPayload", "status", payloadStatus, "blockSlot", block.Block.Slot)

			// Track payload status by execution block hash for GLOAS parent payload validation
			executionBlockHash := block.Block.Body.ExecutionPayload.BlockHash
			f.executionPayloadStatus.Add(executionBlockHash, payloadStatus)

			switch payloadStatus {
			case execution_client.PayloadStatusNotValidated:
				log.Debug("OnBlock: block is not validated yet", "block", common.Hash(blockRoot))
				// optimistic block candidate
				if err := f.optimisticStore.AddOptimisticCandidate(block.Block); err != nil {
					return fmt.Errorf("failed to add block to optimistic store: %v", err)
				}
			case execution_client.PayloadStatusInvalidated:
				log.Warn("OnBlock: block is invalid", "block", common.Hash(blockRoot), "err", err)
				f.forkGraph.MarkHeaderAsInvalid(blockRoot)
				// remove from optimistic candidate
				if err := f.optimisticStore.InvalidateBlock(block.Block); err != nil {
					return fmt.Errorf("failed to remove block from optimistic store: %v", err)
				}
				return errors.New("block is invalid")
			case execution_client.PayloadStatusValidated:
				log.Trace("OnBlock: block is validated", "block", common.Hash(blockRoot))
				// remove from optimistic candidate
				if err := f.optimisticStore.ValidateBlock(block.Block); err != nil {
					return fmt.Errorf("failed to validate block in optimistic store: %v", err)
				}
				f.verifiedExecutionPayload.Add(blockRoot, struct{}{})
			}
			if err != nil {
				return fmt.Errorf("newPayload failed: %v", err)
			}
		}
	}
	log.Trace("OnBlock: engine", "elapsed", time.Since(startEngine))
	// Update highestSeen early so aggregate/attestation acceptance uses the
	// latest slot even if AddChainSegment returns PreValidated.
	if block.Block.Slot > f.highestSeen.Load() {
		f.highestSeen.Store(block.Block.Slot)
	}
	startStateProcess := time.Now()

	// [New in Gloas:EIP7732] Determine starting state based on parent payload status.
	// In GLOAS, beacon block and execution payload have separate state transitions:
	//   - If parent is FULL: start from execution_payload_states[parent_root]
	//     (post-execution-payload state, includes execution layer changes)
	//   - If parent is EMPTY: start from block_states[parent_root]
	//     (post-beacon-block state, no execution payload was applied)
	// This ensures the new block builds on the correct canonical state.
	var parentFullState *state.CachingBeaconState
	if blockVersion >= clparams.GloasVersion {
		isParentFull := f.isParentNodeFull(block.Block)
		hasEnvelope := f.forkGraph.HasEnvelope(block.Block.ParentRoot)
		parentBlock, parentInForkChoice := f.forkGraph.GetBlock(block.Block.ParentRoot)
		log.Info("[DEBUG] OnBlock GLOAS parent detection",
			"slot", block.Block.Slot,
			"parentRoot", block.Block.ParentRoot,
			"isParentFull", isParentFull,
			"hasEnvelope", hasEnvelope,
			"parentInForkChoice", parentInForkChoice,
		)
		if isParentFull {
			if hasEnvelope {
				// Reconstruct the execution payload state from disk
				parentFullState, err = f.forkGraph.GetExecutionPayloadState(block.Block.ParentRoot)
				if err != nil {
					log.Warn("OnBlock: FULL parent but GetExecutionPayloadState failed",
						"slot", block.Block.Slot, "parentRoot", block.Block.ParentRoot, "err", err)
					parentFullState = nil
				}
			} else if parentInForkChoice && parentBlock != nil && parentBlock.Block.Slot == f.forkGraph.AnchorSlot() {
				// [New in Gloas:EIP7732] Anchor block from checkpoint sync: the checkpoint
				// state is already post-envelope (finalized = fully processed), so getState()
				// returns the correct parent state without needing the envelope on disk.
				log.Info("[OnBlock] Parent is anchor block from checkpoint sync, using post-envelope state directly",
					"slot", block.Block.Slot, "anchorSlot", f.forkGraph.AnchorSlot())
				parentFullState, err = f.forkGraph.GetState(block.Block.ParentRoot, true)
				if err != nil {
					log.Warn("OnBlock: anchor parent GetState failed",
						"slot", block.Block.Slot, "parentRoot", block.Block.ParentRoot, "err", err)
					parentFullState = nil
				}
			} else if parentInForkChoice {
				// Parent is FULL but envelope hasn't arrived yet. Defer processing
				// so the block can be retried once the envelope is available.
				return ErrParentEnvelopePending
			}
		}
	}

	lastProcessedState, status, err := f.forkGraph.AddChainSegment(block, fullValidation, parentFullState)
	if err != nil {
		return err
	}
	monitor.ObserveFullBlockProcessingTime(startStateProcess)
	if status != fork_graph.Success {
		log.Debug("[OnBlock] AddChainSegment non-success", "status", status.String(), "slot", block.Block.Slot)
	}
	switch status {
	case fork_graph.PreValidated:
		return nil
	case fork_graph.Success:
		f.updateChildren(block.Block.Slot-1, block.Block.ParentRoot, blockRoot) // parent slot can be innacurate
	case fork_graph.BelowAnchor:
		log.Debug("replay block", "status", status.String())
		return nil
	case fork_graph.MissingSegment:
		return ErrMissingSegment
	default:
		return fmt.Errorf("replay block, status %+v", status)
	}
	if block.Block.Body.ExecutionPayload != nil {
		f.eth2Roots.Add(blockRoot, block.Block.Body.ExecutionPayload.BlockHash)
	} else if blockVersion >= clparams.GloasVersion {
		// [New in Gloas:EIP7732] No ExecutionPayload in beacon block (GLOAS separates them).
		// Inherit parent's execution head as a placeholder so FCU sends a valid (non-zero) hash.
		// OnExecutionPayload will overwrite this with the real payload hash if the envelope arrives (FULL path).
		// If the envelope never arrives (EMPTY path), the parent hash is the correct EL head anyway.
		if parentHash, ok := f.eth2Roots.Get(block.Block.ParentRoot); ok {
			f.eth2Roots.Add(blockRoot, parentHash)
		}
	}
	// Note: highestSeen was already updated before AddChainSegment (line ~216)
	// so aggregates/attestations for this slot are accepted promptly. No second
	// update needed here.

	// Remove the parent from the head set
	delete(f.headSet, block.Block.ParentRoot)
	f.headSet[blockRoot] = struct{}{}
	// Add proposer score boost if the block is timely
	timeIntoSlot := (f.time.Load() - f.genesisTime) % lastProcessedState.BeaconConfig().SecondsPerSlot
	isBeforeAttestingInterval := timeIntoSlot < f.beaconCfg.SecondsPerSlot/f.beaconCfg.IntervalsPerSlot
	if f.Slot() == block.Block.Slot && isBeforeAttestingInterval && f.proposerBoostRoot.Load().(common.Hash) == (common.Hash{}) {
		f.proposerBoostRoot.Store(common.Hash(blockRoot))
	}

	// [New in Gloas:EIP7732] GLOAS-specific on_block logic (post state transition)
	if blockVersion >= clparams.GloasVersion {
		// Initialize payload timeliness and data availability votes for this block
		f.payloadTimelinessVote.Store(common.Hash(blockRoot), [clparams.PtcSize]bool{})
		f.payloadDataAvailabilityVote.Store(common.Hash(blockRoot), [clparams.PtcSize]bool{})

		// Notify PTC messages from payload attestations in the block.
		// Skip during forward sync (newPayload=false) — PTC votes only matter
		// for fork choice at the chain tip, not for historical blocks.
		if block.Block.Body.PayloadAttestations != nil && newPayload {
			f.notifyPtcMessages(lastProcessedState, block.Block.Body.PayloadAttestations)
		}
	}
	if lastProcessedState.Slot()%f.beaconCfg.SlotsPerEpoch == 0 {
		// Update randao mixes
		r := solid.NewHashVector(int(f.beaconCfg.EpochsPerHistoricalVector))
		lastProcessedState.RandaoMixes().CopyTo(r)
		f.randaoMixesLists.Add(blockRoot, r)
	} else {
		f.randaoDeltas.Add(blockRoot, randaoDelta{
			epoch: state.Epoch(lastProcessedState),
			delta: lastProcessedState.GetRandaoMixes(state.Epoch(lastProcessedState)),
		})
	}
	f.participation.Add(state.Epoch(lastProcessedState), lastProcessedState.CurrentEpochParticipation().Copy())
	f.preverifiedSizes.Add(blockRoot, preverifiedAppendListsSizes{
		validatorLength:           uint64(lastProcessedState.ValidatorLength()),
		historicalRootsLength:     lastProcessedState.HistoricalRootsLength(),
		historicalSummariesLength: lastProcessedState.HistoricalSummariesLength(),
	})
	f.finalityCheckpoints.Add(blockRoot, finalityCheckpoints{
		finalizedCheckpoint:         lastProcessedState.FinalizedCheckpoint(),
		currentJustifiedCheckpoint:  lastProcessedState.CurrentJustifiedCheckpoint(),
		previousJustifiedCheckpoint: lastProcessedState.PreviousJustifiedCheckpoint(),
	})

	if err := f.addPendingConsolidations(blockRoot, lastProcessedState.PendingConsolidations()); err != nil {
		return err
	}
	if err := f.addPendingDeposits(blockRoot, lastProcessedState.PendingDeposits()); err != nil {
		return err
	}
	if err := f.addPendingPartialWithdrawals(blockRoot, lastProcessedState.PendingPartialWithdrawals()); err != nil {
		return err
	}
	if err := f.addProposerLookahead(block.Block.Slot, lastProcessedState.ProposerLookahead()); err != nil {
		return err
	}

	f.totalActiveBalances.Add(blockRoot, lastProcessedState.GetTotalActiveBalance())
	// Update checkpoints
	f.updateCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint(), lastProcessedState.FinalizedCheckpoint())
	// First thing save previous values of the checkpoints (avoid memory copy of all states and ensure easy revert)
	var (
		previousJustifiedCheckpoint = lastProcessedState.PreviousJustifiedCheckpoint()
		currentJustifiedCheckpoint  = lastProcessedState.CurrentJustifiedCheckpoint()
		stateFinalized              = lastProcessedState.FinalizedCheckpoint()
		justificationBits           = lastProcessedState.JustificationBits().Copy()
	)
	f.operationsPool.NotifyBlock(block.Block)

	// Eagerly compute unrealized justification and finality
	if err := statechange.ProcessJustificationBitsAndFinality(lastProcessedState, nil); err != nil {
		return err
	}
	// Store per-block unrealized checkpoints (spec: store.unrealized_justifications[block_root])
	f.unrealizedJustifications.Store(common.Hash(blockRoot), lastProcessedState.CurrentJustifiedCheckpoint())
	f.unrealizedFinalizations.Store(common.Hash(blockRoot), lastProcessedState.FinalizedCheckpoint())
	f.updateUnrealizedCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint(), lastProcessedState.FinalizedCheckpoint())
	// Set the changed value pre-simulation
	lastProcessedState.SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint)
	lastProcessedState.SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint)
	lastProcessedState.SetFinalizedCheckpoint(stateFinalized)
	lastProcessedState.SetJustificationBits(justificationBits)

	// If the block is from a prior epoch, apply the realized values
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	if blockEpoch < currentEpoch {
		f.updateCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint(), lastProcessedState.FinalizedCheckpoint())
	}
	f.emitters.State().SendBlock(&beaconevents.BlockData{
		Slot:                block.Block.Slot,
		Block:               blockRoot,
		ExecutionOptimistic: f.optimisticStore.IsOptimistic(blockRoot),
	})

	if !isVerifiedExecutionPayload {
		log.Debug("OnBlock", "elapsed", time.Since(start), "slot", block.Block.Slot)
	}

	if f.peerDas != nil {
		if connectedValidators := f.localValidators.GetValidators(); len(connectedValidators) > 0 {
			// update the custody requirement whenever we see a new block
			custodyRequirement := state.GetValidatorsCustodyRequirement(lastProcessedState, connectedValidators)
			f.peerDas.UpdateValidatorsCustody(custodyRequirement)
		}
	}

	// [New in Gloas:EIP7732] Check if there's a pending envelope waiting for this block
	// This handles the case where envelope arrives before the block via gossip.
	// We need to release the lock before calling OnExecutionPayload to avoid deadlock.
	var pendingEnvelope *cltypes.SignedExecutionPayloadEnvelope
	if blockVersion >= clparams.GloasVersion {
		if pending, ok := f.pendingEnvelopes.Get(common.Hash(blockRoot)); ok {
			f.pendingEnvelopes.Remove(common.Hash(blockRoot))
			pendingEnvelope = pending
		}
	}

	// Release lock before processing pending envelope
	unlocked = true
	f.mu.Unlock()

	if pendingEnvelope != nil {
		log.Debug("OnBlock: processing pending envelope", "blockRoot", common.Hash(blockRoot))
		// Always validate payload with EL for pending envelopes, regardless of the caller's newPayload flag.
		// During forward sync newPayload is false, but the envelope still needs to reach the EL;
		// otherwise the EL never learns about this block and the chain stalls.
		if err := f.OnExecutionPayload(ctx, pendingEnvelope, checkDataAvaiability, true); err != nil {
			log.Warn("OnBlock: failed to process pending envelope", "blockRoot", common.Hash(blockRoot), "err", err)
		}
	}

	return nil
}

func (f *ForkChoiceStore) isDataAvailable(ctx context.Context, slot uint64, blockRoot common.Hash, blobKzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) error {
	if f.blobStorage == nil {
		return nil
	}

	commitmentsLeftToCheck := map[common.Bytes48]struct{}{}
	blobKzgCommitments.Range(func(index int, value *cltypes.KZGCommitment, length int) bool {
		commitmentsLeftToCheck[common.Bytes48(*value)] = struct{}{}
		return true
	})
	// Blobs are preverified so we skip verification, we just need to check if commitments checks out.
	sidecars, foundOnDisk, err := f.blobStorage.ReadBlobSidecars(ctx, slot, blockRoot)
	if err != nil {
		return fmt.Errorf("cannot check data avaiability. failed to read blob sidecars: %v", err)
	}
	if !foundOnDisk {
		sidecars = f.hotSidecars[blockRoot] // take it from memory
	}

	if blobKzgCommitments.Len() != len(sidecars) {
		return ErrEIP4844DataNotAvailable // This should then schedule the block for reprocessing
	}
	for _, sidecar := range sidecars {
		delete(commitmentsLeftToCheck, sidecar.KzgCommitment)
	}
	if len(commitmentsLeftToCheck) > 0 {
		return ErrEIP4844DataNotAvailable // This should then schedule the block for reprocessing
	}
	if !foundOnDisk {
		// If we didn't find the sidecars on disk, we should write them to disk now
		sort.Slice(sidecars, func(i, j int) bool {
			return sidecars[i].Index < sidecars[j].Index
		})
		if err := f.blobStorage.WriteBlobSidecars(ctx, blockRoot, sidecars); err != nil {
			return fmt.Errorf("failed to write blob sidecars: %v", err)
		}
	}
	return nil
}
