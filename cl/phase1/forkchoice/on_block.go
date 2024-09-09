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

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethutils"
)

const foreseenProposers = 16

var ErrEIP4844DataNotAvailable = errors.New("EIP-4844 blob data is not available")

func verifyKzgCommitmentsAgainstTransactions(cfg *clparams.BeaconChainConfig, block *cltypes.Eth1Block, kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) error {
	expectedBlobHashes := []common.Hash{}
	transactions, err := types.DecodeTransactions(block.Transactions.UnderlyngReference())
	if err != nil {
		return fmt.Errorf("unable to decode transactions: %v", err)
	}
	kzgCommitments.Range(func(index int, value *cltypes.KZGCommitment, length int) bool {
		var kzg libcommon.Hash
		kzg, err = utils.KzgCommitmentToVersionedHash(libcommon.Bytes48(*value))
		if err != nil {
			return false
		}
		expectedBlobHashes = append(expectedBlobHashes, kzg)
		return true
	})
	if err != nil {
		return err
	}

	return ethutils.ValidateBlobs(block.BlobGasUsed, cfg.MaxBlobGasPerBlock, cfg.MaxBlobsPerBlock, expectedBlobHashes, &transactions)
}

func (f *ForkChoiceStore) OnBlock(ctx context.Context, block *cltypes.SignedBeaconBlock, newPayload, fullValidation, checkDataAvaiability bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headHash = libcommon.Hash{}
	start := time.Now()
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	if f.Slot() < block.Block.Slot {
		return errors.New("block is too early compared to current_slot")
	}

	// Check that block is later than the finalized epoch slot (optimization to reduce calls to get_ancestor)
	finalizedSlot := f.computeStartSlotAtEpoch(f.finalizedCheckpoint.Load().(solid.Checkpoint).Epoch())
	if block.Block.Slot <= finalizedSlot {
		return nil
	}
	// Now we find the versioned hashes
	var versionedHashes []libcommon.Hash
	if newPayload && f.engine != nil && block.Version() >= clparams.DenebVersion {
		versionedHashes = []libcommon.Hash{}
		solid.RangeErr[*cltypes.KZGCommitment](block.Block.Body.BlobKzgCommitments, func(i1 int, k *cltypes.KZGCommitment, i2 int) error {
			versionedHash, err := utils.KzgCommitmentToVersionedHash(libcommon.Bytes48(*k))
			if err != nil {
				return err
			}
			versionedHashes = append(versionedHashes, versionedHash)
			return nil
		})
	}

	// Check if blob data is available
	if block.Version() >= clparams.DenebVersion && checkDataAvaiability {
		if err := f.isDataAvailable(ctx, block.Block.Slot, blockRoot, block.Block.Body.BlobKzgCommitments); err != nil {
			if err == ErrEIP4844DataNotAvailable {
				return err
			}
			return fmt.Errorf("OnBlock: data is not available for block %x: %v", libcommon.Hash(blockRoot), err)
		}
	}

	startEngine := time.Now()
	if newPayload && f.engine != nil {
		if block.Version() >= clparams.DenebVersion {
			if err := verifyKzgCommitmentsAgainstTransactions(f.beaconCfg, block.Block.Body.ExecutionPayload, block.Block.Body.BlobKzgCommitments); err != nil {
				return fmt.Errorf("OnBlock: failed to process kzg commitments: %v", err)
			}
		}
		payloadStatus, err := f.engine.NewPayload(ctx, block.Block.Body.ExecutionPayload, &block.Block.ParentRoot, versionedHashes)
		switch payloadStatus {
		case execution_client.PayloadStatusNotValidated:
			log.Debug("OnBlock: block is not validated yet", "block", libcommon.Hash(blockRoot))
			// optimistic block candidate
			if err := f.optimisticStore.AddOptimisticCandidate(block.Block); err != nil {
				return fmt.Errorf("failed to add block to optimistic store: %v", err)
			}
		case execution_client.PayloadStatusInvalidated:
			log.Warn("OnBlock: block is invalid", "block", libcommon.Hash(blockRoot), "err", err)
			log.Debug("OnBlock: block is invalid", "block", libcommon.Hash(blockRoot))
			f.forkGraph.MarkHeaderAsInvalid(blockRoot)
			// remove from optimistic candidate
			if err := f.optimisticStore.InvalidateBlock(block.Block); err != nil {
				return fmt.Errorf("failed to remove block from optimistic store: %v", err)
			}
			return errors.New("block is invalid")
		case execution_client.PayloadStatusValidated:
			log.Trace("OnBlock: block is validated", "block", libcommon.Hash(blockRoot))
			// remove from optimistic candidate
			if err := f.optimisticStore.ValidateBlock(block.Block); err != nil {
				return fmt.Errorf("failed to validate block in optimistic store: %v", err)
			}
		}
		if err != nil {
			return fmt.Errorf("newPayload failed: %v", err)
		}
	}
	log.Trace("OnBlock: engine", "elapsed", time.Since(startEngine))
	lastProcessedState, status, err := f.forkGraph.AddChainSegment(block, fullValidation)
	if err != nil {
		return err
	}
	switch status {
	case fork_graph.PreValidated:
		return nil
	case fork_graph.Success:
		f.updateChildren(block.Block.Slot-1, block.Block.ParentRoot, blockRoot) // parent slot can be innacurate
	case fork_graph.BelowAnchor:
		log.Debug("replay block", "status", status.String())
		return nil
	default:
		return fmt.Errorf("replay block, status %+v", status)
	}
	if block.Block.Body.ExecutionPayload != nil {
		f.eth2Roots.Add(blockRoot, block.Block.Body.ExecutionPayload.BlockHash)
	}

	if block.Block.Slot > f.highestSeen.Load() {
		f.highestSeen.Store(block.Block.Slot)
	}
	// Remove the parent from the head set
	delete(f.headSet, block.Block.ParentRoot)
	f.headSet[blockRoot] = struct{}{}
	// Add proposer score boost if the block is timely
	timeIntoSlot := (f.time.Load() - f.genesisTime) % lastProcessedState.BeaconConfig().SecondsPerSlot
	isBeforeAttestingInterval := timeIntoSlot < f.beaconCfg.SecondsPerSlot/f.beaconCfg.IntervalsPerSlot
	if f.Slot() == block.Block.Slot && isBeforeAttestingInterval && f.proposerBoostRoot.Load().(libcommon.Hash) == (libcommon.Hash{}) {
		f.proposerBoostRoot.Store(libcommon.Hash(blockRoot))
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
		finalizedCheckpoint:         lastProcessedState.FinalizedCheckpoint().Copy(),
		currentJustifiedCheckpoint:  lastProcessedState.CurrentJustifiedCheckpoint().Copy(),
		previousJustifiedCheckpoint: lastProcessedState.PreviousJustifiedCheckpoint().Copy(),
	})

	f.totalActiveBalances.Add(blockRoot, lastProcessedState.GetTotalActiveBalance())
	// Update checkpoints
	f.updateCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint().Copy(), lastProcessedState.FinalizedCheckpoint().Copy())
	// First thing save previous values of the checkpoints (avoid memory copy of all states and ensure easy revert)
	var (
		previousJustifiedCheckpoint = lastProcessedState.PreviousJustifiedCheckpoint().Copy()
		currentJustifiedCheckpoint  = lastProcessedState.CurrentJustifiedCheckpoint().Copy()
		finalizedCheckpoint         = lastProcessedState.FinalizedCheckpoint().Copy()
		justificationBits           = lastProcessedState.JustificationBits().Copy()
	)
	// Eagerly compute unrealized justification and finality
	if err := statechange.ProcessJustificationBitsAndFinality(lastProcessedState, nil); err != nil {
		return err
	}
	f.operationsPool.NotifyBlock(block.Block)
	f.updateUnrealizedCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint().Copy(), lastProcessedState.FinalizedCheckpoint().Copy())
	// Set the changed value pre-simulation
	lastProcessedState.SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint)
	lastProcessedState.SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint)
	lastProcessedState.SetFinalizedCheckpoint(finalizedCheckpoint)
	lastProcessedState.SetJustificationBits(justificationBits)
	// Load next proposer indicies for the parent root
	idxs := make([]uint64, 0, foreseenProposers)
	for i := lastProcessedState.Slot() + 1; i < f.beaconCfg.SlotsPerEpoch; i++ {
		idx, err := lastProcessedState.GetBeaconProposerIndexForSlot(i)
		if err != nil {
			return err
		}
		idxs = append(idxs, idx)
	}
	f.nextBlockProposers.Add(blockRoot, idxs)
	// If the block is from a prior epoch, apply the realized values
	blockEpoch := f.computeEpochAtSlot(block.Block.Slot)
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	if blockEpoch < currentEpoch {
		f.updateCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint().Copy(), lastProcessedState.FinalizedCheckpoint().Copy())
	}
	f.emitters.State().SendBlock(&beaconevents.BlockData{
		Slot:                block.Block.Slot,
		Block:               blockRoot,
		ExecutionOptimistic: f.optimisticStore.IsOptimistic(blockRoot),
	})
	if f.validatorMonitor != nil {
		f.validatorMonitor.OnNewBlock(lastProcessedState, block.Block)
	}
	log.Trace("OnBlock", "elapsed", time.Since(start))
	return nil
}

func (f *ForkChoiceStore) isDataAvailable(ctx context.Context, slot uint64, blockRoot libcommon.Hash, blobKzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) error {
	if f.blobStorage == nil {
		return nil
	}

	commitmentsLeftToCheck := map[libcommon.Bytes48]struct{}{}
	blobKzgCommitments.Range(func(index int, value *cltypes.KZGCommitment, length int) bool {
		commitmentsLeftToCheck[libcommon.Bytes48(*value)] = struct{}{}
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
