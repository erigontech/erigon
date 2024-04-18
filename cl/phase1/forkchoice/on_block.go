package forkchoice

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2/statechange"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethutils"
)

const foreseenProposers = 16

var ErrEIP4844DataNotAvailable = fmt.Errorf("EIP-4844 blob data is not available")

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
		return fmt.Errorf("block is too early compared to current_slot")
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
			return fmt.Errorf("OnBlock: data is not available for block %x: %v", blockRoot, err)
		}
	}

	var invalidBlock bool
	startEngine := time.Now()
	if newPayload && f.engine != nil {
		if block.Version() >= clparams.DenebVersion {
			if err := verifyKzgCommitmentsAgainstTransactions(f.beaconCfg, block.Block.Body.ExecutionPayload, block.Block.Body.BlobKzgCommitments); err != nil {
				return fmt.Errorf("OnBlock: failed to process kzg commitments: %v", err)
			}
		}

		if invalidBlock, err = f.engine.NewPayload(ctx, block.Block.Body.ExecutionPayload, &block.Block.ParentRoot, versionedHashes); err != nil {
			if invalidBlock {
				f.forkGraph.MarkHeaderAsInvalid(blockRoot)
			}
			return fmt.Errorf("newPayload failed: %v", err)
		}
		if invalidBlock {
			f.forkGraph.MarkHeaderAsInvalid(blockRoot)
			return fmt.Errorf("execution client failed")
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
		log.Debug("replay block", "code", status)
		return nil
	default:
		return fmt.Errorf("replay block, code: %+v", status)
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
	log.Debug("OnBlock", "elapsed", time.Since(start))
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
