package forkchoice

import (
	"context"
	"fmt"
	"sort"
	"time"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/crypto/kzg"

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

var errEIP4844DataNotAvailable = fmt.Errorf("EIP-4844 blob data is not available")

func (f *ForkChoiceStore) deriveNonAnchorPublicKeys(s *state.CachingBeaconState) ([]byte, error) {
	l := len(f.anchorPublicKeys) / length.Bytes48
	buf := make([]byte, (s.ValidatorLength()-l)*length.Bytes48)
	for i := l; i < s.ValidatorLength(); i++ {
		pk, err := s.ValidatorPublicKey(i)
		if err != nil {
			return nil, err
		}
		copy(buf[(i-l)*length.Bytes48:], pk[:])
	}
	return buf, nil
}

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
			if err == errEIP4844DataNotAvailable {
				log.Debug("Blob data is not available, the block will be scheduled for later processing", "slot", block.Block.Slot, "blockRoot", libcommon.Hash(blockRoot))
				f.scheduleBlockForLaterProcessing(block)
				return err
			}
			return fmt.Errorf("OnBlock: data is not available for block %x: %v", blockRoot, err)
		}
	}

	var invalidBlock bool
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
			log.Warn("newPayload failed", "err", err)
			return err
		}
		if invalidBlock {
			f.forkGraph.MarkHeaderAsInvalid(blockRoot)
			return fmt.Errorf("execution client failed")
		}
	}

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
	pks, err := f.deriveNonAnchorPublicKeys(lastProcessedState)
	if err != nil {
		return err
	}
	f.publicKeysPerState.Store(libcommon.Hash(blockRoot), pks)
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
		return errEIP4844DataNotAvailable // This should then schedule the block for reprocessing
	}
	for _, sidecar := range sidecars {
		delete(commitmentsLeftToCheck, sidecar.KzgCommitment)
	}
	if len(commitmentsLeftToCheck) > 0 {
		return errEIP4844DataNotAvailable // This should then schedule the block for reprocessing
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

func (f *ForkChoiceStore) OnBlobSidecar(blobSidecar *cltypes.BlobSidecar, test bool) error {
	kzgCtx := kzg.Ctx()

	parentHeader, has := f.GetHeader(blobSidecar.SignedBlockHeader.Header.ParentRoot)
	if !has {
		return fmt.Errorf("parent header not found")
	}
	if blobSidecar.SignedBlockHeader.Header.Slot <= parentHeader.Slot {
		return fmt.Errorf("blob sidecar has invalid slot")
	}
	expectedProposers, has := f.nextBlockProposers.Get(blobSidecar.SignedBlockHeader.Header.ParentRoot)
	proposerSubIdx := blobSidecar.SignedBlockHeader.Header.Slot - parentHeader.Slot
	if !test && has && proposerSubIdx < foreseenProposers && len(expectedProposers) > int(proposerSubIdx) {
		// Do extra checks on the proposer.
		expectedProposer := expectedProposers[proposerSubIdx]
		if blobSidecar.SignedBlockHeader.Header.ProposerIndex != expectedProposer {
			return fmt.Errorf("incorrect proposer index")
		}
		// verify the signature finally
		verifyFn := VerifyHeaderSignatureAgainstForkChoiceStoreFunction(f, f.beaconCfg, f.genesisValidatorsRoot)
		if err := verifyFn(blobSidecar.SignedBlockHeader); err != nil {
			return err
		}
	}

	if !test && !cltypes.VerifyCommitmentInclusionProof(blobSidecar.KzgCommitment, blobSidecar.CommitmentInclusionProof, blobSidecar.Index,
		clparams.DenebVersion, blobSidecar.SignedBlockHeader.Header.BodyRoot) {
		return fmt.Errorf("commitment inclusion proof failed")
	}

	if err := kzgCtx.VerifyBlobKZGProof(gokzg4844.Blob(blobSidecar.Blob), gokzg4844.KZGCommitment(blobSidecar.KzgCommitment), gokzg4844.KZGProof(blobSidecar.KzgProof)); err != nil {
		return fmt.Errorf("blob KZG proof verification failed: %v", err)
	}

	blockRoot, err := blobSidecar.SignedBlockHeader.Header.HashSSZ()
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// operation is not thread safe here.
	f.hotSidecars[blockRoot] = append(f.hotSidecars[blockRoot], blobSidecar)
	for _, sidecar := range f.hotSidecars[blockRoot] {
		if sidecar.SignedBlockHeader.Header.Slot == blobSidecar.SignedBlockHeader.Header.Slot &&
			sidecar.SignedBlockHeader.Header.ProposerIndex == blobSidecar.SignedBlockHeader.Header.ProposerIndex &&
			sidecar.Index == blobSidecar.Index {
			return nil // ignore if we already have it
		}
	}

	blobsMaxAge := 4 // a slot can live for up to 4 slots in the pool of hot sidecars.
	currentSlot := utils.GetCurrentSlot(f.genesisTime, f.beaconCfg.SecondsPerSlot)
	var pruneSlot uint64
	if currentSlot > uint64(blobsMaxAge) {
		pruneSlot = currentSlot - uint64(blobsMaxAge)
	}
	// also clean up all old blobs that may have been accumulating
	for blockRoot := range f.hotSidecars {
		h, has := f.GetHeader(blockRoot)
		if !has || h == nil || h.Slot < pruneSlot {
			delete(f.hotSidecars, blockRoot)
		}
	}

	return nil
}
