package forkchoice

import (
	"fmt"
	"time"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2/statechange"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethutils"
)

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

func (f *ForkChoiceStore) OnBlock(block *cltypes.SignedBeaconBlock, newPayload, fullValidation bool) error {
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
	if block.Version() >= clparams.DenebVersion {
		if err := isDataAvailable(blockRoot, block.Block.Body.BlobKzgCommitments); err != nil {
			return fmt.Errorf("OnBlock: data is not available")
		}
	}

	var invalidBlock bool
	if newPayload && f.engine != nil {
		if block.Version() >= clparams.DenebVersion {
			if err := verifyKzgCommitmentsAgainstTransactions(f.beaconCfg, block.Block.Body.ExecutionPayload, block.Block.Body.BlobKzgCommitments); err != nil {
				return fmt.Errorf("OnBlock: failed to process kzg commitments: %v", err)
			}
		}

		if invalidBlock, err = f.engine.NewPayload(block.Block.Body.ExecutionPayload, &block.Block.ParentRoot, versionedHashes); err != nil {
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
		if err := freezer.PutObjectSSZIntoFreezer("beaconState", "caplin_core", lastProcessedState.Slot(), lastProcessedState, f.recorder); err != nil {
			return err
		}
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
	// If the block is from a prior epoch, apply the realized values
	blockEpoch := f.computeEpochAtSlot(block.Block.Slot)
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	if blockEpoch < currentEpoch {
		f.updateCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint().Copy(), lastProcessedState.FinalizedCheckpoint().Copy())
	}
	log.Debug("OnBlock", "elapsed", time.Since(start))
	return nil
}

const (
	LEN_48 = 48 // KZGCommitment & KZGProof sizes
)

type KZGCommitment [LEN_48]byte // Compressed BLS12-381 G1 element
type KZGProof [LEN_48]byte
type Blob [fixedgas.BlobSize]byte

type BlobKzgs []KZGCommitment
type KZGProofs []KZGProof
type Blobs []Blob

func toBlobs(_blobs Blobs) []gokzg4844.Blob {
	blobs := make([]gokzg4844.Blob, len(_blobs))
	for i, _blob := range _blobs {
		blobs[i] = gokzg4844.Blob(_blob)
	}
	return blobs
}
func toComms(_comms BlobKzgs) []gokzg4844.KZGCommitment {
	comms := make([]gokzg4844.KZGCommitment, len(_comms))
	for i, _comm := range _comms {
		comms[i] = gokzg4844.KZGCommitment(_comm)
	}
	return comms
}
func toProofs(_proofs KZGProofs) []gokzg4844.KZGProof {
	proofs := make([]gokzg4844.KZGProof, len(_proofs))
	for i, _proof := range _proofs {
		proofs[i] = gokzg4844.KZGProof(_proof)
	}
	return proofs
}

func isDataAvailable(blockRoot libcommon.Hash, blobKzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) error {
	//FIXME: retrive Blobs and Proofs and verify them
	// blobs, proofs := retrieveBlobsAndProofs(blockRoot)

	// kzgCtx := libkzg.Ctx()
	// return kzgCtx.VerifyBlobKZGProofBatch(toBlobs(blobs), toComms(blobKzgCommitments), toProofs(proofs))
	return nil
}
