package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	st "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

var (
	verifyDataColumnSidecarInclusionProof = das.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs      = das.VerifyDataColumnSidecarKZGProofs
	verifyDataColumnSidecar               = das.VerifyDataColumnSidecar
	computeSubnetForDataColumnSidecar     = das.ComputeSubnetForDataColumnSidecar
)

type dataColumnSidecarService struct {
	cfg *clparams.BeaconChainConfig
	//beaconState     *state.CachingBeaconState
	ethClock             eth_clock.EthereumClock
	forkChoice           forkchoice.ForkChoiceStorage
	syncDataManager      synced_data.SyncedData
	seenSidecar          *lru.Cache[seenSidecarKey, struct{}]
	columnSidecarStorage blob_storage.DataColumnStorage
	emitters             *beaconevents.EventEmitter
}

func NewDataColumnSidecarService(
	cfg *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	forkChoice forkchoice.ForkChoiceStorage,
	syncDataManager synced_data.SyncedData,
	columnSidecarStorage blob_storage.DataColumnStorage,
	emitters *beaconevents.EventEmitter,
) DataColumnSidecarService {
	size := cfg.NumberOfColumns * cfg.SlotsPerEpoch * 4
	seenSidecar, err := lru.New[seenSidecarKey, struct{}]("seenDataColumnSidecar", int(size))
	if err != nil {
		panic(err)
	}
	return &dataColumnSidecarService{
		cfg:                  cfg,
		ethClock:             ethClock,
		forkChoice:           forkChoice,
		syncDataManager:      syncDataManager,
		seenSidecar:          seenSidecar,
		columnSidecarStorage: columnSidecarStorage,
		emitters:             emitters,
	}
}

type seenSidecarKey struct {
	slot          uint64
	proposerIndex uint64
	index         uint64
}

func (s *dataColumnSidecarService) ProcessMessage(ctx context.Context, subnet *uint64, msg *cltypes.DataColumnSidecar) error {
	if s.syncDataManager.Syncing() {
		// maybe later processing
		return ErrIgnore
	}

	// reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/fulu/p2p-interface.md
	blockHeader := msg.SignedBlockHeader.Header
	seenKey := seenSidecarKey{
		slot:          blockHeader.Slot,
		proposerIndex: blockHeader.ProposerIndex,
		index:         msg.Index,
	}

	// [IGNORE] The sidecar is the first sidecar for the tuple (block_header.slot, block_header.proposer_index, sidecar.index) with valid header signature, sidecar inclusion proof, and kzg proof.
	if _, ok := s.seenSidecar.Get(seenKey); ok {
		return ErrIgnore
	}

	blockRoot, err := msg.SignedBlockHeader.Header.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to get block root: %v", err)
	}
	s.seenSidecar.Add(seenKey, struct{}{})

	if s.forkChoice.GetPeerDas().IsArchivedMode() {
		if s.forkChoice.GetPeerDas().IsColumnOverHalf(blockHeader.Slot, blockRoot) ||
			s.forkChoice.GetPeerDas().IsBlobAlreadyRecovered(blockRoot) {
			// already processed
			return ErrIgnore
		}
	} else {
		myCustodyColumns, err := s.forkChoice.GetPeerDas().StateReader().GetMyCustodyColumns()
		if err != nil {
			return fmt.Errorf("failed to get my custody columns: %v", err)
		}
		if _, ok := myCustodyColumns[msg.Index]; !ok {
			// not my custody column
			log.Debug("not my custody column")
			return ErrIgnore
		}
	}

	// [REJECT] The sidecar is valid as verified by verify_data_column_sidecar(sidecar).
	if !verifyDataColumnSidecar(msg) {
		return errors.New("invalid data column sidecar")
	}

	// [REJECT] The sidecar is for the correct subnet -- i.e. compute_subnet_for_data_column_sidecar(sidecar.index) == subnet_id.
	if subnet != nil && *subnet != computeSubnetForDataColumnSidecar(msg.Index) {
		return fmt.Errorf("incorrect subnet %d for data column sidecar index %d", *subnet, msg.Index)
	}

	// [IGNORE] The sidecar is not from a future slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) --
	// i.e. validate that block_header.slot <= current_slot (a client MAY queue future sidecars for processing at the appropriate slot).
	if blockHeader.Slot > s.ethClock.GetCurrentSlot() && !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(blockHeader.Slot) {
		return ErrIgnore
	}

	// [IGNORE] The sidecar is from a slot greater than the latest finalized slot -- i.e. validate that block_header.slot > compute_start_slot_at_epoch(state.finalized_checkpoint.epoch)
	if blockHeader.Slot <= s.forkChoice.FinalizedSlot() {
		return ErrIgnore
	}

	// [REJECT] The proposer signature of sidecar.signed_block_header, is valid with respect to the block_header.proposer_index pubkey.
	if pass, err := s.verifyProposerSignature(blockHeader.ProposerIndex, msg.SignedBlockHeader); err != nil {
		return fmt.Errorf("invalid proposer signature for data column sidecar: %v", err)
	} else if !pass {
		return errors.New("invalid proposer signature for data column sidecar")
	}

	// [IGNORE] The sidecar's block's parent (defined by block_header.parent_root) has been seen (via gossip or non-gossip sources)
	// (a client MAY queue sidecars for processing once the parent block is retrieved).
	// [REJECT] The sidecar's block's parent (defined by block_header.parent_root) passes validation.
	parentHeader, ok := s.forkChoice.GetHeader(blockHeader.ParentRoot)
	if !ok {
		return ErrIgnore
	}

	// [REJECT] The sidecar is from a higher slot than the sidecar's block's parent (defined by block_header.parent_root).
	if blockHeader.Slot <= parentHeader.Slot {
		return fmt.Errorf("data column sidecar should be from a higher slot than the parent block, but got %d <= %d", blockHeader.Slot, parentHeader.Slot)
	}

	// [REJECT] The current finalized_checkpoint is an ancestor of the sidecar's block --
	// i.e. get_checkpoint_block(store, block_header.parent_root, store.finalized_checkpoint.epoch) == store.finalized_checkpoint.root.
	finalizedCheckpoint := s.forkChoice.FinalizedCheckpoint()
	finalizedSlot := finalizedCheckpoint.Epoch * s.cfg.SlotsPerEpoch
	if s.forkChoice.Ancestor(blockHeader.ParentRoot, finalizedSlot) != finalizedCheckpoint.Root {
		return errors.New("finalized checkpoint is not an ancestor of the sidecar's block")
	}

	// [REJECT] The sidecar's kzg_commitments field inclusion proof is valid as verified by verify_data_column_sidecar_inclusion_proof(sidecar).
	if !verifyDataColumnSidecarInclusionProof(msg) {
		return errors.New("invalid inclusion proof for data column sidecar")
	}

	// [REJECT] The sidecar's column data is valid as verified by verify_data_column_sidecar_kzg_proofs(sidecar).
	if !verifyDataColumnSidecarKZGProofs(msg) {
		return errors.New("invalid kzg proofs for data column sidecar")
	}

	if err := s.columnSidecarStorage.WriteColumnSidecars(ctx, blockRoot, int64(msg.Index), msg); err != nil {
		return fmt.Errorf("failed to write data column sidecar: %v", err)
	}
	if s.forkChoice.GetPeerDas().IsArchivedMode() {
		if err := s.forkChoice.GetPeerDas().TryScheduleRecover(blockHeader.Slot, blockRoot); err != nil {
			log.Warn("failed to schedule recover", "err", err, "slot", blockHeader.Slot, "blockRoot", common.Hash(blockRoot).String())
		}
	}
	log.Trace("[dataColumnSidecarService] processed data column sidecar", "slot", blockHeader.Slot, "blockRoot", common.Hash(blockRoot).String(), "index", msg.Index)
	return nil
}

func (s *dataColumnSidecarService) verifyProposerSignature(proposerIndex uint64, signedBlockHeader *cltypes.SignedBeaconBlockHeader) (bool, error) {
	var (
		valid       bool
		pk          common.Bytes48
		signingRoot common.Hash
	)
	err := s.syncDataManager.ViewHeadState(func(state *st.CachingBeaconState) error {
		proposer, err := state.ValidatorForValidatorIndex(int(proposerIndex))
		if err != nil {
			return fmt.Errorf("unable to retrieve state: %v", err)
		}

		// Verify signatures for both headers
		domain, err := state.GetDomain(s.cfg.DomainBeaconProposer, st.GetEpochAtSlot(s.cfg, signedBlockHeader.Header.Slot))
		if err != nil {
			return fmt.Errorf("unable to get domain: %v", err)
		}
		pk = proposer.PublicKey()
		signingRoot, err = computeSigningRoot(signedBlockHeader.Header, domain)
		if err != nil {
			return fmt.Errorf("unable to compute signing root: %v", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	valid, err = blsVerify(signedBlockHeader.Signature[:], signingRoot[:], pk[:])
	if err != nil {
		return false, fmt.Errorf("unable to verify signature: %v", err)
	}
	return valid, nil
}
