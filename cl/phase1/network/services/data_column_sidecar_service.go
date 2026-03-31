package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	st "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// pendingGloasSidecarExpiry is how long to keep pending sidecars before expiring (2 slots)
	pendingGloasSidecarExpiry = 24 * time.Second // ~2 slots at 12s per slot
	// pendingGloasSidecarTick is how often to check pending sidecars
	pendingGloasSidecarTick = 500 * time.Millisecond
)

var (
	verifyDataColumnSidecarInclusionProof           = das.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs                = das.VerifyDataColumnSidecarKZGProofs
	verifyDataColumnSidecar                         = das.VerifyDataColumnSidecar
	verifyDataColumnSidecarWithCommitments          = das.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = das.VerifyDataColumnSidecarKZGProofsWithCommitments
	computeSubnetForDataColumnSidecar               = das.ComputeSubnetForDataColumnSidecar
)

type dataColumnSidecarService struct {
	cfg                  *clparams.BeaconChainConfig
	ethClock             eth_clock.EthereumClock
	forkChoice           forkchoice.ForkChoiceStorage
	syncDataManager      synced_data.SyncedData
	seenSidecar          *lru.Cache[seenSidecarKey, struct{}]
	seenGloasSidecar     *lru.Cache[seenGloasSidecarKey, struct{}] // [New in Gloas:EIP7732]
	columnSidecarStorage blob_storage.DataColumnStorage
	emitters             *beaconevents.EventEmitter

	// [New in Gloas:EIP7732] Pending sidecars waiting for block to arrive
	pendingGloasSidecars sync.Map // map[seenGloasSidecarKey]*pendingGloasSidecarJob
}

// pendingGloasSidecarJob holds a sidecar that is waiting for its block to arrive
type pendingGloasSidecarJob struct {
	sidecar      *cltypes.DataColumnSidecar
	subnet       *uint64
	creationTime time.Time
}

// seenSidecarKey is used for Fulu (pre-GLOAS) seen tracking
type seenSidecarKey struct {
	slot          uint64
	proposerIndex uint64
	index         uint64
}

// seenGloasSidecarKey is used for GLOAS seen tracking
// [New in Gloas:EIP7732] The sidecar is the first sidecar for the tuple (sidecar.beacon_block_root, sidecar.index)
type seenGloasSidecarKey struct {
	beaconBlockRoot common.Hash
	index           uint64
}

func NewDataColumnSidecarService(
	ctx context.Context,
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
	seenGloasSidecar, err := lru.New[seenGloasSidecarKey, struct{}]("seenGloasDataColumnSidecar", int(size))
	if err != nil {
		panic(err)
	}
	s := &dataColumnSidecarService{
		cfg:                  cfg,
		ethClock:             ethClock,
		forkChoice:           forkChoice,
		syncDataManager:      syncDataManager,
		seenSidecar:          seenSidecar,
		seenGloasSidecar:     seenGloasSidecar,
		columnSidecarStorage: columnSidecarStorage,
		emitters:             emitters,
	}
	go s.loopPendingGloasSidecars(ctx)
	return s
}

func (s *dataColumnSidecarService) Names() []string {
	names := make([]string, 0, s.cfg.DataColumnSidecarSubnetCount)
	for i := 0; i < int(s.cfg.DataColumnSidecarSubnetCount); i++ {
		names = append(names, gossip.TopicNameDataColumnSidecar(uint64(i)))
	}
	return names
}

func (s *dataColumnSidecarService) IsMyGossipMessage(name string) bool {
	return gossip.IsTopicDataColumnSidecar(name)
}

func (s *dataColumnSidecarService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.DataColumnSidecar, error) {
	obj := cltypes.NewDataColumnSidecarWithVersion(version)
	if err := obj.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *dataColumnSidecarService) ProcessMessage(ctx context.Context, subnet *uint64, msg *cltypes.DataColumnSidecar) error {
	if s.syncDataManager.Syncing() {
		return ErrIgnore
	}

	// Version-aware processing
	if msg.Version() >= clparams.GloasVersion {
		return s.processGloasMessage(ctx, subnet, msg)
	}
	return s.processFuluMessage(ctx, subnet, msg)
}

// processFuluMessage handles Fulu (pre-GLOAS) data column sidecar validation
// Reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/fulu/p2p-interface.md
func (s *dataColumnSidecarService) processFuluMessage(ctx context.Context, subnet *uint64, msg *cltypes.DataColumnSidecar) error {
	if msg.SignedBlockHeader == nil || msg.SignedBlockHeader.Header == nil {
		return errors.New("missing signed block header for fulu sidecar")
	}

	blockHeader := msg.SignedBlockHeader.Header
	seenKey := seenSidecarKey{
		slot:          blockHeader.Slot,
		proposerIndex: blockHeader.ProposerIndex,
		index:         msg.Index,
	}

	// [IGNORE] The sidecar is the first sidecar for the tuple (block_header.slot, block_header.proposer_index, sidecar.index)
	if _, ok := s.seenSidecar.Get(seenKey); ok {
		return nil
	}

	blockRoot, err := blockHeader.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to get block root: %v", err)
	}

	if s.forkChoice.GetPeerDas().IsArchivedMode() {
		if s.forkChoice.GetPeerDas().IsColumnOverHalf(blockHeader.Slot, blockRoot) ||
			s.forkChoice.GetPeerDas().IsBlobAlreadyRecovered(blockRoot) {
			return ErrIgnore
		}
	} else {
		myCustodyColumns, err := s.forkChoice.GetPeerDas().StateReader().GetMyCustodyColumns()
		if err != nil {
			return fmt.Errorf("failed to get my custody columns: %v", err)
		}
		if _, ok := myCustodyColumns[msg.Index]; !ok {
			return ErrIgnore
		}
	}

	blobParameters := s.cfg.GetBlobParameters(blockHeader.Slot / s.cfg.SlotsPerEpoch)
	if msg.Column.Len() > int(blobParameters.MaxBlobsPerBlock) {
		log.Warn("invalid column sidecar length", "blockRoot", blockRoot, "columnIndex", msg.Index, "columnLen", msg.Column.Len())
		return errors.New("invalid column sidecar length")
	}

	// [REJECT] The sidecar is valid as verified by verify_data_column_sidecar(sidecar).
	if !verifyDataColumnSidecar(msg) {
		return errors.New("invalid data column sidecar")
	}

	// [REJECT] The sidecar is for the correct subnet
	if subnet != nil && *subnet != computeSubnetForDataColumnSidecar(msg.Index) {
		return fmt.Errorf("incorrect subnet %d for data column sidecar index %d", *subnet, msg.Index)
	}

	// [IGNORE] The sidecar is not from a future slot
	if blockHeader.Slot > s.ethClock.GetCurrentSlot() && !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(blockHeader.Slot) {
		return ErrIgnore
	}

	// [IGNORE] The sidecar is from a slot greater than the latest finalized slot
	if blockHeader.Slot <= s.forkChoice.FinalizedSlot() {
		return ErrIgnore
	}

	// [REJECT] The proposer signature is valid
	if pass, err := s.verifyProposerSignature(blockHeader.ProposerIndex, msg.SignedBlockHeader); err != nil {
		return fmt.Errorf("invalid proposer signature for data column sidecar: %v", err)
	} else if !pass {
		return errors.New("invalid proposer signature for data column sidecar")
	}

	// [IGNORE] The sidecar's block's parent has been seen
	parentHeader, ok := s.forkChoice.GetHeader(blockHeader.ParentRoot)
	if !ok {
		return ErrIgnore
	}

	// [REJECT] The sidecar is from a higher slot than parent
	if blockHeader.Slot <= parentHeader.Slot {
		return fmt.Errorf("data column sidecar should be from a higher slot than the parent block, but got %d <= %d", blockHeader.Slot, parentHeader.Slot)
	}

	// [REJECT] The finalized checkpoint is an ancestor
	finalizedCheckpoint := s.forkChoice.FinalizedCheckpoint()
	finalizedSlot := finalizedCheckpoint.Epoch * s.cfg.SlotsPerEpoch
	if s.forkChoice.Ancestor(blockHeader.ParentRoot, finalizedSlot).Root != finalizedCheckpoint.Root {
		return errors.New("finalized checkpoint is not an ancestor of the sidecar's block")
	}

	// [REJECT] The inclusion proof is valid
	if !verifyDataColumnSidecarInclusionProof(msg) {
		return errors.New("invalid inclusion proof for data column sidecar")
	}

	// [REJECT] The KZG proofs are valid
	if !verifyDataColumnSidecarKZGProofs(msg) {
		return errors.New("invalid kzg proofs for data column sidecar")
	}

	if err := s.columnSidecarStorage.WriteColumnSidecars(ctx, blockRoot, int64(msg.Index), msg); err != nil {
		return fmt.Errorf("failed to write data column sidecar: %v", err)
	}
	s.seenSidecar.Add(seenKey, struct{}{})

	if err := s.forkChoice.GetPeerDas().TryScheduleRecover(blockHeader.Slot, blockRoot); err != nil {
		log.Warn("failed to schedule recover", "err", err, "slot", blockHeader.Slot, "blockRoot", common.Hash(blockRoot).String())
	}
	log.Trace("[dataColumnSidecarService] processed fulu data column sidecar", "slot", blockHeader.Slot, "blockRoot", common.Hash(blockRoot).String(), "index", msg.Index)
	return nil
}

// processGloasMessage handles GLOAS data column sidecar validation
// Reference: https://github.com/ethereum/consensus-specs/blob/master/specs/gloas/p2p-interface.md
func (s *dataColumnSidecarService) processGloasMessage(ctx context.Context, subnet *uint64, msg *cltypes.DataColumnSidecar) error {
	slot := msg.Slot
	blockRoot := msg.BeaconBlockRoot

	seenKey := seenGloasSidecarKey{
		beaconBlockRoot: blockRoot,
		index:           msg.Index,
	}

	// [IGNORE] The sidecar is the first sidecar for the tuple (sidecar.beacon_block_root, sidecar.index)
	if _, ok := s.seenGloasSidecar.Get(seenKey); ok {
		return nil
	}

	// [IGNORE] The sidecar is not from a future slot (with some tolerance for clock disparity)
	if slot > s.ethClock.GetCurrentSlot() && !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(slot) {
		return ErrIgnore
	}

	// [IGNORE] The sidecar is from a slot greater than the latest finalized slot
	if slot <= s.forkChoice.FinalizedSlot() {
		return ErrIgnore
	}

	// Check custody columns
	if s.forkChoice.GetPeerDas().IsArchivedMode() {
		if s.forkChoice.GetPeerDas().IsColumnOverHalf(slot, blockRoot) ||
			s.forkChoice.GetPeerDas().IsBlobAlreadyRecovered(blockRoot) {
			return ErrIgnore
		}
	} else {
		myCustodyColumns, err := s.forkChoice.GetPeerDas().StateReader().GetMyCustodyColumns()
		if err != nil {
			return fmt.Errorf("failed to get my custody columns: %v", err)
		}
		if _, ok := myCustodyColumns[msg.Index]; !ok {
			return ErrIgnore
		}
	}

	// [IGNORE] A valid block for the sidecar's slot has been seen.
	// Only checks recent blocks in forkChoice memory - older blocks don't need sidecar validation.
	// If not yet seen, queue for deferred validation.
	block, ok := s.forkChoice.GetBlock(blockRoot)
	if !ok {
		s.scheduleSidecarForLaterProcessing(msg, subnet)
		return ErrIgnore
	}

	// [REJECT] The sidecar's slot matches the slot of the block
	if slot != block.Block.Slot {
		return fmt.Errorf("sidecar slot %d does not match block slot %d", slot, block.Block.Slot)
	}

	// Get kzg_commitments from bid = block.body.signed_execution_payload_bid.message
	if block.Block.Body.SignedExecutionPayloadBid == nil ||
		block.Block.Body.SignedExecutionPayloadBid.Message == nil {
		return errors.New("block does not have SignedExecutionPayloadBid")
	}
	kzgCommitments := &block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments

	blobParameters := s.cfg.GetBlobParameters(slot / s.cfg.SlotsPerEpoch)
	if msg.Column.Len() > int(blobParameters.MaxBlobsPerBlock) {
		log.Warn("invalid column sidecar length", "blockRoot", blockRoot, "columnIndex", msg.Index, "columnLen", msg.Column.Len())
		return errors.New("invalid column sidecar length")
	}

	// [REJECT] The sidecar is valid as verified by verify_data_column_sidecar(sidecar, bid.blob_kzg_commitments)
	if !verifyDataColumnSidecarWithCommitments(msg, kzgCommitments) {
		return errors.New("invalid data column sidecar")
	}

	// [REJECT] The sidecar is for the correct subnet
	if subnet != nil && *subnet != computeSubnetForDataColumnSidecar(msg.Index) {
		return fmt.Errorf("incorrect subnet %d for data column sidecar index %d", *subnet, msg.Index)
	}

	// [REJECT] The sidecar's column data is valid as verified by verify_data_column_sidecar_kzg_proofs(sidecar, bid.blob_kzg_commitments)
	if !verifyDataColumnSidecarKZGProofsWithCommitments(msg, kzgCommitments) {
		return errors.New("invalid kzg proofs for data column sidecar")
	}

	if err := s.columnSidecarStorage.WriteColumnSidecars(ctx, blockRoot, int64(msg.Index), msg); err != nil {
		return fmt.Errorf("failed to write data column sidecar: %v", err)
	}
	s.seenGloasSidecar.Add(seenKey, struct{}{})

	if err := s.forkChoice.GetPeerDas().TryScheduleRecover(slot, blockRoot); err != nil {
		log.Warn("failed to schedule recover", "err", err, "slot", slot, "blockRoot", blockRoot.String())
	}
	log.Trace("[dataColumnSidecarService] processed gloas data column sidecar", "slot", slot, "blockRoot", blockRoot.String(), "index", msg.Index)
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

// ValidatePartialDataColumnHeader validates a PartialDataColumnHeader against the known state.
// It performs:
// 1. Bid seen check: verifies the block root references a known block with a valid bid
// 2. Slot match: verifies the header's slot matches the block's slot
// 3. Commitments root match: verifies the kzg_commitments root matches the bid's commitments
func (s *dataColumnSidecarService) ValidatePartialDataColumnHeader(header *cltypes.PartialDataColumnHeader) error {
	if header == nil {
		return errors.New("nil partial data column header")
	}

	if header.Version() >= clparams.GloasVersion {
		return s.validateGloasPartialHeader(header)
	}
	return s.validateFuluPartialHeader(header)
}

// validateGloasPartialHeader validates a GLOAS-era PartialDataColumnHeader.
func (s *dataColumnSidecarService) validateGloasPartialHeader(header *cltypes.PartialDataColumnHeader) error {
	slot := header.Slot
	blockRoot := header.BeaconBlockRoot

	// [IGNORE] Not from a future slot
	if slot > s.ethClock.GetCurrentSlot() && !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(slot) {
		return ErrIgnore
	}

	// [IGNORE] From a slot greater than the latest finalized slot
	if slot <= s.forkChoice.FinalizedSlot() {
		return ErrIgnore
	}

	// Bid seen check: block must be known in fork choice
	block, ok := s.forkChoice.GetBlock(blockRoot)
	if !ok {
		return ErrIgnore
	}

	// Slot match: header slot must match block slot
	if slot != block.Block.Slot {
		return fmt.Errorf("partial header slot %d does not match block slot %d", slot, block.Block.Slot)
	}

	// Commitments root match: verify against bid's blob_kzg_commitments
	if block.Block.Body.SignedExecutionPayloadBid == nil ||
		block.Block.Body.SignedExecutionPayloadBid.Message == nil {
		return errors.New("block does not have SignedExecutionPayloadBid")
	}
	bidCommitments := &block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments
	bidCommitmentsRoot, err := bidCommitments.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to hash bid commitments: %v", err)
	}

	headerCommitmentsRoot, err := header.KzgCommitments.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to hash header commitments: %v", err)
	}

	if bidCommitmentsRoot != headerCommitmentsRoot {
		return fmt.Errorf("partial header commitments root mismatch: header=%x bid=%x", headerCommitmentsRoot, bidCommitmentsRoot)
	}

	return nil
}

// validateFuluPartialHeader validates a Fulu-era PartialDataColumnHeader.
func (s *dataColumnSidecarService) validateFuluPartialHeader(header *cltypes.PartialDataColumnHeader) error {
	if header.SignedBlockHeader == nil || header.SignedBlockHeader.Header == nil {
		return errors.New("missing signed block header in partial data column header")
	}

	blockHeader := header.SignedBlockHeader.Header

	// [IGNORE] Not from a future slot
	if blockHeader.Slot > s.ethClock.GetCurrentSlot() && !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(blockHeader.Slot) {
		return ErrIgnore
	}

	// [IGNORE] From a slot greater than the latest finalized slot
	if blockHeader.Slot <= s.forkChoice.FinalizedSlot() {
		return ErrIgnore
	}

	blockRoot, err := blockHeader.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to get block root: %v", err)
	}

	// Bid seen check: block's parent must be seen in fork choice
	parentHeader, ok := s.forkChoice.GetHeader(blockHeader.ParentRoot)
	if !ok {
		return ErrIgnore
	}

	// Slot match: header slot must be higher than parent
	if blockHeader.Slot <= parentHeader.Slot {
		return fmt.Errorf("partial header slot %d not higher than parent slot %d", blockHeader.Slot, parentHeader.Slot)
	}

	// [REJECT] The proposer signature is valid
	if pass, err := s.verifyProposerSignature(blockHeader.ProposerIndex, header.SignedBlockHeader); err != nil {
		return fmt.Errorf("invalid proposer signature for partial header: %v", err)
	} else if !pass {
		return errors.New("invalid proposer signature for partial header")
	}

	_ = blockRoot // Used for logging in production

	return nil
}

// scheduleSidecarForLaterProcessing queues a GLOAS sidecar for later processing when its block arrives
func (s *dataColumnSidecarService) scheduleSidecarForLaterProcessing(sidecar *cltypes.DataColumnSidecar, subnet *uint64) {
	key := seenGloasSidecarKey{
		beaconBlockRoot: sidecar.BeaconBlockRoot,
		index:           sidecar.Index,
	}

	// Don't schedule if already pending
	if _, loaded := s.pendingGloasSidecars.LoadOrStore(key, &pendingGloasSidecarJob{
		sidecar:      sidecar,
		subnet:       subnet,
		creationTime: time.Now(),
	}); loaded {
		return
	}

	log.Debug("[dataColumnSidecarService] scheduled GLOAS sidecar for later processing",
		"slot", sidecar.Slot, "blockRoot", sidecar.BeaconBlockRoot.String(), "index", sidecar.Index)
}

// loopPendingGloasSidecars periodically retries processing pending sidecars
func (s *dataColumnSidecarService) loopPendingGloasSidecars(ctx context.Context) {
	ticker := time.NewTicker(pendingGloasSidecarTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		s.pendingGloasSidecars.Range(func(key, value any) bool {
			job := value.(*pendingGloasSidecarJob)
			sidecarKey := key.(seenGloasSidecarKey)

			// Check if expired
			if time.Since(job.creationTime) > pendingGloasSidecarExpiry {
				s.pendingGloasSidecars.Delete(sidecarKey)
				log.Debug("[dataColumnSidecarService] expired pending GLOAS sidecar",
					"slot", job.sidecar.Slot, "blockRoot", job.sidecar.BeaconBlockRoot.String(), "index", job.sidecar.Index)
				return true
			}

			// Check if slot has become finalized while waiting
			if job.sidecar.Slot <= s.forkChoice.FinalizedSlot() {
				s.pendingGloasSidecars.Delete(sidecarKey)
				log.Debug("[dataColumnSidecarService] pending GLOAS sidecar slot is now finalized",
					"slot", job.sidecar.Slot, "blockRoot", job.sidecar.BeaconBlockRoot.String(), "index", job.sidecar.Index)
				return true
			}

			// Only retry if block is now available in forkChoice (recent blocks only)
			if _, ok := s.forkChoice.GetBlock(job.sidecar.BeaconBlockRoot); !ok {
				// Block still not available, keep waiting
				return true
			}

			// Block is available, try to process
			if err := s.processGloasMessage(ctx, job.subnet, job.sidecar); err != nil {
				// Processing failed for another reason (not block delay), remove from pending
				s.pendingGloasSidecars.Delete(sidecarKey)
				if !errors.Is(err, ErrIgnore) {
					log.Trace("[dataColumnSidecarService] failed to process pending GLOAS sidecar",
						"slot", job.sidecar.Slot, "blockRoot", job.sidecar.BeaconBlockRoot.String(), "index", job.sidecar.Index, "err", err)
				}
				return true
			}

			// Successfully processed, remove from pending
			s.pendingGloasSidecars.Delete(sidecarKey)
			log.Debug("[dataColumnSidecarService] successfully processed pending GLOAS sidecar",
				"slot", job.sidecar.Slot, "blockRoot", job.sidecar.BeaconBlockRoot.String(), "index", job.sidecar.Index)
			return true
		})
	}
}
