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

package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/erigontech/erigon/cl/utils/bls"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

type blobSidecarService struct {
	forkchoiceStore   forkchoice.ForkChoiceStorage
	beaconCfg         *clparams.BeaconChainConfig
	syncedDataManager *synced_data.SyncedDataManager
	ethClock          eth_clock.EthereumClock
	emitters          *beaconevents.EventEmitter

	blobSidecarsScheduledForLaterExecution sync.Map
	test                                   bool
}

type blobSidecarJob struct {
	blobSidecar  *cltypes.BlobSidecar
	creationTime time.Time
}

// NewBlobSidecarService creates a new blob sidecar service
func NewBlobSidecarService(
	ctx context.Context,
	beaconCfg *clparams.BeaconChainConfig,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	syncedDataManager *synced_data.SyncedDataManager,
	ethClock eth_clock.EthereumClock,
	emitters *beaconevents.EventEmitter,
	test bool,
) BlobSidecarsService {
	b := &blobSidecarService{
		beaconCfg:         beaconCfg,
		forkchoiceStore:   forkchoiceStore,
		syncedDataManager: syncedDataManager,
		test:              test,
		ethClock:          ethClock,
		emitters:          emitters,
	}
	// go b.loop(ctx)
	return b
}

// ProcessMessage processes a blob sidecar message
func (b *blobSidecarService) ProcessMessage(ctx context.Context, subnetId *uint64, msg *cltypes.BlobSidecar) error {
	if b.test {
		return b.verifyAndStoreBlobSidecar(msg)
	}

	sidecarVersion := b.beaconCfg.GetCurrentStateVersion(msg.SignedBlockHeader.Header.Slot / b.beaconCfg.SlotsPerEpoch)
	// [REJECT] The sidecar's index is consistent with MAX_BLOBS_PER_BLOCK -- i.e. blob_sidecar.index < MAX_BLOBS_PER_BLOCK.
	maxBlobsPerBlock := b.beaconCfg.MaxBlobsPerBlockByVersion(sidecarVersion)
	if msg.Index >= maxBlobsPerBlock {
		return errors.New("blob index out of range")
	}
	// [REJECT] The sidecar is for the correct subnet -- i.e. compute_subnet_for_blob_sidecar(blob_sidecar.index) == subnet_id
	sidecarSubnetIndex := msg.Index % b.beaconCfg.BlobSidecarSubnetCountByVersion(sidecarVersion)
	if sidecarSubnetIndex != *subnetId {
		return ErrBlobIndexOutOfRange
	}
	currentSlot := b.ethClock.GetCurrentSlot()
	sidecarSlot := msg.SignedBlockHeader.Header.Slot
	// [IGNORE] The block is not from a future slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) -- i.e. validate that
	// signed_beacon_block.message.slot <= current_slot (a client MAY queue future blocks for processing at the appropriate slot).
	if currentSlot < sidecarSlot && !b.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(sidecarSlot) {
		return ErrIgnore
	}

	// [IGNORE] The sidecar is from a slot greater than the latest finalized slot -- i.e. validate that block_header.slot > compute_start_slot_at_epoch(store.finalized_checkpoint.epoch)
	if b.forkchoiceStore.FinalizedSlot() >= sidecarSlot {
		return ErrIgnore
	}

	blockRoot, err := msg.SignedBlockHeader.Header.HashSSZ()
	if err != nil {
		return err
	}
	// Do not bother with blocks processed by fork choice already.
	if _, has := b.forkchoiceStore.GetHeader(blockRoot); has {
		return ErrIgnore
	}

	parentHeader, has := b.forkchoiceStore.GetHeader(msg.SignedBlockHeader.Header.ParentRoot)
	if !has {
		b.scheduleBlobSidecarForLaterExecution(msg)
		return ErrIgnore
	}
	if msg.SignedBlockHeader.Header.Slot <= parentHeader.Slot {
		return ErrInvalidSidecarSlot
	}

	if err := b.verifyAndStoreBlobSidecar(msg); err != nil {
		return err
	}
	b.emitters.Operation().SendBlobSidecar(msg)
	return nil
}

func (b *blobSidecarService) verifyAndStoreBlobSidecar(msg *cltypes.BlobSidecar) error {
	kzgCtx := kzg.Ctx()

	if !b.test && !cltypes.VerifyCommitmentInclusionProof(msg.KzgCommitment, msg.CommitmentInclusionProof, msg.Index,
		clparams.DenebVersion, msg.SignedBlockHeader.Header.BodyRoot) {
		return ErrCommitmentsInclusionProofFailed
	}

	start := time.Now()
	if err := kzgCtx.VerifyBlobKZGProof((*goethkzg.Blob)(&msg.Blob), goethkzg.KZGCommitment(msg.KzgCommitment), goethkzg.KZGProof(msg.KzgProof)); err != nil {
		return fmt.Errorf("blob KZG proof verification failed: %v", err)
	}

	if !b.test {
		if err := b.verifySidecarsSignature(msg.SignedBlockHeader); err != nil {
			return err
		}
	}
	monitor.ObserveBlobVerificationTime(start)
	// operation is not thread safe from here.
	return b.forkchoiceStore.AddPreverifiedBlobSidecar(msg)
}

func (b *blobSidecarService) verifySidecarsSignature(header *cltypes.SignedBeaconBlockHeader) error {
	parentHeader, ok := b.forkchoiceStore.GetHeader(header.Header.ParentRoot)
	if !ok {
		return errors.New("parent header not found")
	}
	currentVersion := b.beaconCfg.GetCurrentStateVersion(parentHeader.Slot / b.beaconCfg.SlotsPerEpoch)
	forkVersion := b.beaconCfg.GetForkVersionByVersion(currentVersion)

	var (
		domain []byte
		pk     common.Bytes48
		err    error
	)
	// Load head state
	if err := b.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		domain, err = fork.ComputeDomain(b.beaconCfg.DomainBeaconProposer[:], utils.Uint32ToBytes4(forkVersion), headState.GenesisValidatorsRoot())
		if err != nil {
			return err
		}

		pk, err = headState.ValidatorPublicKey(int(header.Header.ProposerIndex))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	sigRoot, err := fork.ComputeSigningRoot(header.Header, domain)
	if err != nil {
		return err
	}

	if ok, err = bls.Verify(header.Signature[:], sigRoot[:], pk[:]); err != nil {
		return err
	}
	if !ok {
		return errors.New("blob signature validation: signature not valid")
	}
	return nil
}

func (b *blobSidecarService) scheduleBlobSidecarForLaterExecution(blobSidecar *cltypes.BlobSidecar) {
	blobSidecarJob := &blobSidecarJob{
		blobSidecar:  blobSidecar,
		creationTime: time.Now(),
	}
	blobSidecarHash, err := blobSidecar.HashSSZ()
	if err != nil {
		return
	}
	b.blobSidecarsScheduledForLaterExecution.Store(blobSidecarHash, blobSidecarJob)
}

// loop is the main loop of the block service
func (b *blobSidecarService) loop(ctx context.Context) {
	ticker := time.NewTicker(blobJobsIntervalTick)
	defer ticker.Stop()
	if b.test {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		b.blobSidecarsScheduledForLaterExecution.Range(func(key, value any) bool {
			job := value.(*blobSidecarJob)
			// check if it has expired
			if time.Since(job.creationTime) > blobJobExpiry {
				b.blobSidecarsScheduledForLaterExecution.Delete(key.([32]byte))
				return true
			}
			blockRoot, err := job.blobSidecar.SignedBlockHeader.Header.HashSSZ()
			if err != nil {
				log.Debug("blob sidecar verification failed", "err", err)
				return true
			}
			if _, has := b.forkchoiceStore.GetHeader(blockRoot); has {
				b.blobSidecarsScheduledForLaterExecution.Delete(key.([32]byte))
				return true
			}
			if err := b.verifyAndStoreBlobSidecar(job.blobSidecar); err != nil {
				log.Trace("blob sidecar verification failed", "err", err,
					"slot", job.blobSidecar.SignedBlockHeader.Header.Slot)
				return true
			}
			b.blobSidecarsScheduledForLaterExecution.Delete(key.([32]byte))
			return true
		})
	}
}
