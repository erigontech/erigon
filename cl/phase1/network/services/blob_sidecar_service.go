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

	"github.com/Giulio2002/bls"
	gokzg4844 "github.com/crate-crypto/go-kzg-4844"

	"github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
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
	go b.loop(ctx)
	return b
}

// ProcessMessage processes a blob sidecar message
func (b *blobSidecarService) ProcessMessage(ctx context.Context, subnetId *uint64, msg *cltypes.BlobSidecar) error {
	if b.test {
		return b.verifyAndStoreBlobSidecar(nil, msg)
	}

	headState := b.syncedDataManager.HeadState()
	if headState == nil {
		b.scheduleBlobSidecarForLaterExecution(msg)
		return ErrIgnore
	}

	// [REJECT] The sidecar's index is consistent with MAX_BLOBS_PER_BLOCK -- i.e. blob_sidecar.index < MAX_BLOBS_PER_BLOCK.
	if msg.Index >= b.beaconCfg.MaxBlobsPerBlock {
		return errors.New("blob index out of range")
	}
	sidecarSubnetIndex := msg.Index % b.beaconCfg.MaxBlobsPerBlock
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

	if err := b.verifyAndStoreBlobSidecar(headState, msg); err != nil {
		return err
	}
	b.emitters.Operation().SendBlobSidecar(msg)
	return nil
}

func (b *blobSidecarService) verifyAndStoreBlobSidecar(headState *state.CachingBeaconState, msg *cltypes.BlobSidecar) error {
	kzgCtx := kzg.Ctx()

	if !b.test && !cltypes.VerifyCommitmentInclusionProof(msg.KzgCommitment, msg.CommitmentInclusionProof, msg.Index,
		clparams.DenebVersion, msg.SignedBlockHeader.Header.BodyRoot) {
		return ErrCommitmentsInclusionProofFailed
	}

	if err := kzgCtx.VerifyBlobKZGProof(gokzg4844.Blob(msg.Blob), gokzg4844.KZGCommitment(msg.KzgCommitment), gokzg4844.KZGProof(msg.KzgProof)); err != nil {
		return fmt.Errorf("blob KZG proof verification failed: %v", err)
	}
	if !b.test {
		if err := b.verifySidecarsSignature(headState, msg.SignedBlockHeader); err != nil {
			return err
		}
	}
	// operation is not thread safe from here.
	return b.forkchoiceStore.AddPreverifiedBlobSidecar(msg)
}

func (b *blobSidecarService) verifySidecarsSignature(headState *state.CachingBeaconState, header *cltypes.SignedBeaconBlockHeader) error {
	parentHeader, ok := b.forkchoiceStore.GetHeader(header.Header.ParentRoot)
	if !ok {
		return errors.New("parent header not found")
	}
	currentVersion := b.beaconCfg.GetCurrentStateVersion(parentHeader.Slot / b.beaconCfg.SlotsPerEpoch)
	forkVersion := b.beaconCfg.GetForkVersionByVersion(currentVersion)
	domain, err := fork.ComputeDomain(b.beaconCfg.DomainBeaconProposer[:], utils.Uint32ToBytes4(forkVersion), headState.GenesisValidatorsRoot())
	if err != nil {
		return err
	}
	sigRoot, err := fork.ComputeSigningRoot(header.Header, domain)
	if err != nil {
		return err
	}
	pk, err := headState.ValidatorPublicKey(int(header.Header.ProposerIndex))
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
		headState := b.syncedDataManager.HeadState()
		if headState == nil {
			continue
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

			if err := b.verifyAndStoreBlobSidecar(headState, job.blobSidecar); err != nil {
				log.Trace("blob sidecar verification failed", "err", err,
					"slot", job.blobSidecar.SignedBlockHeader.Header.Slot)
				return true
			}
			b.blobSidecarsScheduledForLaterExecution.Delete(key.([32]byte))
			return true
		})
	}
}
