package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Giulio2002/bls"
	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/ledgerwatch/erigon-lib/crypto/kzg"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

type blobSidecarService struct {
	forkchoiceStore   forkchoice.ForkChoiceStorage
	beaconCfg         *clparams.BeaconChainConfig
	syncedDataManager *synced_data.SyncedDataManager

	blobSidecarsScheduledForLaterExecution sync.Map
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
	syncedDataManager *synced_data.SyncedDataManager) BlobSidecarsService {
	b := &blobSidecarService{
		beaconCfg:         beaconCfg,
		forkchoiceStore:   forkchoiceStore,
		syncedDataManager: syncedDataManager,
	}
	go b.loop(ctx)
	return b
}

// ProcessMessage processes a blob sidecar message
func (b *blobSidecarService) ProcessMessage(ctx context.Context, msg *cltypes.BlobSidecar) error {
	headState := b.syncedDataManager.HeadState()
	if headState == nil {
		b.scheduleBlobSidecarForLaterExecution(msg)
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
	return b.verifyAndStoreBlobSidecar(headState, msg)
}

func (b *blobSidecarService) verifyAndStoreBlobSidecar(headState *state.CachingBeaconState, msg *cltypes.BlobSidecar) error {
	kzgCtx := kzg.Ctx()

	if !cltypes.VerifyCommitmentInclusionProof(msg.KzgCommitment, msg.CommitmentInclusionProof, msg.Index,
		clparams.DenebVersion, msg.SignedBlockHeader.Header.BodyRoot) {
		return ErrCommitmentsInclusionProofFailed
	}

	if err := kzgCtx.VerifyBlobKZGProof(gokzg4844.Blob(msg.Blob), gokzg4844.KZGCommitment(msg.KzgCommitment), gokzg4844.KZGProof(msg.KzgProof)); err != nil {
		return fmt.Errorf("blob KZG proof verification failed: %v", err)
	}

	if err := b.verifySidecarsSignature(headState, msg.SignedBlockHeader); err != nil {
		return err
	}
	// operation is not thread safe from here.
	return b.forkchoiceStore.AddPreverifiedBlobSidecar(msg)
}

func (b *blobSidecarService) verifySidecarsSignature(headState *state.CachingBeaconState, header *cltypes.SignedBeaconBlockHeader) error {
	parentHeader, ok := b.forkchoiceStore.GetHeader(header.Header.ParentRoot)
	if !ok {
		return fmt.Errorf("parent header not found")
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
		return fmt.Errorf("blob signature validation: signature not valid")
	}
	return nil
}

func (b *blobSidecarService) scheduleBlobSidecarForLaterExecution(blobSidecar *cltypes.BlobSidecar) {
	blobSidecarJob := &blobSidecarJob{
		blobSidecar:  blobSidecar,
		creationTime: time.Now(),
	}
	b.blobSidecarsScheduledForLaterExecution.Store(blobSidecarJob, struct{}{})
}

// loop is the main loop of the block service
func (b *blobSidecarService) loop(ctx context.Context) {
	ticker := time.NewTicker(blobJobsIntervalTick)
	defer ticker.Stop()

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
		b.blobSidecarsScheduledForLaterExecution.Range(func(key, _ any) bool {
			job := key.(*blobSidecarJob)
			// check if it has expired
			if time.Since(job.creationTime) > blobJobExpiry {
				b.blobSidecarsScheduledForLaterExecution.Delete(key)
				return true
			}
			if err := b.verifyAndStoreBlobSidecar(headState, job.blobSidecar); err != nil {
				log.Debug("blob sidecar verification failed", "err", err)
				return true
			}
			b.blobSidecarsScheduledForLaterExecution.Delete(key)
			return true
		})
	}
}
