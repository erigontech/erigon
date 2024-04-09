package forkchoice

import (
	"fmt"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/ledgerwatch/erigon-lib/crypto/kzg"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

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

	// operation is not thread safe from here.
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, sidecar := range f.hotSidecars[blockRoot] {
		if sidecar.SignedBlockHeader.Header.Slot == blobSidecar.SignedBlockHeader.Header.Slot &&
			sidecar.SignedBlockHeader.Header.ProposerIndex == blobSidecar.SignedBlockHeader.Header.ProposerIndex &&
			sidecar.Index == blobSidecar.Index {
			return nil // ignore if we already have it
		}
	}
	f.hotSidecars[blockRoot] = append(f.hotSidecars[blockRoot], blobSidecar)

	blobsMaxAge := 4 // a slot can live for up to 4 slots in the pool of hot sidecars.
	currentSlot := f.highestSeen.Load()
	var pruneSlot uint64
	if currentSlot > uint64(blobsMaxAge) {
		pruneSlot = currentSlot - uint64(blobsMaxAge)
	}
	// also clean up all old blobs that may have been accumulating
	for blockRoot := range f.hotSidecars {
		if len(f.hotSidecars) == 0 || f.hotSidecars[blockRoot][0].SignedBlockHeader.Header.Slot < pruneSlot {
			delete(f.hotSidecars, blockRoot)
		}
	}

	return nil
}
