package das

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
)

// ImportColumnsFromBlobs builds the data column sidecars of a Fulu block from an
// engine_getBlobsV2 response and stores the columns this node keeps. The response
// must hold every blob of the block, each with one proof per cell; otherwise nothing
// is stored. The cell proofs are not verified: the EL verified them when it admitted
// the blob transaction, and the Fulu p2p spec says to skip verification of blobs
// retrieved from the local EL.
func (d *peerdas) ImportColumnsFromBlobs(ctx context.Context, block *cltypes.SignedBeaconBlock, blobs [][]byte, cellProofs [][][]byte) error {
	commitments := block.GetBlobKzgCommitments()
	if len(blobs) != commitments.Len() || len(cellProofs) != commitments.Len() {
		return fmt.Errorf("incomplete blobs: got %d blobs and %d proof sets for %d commitments", len(blobs), len(cellProofs), commitments.Len())
	}
	numberOfColumns := int(d.beaconConfig.NumberOfColumns)
	for i, blob := range blobs {
		if len(blob) != cltypes.BYTES_PER_BLOB || len(cellProofs[i]) != numberOfColumns {
			return fmt.Errorf("blob %d: got %d bytes and %d proofs", i, len(blob), len(cellProofs[i]))
		}
		for j, proof := range cellProofs[i] {
			if len(proof) != len(cltypes.KZGProof{}) {
				return fmt.Errorf("blob %d: proof %d has %d bytes", i, j, len(proof))
			}
		}
	}
	cellsAndProofs := make([]peerdasutils.CellsAndKZGProofs, len(blobs))
	for i, blob := range blobs {
		blobCells, err := ComputeCells((*cltypes.Blob)(blob))
		if err != nil {
			return fmt.Errorf("blob %d: %w", i, err)
		}
		cellsAndProofs[i] = peerdasutils.CellsAndKZGProofs{Blobs: blobCells, Proofs: make([]cltypes.KZGProof, numberOfColumns)}
		for j := range cellsAndProofs[i].Proofs {
			copy(cellsAndProofs[i].Proofs[j][:], cellProofs[i][j])
		}
	}

	inclusionProofRaw, err := block.Block.Body.KzgCommitmentsInclusionProof()
	if err != nil {
		return err
	}
	inclusionProof := solid.NewHashVector(cltypes.KzgCommitmentsInclusionProofDepth)
	for i := range inclusionProofRaw {
		inclusionProof.Set(i, inclusionProofRaw[i])
	}
	sidecars, err := peerdasutils.GetDataColumnSidecars(block.SignedBeaconBlockHeader(), commitments, inclusionProof, cellsAndProofs)
	if err != nil {
		return err
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}

	archive := d.IsArchivedMode()
	custody, err := d.state.GetMyCustodyColumns()
	if err != nil {
		return err
	}
	for _, sidecar := range sidecars {
		if !archive && len(custody) > 0 && !custody[sidecar.Index] {
			continue
		}
		if err := d.columnStorage.WriteColumnSidecars(ctx, blockRoot, int64(sidecar.Index), sidecar); err != nil {
			return err
		}
	}
	if archive {
		return d.TryScheduleRecover(block.Block.Slot, blockRoot)
	}
	return nil
}
