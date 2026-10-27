package das

import (
	"context"
	"fmt"

	goethkzg "github.com/crate-crypto/go-eth-kzg"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/common/crypto/kzg"
)

// ImportColumnsFromBlobs builds the data column sidecars of a Fulu block from an
// engine_getBlobsV2 response and stores the columns this node keeps. The response
// must hold every blob of the block, each with one proof per cell, and every cell
// must verify against the block's commitments; otherwise nothing is stored.
func (d *peerdas) ImportColumnsFromBlobs(ctx context.Context, block *cltypes.SignedBeaconBlock, blobs [][]byte, cellProofs [][][]byte) error {
	commitments := block.GetBlobKzgCommitments()
	if len(blobs) != commitments.Len() || len(cellProofs) != commitments.Len() {
		return fmt.Errorf("incomplete blobs: got %d blobs and %d proof sets for %d commitments", len(blobs), len(cellProofs), commitments.Len())
	}
	numberOfColumns := int(d.beaconConfig.NumberOfColumns)
	cellsAndProofs := make([]peerdasutils.CellsAndKZGProofs, len(blobs))
	kzgCommitments := make([]goethkzg.KZGCommitment, 0, len(blobs)*numberOfColumns)
	cellIndices := make([]uint64, 0, len(blobs)*numberOfColumns)
	cells := make([]*goethkzg.Cell, 0, len(blobs)*numberOfColumns)
	proofs := make([]goethkzg.KZGProof, 0, len(blobs)*numberOfColumns)
	for i, blob := range blobs {
		if len(blob) != cltypes.BYTES_PER_BLOB || len(cellProofs[i]) != numberOfColumns {
			return fmt.Errorf("blob %d: got %d bytes and %d proofs", i, len(blob), len(cellProofs[i]))
		}
		blobCells, err := ComputeCells((*cltypes.Blob)(blob))
		if err != nil {
			return fmt.Errorf("blob %d: %w", i, err)
		}
		cellsAndProofs[i] = peerdasutils.CellsAndKZGProofs{Blobs: blobCells, Proofs: make([]cltypes.KZGProof, numberOfColumns)}
		for j := range blobCells {
			if len(cellProofs[i][j]) != len(cltypes.KZGProof{}) {
				return fmt.Errorf("blob %d: proof %d has %d bytes", i, j, len(cellProofs[i][j]))
			}
			copy(cellsAndProofs[i].Proofs[j][:], cellProofs[i][j])
			kzgCommitments = append(kzgCommitments, goethkzg.KZGCommitment(*commitments.Get(i)))
			cellIndices = append(cellIndices, uint64(j))
			cells = append(cells, (*goethkzg.Cell)(&blobCells[j]))
			proofs = append(proofs, goethkzg.KZGProof(cellsAndProofs[i].Proofs[j]))
		}
	}
	if err := kzg.Ctx().VerifyCellKZGProofBatch(kzgCommitments, cellIndices, cells, proofs); err != nil {
		return fmt.Errorf("verify cell proofs: %w", err)
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
