package peerdasutils

import (
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

const (
	CELLS_PER_EXT_BLOB = 128
)

type CellsAndKZGProofs = cltypes.CellsAndKZGProofs

// GetCellsAndKZGProofsFromBlobsBundle extracts cells and KZG proofs from a blobs bundle
func GetCellsAndKZGProofsFromBlobsBundle(blobsBundle *engine_types.BlobsBundle) ([]CellsAndKZGProofs, error) {
	cellsAndKZGProofs := make([]CellsAndKZGProofs, 0)
	for i, blob := range blobsBundle.Blobs {
		cells, proofs, err := ComputeCellsAndKZGProofs(blob)
		if err != nil {
			return nil, fmt.Errorf("failed to compute cells and proofs for blob %d: %w", i, err)
		}
		cellsAndKZGProofs = append(cellsAndKZGProofs, CellsAndKZGProofs{
			Blobs:  cells,
			Proofs: proofs,
		})
	}
	return cellsAndKZGProofs, nil
}

// GetDataColumnSidecars assembles sidecars that can be distributed to peers given a signed block header
// and the commitments, inclusion proof, cells/proofs associated with each blob in the block.
func GetDataColumnSidecars(
	signedBlockHeader *cltypes.SignedBeaconBlockHeader,
	kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment],
	kzgCommitmentsInclusionProof solid.HashVectorSSZ,
	cellsAndKZGProofs []CellsAndKZGProofs,
) ([]*cltypes.DataColumnSidecar, error) {

	if len(cellsAndKZGProofs) != kzgCommitments.Len() {
		return nil, fmt.Errorf("number of cells/proofs entries (%d) does not match number of KZG commitments (%d)", len(cellsAndKZGProofs), kzgCommitments.Len())
	}

	cfg := clparams.GetBeaconConfig()
	sidecars := make([]*cltypes.DataColumnSidecar, cfg.NumberOfColumns)

	// Initialize sidecars for each column
	for columnIndex := uint64(0); columnIndex < cfg.NumberOfColumns; columnIndex++ {
		columnCells := solid.NewStaticListSSZ[*cltypes.Cell](int(cfg.MaxBlobCommittmentsPerBlock), cltypes.BytesPerCell)
		columnProofs := solid.NewStaticListSSZ[*cltypes.KZGProof](int(cfg.MaxBlobCommittmentsPerBlock), 48)

		// For each blob, extract the cell and proof for this column
		for blobIndex := range cellsAndKZGProofs {
			cell := &cltypes.Cell{}
			copy(cell[:], cellsAndKZGProofs[blobIndex].Blobs[columnIndex][:])
			columnCells.Append(cell)

			proof := &cltypes.KZGProof{}
			copy(proof[:], cellsAndKZGProofs[blobIndex].Proofs[columnIndex][:])
			columnProofs.Append(proof)
		}

		sidecars[columnIndex] = &cltypes.DataColumnSidecar{
			Index:                        columnIndex,
			Column:                       columnCells,
			KzgCommitments:               kzgCommitments,
			KzgProofs:                    columnProofs,
			SignedBlockHeader:            signedBlockHeader,
			KzgCommitmentsInclusionProof: kzgCommitmentsInclusionProof,
		}
	}

	return sidecars, nil
}

// [New in Gloas:EIP7732] GetDataColumnSidecarsGloas assembles GLOAS-style sidecars with Slot and BeaconBlockRoot
// instead of SignedBlockHeader, KzgCommitments, and KzgCommitmentsInclusionProof.
// Note: In GLOAS, kzg_commitments are no longer stored in the sidecar structure.
func GetDataColumnSidecarsGloas(
	slot uint64,
	beaconBlockRoot common.Hash,
	cellsAndKZGProofs []CellsAndKZGProofs,
) ([]*cltypes.DataColumnSidecar, error) {
	cfg := clparams.GetBeaconConfig()
	sidecars := make([]*cltypes.DataColumnSidecar, cfg.NumberOfColumns)

	// Initialize sidecars for each column
	for columnIndex := uint64(0); columnIndex < cfg.NumberOfColumns; columnIndex++ {
		columnCells := solid.NewStaticListSSZ[*cltypes.Cell](int(cfg.MaxBlobCommittmentsPerBlock), cltypes.BytesPerCell)
		columnProofs := solid.NewStaticListSSZ[*cltypes.KZGProof](int(cfg.MaxBlobCommittmentsPerBlock), 48)

		// For each blob, extract the cell and proof for this column
		for blobIndex := range cellsAndKZGProofs {
			cell := &cltypes.Cell{}
			copy(cell[:], cellsAndKZGProofs[blobIndex].Blobs[columnIndex][:])
			columnCells.Append(cell)

			proof := &cltypes.KZGProof{}
			copy(proof[:], cellsAndKZGProofs[blobIndex].Proofs[columnIndex][:])
			columnProofs.Append(proof)
		}

		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		sidecar.Index = columnIndex
		sidecar.Column = columnCells
		sidecar.KzgProofs = columnProofs
		sidecar.Slot = slot
		sidecar.BeaconBlockRoot = beaconBlockRoot
		sidecars[columnIndex] = sidecar
	}

	return sidecars, nil
}

