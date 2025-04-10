package das

import (
	"github.com/erigontech/erigon/cl/clparams"
)

/*
type DataColumnIdentifier struct {
	BlockRoot common.Hash
	Index     ColumnIndex
}
*/

// VerifyDataColumnSidecar verifies if the data column sidecar is valid according to protocol rules.
// This function is re-entrant and thread-safe.
func VerifyDataColumnSidecar(sidecar *DataColumnSidecar) bool {
	// The sidecar index must be within the valid range
	if sidecar.Index >= clparams.GetBeaconConfig().NumberOfColumns {
		return false
	}

	// A sidecar for zero blobs is invalid
	if sidecar.KzgCommitments.Len() == 0 {
		return false
	}

	// The commitments and proofs lengths must match
	if sidecar.KzgCommitments.Len() != sidecar.KzgProofs.Len() {
		return false
	}

	return true
}

// VerifyDataColumnSidecarKZGProofs verifies if the KZG proofs in the sidecar are correct.
// This function is re-entrant and thread-safe.
func VerifyDataColumnSidecarKZGProofs(sidecar *DataColumnSidecar) bool {
	// The column index represents the cell index for each proof
	cellIndices := make([]uint64, sidecar.Column.Len())
	for i := range cellIndices {
		cellIndices[i] = uint64(sidecar.Index)
	}

	// Batch verify that the cells match the corresponding commitments and proofs
	return VerifyCellKZGProofBatch( // in kzg pkg
		sidecar.KzgCommitments,
		cellIndices,
		sidecar.Column,
		sidecar.KzgProofs,
	)
}
