package das

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

type DataColumnsByRootIdentifier struct {
	BlockRoot common.Hash
	Columns   []cltypes.ColumnIndex
}

// VerifyDataColumnSidecar verifies if the data column sidecar is valid according to protocol rules.
// This function is re-entrant and thread-safe.
func VerifyDataColumnSidecar(sidecar *cltypes.DataColumnSidecar) bool {
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
func VerifyDataColumnSidecarKZGProofs(sidecar *cltypes.DataColumnSidecar) bool {
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

// ComputeSubnetForDataColumnSidecar computes the subnet ID for a given data column sidecar index.
// This function is re-entrant and thread-safe.
func ComputeSubnetForDataColumnSidecar(columnIndex ColumnIndex) uint64 {
	return columnIndex % clparams.GetBeaconConfig().DataColumnSidecarSubnetCount
}
