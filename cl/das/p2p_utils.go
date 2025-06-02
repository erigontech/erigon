package das

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ckzg "github.com/ethereum/c-kzg-4844/v2/bindings/go"
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
	ok, err := VerifyCellKZGProofBatch( // in kzg pkg
		sidecar.KzgCommitments,
		cellIndices,
		sidecar.Column,
		sidecar.KzgProofs,
	)
	if err != nil {
		log.Warn("failed to verify cell kzg proofs", "error", err)
		return false
	}
	return ok
}

// ComputeSubnetForDataColumnSidecar computes the subnet ID for a given data column sidecar index.
// This function is re-entrant and thread-safe.
func ComputeSubnetForDataColumnSidecar(columnIndex ColumnIndex) uint64 {
	return columnIndex % clparams.GetBeaconConfig().DataColumnSidecarSubnetCount
}

func VerifyCellKZGProofBatch(commitments *solid.ListSSZ[*cltypes.KZGCommitment], cellIndices []uint64, cells *solid.ListSSZ[cltypes.Cell], proofs *solid.ListSSZ[*cltypes.KZGProof]) (bool, error) {
	ckzgCommitments := make([]ckzg.Bytes48, commitments.Len())
	for i := range ckzgCommitments {
		copy(ckzgCommitments[i][:], commitments.Get(i)[:])
	}

	ckzgCells := make([]ckzg.Cell, cells.Len())
	for i := range ckzgCells {
		ckzgCells[i] = ckzg.Cell(cells.Get(i))
	}

	ckzgProofs := make([]ckzg.Bytes48, proofs.Len())
	for i := range ckzgProofs {
		copy(ckzgProofs[i][:], proofs.Get(i)[:])
	}

	return ckzg.VerifyCellKZGProofBatch(ckzgCommitments, cellIndices, ckzgCells, ckzgProofs)
}
