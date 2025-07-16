package das

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
	ckzg "github.com/ethereum/c-kzg-4844/v2/bindings/go"
)

const (
	// get_generalized_index(BeaconBlockBody, 'blob_kzg_commitments') = 27
	BlobKzgCommitmentsGeneralizedIndex = 27
	// get_subtree_index(get_generalized_index(BeaconBlockBody, 'blob_kzg_commitments')) = 11
	BlobKzgCommitmentsSubtreeIndex = 11

	// floorlog2(get_generalized_index(BeaconBlockBody, 'blob_kzg_commitments')) = 4
	KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH = 4
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
		cellIndices[i] = sidecar.Index
	}

	ckzgCommitments := make([]ckzg.Bytes48, sidecar.KzgCommitments.Len())
	for i := range ckzgCommitments {
		copy(ckzgCommitments[i][:], sidecar.KzgCommitments.Get(i)[:])
	}

	ckzgCells := make([]ckzg.Cell, sidecar.Column.Len())
	for i := range ckzgCells {
		cell := sidecar.Column.Get(i)
		copy(ckzgCells[i][:], cell[:])
	}

	ckzgProofs := make([]ckzg.Bytes48, sidecar.KzgProofs.Len())
	for i := range ckzgProofs {
		copy(ckzgProofs[i][:], sidecar.KzgProofs.Get(i)[:])
	}

	ok, err := ckzg.VerifyCellKZGProofBatch(ckzgCommitments, cellIndices, ckzgCells, ckzgProofs)
	if err != nil {
		log.Warn("failed to verify cell kzg proofs", "error", err)
		return false
	}
	return ok
}

func ComputeCells(blobs *cltypes.Blob) ([]cltypes.Cell, error) {
	cells, err := ckzg.ComputeCells((*ckzg.Blob)(blobs))
	if err != nil {
		return nil, err
	}
	ret := make([]cltypes.Cell, len(cells))
	for i, cell := range &cells {
		ret[i] = cltypes.Cell(cell)
	}
	return ret, nil
}

// ComputeSubnetForDataColumnSidecar computes the subnet ID for a given data column sidecar index.
// This function is re-entrant and thread-safe.
func ComputeSubnetForDataColumnSidecar(columnIndex cltypes.ColumnIndex) uint64 {
	return columnIndex % clparams.GetBeaconConfig().DataColumnSidecarSubnetCount
}

// VerifyDataColumnSidecarInclusionProof verifies if the inclusion proof in the sidecar is correct.
// This function is re-entrant and thread-safe.
func VerifyDataColumnSidecarInclusionProof(sidecar *cltypes.DataColumnSidecar) bool {
	// Convert branch to hashes for merkle proof verification
	branch := make([]common.Hash, sidecar.KzgCommitmentsInclusionProof.Length())
	for i := range branch {
		branch[i] = sidecar.KzgCommitmentsInclusionProof.Get(i)
	}

	hashRoot, err := sidecar.KzgCommitments.HashSSZ()
	if err != nil {
		return false
	}
	// Verify the merkle branch
	return utils.IsValidMerkleBranch(
		hashRoot,
		branch,
		KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH,
		BlobKzgCommitmentsSubtreeIndex,
		sidecar.SignedBlockHeader.Header.BodyRoot,
	)
}
