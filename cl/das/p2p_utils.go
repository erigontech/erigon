package das

import (
	goethkzg "github.com/crate-crypto/go-eth-kzg"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
)

const (
	// get_generalized_index(BeaconBlockBody, 'blob_kzg_commitments') = 27
	BlobKzgCommitmentsGeneralizedIndex = 27
	// get_subtree_index(get_generalized_index(BeaconBlockBody, 'blob_kzg_commitments')) = 11
	BlobKzgCommitmentsSubtreeIndex = 11

	// floorlog2(get_generalized_index(BeaconBlockBody, 'blob_kzg_commitments')) = 4
	KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH = 4
)

// VerifyDataColumnSidecar verifies if the data column sidecar is valid according to protocol rules.
// This function is re-entrant and thread-safe.
// For Fulu: uses KzgCommitments from the sidecar.
// For GLOAS: use VerifyDataColumnSidecarWithCommitments instead.
func VerifyDataColumnSidecar(sidecar *cltypes.DataColumnSidecar) bool {
	// The sidecar index must be within the valid range
	if sidecar.Index >= clparams.GetBeaconConfig().NumberOfColumns {
		return false
	}

	// Column and KzgProofs must exist and have matching lengths
	if sidecar.Column == nil || sidecar.KzgProofs == nil {
		return false
	}
	if sidecar.Column.Len() == 0 {
		return false
	}
	if sidecar.Column.Len() != sidecar.KzgProofs.Len() {
		return false
	}

	// For Fulu (pre-GLOAS): verify KzgCommitments matches
	if sidecar.Version() < clparams.GloasVersion {
		if sidecar.KzgCommitments == nil || sidecar.KzgCommitments.Len() == 0 {
			return false
		}
		if sidecar.KzgCommitments.Len() != sidecar.Column.Len() {
			return false
		}
	}

	return true
}

// VerifyDataColumnSidecarWithCommitments verifies if the data column sidecar is valid according to GLOAS protocol rules.
// [Modified in Gloas:EIP7732] kzg_commitments is now passed as a parameter.
// This function is re-entrant and thread-safe.
func VerifyDataColumnSidecarWithCommitments(sidecar *cltypes.DataColumnSidecar, kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) bool {
	// The sidecar index must be within the valid range
	if sidecar.Index >= clparams.GetBeaconConfig().NumberOfColumns {
		return false
	}

	// Column must exist and not be empty
	if sidecar.Column == nil || sidecar.Column.Len() == 0 {
		return false
	}

	// KzgProofs must exist
	if sidecar.KzgProofs == nil {
		return false
	}

	// [Modified in Gloas:EIP7732] len(sidecar.column) != len(kzg_commitments) or len(sidecar.column) != len(sidecar.kzg_proofs)
	if kzgCommitments == nil {
		return false
	}
	if sidecar.Column.Len() != kzgCommitments.Len() || sidecar.Column.Len() != sidecar.KzgProofs.Len() {
		return false
	}

	return true
}

// VerifyDataColumnSidecarKZGProofs verifies if the KZG proofs in the sidecar are correct.
// This function is re-entrant and thread-safe.
// For Fulu: uses KzgCommitments from the sidecar.
// For GLOAS: use VerifyDataColumnSidecarKZGProofsWithCommitments instead.
func VerifyDataColumnSidecarKZGProofs(sidecar *cltypes.DataColumnSidecar) bool {
	// For GLOAS: KzgCommitments are not in the sidecar, use VerifyDataColumnSidecarKZGProofsWithCommitments
	if sidecar.Version() >= clparams.GloasVersion {
		// GLOAS sidecars don't have KzgCommitments in the sidecar itself
		// Caller should use VerifyDataColumnSidecarKZGProofsWithCommitments with external commitments
		return true // Skip for now, caller must use the WithCommitments variant
	}

	// Fulu verification
	if sidecar.KzgCommitments == nil || sidecar.KzgCommitments.Len() == 0 {
		return false
	}

	return VerifyDataColumnSidecarsKZGProofsWithCommitments([]*cltypes.DataColumnSidecar{sidecar}, sidecar.KzgCommitments)
}

// VerifyDataColumnSidecarKZGProofsWithCommitments verifies if the KZG proofs in the sidecar are correct.
// [Modified in Gloas:EIP7732] kzg_commitments is now passed as a parameter.
// This function is re-entrant and thread-safe.
func VerifyDataColumnSidecarKZGProofsWithCommitments(sidecar *cltypes.DataColumnSidecar, kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) bool {
	return VerifyDataColumnSidecarsKZGProofsWithCommitments([]*cltypes.DataColumnSidecar{sidecar}, kzgCommitments)
}

// VerifyDataColumnSidecarsKZGProofsWithCommitments verifies several columns in one KZG batch.
func VerifyDataColumnSidecarsKZGProofsWithCommitments(sidecars []*cltypes.DataColumnSidecar, kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) bool {
	if len(sidecars) == 0 || kzgCommitments == nil || kzgCommitments.Len() == 0 ||
		len(sidecars) > int(^uint(0)>>1)/kzgCommitments.Len() {
		return false
	}
	proofCount := len(sidecars) * kzgCommitments.Len()
	ckzgCommitments := make([]goethkzg.KZGCommitment, 0, proofCount)
	cellIndices := make([]uint64, 0, proofCount)
	ckzgCells := make([]*goethkzg.Cell, 0, proofCount)
	ckzgProofs := make([]goethkzg.KZGProof, 0, proofCount)
	for _, sidecar := range sidecars {
		if sidecar == nil || sidecar.Column == nil || sidecar.KzgProofs == nil ||
			sidecar.Column.Len() != kzgCommitments.Len() || sidecar.KzgProofs.Len() != kzgCommitments.Len() {
			return false
		}
		for i := range kzgCommitments.Len() {
			commitment := kzgCommitments.Get(i)
			proof := sidecar.KzgProofs.Get(i)
			cell := sidecar.Column.Get(i)
			if commitment == nil || proof == nil || cell == nil {
				return false
			}
			ckzgCommitments = append(ckzgCommitments, goethkzg.KZGCommitment(*commitment))
			cellIndices = append(cellIndices, sidecar.Index)
			ckzgCells = append(ckzgCells, (*goethkzg.Cell)(cell))
			ckzgProofs = append(ckzgProofs, goethkzg.KZGProof(*proof))
		}
	}
	err := kzg.Ctx().VerifyCellKZGProofBatch(ckzgCommitments, cellIndices, ckzgCells, ckzgProofs)
	if err != nil {
		log.Warn("failed to verify cell kzg proofs", "error", err)
		return false
	}
	return true
}

func ComputeCells(blobs *cltypes.Blob) ([]cltypes.Cell, error) {
	cells, err := kzg.Ctx().ComputeCells((*goethkzg.Blob)(blobs), 0 /* numGoRoutines */)
	if err != nil {
		return nil, err
	}
	ret := make([]cltypes.Cell, len(cells))
	for i, cell := range &cells {
		ret[i] = cltypes.Cell(*cell)
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
// Version-aware: handles both Fulu and GLOAS sidecars.
func VerifyDataColumnSidecarInclusionProof(sidecar *cltypes.DataColumnSidecar) bool {
	// GLOAS removes KzgCommitmentsInclusionProof from DataColumnSidecar
	// (consensus-specs v1.7.0-alpha.5). KZG commitments are verified against
	// the builder's bid instead, so no Merkle inclusion proof is needed here.
	if sidecar.Version() >= clparams.GloasVersion {
		return true
	}

	// Fulu verification
	if sidecar.KzgCommitmentsInclusionProof == nil || sidecar.KzgCommitments == nil || sidecar.SignedBlockHeader == nil {
		return false
	}

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
