package cltypes

import "github.com/erigontech/erigon/cl/cltypes/solid"

type DataColumnSidecar struct {
	Index                        uint64                         `json:"index"`
	Column                       *solid.ListSSZ[Cell]           `json:"column"`
	KzgCommitments               *solid.ListSSZ[*KZGCommitment] `json:"kzg_commitments"`
	KzgProofs                    *solid.ListSSZ[*KZGProof]      `json:"kzg_proofs"`
	SignedBlockHeader            *SignedBeaconBlockHeader       `json:"signed_block_header"`
	KzgCommitmentsInclusionProof solid.HashVectorSSZ            `json:"kzg_commitments_inclusion_proof"`
}

type MatrixEntry struct {
	Cell        Cell        `json:"cell"`
	KzgProof    *KZGProof   `json:"kzg_proof"`
	ColumnIndex ColumnIndex `json:"column_index"`
	RowIndex    RowIndex    `json:"row_index"`
}

type Cell []byte

type (
	CustodyIndex = uint64
	ColumnIndex  = uint64
	RowIndex     = uint64
)
