package cltypes

import (
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

var (
	_ ssz2.ObjectSSZ = (*DataColumnSidecar)(nil)
)

type DataColumnSidecar struct {
	Index                        uint64                         `json:"index"` // index of the column
	Column                       *solid.ListSSZ[Cell]           `json:"column"`
	KzgCommitments               *solid.ListSSZ[*KZGCommitment] `json:"kzg_commitments"`
	KzgProofs                    *solid.ListSSZ[*KZGProof]      `json:"kzg_proofs"`
	SignedBlockHeader            *SignedBeaconBlockHeader       `json:"signed_block_header"`
	KzgCommitmentsInclusionProof solid.HashVectorSSZ            `json:"kzg_commitments_inclusion_proof"`
}

func (d *DataColumnSidecar) Clone() clonable.Clonable {
	return &DataColumnSidecar{
		Index:             d.Index,
		Column:            d.Column.Clone().(*solid.ListSSZ[Cell]),
		KzgCommitments:    d.KzgCommitments.Clone().(*solid.ListSSZ[*KZGCommitment]),
		KzgProofs:         d.KzgProofs.Clone().(*solid.ListSSZ[*KZGProof]),
		SignedBlockHeader: d.SignedBlockHeader.Clone().(*SignedBeaconBlockHeader),
		//KzgCommitmentsInclusionProof: d.KzgCommitmentsInclusionProof.Clone().(*solid.HashVectorSSZ),
	}
}

func (d *DataColumnSidecar) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, d.getSchema()...)
}

func (d *DataColumnSidecar) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, d.getSchema()...)
}

func (d *DataColumnSidecar) getSchema() []interface{} {
	return []interface{}{d.Index, d.Column, d.KzgCommitments, d.KzgProofs, d.SignedBlockHeader, d.KzgCommitmentsInclusionProof}
}

func (d *DataColumnSidecar) EncodingSizeSSZ() int {
	return 8 + d.Column.EncodingSizeSSZ() + d.KzgCommitments.EncodingSizeSSZ() + d.KzgProofs.EncodingSizeSSZ() +
		d.SignedBlockHeader.EncodingSizeSSZ() + d.KzgCommitmentsInclusionProof.EncodingSizeSSZ()
}

func (d *DataColumnSidecar) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(d.getSchema()...)
}

type Cell []byte

func (c Cell) Clone() clonable.Clonable {
	return &c
}

func (c Cell) DecodeSSZ(buf []byte, version int) error {
	//return ssz2.UnmarshalSSZ(buf, version, c.getSchema()...)
	// copy the buf to the cell
	copy(c, buf)
	return nil
}

func (c Cell) EncodingSizeSSZ() int {
	return len(c)
}

func (c Cell) EncodeSSZ(buf []byte) ([]byte, error) {
	//return ssz2.MarshalSSZ(buf, c.getSchema()...)
	// copy the cell to the buf
	copy(buf, c)
	return buf, nil
}

func (c Cell) getSchema() []interface{} {
	return []interface{}{c}
}

func (c Cell) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(c.getSchema()...)
}

type MatrixEntry struct {
	Cell        Cell        `json:"cell"`
	KzgProof    *KZGProof   `json:"kzg_proof"`
	ColumnIndex ColumnIndex `json:"column_index"`
	RowIndex    RowIndex    `json:"row_index"`
}

type (
	CustodyIndex = uint64
	ColumnIndex  = uint64
	RowIndex     = uint64
)
