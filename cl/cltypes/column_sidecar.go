package cltypes

import (
	"encoding/json"
	"reflect"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

const (
	KzgCommitmentsInclusionProofDepth = 4
	BytesPerCell                      = 2048
	BytesPerBlob                      = BytesPerCell * 64
)

var (
	_ ssz2.SizedObjectSSZ = (*DataColumnSidecar)(nil)
	_ ssz2.SizedObjectSSZ = (*ColumnSidecarsByRangeRequest)(nil)

	_ ssz2.SizedObjectSSZ        = (*DataColumnsByRootIdentifier)(nil)
	_ solid.EncodableHashableSSZ = (*DataColumnsByRootIdentifier)(nil)

	_ ssz2.SizedObjectSSZ = (*MatrixEntry)(nil)

	_ ssz2.SizedObjectSSZ = (*Cell)(nil)
)

type DataColumnSidecar struct {
	Index                        uint64                         `json:"index"` // index of the column
	Column                       *solid.ListSSZ[*Cell]          `json:"column"`
	KzgCommitments               *solid.ListSSZ[*KZGCommitment] `json:"kzg_commitments"`
	KzgProofs                    *solid.ListSSZ[*KZGProof]      `json:"kzg_proofs"`
	SignedBlockHeader            *SignedBeaconBlockHeader       `json:"signed_block_header"`
	KzgCommitmentsInclusionProof solid.HashVectorSSZ            `json:"kzg_commitments_inclusion_proof"`
}

func NewDataColumnSidecar() *DataColumnSidecar {
	d := &DataColumnSidecar{}
	d.tryInit()
	return d
}

func (d *DataColumnSidecar) Clone() clonable.Clonable {
	newSidecar := &DataColumnSidecar{}
	newSidecar.tryInit()
	return newSidecar
}

func (d *DataColumnSidecar) tryInit() {
	cfg := clparams.GetBeaconConfig()
	if d.Column == nil {
		d.Column = solid.NewStaticListSSZ[*Cell](int(cfg.MaxBlobCommittmentsPerBlock), BytesPerCell)
	}
	if d.KzgCommitments == nil {
		d.KzgCommitments = solid.NewStaticListSSZ[*KZGCommitment](int(cfg.MaxBlobCommittmentsPerBlock), 48)
	}
	if d.KzgProofs == nil {
		d.KzgProofs = solid.NewStaticListSSZ[*KZGProof](int(cfg.MaxBlobCommittmentsPerBlock), 48)
	}
	if d.SignedBlockHeader == nil {
		d.SignedBlockHeader = &SignedBeaconBlockHeader{}
		d.SignedBlockHeader.Header = &BeaconBlockHeader{}
	}
	if d.KzgCommitmentsInclusionProof == nil {
		d.KzgCommitmentsInclusionProof = solid.NewHashVector(KzgCommitmentsInclusionProofDepth)
	}
}

func (d *DataColumnSidecar) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, d.getSchema()...)
}

func (d *DataColumnSidecar) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, d.getSchema()...)
}

func (d *DataColumnSidecar) getSchema() []interface{} {
	d.tryInit()
	return []interface{}{&d.Index, d.Column, d.KzgCommitments, d.KzgProofs, d.SignedBlockHeader, d.KzgCommitmentsInclusionProof}
}

func (d *DataColumnSidecar) EncodingSizeSSZ() int {
	d.tryInit()
	return 8 + d.Column.EncodingSizeSSZ() + d.KzgCommitments.EncodingSizeSSZ() + d.KzgProofs.EncodingSizeSSZ() +
		d.SignedBlockHeader.EncodingSizeSSZ() + d.KzgCommitmentsInclusionProof.EncodingSizeSSZ()
}

func (d *DataColumnSidecar) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(d.getSchema()...)
}

func (d *DataColumnSidecar) Static() bool {
	return false
}

type Cell [BytesPerCell]byte

func (c *Cell) Clone() clonable.Clonable {
	return &Cell{}
}

func (c *Cell) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, c[:])
}

func (c *Cell) EncodingSizeSSZ() int {
	return BytesPerCell
}

func (c *Cell) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, c[:]...), nil
}

func (c *Cell) HashSSZ() ([32]byte, error) {
	return merkle_tree.BytesRoot(c[:])
}

func (c *Cell) Static() bool {
	return true
}

func (c *Cell) MarshalJSON() ([]byte, error) {
	return json.Marshal(hexutil.Bytes(c[:]))
}

var cellType = reflect.TypeOf(Cell{})

func (c *Cell) UnmarshalJSON(in []byte) error {
	return hexutil.UnmarshalFixedJSON(cellType, in, c[:])
}

// CellsAndKZGProofs is a struct that contains a list of cells and a list of KZG proofs
type CellsAndKZGProofs struct {
	Blobs  []Cell
	Proofs []KZGProof
}

type MatrixEntry struct {
	Cell        Cell        `json:"cell"`
	KzgProof    KZGProof    `json:"kzg_proof"`
	ColumnIndex ColumnIndex `json:"column_index"`
	RowIndex    RowIndex    `json:"row_index"`
}

func (m *MatrixEntry) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, m.Cell[:], m.KzgProof[:], m.ColumnIndex, m.RowIndex)
}

func (m *MatrixEntry) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, m.Cell[:], m.KzgProof[:], &m.ColumnIndex, &m.RowIndex)
}

func (m *MatrixEntry) EncodingSizeSSZ() int {
	return 2048 + 48 + 8 + 8
}

func (m *MatrixEntry) Clone() clonable.Clonable {
	return &MatrixEntry{}
}

func (m *MatrixEntry) Static() bool {
	return false
}

func (m *MatrixEntry) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(m.Cell[:], m.KzgProof[:], m.ColumnIndex, m.RowIndex)
}

type (
	CustodyIndex = uint64
	ColumnIndex  = uint64
	RowIndex     = uint64
)

// ColumnSidecarsByRangeRequest is the request for getting a range of column sidecars.
type ColumnSidecarsByRangeRequest struct {
	/*
	  start_slot: Slot
	  count: uint64
	  columns: List[ColumnIndex, NUMBER_OF_COLUMNS]
	*/
	StartSlot uint64
	Count     uint64
	Columns   solid.Uint64ListSSZ
}

func (c *ColumnSidecarsByRangeRequest) tryInit() {
	if c.Columns == nil {
		c.Columns = solid.NewUint64ListSSZ(int(clparams.GetBeaconConfig().NumberOfColumns))
	}
}

func (c *ColumnSidecarsByRangeRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	c.tryInit()
	return ssz2.MarshalSSZ(buf, &c.StartSlot, &c.Count, c.Columns)
}

func (c *ColumnSidecarsByRangeRequest) DecodeSSZ(buf []byte, _ int) error {
	c.tryInit()
	return ssz2.UnmarshalSSZ(buf, 0, &c.StartSlot, &c.Count, c.Columns)
}

func (c *ColumnSidecarsByRangeRequest) EncodingSizeSSZ() int {
	c.tryInit()
	return 16 + c.Columns.EncodingSizeSSZ()
}

func (*ColumnSidecarsByRangeRequest) Clone() clonable.Clonable {
	return &ColumnSidecarsByRangeRequest{}
}

func (c *ColumnSidecarsByRangeRequest) Static() bool {
	return false
}

// DataColumnsByRootIdentifier is the request for getting a range of column sidecars by root identifier.
type DataColumnsByRootIdentifier struct {
	BlockRoot common.Hash
	Columns   solid.Uint64ListSSZ
}

func NewDataColumnsByRootIdentifier() *DataColumnsByRootIdentifier {
	d := &DataColumnsByRootIdentifier{}
	d.tryInit()
	return d
}

func (d *DataColumnsByRootIdentifier) tryInit() {
	if d.Columns == nil {
		d.Columns = solid.NewUint64ListSSZ(int(clparams.GetBeaconConfig().NumberOfColumns))
	}
}

func (d *DataColumnsByRootIdentifier) EncodeSSZ(buf []byte) ([]byte, error) {
	d.tryInit()
	return ssz2.MarshalSSZ(buf, d.BlockRoot[:], d.Columns)
}

func (d *DataColumnsByRootIdentifier) DecodeSSZ(buf []byte, _ int) error {
	d.tryInit()
	return ssz2.UnmarshalSSZ(buf, 0, d.BlockRoot[:], d.Columns)
}

func (d *DataColumnsByRootIdentifier) EncodingSizeSSZ() int {
	d.tryInit()
	// TODO: check the additional 4 bytes for the length of the columns
	return 32 + 4 + d.Columns.EncodingSizeSSZ()
}

func (*DataColumnsByRootIdentifier) Clone() clonable.Clonable {
	return &DataColumnsByRootIdentifier{}
}

func (d *DataColumnsByRootIdentifier) Static() bool {
	return false
}

func (d *DataColumnsByRootIdentifier) HashSSZ() ([32]byte, error) {
	d.tryInit()
	return merkle_tree.HashTreeRoot(d.BlockRoot[:], d.Columns)
}
