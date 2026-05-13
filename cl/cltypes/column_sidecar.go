package cltypes

import (
	"encoding/json"
	"reflect"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
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
	BlockRoot                    common.Hash                    `json:"-"`
	Index                        uint64                         `json:"index,string"` // index of the column
	Column                       *solid.ListSSZ[*Cell]          `json:"column"`
	KzgProofs                    *solid.ListSSZ[*KZGProof]      `json:"kzg_proofs"`
	Slot                         uint64                         `json:"slot"`                                      // [New in Gloas:EIP7732]
	BeaconBlockRoot              common.Hash                    `json:"beacon_block_root"`                         // [New in Gloas:EIP7732]
	KzgCommitments               *solid.ListSSZ[*KZGCommitment] `json:"kzg_commitments,omitempty"`                 // [Removed in Gloas:EIP7732]
	SignedBlockHeader            *SignedBeaconBlockHeader       `json:"signed_block_header,omitempty"`             // [Removed in Gloas:EIP7732]
	KzgCommitmentsInclusionProof solid.HashVectorSSZ            `json:"kzg_commitments_inclusion_proof,omitempty"` // [Removed in Gloas:EIP7732]

	version clparams.StateVersion // internal: tracks the version for encoding
}

func NewDataColumnSidecar() *DataColumnSidecar {
	d := &DataColumnSidecar{}
	d.tryInit()
	return d
}

// NewDataColumnSidecarWithVersion creates a new DataColumnSidecar with a specific version.
// Use this when you need version-aware encoding.
func NewDataColumnSidecarWithVersion(version clparams.StateVersion) *DataColumnSidecar {
	d := &DataColumnSidecar{version: version}
	d.tryInitWithVersion(version)
	return d
}

// Version returns the version used for encoding.
func (d *DataColumnSidecar) Version() clparams.StateVersion {
	return d.version
}

func (d *DataColumnSidecar) Clone() clonable.Clonable {
	newSidecar := &DataColumnSidecar{
		BlockRoot:       d.BlockRoot,
		Slot:            d.Slot,
		BeaconBlockRoot: d.BeaconBlockRoot,
		version:         d.version,
	}
	newSidecar.tryInitWithVersion(d.version)
	return newSidecar
}

func (d *DataColumnSidecar) tryInit() {
	d.tryInitWithVersion(d.version)
}

func (d *DataColumnSidecar) tryInitWithVersion(version clparams.StateVersion) {
	cfg := clparams.GetBeaconConfig()
	if d.Column == nil {
		d.Column = solid.NewStaticListSSZ[*Cell](int(cfg.MaxBlobCommittmentsPerBlock), BytesPerCell)
	}
	if d.KzgProofs == nil {
		d.KzgProofs = solid.NewStaticListSSZ[*KZGProof](int(cfg.MaxBlobCommittmentsPerBlock), 48)
	}
	// Pre-Gloas fields (Fulu and earlier)
	if version < clparams.GloasVersion {
		if d.KzgCommitments == nil {
			d.KzgCommitments = solid.NewStaticListSSZ[*KZGCommitment](int(cfg.MaxBlobCommittmentsPerBlock), 48)
		}
		if d.SignedBlockHeader == nil {
			d.SignedBlockHeader = &SignedBeaconBlockHeader{}
			d.SignedBlockHeader.Header = &BeaconBlockHeader{}
		}
		if d.KzgCommitmentsInclusionProof == nil {
			d.KzgCommitmentsInclusionProof = solid.NewHashVector(KzgCommitmentsInclusionProofDepth)
		}
	}
}

func (d *DataColumnSidecar) DecodeSSZ(buf []byte, version int) error {
	d.version = clparams.StateVersion(version)
	d.tryInitWithVersion(d.version)
	return ssz2.UnmarshalSSZ(buf, version, d.getSchemaForVersion(d.version)...)
}

func (d *DataColumnSidecar) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, d.getSchemaForVersion(d.version)...)
}

// getSchemaForVersion returns the SSZ schema for the given version.
// Fulu and earlier: Index, Column, KzgCommitments, KzgProofs, SignedBlockHeader, KzgCommitmentsInclusionProof
// Gloas and later: Index, Column, KzgProofs, Slot, BeaconBlockRoot
func (d *DataColumnSidecar) getSchemaForVersion(version clparams.StateVersion) []any {
	d.tryInitWithVersion(version)
	if version >= clparams.GloasVersion {
		return []any{&d.Index, d.Column, d.KzgProofs, &d.Slot, d.BeaconBlockRoot[:]}
	}
	// Fulu and earlier
	return []any{&d.Index, d.Column, d.KzgCommitments, d.KzgProofs, d.SignedBlockHeader, d.KzgCommitmentsInclusionProof}
}

// getSchema returns the SSZ schema using the stored version.
func (d *DataColumnSidecar) getSchema() []any {
	return d.getSchemaForVersion(d.version)
}

func (d *DataColumnSidecar) EncodingSizeSSZ() int {
	return d.EncodingSizeSSZForVersion(d.version)
}

// EncodingSizeSSZForVersion returns the encoding size for the given version.
func (d *DataColumnSidecar) EncodingSizeSSZForVersion(version clparams.StateVersion) int {
	d.tryInitWithVersion(version)
	if version >= clparams.GloasVersion {
		// Index (8) + Column (variable) + KzgProofs (variable) + Slot (8) + BeaconBlockRoot (32)
		return 8 + d.Column.EncodingSizeSSZ() + d.KzgProofs.EncodingSizeSSZ() + 8 + 32
	}
	// Fulu and earlier
	return 8 + d.Column.EncodingSizeSSZ() + d.KzgCommitments.EncodingSizeSSZ() + d.KzgProofs.EncodingSizeSSZ() +
		d.SignedBlockHeader.EncodingSizeSSZ() + d.KzgCommitmentsInclusionProof.EncodingSizeSSZ()
}

func (d *DataColumnSidecar) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(d.getSchemaForVersion(d.version)...)
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

var cellType = reflect.TypeFor[Cell]()

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
