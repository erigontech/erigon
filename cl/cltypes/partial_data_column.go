package cltypes

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
)

// Compile-time interface checks
var (
	_ ssz2.SizedObjectSSZ = (*PartialDataColumnHeader)(nil)
	_ ssz2.SizedObjectSSZ = (*PartialDataColumnSidecar)(nil)
	_ ssz2.SizedObjectSSZ = (*PartialDataColumnPartsMetadata)(nil)
)

// PartialDataColumnHeader is the header common to all columns for a given block.
// It enables peers to identify which blobs are included in a block, as well as
// validating cells and proofs.
//
// Fulu schema: kzg_commitments, signed_block_header, kzg_commitments_inclusion_proof
// Gloas schema: kzg_commitments, slot, beacon_block_root
type PartialDataColumnHeader struct {
	KzgCommitments               *solid.ListSSZ[*KZGCommitment] `json:"kzg_commitments"`
	SignedBlockHeader            *SignedBeaconBlockHeader       `json:"signed_block_header,omitempty"`             // [Removed in Gloas:EIP7732]
	KzgCommitmentsInclusionProof solid.HashVectorSSZ            `json:"kzg_commitments_inclusion_proof,omitempty"` // [Removed in Gloas:EIP7732]
	Slot                         uint64                         `json:"slot,omitempty"`                            // [New in Gloas:EIP7732]
	BeaconBlockRoot              common.Hash                    `json:"beacon_block_root,omitempty"`               // [New in Gloas:EIP7732]

	version clparams.StateVersion
}

func NewPartialDataColumnHeader(version clparams.StateVersion) *PartialDataColumnHeader {
	h := &PartialDataColumnHeader{version: version}
	h.init()
	return h
}

func (h *PartialDataColumnHeader) init() {
	cfg := clparams.GetBeaconConfig()
	if h.KzgCommitments == nil {
		h.KzgCommitments = solid.NewStaticListSSZ[*KZGCommitment](int(cfg.MaxBlobCommittmentsPerBlock), 48)
	}
	if h.version < clparams.GloasVersion {
		if h.SignedBlockHeader == nil {
			h.SignedBlockHeader = &SignedBeaconBlockHeader{Header: &BeaconBlockHeader{}}
		}
		if h.KzgCommitmentsInclusionProof == nil {
			h.KzgCommitmentsInclusionProof = solid.NewHashVector(KzgCommitmentsInclusionProofDepth)
		}
	}
}

func (h *PartialDataColumnHeader) Version() clparams.StateVersion { return h.version }

func (h *PartialDataColumnHeader) SetVersion(v clparams.StateVersion) {
	h.version = v
	h.init()
}

func (h *PartialDataColumnHeader) getSchema() []any {
	h.init()
	if h.version >= clparams.GloasVersion {
		return []any{h.KzgCommitments, &h.Slot, h.BeaconBlockRoot[:]}
	}
	return []any{h.KzgCommitments, h.SignedBlockHeader, h.KzgCommitmentsInclusionProof}
}

func (h *PartialDataColumnHeader) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, h.getSchema()...)
}

func (h *PartialDataColumnHeader) DecodeSSZ(buf []byte, version int) error {
	h.version = clparams.StateVersion(version)
	h.init()
	return ssz2.UnmarshalSSZ(buf, version, h.getSchema()...)
}

func (h *PartialDataColumnHeader) EncodingSizeSSZ() int {
	h.init()
	if h.version >= clparams.GloasVersion {
		// KzgCommitments (variable) + Slot (8) + BeaconBlockRoot (32)
		return h.KzgCommitments.EncodingSizeSSZ() + 8 + 32
	}
	// KzgCommitments (variable) + SignedBlockHeader (static) + KzgCommitmentsInclusionProof (static)
	return h.KzgCommitments.EncodingSizeSSZ() +
		h.SignedBlockHeader.EncodingSizeSSZ() +
		h.KzgCommitmentsInclusionProof.EncodingSizeSSZ()
}

func (h *PartialDataColumnHeader) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(h.getSchema()...)
}

func (h *PartialDataColumnHeader) Clone() clonable.Clonable {
	if h == nil {
		return &PartialDataColumnHeader{}
	}
	return NewPartialDataColumnHeader(h.version)
}

func (h *PartialDataColumnHeader) Static() bool {
	return false
}

// PartialDataColumnSidecar holds only the cells and proofs identified by the bitmap,
// with an optional header (only sent on eager pushes).
//
// Schema: cells_present_bitmap, partial_column, kzg_proofs, header
type PartialDataColumnSidecar struct {
	CellsPresentBitmap *solid.BitList                           `json:"cells_present_bitmap"`
	PartialColumn      *solid.ListSSZ[*Cell]                    `json:"partial_column"`
	KzgProofs          *solid.ListSSZ[*KZGProof]                `json:"kzg_proofs"`
	Header             *solid.ListSSZ[*PartialDataColumnHeader] `json:"header"`

	version clparams.StateVersion
}

func NewPartialDataColumnSidecar(version clparams.StateVersion) *PartialDataColumnSidecar {
	s := &PartialDataColumnSidecar{version: version}
	s.init()
	return s
}

func (s *PartialDataColumnSidecar) init() {
	cfg := clparams.GetBeaconConfig()
	if s.CellsPresentBitmap == nil {
		s.CellsPresentBitmap = solid.NewBitList(0, int(cfg.MaxBlobCommittmentsPerBlock))
	}
	if s.PartialColumn == nil {
		s.PartialColumn = solid.NewStaticListSSZ[*Cell](int(cfg.MaxBlobCommittmentsPerBlock), BytesPerCell)
	}
	if s.KzgProofs == nil {
		s.KzgProofs = solid.NewStaticListSSZ[*KZGProof](int(cfg.MaxBlobCommittmentsPerBlock), 48)
	}
	if s.Header == nil {
		s.Header = solid.NewDynamicListSSZ[*PartialDataColumnHeader](1)
	}
}

func (s *PartialDataColumnSidecar) Version() clparams.StateVersion { return s.version }

func (s *PartialDataColumnSidecar) SetVersion(v clparams.StateVersion) {
	s.version = v
	s.init()
}

func (s *PartialDataColumnSidecar) getSchema() []any {
	s.init()
	if s.version >= clparams.GloasVersion {
		// [Modified in Gloas:EIP7732] Removed `header`
		return []any{s.CellsPresentBitmap, s.PartialColumn, s.KzgProofs}
	}
	return []any{s.CellsPresentBitmap, s.PartialColumn, s.KzgProofs, s.Header}
}

func (s *PartialDataColumnSidecar) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, s.getSchema()...)
}

func (s *PartialDataColumnSidecar) DecodeSSZ(buf []byte, version int) error {
	s.version = clparams.StateVersion(version)
	s.init()
	return ssz2.UnmarshalSSZ(buf, version, s.getSchema()...)
}

func (s *PartialDataColumnSidecar) EncodingSizeSSZ() int {
	s.init()
	size := s.CellsPresentBitmap.EncodingSizeSSZ() +
		s.PartialColumn.EncodingSizeSSZ() +
		s.KzgProofs.EncodingSizeSSZ()
	if s.version < clparams.GloasVersion {
		size += s.Header.EncodingSizeSSZ()
	}
	return size
}

func (s *PartialDataColumnSidecar) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(s.getSchema()...)
}

func (s *PartialDataColumnSidecar) Clone() clonable.Clonable {
	if s == nil {
		return &PartialDataColumnSidecar{}
	}
	return NewPartialDataColumnSidecar(s.version)
}

func (s *PartialDataColumnSidecar) Static() bool {
	return false
}

// GetHeader returns the first (and only) header from the list, or nil if empty.
func (s *PartialDataColumnSidecar) GetHeader() *PartialDataColumnHeader {
	if s.Header == nil || s.Header.Len() == 0 {
		return nil
	}
	return s.Header.Get(0)
}

// PartialDataColumnPartsMetadata encodes peer cell availability.
// `available` indicates which cells a peer possesses.
// `requests` indicates desired cells.
//
// Schema: available, requests
type PartialDataColumnPartsMetadata struct {
	Available *solid.BitList `json:"available"`
	Requests  *solid.BitList `json:"requests"`
}

func NewPartialDataColumnPartsMetadata() *PartialDataColumnPartsMetadata {
	m := &PartialDataColumnPartsMetadata{}
	m.init()
	return m
}

func (m *PartialDataColumnPartsMetadata) init() {
	cfg := clparams.GetBeaconConfig()
	if m.Available == nil {
		m.Available = solid.NewBitList(0, int(cfg.MaxBlobCommittmentsPerBlock))
	}
	if m.Requests == nil {
		m.Requests = solid.NewBitList(0, int(cfg.MaxBlobCommittmentsPerBlock))
	}
}

func (m *PartialDataColumnPartsMetadata) EncodeSSZ(buf []byte) ([]byte, error) {
	m.init()
	return ssz2.MarshalSSZ(buf, m.Available, m.Requests)
}

func (m *PartialDataColumnPartsMetadata) DecodeSSZ(buf []byte, version int) error {
	m.init()
	return ssz2.UnmarshalSSZ(buf, version, m.Available, m.Requests)
}

func (m *PartialDataColumnPartsMetadata) EncodingSizeSSZ() int {
	m.init()
	return m.Available.EncodingSizeSSZ() + m.Requests.EncodingSizeSSZ()
}

func (m *PartialDataColumnPartsMetadata) HashSSZ() ([32]byte, error) {
	m.init()
	return merkle_tree.HashTreeRoot(m.Available, m.Requests)
}

func (m *PartialDataColumnPartsMetadata) Clone() clonable.Clonable {
	return NewPartialDataColumnPartsMetadata()
}

func (m *PartialDataColumnPartsMetadata) Static() bool {
	return false
}
