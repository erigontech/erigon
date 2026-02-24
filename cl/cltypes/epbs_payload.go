package cltypes

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/ssz"
)

var (
	_ ssz.HashableSSZ = (*PayloadAttestationData)(nil)
	_ ssz.HashableSSZ = (*PayloadAttestation)(nil)
	_ ssz.HashableSSZ = (*ExecutionPayloadBid)(nil)
	_ ssz.HashableSSZ = (*SignedExecutionPayloadBid)(nil)
	_ ssz.HashableSSZ = (*ExecutionPayloadEnvelope)(nil)

	_ ssz2.SizedObjectSSZ = (*PayloadAttestationData)(nil)
	_ ssz2.SizedObjectSSZ = (*PayloadAttestation)(nil)
	_ ssz2.SizedObjectSSZ = (*PayloadAttestationMessage)(nil)
	_ ssz2.SizedObjectSSZ = (*IndexedPayloadAttestation)(nil)
	_ ssz2.SizedObjectSSZ = (*ExecutionPayloadBid)(nil)
	_ ssz2.SizedObjectSSZ = (*SignedExecutionPayloadBid)(nil)
	_ ssz2.SizedObjectSSZ = (*ExecutionPayloadEnvelope)(nil)
	_ ssz2.SizedObjectSSZ = (*SignedExecutionPayloadEnvelope)(nil)
)

type PayloadStatus uint64

const (
	PayloadStatusPending PayloadStatus = 0
	PayloadStatusEmpty   PayloadStatus = 1
	PayloadStatusFull    PayloadStatus = 2
)

// PayloadAttestationData represents attestation data for a payload.
type PayloadAttestationData struct {
	BeaconBlockRoot   common.Hash `json:"beacon_block_root"`
	Slot              uint64      `json:"slot"`
	PayloadPresent    bool        `json:"payload_present"`
	BlobDataAvailable bool        `json:"blob_data_available"`
}

func (p *PayloadAttestationData) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(
		p.BeaconBlockRoot[:],
		p.Slot,
		ssz.BoolSSZ(p.PayloadPresent),
		ssz.BoolSSZ(p.BlobDataAvailable),
	)
}

func (p *PayloadAttestationData) EncodingSizeSSZ() int {
	return length.Hash + 8 + 1 + 1
}

func (p *PayloadAttestationData) Static() bool {
	return true
}

func (p *PayloadAttestationData) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.BeaconBlockRoot[:], p.Slot, p.PayloadPresent, p.BlobDataAvailable)
}

func (p *PayloadAttestationData) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, p.BeaconBlockRoot[:], &p.Slot, &p.PayloadPresent, &p.BlobDataAvailable)
}

func (p *PayloadAttestationData) Clone() clonable.Clonable {
	return &PayloadAttestationData{
		BeaconBlockRoot:   p.BeaconBlockRoot,
		Slot:              p.Slot,
		PayloadPresent:    p.PayloadPresent,
		BlobDataAvailable: p.BlobDataAvailable,
	}
}

// PayloadAttestation represents an attestation for a payload.
type PayloadAttestation struct {
	AggregationBits *solid.BitVector        `json:"aggregation_bits"`
	Data            *PayloadAttestationData `json:"data"`
	Signature       common.Bytes96          `json:"signature"`
}

func (p *PayloadAttestation) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.AggregationBits, p.Data, p.Signature[:])
}

func (p *PayloadAttestation) EncodingSizeSSZ() int {
	if p.AggregationBits == nil {
		p.AggregationBits = solid.NewBitVector(int(clparams.PtcSize))
	}
	if p.Data == nil {
		p.Data = new(PayloadAttestationData)
	}
	return p.AggregationBits.EncodingSizeSSZ() + p.Data.EncodingSizeSSZ() + length.Bytes96
}

func (p *PayloadAttestation) Static() bool {
	return true
}

func (p *PayloadAttestation) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.AggregationBits, p.Data, p.Signature[:])
}

func (p *PayloadAttestation) DecodeSSZ(buf []byte, version int) error {
	p.AggregationBits = solid.NewBitVector(int(clparams.PtcSize))
	p.Data = new(PayloadAttestationData)
	return ssz2.UnmarshalSSZ(buf, version, p.AggregationBits, p.Data, p.Signature[:])
}

func (p *PayloadAttestation) Clone() clonable.Clonable {
	return &PayloadAttestation{
		AggregationBits: p.AggregationBits.Copy(),
		Data:            p.Data.Clone().(*PayloadAttestationData),
		Signature:       p.Signature,
	}
}

// PayloadAttestationMessage represents a single validator's payload attestation message.
type PayloadAttestationMessage struct {
	ValidatorIndex uint64                  `json:"validator_index,string"`
	Data           *PayloadAttestationData `json:"data"`
	Signature      common.Bytes96          `json:"signature"`
}

func (p *PayloadAttestationMessage) EncodingSizeSSZ() int {
	return 8 + p.Data.EncodingSizeSSZ() + length.Bytes96
}

func (p *PayloadAttestationMessage) Static() bool {
	return true
}

func (p *PayloadAttestationMessage) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.ValidatorIndex, p.Data, p.Signature[:])
}

func (p *PayloadAttestationMessage) DecodeSSZ(buf []byte, version int) error {
	p.Data = new(PayloadAttestationData)
	return ssz2.UnmarshalSSZ(buf, version, &p.ValidatorIndex, p.Data, p.Signature[:])
}

func (p *PayloadAttestationMessage) Clone() clonable.Clonable {
	return &PayloadAttestationMessage{
		ValidatorIndex: p.ValidatorIndex,
		Data:           p.Data.Clone().(*PayloadAttestationData),
		Signature:      p.Signature,
	}
}

// IndexedPayloadAttestation represents an indexed payload attestation with attesting validator indices.
type IndexedPayloadAttestation struct {
	AttestingIndices *solid.RawUint64List    `json:"attesting_indices"`
	Data             *PayloadAttestationData `json:"data"`
	Signature        common.Bytes96          `json:"signature"`
}

func NewIndexedPayloadAttestation() *IndexedPayloadAttestation {
	return &IndexedPayloadAttestation{
		AttestingIndices: solid.NewRawUint64List(int(clparams.PtcSize), []uint64{}),
		Data:             &PayloadAttestationData{},
	}
}

func (i *IndexedPayloadAttestation) Static() bool {
	return false
}

func (i *IndexedPayloadAttestation) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, i.AttestingIndices, i.Data, i.Signature[:])
}

func (i *IndexedPayloadAttestation) DecodeSSZ(buf []byte, version int) error {
	i.AttestingIndices = solid.NewRawUint64List(int(clparams.PtcSize), nil)
	i.Data = new(PayloadAttestationData)
	return ssz2.UnmarshalSSZ(buf, version, i.AttestingIndices, i.Data, i.Signature[:])
}

func (i *IndexedPayloadAttestation) EncodingSizeSSZ() int {
	return i.AttestingIndices.EncodingSizeSSZ() + i.Data.EncodingSizeSSZ() + length.Bytes96
}

func (i *IndexedPayloadAttestation) Clone() clonable.Clonable {
	newIndices := solid.NewRawUint64List(int(clparams.PtcSize), make([]uint64, i.AttestingIndices.Length()))
	for idx := 0; idx < i.AttestingIndices.Length(); idx++ {
		newIndices.Set(idx, i.AttestingIndices.Get(idx))
	}
	return &IndexedPayloadAttestation{
		AttestingIndices: newIndices,
		Data:             i.Data.Clone().(*PayloadAttestationData),
		Signature:        i.Signature,
	}
}

// ExecutionPayloadBid represents a bid for an execution payload from a builder.
type ExecutionPayloadBid struct {
	ParentBlockHash    common.Hash                   `json:"parent_block_hash"`
	ParentBlockRoot    common.Hash                   `json:"parent_block_root"`
	BlockHash          common.Hash                   `json:"block_hash"`
	PrevRandao         common.Hash                   `json:"prev_randao"`
	FeeRecipient       common.Address                `json:"fee_recipient"`
	GasLimit           uint64                        `json:"gas_limit,string"`
	BuilderIndex       uint64                        `json:"builder_index,string"`
	Slot               uint64                        `json:"slot,string"`
	Value              uint64                        `json:"value,string"`
	ExecutionPayment   uint64                        `json:"execution_payment,string"`
	BlobKzgCommitments solid.ListSSZ[*KZGCommitment] `json:"blob_kzg_commitments"`
}

func (e *ExecutionPayloadBid) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(
		e.ParentBlockHash[:],
		e.ParentBlockRoot[:],
		e.BlockHash[:],
		e.PrevRandao[:],
		e.FeeRecipient[:],
		e.GasLimit,
		e.BuilderIndex,
		e.Slot,
		e.Value,
		e.ExecutionPayment,
		&e.BlobKzgCommitments,
	)
}

func (e *ExecutionPayloadBid) EncodingSizeSSZ() int {
	return length.Hash*4 + length.Addr + 8 + 8 + 8 + 8 + 8 +
		e.BlobKzgCommitments.EncodingSizeSSZ()
}

func (e *ExecutionPayloadBid) Static() bool {
	return false
}

func (e *ExecutionPayloadBid) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf,
		e.ParentBlockHash[:],
		e.ParentBlockRoot[:],
		e.BlockHash[:],
		e.PrevRandao[:],
		e.FeeRecipient[:],
		e.GasLimit,
		e.BuilderIndex,
		e.Slot,
		e.Value,
		e.ExecutionPayment,
		&e.BlobKzgCommitments,
	)
}

func (e *ExecutionPayloadBid) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version,
		e.ParentBlockHash[:],
		e.ParentBlockRoot[:],
		e.BlockHash[:],
		e.PrevRandao[:],
		e.FeeRecipient[:],
		&e.GasLimit,
		&e.BuilderIndex,
		&e.Slot,
		&e.Value,
		&e.ExecutionPayment,
		&e.BlobKzgCommitments,
	)
}

func (e *ExecutionPayloadBid) Clone() clonable.Clonable {
	return &ExecutionPayloadBid{}
}

func (e *ExecutionPayloadBid) Copy() *ExecutionPayloadBid {
	return &ExecutionPayloadBid{
		ParentBlockHash:    e.ParentBlockHash,
		ParentBlockRoot:    e.ParentBlockRoot,
		BlockHash:          e.BlockHash,
		PrevRandao:         e.PrevRandao,
		FeeRecipient:       e.FeeRecipient,
		GasLimit:           e.GasLimit,
		BuilderIndex:       e.BuilderIndex,
		Slot:               e.Slot,
		Value:              e.Value,
		ExecutionPayment:   e.ExecutionPayment,
		BlobKzgCommitments: e.BlobKzgCommitments,
	}
}

// SignedExecutionPayloadBid represents a signed execution payload bid.
type SignedExecutionPayloadBid struct {
	Message   *ExecutionPayloadBid `json:"message"`
	Signature common.Bytes96       `json:"signature"`
}

func (s *SignedExecutionPayloadBid) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadBid) EncodingSizeSSZ() int {
	return s.Message.EncodingSizeSSZ() + length.Bytes96
}

func (s *SignedExecutionPayloadBid) Static() bool {
	return true
}

func (s *SignedExecutionPayloadBid) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadBid) DecodeSSZ(buf []byte, version int) error {
	s.Message = new(ExecutionPayloadBid)
	return ssz2.UnmarshalSSZ(buf, version, s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadBid) Clone() clonable.Clonable {
	return &SignedExecutionPayloadBid{
		Message:   s.Message.Clone().(*ExecutionPayloadBid),
		Signature: s.Signature,
	}
}

// ExecutionPayloadEnvelope represents an execution payload envelope with associated metadata.
type ExecutionPayloadEnvelope struct {
	Payload           *Eth1Block         `json:"payload"`
	ExecutionRequests *ExecutionRequests `json:"execution_requests"`
	BuilderIndex      uint64             `json:"builder_index,string"`
	BeaconBlockRoot   common.Hash        `json:"beacon_block_root"`
	Slot              uint64             `json:"slot,string"`
	StateRoot         common.Hash        `json:"state_root"`
}

func NewExecutionPayloadEnvelope(cfg *clparams.BeaconChainConfig) *ExecutionPayloadEnvelope {
	return &ExecutionPayloadEnvelope{
		Payload:           NewEth1Block(clparams.BellatrixVersion, cfg),
		ExecutionRequests: NewExecutionRequests(cfg),
		BuilderIndex:      0,
		BeaconBlockRoot:   common.Hash{},
		Slot:              0,
		StateRoot:         common.Hash{},
	}
}

func (e *ExecutionPayloadEnvelope) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(
		e.Payload,
		e.ExecutionRequests,
		e.BuilderIndex,
		e.BeaconBlockRoot[:],
		e.Slot,
		e.StateRoot[:],
	)
}

func (e *ExecutionPayloadEnvelope) Static() bool {
	return false
}

func (e *ExecutionPayloadEnvelope) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf,
		e.Payload,
		e.ExecutionRequests,
		e.BuilderIndex,
		e.BeaconBlockRoot[:],
		e.Slot,
		e.StateRoot[:],
	)
}

func (e *ExecutionPayloadEnvelope) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version,
		e.Payload,
		e.ExecutionRequests,
		&e.BuilderIndex,
		e.BeaconBlockRoot[:],
		&e.Slot,
		e.StateRoot[:],
	)
}

func (e *ExecutionPayloadEnvelope) EncodingSizeSSZ() int {
	return e.Payload.EncodingSizeSSZ() +
		e.ExecutionRequests.EncodingSizeSSZ() +
		8 + length.Hash + 8 + length.Hash
}

func (e *ExecutionPayloadEnvelope) Clone() clonable.Clonable {
	return &ExecutionPayloadEnvelope{
		Payload:           e.Payload,
		ExecutionRequests: e.ExecutionRequests,
		BuilderIndex:      e.BuilderIndex,
		BeaconBlockRoot:   e.BeaconBlockRoot,
		Slot:              e.Slot,
		StateRoot:         e.StateRoot,
	}
}

// SignedExecutionPayloadEnvelope represents a signed execution payload envelope.
type SignedExecutionPayloadEnvelope struct {
	Message   *ExecutionPayloadEnvelope `json:"message"`
	Signature common.Bytes96            `json:"signature"`
}

func (s *SignedExecutionPayloadEnvelope) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadEnvelope) Static() bool {
	return false
}

func (s *SignedExecutionPayloadEnvelope) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadEnvelope) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadEnvelope) EncodingSizeSSZ() int {
	return s.Message.EncodingSizeSSZ() + length.Bytes96
}

func (s *SignedExecutionPayloadEnvelope) Clone() clonable.Clonable {
	return &SignedExecutionPayloadEnvelope{
		Message:   s.Message.Clone().(*ExecutionPayloadEnvelope),
		Signature: s.Signature,
	}
}
