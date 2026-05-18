// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

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
	_ ssz.HashableSSZ = (*PayloadAttestationMessage)(nil)
	_ ssz.HashableSSZ = (*IndexedPayloadAttestation)(nil)
	_ ssz.HashableSSZ = (*ExecutionPayloadBid)(nil)
	_ ssz.HashableSSZ = (*SignedExecutionPayloadBid)(nil)
	_ ssz.HashableSSZ = (*ExecutionPayloadEnvelope)(nil)
	_ ssz.HashableSSZ = (*SignedExecutionPayloadEnvelope)(nil)

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
	PayloadStatusEmpty   PayloadStatus = 0 // PAYLOAD_STATUS_EMPTY per EIP-7732 fork-choice spec
	PayloadStatusFull    PayloadStatus = 1 // PAYLOAD_STATUS_FULL per EIP-7732 fork-choice spec
	PayloadStatusPending PayloadStatus = 2 // PAYLOAD_STATUS_PENDING per EIP-7732 fork-choice spec
)

// PayloadAttestationSSZSize is the fixed SSZ encoding size of PayloadAttestation
// using the mainnet PTC_SIZE (512). For preset-aware code, use
// PayloadAttestationSSZSizeWithPtcSize(cfg.PtcSize) instead.
//
// AggregationBits (PtcSize/8) + PayloadAttestationData (Hash + uint64 + bool + bool) + Signature (Bytes96)
const PayloadAttestationSSZSize = int(clparams.MaxPtcSize/8) + length.Hash + 8 + 1 + 1 + length.Bytes96

// PayloadAttestationSSZSizeWithPtcSize returns the fixed SSZ encoding size of a
// PayloadAttestation for a given PTC_SIZE preset value.
func PayloadAttestationSSZSizeWithPtcSize(ptcSize uint64) int {
	return int(ptcSize/8) + length.Hash + 8 + 1 + 1 + length.Bytes96
}

// PayloadAttestationData represents attestation data for a payload.
type PayloadAttestationData struct {
	BeaconBlockRoot   common.Hash `json:"beacon_block_root"`
	Slot              uint64      `json:"slot,string"`
	PayloadPresent    bool        `json:"payload_present"`
	BlobDataAvailable bool        `json:"blob_data_available"`
}

func boolToUint64(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

func (p *PayloadAttestationData) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(
		p.BeaconBlockRoot[:],
		p.Slot,
		boolToUint64(p.PayloadPresent),
		boolToUint64(p.BlobDataAvailable),
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
		p.AggregationBits = solid.NewBitVector(int(clparams.MaxPtcSize))
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
	// Infer PTC_SIZE from the buffer length. The fixed structure is:
	//   AggregationBits (PTC_SIZE/8) + PayloadAttestationData (42) + Signature (96)
	// so PTC_SIZE = (len(buf) - 42 - 96) * 8
	dataAndSigSize := length.Hash + 8 + 1 + 1 + length.Bytes96 // 42 + 96 = 138
	ptcSize := (len(buf) - dataAndSigSize) * 8
	if ptcSize <= 0 {
		ptcSize = int(clparams.MaxPtcSize) // fallback
	}
	p.AggregationBits = solid.NewBitVector(ptcSize)
	p.Data = new(PayloadAttestationData)
	return ssz2.UnmarshalSSZ(buf, version, p.AggregationBits, p.Data, p.Signature[:])
}

func (p *PayloadAttestation) Clone() clonable.Clonable {
	ptcSize := int(clparams.MaxPtcSize)
	if p != nil && p.AggregationBits != nil {
		ptcSize = p.AggregationBits.BitCap()
	}
	return &PayloadAttestation{
		AggregationBits: solid.NewBitVector(ptcSize),
		Data:            new(PayloadAttestationData),
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

func (p *PayloadAttestationMessage) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.ValidatorIndex, p.Data, p.Signature[:])
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
		AttestingIndices: solid.NewRawUint64List(int(clparams.MaxPtcSize), []uint64{}),
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
	i.AttestingIndices = solid.NewRawUint64List(int(clparams.MaxPtcSize), nil)
	i.Data = new(PayloadAttestationData)
	return ssz2.UnmarshalSSZ(buf, version, i.AttestingIndices, i.Data, i.Signature[:])
}

func (i *IndexedPayloadAttestation) EncodingSizeSSZ() int {
	return 4 + i.AttestingIndices.EncodingSizeSSZ() + // offset for AttestingIndices (variable-length)
		i.Data.EncodingSizeSSZ() + length.Bytes96
}

func (i *IndexedPayloadAttestation) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(i.AttestingIndices, i.Data, i.Signature[:])
}

func (i *IndexedPayloadAttestation) Clone() clonable.Clonable {
	newIndices := solid.NewRawUint64List(int(clparams.MaxPtcSize), make([]uint64, i.AttestingIndices.Length()))
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
// Field order matches the alpha7 spec: consensus-specs/specs/gloas/beacon-chain.md
type ExecutionPayloadBid struct {
	ParentBlockHash       common.Hash                   `json:"parent_block_hash"`
	ParentBlockRoot       common.Hash                   `json:"parent_block_root"`
	BlockHash             common.Hash                   `json:"block_hash"`
	PrevRandao            common.Hash                   `json:"prev_randao"`
	FeeRecipient          common.Address                `json:"fee_recipient"`
	GasLimit              uint64                        `json:"gas_limit,string"`
	BuilderIndex          uint64                        `json:"builder_index,string"`
	Slot                  uint64                        `json:"slot,string"`
	Value                 uint64                        `json:"value,string"`
	ExecutionPayment      uint64                        `json:"execution_payment,string"`
	BlobKzgCommitments    solid.ListSSZ[*KZGCommitment] `json:"blob_kzg_commitments"`
	ExecutionRequestsRoot common.Hash                   `json:"execution_requests_root"`
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
		e.ExecutionRequestsRoot[:],
	)
}

func (e *ExecutionPayloadBid) EncodingSizeSSZ() int {
	// 4 Hash32 fields (ParentBlockHash, ParentBlockRoot, BlockHash, PrevRandao) = 32*4 = 128
	// 1 ExecutionAddress (FeeRecipient) = 20
	// 5 uint64 fields (GasLimit, BuilderIndex, Slot, Value, ExecutionPayment) = 8*5 = 40
	// 1 offset for BlobKzgCommitments = 4
	// 1 Hash32 field (ExecutionRequestsRoot) = 32
	// Total fixed = 128 + 20 + 40 + 4 + 32 = 224
	return length.Hash*4 + length.Addr + 8*5 +
		4 + // offset for BlobKzgCommitments (variable-length field)
		length.Hash + // ExecutionRequestsRoot
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
		e.ExecutionRequestsRoot[:],
	)
}

func (e *ExecutionPayloadBid) DecodeSSZ(buf []byte, version int) error {
	e.BlobKzgCommitments = *solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48)
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
		e.ExecutionRequestsRoot[:],
	)
}

func (e *ExecutionPayloadBid) Clone() clonable.Clonable {
	return &ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48),
	}
}

func (e *ExecutionPayloadBid) Copy() *ExecutionPayloadBid {
	return &ExecutionPayloadBid{
		ParentBlockHash:       e.ParentBlockHash,
		ParentBlockRoot:       e.ParentBlockRoot,
		BlockHash:             e.BlockHash,
		PrevRandao:            e.PrevRandao,
		FeeRecipient:          e.FeeRecipient,
		GasLimit:              e.GasLimit,
		BuilderIndex:          e.BuilderIndex,
		Slot:                  e.Slot,
		Value:                 e.Value,
		ExecutionPayment:      e.ExecutionPayment,
		BlobKzgCommitments:    *e.BlobKzgCommitments.ShallowCopy(),
		ExecutionRequestsRoot: e.ExecutionRequestsRoot,
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
	return 4 + s.Message.EncodingSizeSSZ() + length.Bytes96 // 4 is the offset for Message (variable-length field)
}

func (s *SignedExecutionPayloadBid) Static() bool {
	return false
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
	Payload               *Eth1Block         `json:"payload"`
	ExecutionRequests     *ExecutionRequests `json:"execution_requests"`
	BuilderIndex          uint64             `json:"builder_index,string"`
	BeaconBlockRoot       common.Hash        `json:"beacon_block_root"`
	ParentBeaconBlockRoot common.Hash        `json:"parent_beacon_block_root"`

	beaconCfg *clparams.BeaconChainConfig
}

func NewExecutionPayloadEnvelope(cfg *clparams.BeaconChainConfig) *ExecutionPayloadEnvelope {
	return &ExecutionPayloadEnvelope{
		Payload:               NewEth1Block(clparams.GloasVersion, cfg),
		ExecutionRequests:     NewExecutionRequests(cfg),
		BuilderIndex:          0,
		BeaconBlockRoot:       common.Hash{},
		ParentBeaconBlockRoot: common.Hash{},
		beaconCfg:             cfg,
	}
}

func (e *ExecutionPayloadEnvelope) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(
		e.Payload,
		e.ExecutionRequests,
		e.BuilderIndex,
		e.BeaconBlockRoot[:],
		e.ParentBeaconBlockRoot[:],
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
		e.ParentBeaconBlockRoot[:],
	)
}

func (e *ExecutionPayloadEnvelope) DecodeSSZ(buf []byte, version int) error {
	if e.Payload == nil {
		e.Payload = NewEth1Block(clparams.StateVersion(version), e.beaconCfg)
	}
	if e.ExecutionRequests == nil {
		e.ExecutionRequests = NewExecutionRequests(e.beaconCfg)
	}
	return ssz2.UnmarshalSSZ(buf, version,
		e.Payload,
		e.ExecutionRequests,
		&e.BuilderIndex,
		e.BeaconBlockRoot[:],
		e.ParentBeaconBlockRoot[:],
	)
}

func (e *ExecutionPayloadEnvelope) EncodingSizeSSZ() int {
	return 4 + e.Payload.EncodingSizeSSZ() + // offset for Payload (variable-length)
		4 + e.ExecutionRequests.EncodingSizeSSZ() + // offset for ExecutionRequests (variable-length)
		8 + length.Hash + length.Hash // BuilderIndex + BeaconBlockRoot + ParentBeaconBlockRoot
}

func (e *ExecutionPayloadEnvelope) Clone() clonable.Clonable {
	cloned := NewExecutionPayloadEnvelope(e.beaconCfg)
	if e.Payload == nil {
		return cloned
	}
	encoded, err := e.EncodeSSZ(nil)
	if err != nil {
		return cloned
	}
	if err := cloned.DecodeSSZ(encoded, int(e.Payload.Version())); err != nil {
		return cloned
	}
	return cloned
}

// SignedExecutionPayloadEnvelope represents a signed execution payload envelope.
type SignedExecutionPayloadEnvelope struct {
	Message   *ExecutionPayloadEnvelope `json:"message"`
	Signature common.Bytes96            `json:"signature"`

	beaconCfg *clparams.BeaconChainConfig
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
	if s.Message == nil {
		s.Message = NewExecutionPayloadEnvelope(s.beaconCfg)
	}
	return ssz2.UnmarshalSSZ(buf, version, s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadEnvelope) EncodingSizeSSZ() int {
	return 4 + s.Message.EncodingSizeSSZ() + length.Bytes96 // 4 is the offset for Message (variable-length)
}

func (s *SignedExecutionPayloadEnvelope) Clone() clonable.Clonable {
	return &SignedExecutionPayloadEnvelope{
		Message:   s.Message.Clone().(*ExecutionPayloadEnvelope),
		Signature: s.Signature,
		beaconCfg: s.beaconCfg,
	}
}
