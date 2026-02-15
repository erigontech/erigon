package cltypes

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
)

// PayloadAttestationData represents payload attestation data (Gloas/EIP-7732)
type PayloadAttestationData struct {
	BeaconBlockRoot common.Hash `json:"beacon_block_root"`
	Slot            uint64      `json:"slot,string"`
	PayloadPresent  bool        `json:"payload_present"`
	BlobDataAvail   bool        `json:"blob_data_available"`
}

func boolToBytes(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func (p *PayloadAttestationData) EncodeSSZ(buf []byte) ([]byte, error) {
	pp := boolToBytes(p.PayloadPresent)
	bda := boolToBytes(p.BlobDataAvail)
	return ssz2.MarshalSSZ(buf, p.BeaconBlockRoot[:], &p.Slot, pp, bda)
}

func (p *PayloadAttestationData) DecodeSSZ(buf []byte, version int) error {
	pp := make([]byte, 1)
	bda := make([]byte, 1)
	if err := ssz2.UnmarshalSSZ(buf, version, p.BeaconBlockRoot[:], &p.Slot, pp, bda); err != nil {
		return err
	}
	p.PayloadPresent = pp[0] != 0
	p.BlobDataAvail = bda[0] != 0
	return nil
}

func (p *PayloadAttestationData) EncodingSizeSSZ() int {
	return 32 + 8 + 1 + 1 // root + slot + payload_present(bool) + blob_data_available(bool)
}

func (p *PayloadAttestationData) HashSSZ() ([32]byte, error) {
	pp := uint64(0)
	if p.PayloadPresent {
		pp = 1
	}
	bda := uint64(0)
	if p.BlobDataAvail {
		bda = 1
	}
	return merkle_tree.HashTreeRoot(p.BeaconBlockRoot[:], &p.Slot, &pp, &bda)
}

func (p *PayloadAttestationData) Static() bool { return true }
func (*PayloadAttestationData) Clone() clonable.Clonable {
	return &PayloadAttestationData{}
}

// PayloadAttestation is an attestation about payload timeliness (Gloas/EIP-7732)
// PTC_SIZE = 512
const PtcSize = 512

type PayloadAttestation struct {
	AggregationBits *solid.BitVector        `json:"aggregation_bits"`
	Data            *PayloadAttestationData `json:"data"`
	Signature       common.Bytes96          `json:"signature"`
}

func NewPayloadAttestation() *PayloadAttestation {
	return &PayloadAttestation{
		AggregationBits: solid.NewBitVector(PtcSize),
		Data:            &PayloadAttestationData{},
	}
}

func (p *PayloadAttestation) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.AggregationBits, p.Data, p.Signature[:])
}

func (p *PayloadAttestation) DecodeSSZ(buf []byte, version int) error {
	if p.AggregationBits == nil {
		p.AggregationBits = solid.NewBitVector(PtcSize)
	}
	if p.Data == nil {
		p.Data = &PayloadAttestationData{}
	}
	return ssz2.UnmarshalSSZ(buf, version, p.AggregationBits, p.Data, p.Signature[:])
}

func (p *PayloadAttestation) EncodingSizeSSZ() int {
	return (PtcSize / 8) + p.Data.EncodingSizeSSZ() + 96 // bitvector(512) = 64 bytes + data + sig
}

func (p *PayloadAttestation) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.AggregationBits, p.Data, p.Signature[:])
}

func (p *PayloadAttestation) Static() bool { return true }
func (p *PayloadAttestation) Clone() clonable.Clonable {
	return NewPayloadAttestation()
}

// GloasExecutionPayloadHeader is the Gloas/EIP-7732 replacement for the Bellatrix ExecutionPayloadHeader.
// In the spec test vectors (v1.6.0-alpha.6), this is called "ExecutionPayloadHeader" and is a STATIC container.
// It replaces the old dynamic Eth1Header at position 24 in the BeaconState.
type GloasExecutionPayloadHeader struct {
	ParentBlockHash          common.Hash    `json:"parent_block_hash"`
	ParentBlockRoot          common.Hash    `json:"parent_block_root"`
	BlockHash                common.Hash    `json:"block_hash"`
	FeeRecipient             common.Address `json:"fee_recipient"`
	GasLimit                 uint64         `json:"gas_limit,string"`
	BuilderIndex             uint64         `json:"builder_index,string"`
	Slot                     uint64         `json:"slot,string"`
	Value                    uint64         `json:"value,string"`
	BlobKzgCommitmentsRoot   common.Hash    `json:"blob_kzg_commitments_root"`
}

func (e *GloasExecutionPayloadHeader) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf,
		e.ParentBlockHash[:], e.ParentBlockRoot[:], e.BlockHash[:],
		e.FeeRecipient[:], &e.GasLimit, &e.BuilderIndex, &e.Slot, &e.Value,
		e.BlobKzgCommitmentsRoot[:],
	)
}

func (e *GloasExecutionPayloadHeader) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version,
		e.ParentBlockHash[:], e.ParentBlockRoot[:], e.BlockHash[:],
		e.FeeRecipient[:], &e.GasLimit, &e.BuilderIndex, &e.Slot, &e.Value,
		e.BlobKzgCommitmentsRoot[:],
	)
}

func (e *GloasExecutionPayloadHeader) EncodingSizeSSZ() int {
	// 3 hashes (32 each) + address (20) + 4 uint64s (8 each) + 1 hash (32)
	return 32*3 + 20 + 8*4 + 32 // = 96 + 20 + 32 + 32 = 180
}

func (e *GloasExecutionPayloadHeader) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(
		e.ParentBlockHash[:], e.ParentBlockRoot[:], e.BlockHash[:],
		e.FeeRecipient[:], &e.GasLimit, &e.BuilderIndex, &e.Slot, &e.Value,
		e.BlobKzgCommitmentsRoot[:],
	)
}

func (e *GloasExecutionPayloadHeader) Static() bool { return true }
func (*GloasExecutionPayloadHeader) Clone() clonable.Clonable {
	return &GloasExecutionPayloadHeader{}
}

// ExecutionPayloadBid is an alias for GloasExecutionPayloadHeader used in BeaconState.
type ExecutionPayloadBid = GloasExecutionPayloadHeader

// NewExecutionPayloadBid creates a new ExecutionPayloadBid (alias for GloasExecutionPayloadHeader).
func NewExecutionPayloadBid() *ExecutionPayloadBid {
	return &GloasExecutionPayloadHeader{}
}

// SignedExecutionPayloadHeader wraps GloasExecutionPayloadHeader with a signature (Gloas/EIP-7732)
type SignedExecutionPayloadHeader struct {
	Message   *GloasExecutionPayloadHeader `json:"message"`
	Signature common.Bytes96               `json:"signature"`
}

// SignedExecutionPayloadBid is an alias for SignedExecutionPayloadHeader used in BeaconBlockBody.
type SignedExecutionPayloadBid = SignedExecutionPayloadHeader

// NewSignedExecutionPayloadBid creates a new SignedExecutionPayloadBid (alias for SignedExecutionPayloadHeader).
func NewSignedExecutionPayloadBid() *SignedExecutionPayloadBid {
	return &SignedExecutionPayloadHeader{
		Message: &GloasExecutionPayloadHeader{},
	}
}

func NewSignedExecutionPayloadHeader() *SignedExecutionPayloadHeader {
	return &SignedExecutionPayloadHeader{
		Message: &GloasExecutionPayloadHeader{},
	}
}

func (s *SignedExecutionPayloadHeader) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadHeader) DecodeSSZ(buf []byte, version int) error {
	if s.Message == nil {
		s.Message = &GloasExecutionPayloadHeader{}
	}
	return ssz2.UnmarshalSSZ(buf, version, s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadHeader) EncodingSizeSSZ() int {
	return s.Message.EncodingSizeSSZ() + 96 // 180 + 96 = 276
}

func (s *SignedExecutionPayloadHeader) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadHeader) Static() bool { return true }
func (s *SignedExecutionPayloadHeader) Clone() clonable.Clonable {
	return NewSignedExecutionPayloadHeader()
}

// Builder represents a builder in the builder registry (Gloas/EIP-7732)
type Builder struct {
	PubKey            common.Bytes48 `json:"pubkey"`
	Version           uint8          `json:"version,string"`
	ExecutionAddress  common.Address `json:"execution_address"`
	Balance           uint64         `json:"balance,string"`
	DepositEpoch      uint64         `json:"deposit_epoch,string"`
	WithdrawableEpoch uint64         `json:"withdrawable_epoch,string"`
}

func (b *Builder) EncodeSSZ(buf []byte) ([]byte, error) {
	v := []byte{b.Version}
	return ssz2.MarshalSSZ(buf, b.PubKey[:], v, b.ExecutionAddress[:], &b.Balance, &b.DepositEpoch, &b.WithdrawableEpoch)
}

func (b *Builder) DecodeSSZ(buf []byte, version int) error {
	v := make([]byte, 1)
	if err := ssz2.UnmarshalSSZ(buf, version, b.PubKey[:], v, b.ExecutionAddress[:], &b.Balance, &b.DepositEpoch, &b.WithdrawableEpoch); err != nil {
		return err
	}
	b.Version = v[0]
	return nil
}

func (b *Builder) EncodingSizeSSZ() int {
	return 48 + 1 + 20 + 8 + 8 + 8 // pubkey + version(uint8) + address + balance + deposit_epoch + withdrawable_epoch
}

func (b *Builder) HashSSZ() ([32]byte, error) {
	v := []byte{b.Version}
	return merkle_tree.HashTreeRoot(b.PubKey[:], v, b.ExecutionAddress[:], &b.Balance, &b.DepositEpoch, &b.WithdrawableEpoch)
}

func (b *Builder) Static() bool { return true }
func (*Builder) Clone() clonable.Clonable {
	return &Builder{}
}

// BuilderPendingWithdrawal represents a pending withdrawal for a builder (Gloas/EIP-7732)
type BuilderPendingWithdrawal struct {
	FeeRecipient      common.Address `json:"fee_recipient"`
	Amount            uint64         `json:"amount,string"`
	BuilderIndex      uint64         `json:"builder_index,string"`
	WithdrawableEpoch uint64         `json:"withdrawable_epoch,string"`
}

func (b *BuilderPendingWithdrawal) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.FeeRecipient[:], &b.Amount, &b.BuilderIndex, &b.WithdrawableEpoch)
}

func (b *BuilderPendingWithdrawal) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.FeeRecipient[:], &b.Amount, &b.BuilderIndex, &b.WithdrawableEpoch)
}

func (b *BuilderPendingWithdrawal) EncodingSizeSSZ() int {
	return 20 + 8 + 8 + 8 // address + amount + builder_index + withdrawable_epoch
}

func (b *BuilderPendingWithdrawal) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.FeeRecipient[:], &b.Amount, &b.BuilderIndex, &b.WithdrawableEpoch)
}

func (b *BuilderPendingWithdrawal) Static() bool { return true }
func (*BuilderPendingWithdrawal) Clone() clonable.Clonable {
	return &BuilderPendingWithdrawal{}
}

// BuilderPendingPayment represents a pending payment for a builder (Gloas/EIP-7732)
type BuilderPendingPayment struct {
	Weight     uint64                    `json:"weight,string"`
	Withdrawal *BuilderPendingWithdrawal `json:"withdrawal"`
}

func NewBuilderPendingPayment() *BuilderPendingPayment {
	return &BuilderPendingPayment{
		Withdrawal: &BuilderPendingWithdrawal{},
	}
}

func (b *BuilderPendingPayment) EncodeSSZ(buf []byte) ([]byte, error) {
	if b.Withdrawal == nil {
		b.Withdrawal = &BuilderPendingWithdrawal{}
	}
	return ssz2.MarshalSSZ(buf, &b.Weight, b.Withdrawal)
}

func (b *BuilderPendingPayment) DecodeSSZ(buf []byte, version int) error {
	if b.Withdrawal == nil {
		b.Withdrawal = &BuilderPendingWithdrawal{}
	}
	return ssz2.UnmarshalSSZ(buf, version, &b.Weight, b.Withdrawal)
}

func (b *BuilderPendingPayment) EncodingSizeSSZ() int {
	return 8 + 44 // weight + BuilderPendingWithdrawal(20+8+8+8)
}

func (b *BuilderPendingPayment) HashSSZ() ([32]byte, error) {
	if b.Withdrawal == nil {
		b.Withdrawal = &BuilderPendingWithdrawal{}
	}
	return merkle_tree.HashTreeRoot(&b.Weight, b.Withdrawal)
}

func (b *BuilderPendingPayment) Static() bool { return true }
func (b *BuilderPendingPayment) Clone() clonable.Clonable {
	return NewBuilderPendingPayment()
}

// ExecutionPayloadEnvelope wraps a full execution payload with metadata (Gloas/EIP-7732)
type ExecutionPayloadEnvelope struct {
	Payload            *Eth1Block                    `json:"payload"`
	ExecutionRequests  *ExecutionRequests             `json:"execution_requests"`
	BuilderIndex       uint64                         `json:"builder_index,string"`
	BeaconBlockRoot    common.Hash                    `json:"beacon_block_root"`
	Slot               uint64                         `json:"slot,string"`
	BlobKzgCommitments *solid.ListSSZ[*KZGCommitment] `json:"blob_kzg_commitments"`
	StateRoot          common.Hash                    `json:"state_root"`
}

func NewExecutionPayloadEnvelope() *ExecutionPayloadEnvelope {
	cfg := &clparams.MainnetBeaconConfig
	return &ExecutionPayloadEnvelope{
		Payload:            NewEth1Block(clparams.GloasVersion, cfg),
		ExecutionRequests:  NewExecutionRequests(cfg),
		BlobKzgCommitments: solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48),
	}
}

func (e *ExecutionPayloadEnvelope) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, e.Payload, e.ExecutionRequests, &e.BuilderIndex, e.BeaconBlockRoot[:], &e.Slot, e.BlobKzgCommitments, e.StateRoot[:])
}

func (e *ExecutionPayloadEnvelope) DecodeSSZ(buf []byte, version int) error {
	cfg := &clparams.MainnetBeaconConfig
	if e.Payload == nil {
		e.Payload = NewEth1Block(clparams.GloasVersion, cfg)
	}
	if e.ExecutionRequests == nil {
		e.ExecutionRequests = NewExecutionRequests(cfg)
	}
	if e.BlobKzgCommitments == nil {
		e.BlobKzgCommitments = solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48)
	}
	return ssz2.UnmarshalSSZ(buf, version, e.Payload, e.ExecutionRequests, &e.BuilderIndex, e.BeaconBlockRoot[:], &e.Slot, e.BlobKzgCommitments, e.StateRoot[:])
}

func (e *ExecutionPayloadEnvelope) EncodingSizeSSZ() int {
	// 3 dynamic offsets (payload, execution_requests, blob_kzg_commitments) + builder_index(8) + beacon_block_root(32) + slot(8) + state_root(32)
	return 4 + 4 + 8 + 32 + 8 + 4 + 32 + e.Payload.EncodingSizeSSZ() + e.ExecutionRequests.EncodingSizeSSZ() + e.BlobKzgCommitments.EncodingSizeSSZ()
}

func (e *ExecutionPayloadEnvelope) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(e.Payload, e.ExecutionRequests, &e.BuilderIndex, e.BeaconBlockRoot[:], &e.Slot, e.BlobKzgCommitments, e.StateRoot[:])
}

func (e *ExecutionPayloadEnvelope) Static() bool { return false }
func (e *ExecutionPayloadEnvelope) Clone() clonable.Clonable {
	return NewExecutionPayloadEnvelope()
}

// SignedExecutionPayloadEnvelope wraps ExecutionPayloadEnvelope with a signature
type SignedExecutionPayloadEnvelope struct {
	Message   *ExecutionPayloadEnvelope `json:"message"`
	Signature common.Bytes96            `json:"signature"`
}

func NewSignedExecutionPayloadEnvelope() *SignedExecutionPayloadEnvelope {
	return &SignedExecutionPayloadEnvelope{
		Message: NewExecutionPayloadEnvelope(),
	}
}

func (s *SignedExecutionPayloadEnvelope) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadEnvelope) DecodeSSZ(buf []byte, version int) error {
	if s.Message == nil {
		s.Message = NewExecutionPayloadEnvelope()
	}
	return ssz2.UnmarshalSSZ(buf, version, s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadEnvelope) EncodingSizeSSZ() int {
	return 4 + 96 + s.Message.EncodingSizeSSZ() // offset + signature + message
}

func (s *SignedExecutionPayloadEnvelope) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(s.Message, s.Signature[:])
}

func (s *SignedExecutionPayloadEnvelope) Static() bool { return false }
func (s *SignedExecutionPayloadEnvelope) Clone() clonable.Clonable {
	return NewSignedExecutionPayloadEnvelope()
}

// ForkChoiceNode represents a node in the fork-choice tree (Gloas/EIP-7732)
type ForkChoiceNode struct {
	Root          common.Hash `json:"root"`
	PayloadStatus uint8       `json:"payload_status"`
}

func (f *ForkChoiceNode) EncodeSSZ(buf []byte) ([]byte, error) {
	ps := []byte{f.PayloadStatus}
	return ssz2.MarshalSSZ(buf, f.Root[:], ps)
}

func (f *ForkChoiceNode) DecodeSSZ(buf []byte, version int) error {
	ps := make([]byte, 1)
	if err := ssz2.UnmarshalSSZ(buf, version, f.Root[:], ps); err != nil {
		return err
	}
	f.PayloadStatus = ps[0]
	return nil
}

func (f *ForkChoiceNode) EncodingSizeSSZ() int {
	return 32 + 1 // root + payload_status(uint8)
}

func (f *ForkChoiceNode) HashSSZ() ([32]byte, error) {
	ps := []byte{f.PayloadStatus}
	return merkle_tree.HashTreeRoot(f.Root[:], ps)
}

func (f *ForkChoiceNode) Static() bool { return true }
func (*ForkChoiceNode) Clone() clonable.Clonable {
	return &ForkChoiceNode{}
}

// PayloadAttestationMessage is a message from a single validator for payload timeliness (Gloas/EIP-7732)
type PayloadAttestationMessage struct {
	ValidatorIndex uint64                  `json:"validator_index,string"`
	Data           *PayloadAttestationData `json:"data"`
	Signature      common.Bytes96          `json:"signature"`
}

func NewPayloadAttestationMessage() *PayloadAttestationMessage {
	return &PayloadAttestationMessage{
		Data: &PayloadAttestationData{},
	}
}

func (p *PayloadAttestationMessage) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, &p.ValidatorIndex, p.Data, p.Signature[:])
}

func (p *PayloadAttestationMessage) DecodeSSZ(buf []byte, version int) error {
	if p.Data == nil {
		p.Data = &PayloadAttestationData{}
	}
	return ssz2.UnmarshalSSZ(buf, version, &p.ValidatorIndex, p.Data, p.Signature[:])
}

func (p *PayloadAttestationMessage) EncodingSizeSSZ() int {
	return 8 + p.Data.EncodingSizeSSZ() + 96 // validator_index + data + sig
}

func (p *PayloadAttestationMessage) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(&p.ValidatorIndex, p.Data, p.Signature[:])
}

func (p *PayloadAttestationMessage) Static() bool { return true }
func (p *PayloadAttestationMessage) Clone() clonable.Clonable {
	return NewPayloadAttestationMessage()
}

// IndexedPayloadAttestation is an indexed version of PayloadAttestation (Gloas/EIP-7732)
type IndexedPayloadAttestation struct {
	AttestingIndices *solid.RawUint64List    `json:"attesting_indices"`
	Data            *PayloadAttestationData `json:"data"`
	Signature       common.Bytes96          `json:"signature"`
}

func NewIndexedPayloadAttestation() *IndexedPayloadAttestation {
	return &IndexedPayloadAttestation{
		AttestingIndices: solid.NewRawUint64List(PtcSize, nil),
		Data:            &PayloadAttestationData{},
	}
}

func (p *IndexedPayloadAttestation) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.AttestingIndices, p.Data, p.Signature[:])
}

func (p *IndexedPayloadAttestation) DecodeSSZ(buf []byte, version int) error {
	if p.AttestingIndices == nil {
		p.AttestingIndices = solid.NewRawUint64List(PtcSize, nil)
	}
	if p.Data == nil {
		p.Data = &PayloadAttestationData{}
	}
	return ssz2.UnmarshalSSZ(buf, version, p.AttestingIndices, p.Data, p.Signature[:])
}

func (p *IndexedPayloadAttestation) EncodingSizeSSZ() int {
	return p.AttestingIndices.EncodingSizeSSZ() + 4 + p.Data.EncodingSizeSSZ() + 96
}

func (p *IndexedPayloadAttestation) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.AttestingIndices, p.Data, p.Signature[:])
}

func (p *IndexedPayloadAttestation) Static() bool { return false }
func (p *IndexedPayloadAttestation) Clone() clonable.Clonable {
	return NewIndexedPayloadAttestation()
}
