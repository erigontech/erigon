package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

type SignedBlindedBeaconBlock struct {
	Signature libcommon.Bytes96   `json:"signature"`
	Block     *BlindedBeaconBlock `json:"message"`
}

type BlindedBeaconBlock struct {
	Slot          uint64             `json:"slot,string"`
	ProposerIndex uint64             `json:"proposer_index,string"`
	ParentRoot    libcommon.Hash     `json:"parent_root"`
	StateRoot     libcommon.Hash     `json:"state_root"`
	Body          *BlindedBeaconBody `json:"body"`
}

type BlindedBeaconBody struct {
	// A byte array used for randomness in the beacon chain
	RandaoReveal libcommon.Bytes96 `json:"randao_reveal"`
	// Data related to the Ethereum 1.0 chain
	Eth1Data *Eth1Data `json:"eth1_data"`
	// A byte array used to customize validators' behavior
	Graffiti libcommon.Hash `json:"graffiti"`
	// A list of slashing events for validators who included invalid blocks in the chain
	ProposerSlashings *solid.ListSSZ[*ProposerSlashing] `json:"proposer_slashings"`
	// A list of slashing events for validators who included invalid attestations in the chain
	AttesterSlashings *solid.ListSSZ[*AttesterSlashing] `json:"attester_slashings"`
	// A list of attestations included in the block
	Attestations *solid.ListSSZ[*solid.Attestation] `json:"attestations"`
	// A list of deposits made to the Ethereum 1.0 chain
	Deposits *solid.ListSSZ[*Deposit] `json:"deposits"`
	// A list of validators who have voluntarily exited the beacon chain
	VoluntaryExits *solid.ListSSZ[*SignedVoluntaryExit] `json:"voluntary_exits"`
	// A summary of the current state of the beacon chain
	SyncAggregate *SyncAggregate `json:"sync_aggregate,omitempty"`
	// Data related to crosslink records and executing operations on the Ethereum 2.0 chain
	ExecutionPayload *Eth1Header `json:"execution_payload_header,omitempty"`
	// Withdrawals Diffs for Execution Layer
	ExecutionChanges *solid.ListSSZ[*SignedBLSToExecutionChange] `json:"execution_changes,omitempty"`
	// The commitments for beacon chain blobs
	// With a max of 4 per block
	BlobKzgCommitments *solid.ListSSZ[*KZGCommitment] `json:"blob_kzg_commitments,omitempty"`
	// The version of the beacon chain
	Version   clparams.StateVersion `json:"-"`
	beaconCfg *clparams.BeaconChainConfig
}

// Getters

func NewSignedBlindedBeaconBlock(beaconCfg *clparams.BeaconChainConfig) *SignedBlindedBeaconBlock {
	return &SignedBlindedBeaconBlock{Block: NewBlindedBeaconBlock(beaconCfg)}
}

func (s *SignedBlindedBeaconBlock) SignedBeaconBlockHeader() *SignedBeaconBlockHeader {
	bodyRoot, err := s.Block.Body.HashSSZ()
	if err != nil {
		panic(err)
	}
	return &SignedBeaconBlockHeader{
		Signature: s.Signature,
		Header: &BeaconBlockHeader{
			Slot:          s.Block.Slot,
			ProposerIndex: s.Block.ProposerIndex,
			ParentRoot:    s.Block.ParentRoot,
			Root:          s.Block.StateRoot,
			BodyRoot:      bodyRoot,
		},
	}
}

func NewBlindedBeaconBlock(beaconCfg *clparams.BeaconChainConfig) *BlindedBeaconBlock {
	return &BlindedBeaconBlock{Body: NewBlindedBeaconBody(beaconCfg)}
}

func NewBlindedBeaconBody(beaconCfg *clparams.BeaconChainConfig) *BlindedBeaconBody {
	return &BlindedBeaconBody{
		beaconCfg: beaconCfg,
	}
}

// Version returns beacon block version.
func (b *SignedBlindedBeaconBlock) Version() clparams.StateVersion {
	return b.Block.Body.Version
}

func (b *SignedBlindedBeaconBlock) Full(txs *solid.TransactionsSSZ, withdrawals *solid.ListSSZ[*Withdrawal]) *SignedBeaconBlock {
	return &SignedBeaconBlock{
		Signature: b.Signature,
		Block:     b.Block.Full(txs, withdrawals),
	}
}

// Version returns beacon block version.
func (b *BlindedBeaconBlock) Version() clparams.StateVersion {
	return b.Body.Version
}

func (b *BlindedBeaconBlock) Full(txs *solid.TransactionsSSZ, withdrawals *solid.ListSSZ[*Withdrawal]) *BeaconBlock {
	return &BeaconBlock{
		Slot:          b.Slot,
		ProposerIndex: b.ProposerIndex,
		ParentRoot:    b.ParentRoot,
		StateRoot:     b.StateRoot,
		Body:          b.Body.Full(txs, withdrawals),
	}
}

func (b *BlindedBeaconBody) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.getSchema(false)...)
}

func (b *BlindedBeaconBody) EncodingSizeSSZ() (size int) {

	if b.Eth1Data == nil {
		b.Eth1Data = &Eth1Data{}
	}
	if b.SyncAggregate == nil {
		b.SyncAggregate = &SyncAggregate{}
	}
	if b.ExecutionPayload == nil {
		b.ExecutionPayload = NewEth1Header(b.Version)
	}
	if b.ProposerSlashings == nil {
		b.ProposerSlashings = solid.NewStaticListSSZ[*ProposerSlashing](MaxProposerSlashings, 416)
	}
	if b.AttesterSlashings == nil {
		b.AttesterSlashings = solid.NewDynamicListSSZ[*AttesterSlashing](MaxAttesterSlashings)
	}
	if b.Attestations == nil {
		b.Attestations = solid.NewDynamicListSSZ[*solid.Attestation](MaxAttestations)
	}
	if b.Deposits == nil {
		b.Deposits = solid.NewStaticListSSZ[*Deposit](MaxDeposits, 1240)
	}
	if b.VoluntaryExits == nil {
		b.VoluntaryExits = solid.NewStaticListSSZ[*SignedVoluntaryExit](MaxVoluntaryExits, 112)
	}
	if b.ExecutionPayload == nil {
		b.ExecutionPayload = NewEth1Header(b.Version)
	}
	if b.ExecutionChanges == nil {
		b.ExecutionChanges = solid.NewStaticListSSZ[*SignedBLSToExecutionChange](MaxExecutionChanges, 172)
	}
	if b.BlobKzgCommitments == nil {
		b.BlobKzgCommitments = solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48)
	}

	size += b.ProposerSlashings.EncodingSizeSSZ()
	size += b.AttesterSlashings.EncodingSizeSSZ()
	size += b.Attestations.EncodingSizeSSZ()
	size += b.Deposits.EncodingSizeSSZ()
	size += b.VoluntaryExits.EncodingSizeSSZ()
	if b.Version >= clparams.BellatrixVersion {
		size += b.ExecutionPayload.EncodingSizeSSZ()
	}
	if b.Version >= clparams.CapellaVersion {
		size += b.ExecutionChanges.EncodingSizeSSZ()
	}
	if b.Version >= clparams.DenebVersion {
		size += b.ExecutionChanges.EncodingSizeSSZ()
	}

	return
}

func (b *BlindedBeaconBody) DecodeSSZ(buf []byte, version int) error {
	b.Version = clparams.StateVersion(version)

	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[BeaconBody] err: %s", ssz.ErrLowBufferSize)
	}

	b.ExecutionPayload = NewEth1Header(b.Version)

	err := ssz2.UnmarshalSSZ(buf, version, b.getSchema(false)...)
	return err
}

func (b *BlindedBeaconBody) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema(false)...)
}

func (b *BlindedBeaconBody) getSchema(storage bool) []interface{} {
	s := []interface{}{b.RandaoReveal[:], b.Eth1Data, b.Graffiti[:], b.ProposerSlashings, b.AttesterSlashings, b.Attestations, b.Deposits, b.VoluntaryExits}
	if b.Version >= clparams.AltairVersion {
		s = append(s, b.SyncAggregate)
	}
	if b.Version >= clparams.BellatrixVersion && !storage {
		s = append(s, b.ExecutionPayload)
	}
	if b.Version >= clparams.CapellaVersion {
		s = append(s, b.ExecutionChanges)
	}
	if b.Version >= clparams.DenebVersion {
		s = append(s, b.BlobKzgCommitments)
	}
	return s
}

func (b *BlindedBeaconBody) Full(txs *solid.TransactionsSSZ, withdrawals *solid.ListSSZ[*Withdrawal]) *BeaconBody {
	// Recover the execution payload
	executionPayload := &Eth1Block{
		ParentHash:    b.ExecutionPayload.ParentHash,
		BlockNumber:   b.ExecutionPayload.BlockNumber,
		StateRoot:     b.ExecutionPayload.StateRoot,
		Time:          b.ExecutionPayload.Time,
		GasLimit:      b.ExecutionPayload.GasLimit,
		GasUsed:       b.ExecutionPayload.GasUsed,
		Extra:         b.ExecutionPayload.Extra,
		ReceiptsRoot:  b.ExecutionPayload.ReceiptsRoot,
		LogsBloom:     b.ExecutionPayload.LogsBloom,
		BaseFeePerGas: b.ExecutionPayload.BaseFeePerGas,
		BlockHash:     b.ExecutionPayload.BlockHash,
		BlobGasUsed:   b.ExecutionPayload.BlobGasUsed,
		ExcessBlobGas: b.ExecutionPayload.ExcessBlobGas,
		FeeRecipient:  b.ExecutionPayload.FeeRecipient,
		PrevRandao:    b.ExecutionPayload.PrevRandao,
		Transactions:  txs,
		Withdrawals:   withdrawals,
		version:       b.ExecutionPayload.version,
		beaconCfg:     b.beaconCfg,
	}

	return &BeaconBody{
		RandaoReveal:       b.RandaoReveal,
		Eth1Data:           b.Eth1Data,
		Graffiti:           b.Graffiti,
		ProposerSlashings:  b.ProposerSlashings,
		AttesterSlashings:  b.AttesterSlashings,
		Attestations:       b.Attestations,
		Deposits:           b.Deposits,
		VoluntaryExits:     b.VoluntaryExits,
		SyncAggregate:      b.SyncAggregate,
		ExecutionPayload:   executionPayload,
		ExecutionChanges:   b.ExecutionChanges,
		BlobKzgCommitments: b.BlobKzgCommitments,
		Version:            b.Version,
		beaconCfg:          b.beaconCfg,
	}
}

func (b *BlindedBeaconBlock) EncodeSSZ(buf []byte) (dst []byte, err error) {
	return ssz2.MarshalSSZ(buf, b.Slot, b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (b *BlindedBeaconBlock) EncodingSizeSSZ() int {
	if b.Body == nil {
		return 80
	}
	return 80 + b.Body.EncodingSizeSSZ()
}

func (b *BlindedBeaconBlock) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &b.Slot, &b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (b *BlindedBeaconBlock) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Slot, b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (b *SignedBlindedBeaconBlock) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.Block, b.Signature[:])
}

func (b *SignedBlindedBeaconBlock) EncodingSizeSSZ() int {
	if b.Block == nil {
		return 100
	}
	return 100 + b.Block.EncodingSizeSSZ()
}

func (b *SignedBlindedBeaconBlock) DecodeSSZ(buf []byte, s int) error {
	return ssz2.UnmarshalSSZ(buf, s, b.Block, b.Signature[:])
}

func (b *SignedBlindedBeaconBlock) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Block, b.Signature[:])
}

func (*BlindedBeaconBody) Static() bool {
	return false
}

func (*BlindedBeaconBlock) Static() bool {
	return false
}

func (b *BlindedBeaconBody) Clone() clonable.Clonable {
	return NewBlindedBeaconBody(b.beaconCfg)
}

func (b *BlindedBeaconBlock) Clone() clonable.Clonable {
	return NewBlindedBeaconBlock(b.Body.beaconCfg)
}

func (b *SignedBlindedBeaconBlock) Clone() clonable.Clonable {
	return NewSignedBlindedBeaconBlock(b.Block.Body.beaconCfg)
}

func (b *BlindedBeaconBody) ExecutionPayloadMerkleProof() ([][32]byte, error) {
	return merkle_tree.MerkleProof(4, 9, b.getSchema(false)...)
}

// make sure that the type implements the interface ssz2.ObjectSSZ
var _ ssz2.ObjectSSZ = (*BlindedBeaconBody)(nil)
var _ ssz2.ObjectSSZ = (*BlindedBeaconBlock)(nil)
var _ ssz2.ObjectSSZ = (*SignedBlindedBeaconBlock)(nil)
