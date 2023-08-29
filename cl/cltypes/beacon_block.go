package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

const (
	MaxAttesterSlashings         = 2
	MaxProposerSlashings         = 16
	MaxAttestations              = 128
	MaxDeposits                  = 16
	MaxVoluntaryExits            = 16
	MaxExecutionChanges          = 16
	MaxBlobsCommittmentsPerBlock = 4096
)

type SignedBeaconBlock struct {
	Signature [96]byte
	Block     *BeaconBlock
}

type BeaconBlock struct {
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    libcommon.Hash
	StateRoot     libcommon.Hash
	Body          *BeaconBody
}

type BeaconBody struct {
	// A byte array used for randomness in the beacon chain
	RandaoReveal [96]byte
	// Data related to the Ethereum 1.0 chain
	Eth1Data *Eth1Data
	// A byte array used to customize validators' behavior
	Graffiti [32]byte
	// A list of slashing events for validators who included invalid blocks in the chain
	ProposerSlashings *solid.ListSSZ[*ProposerSlashing]
	// A list of slashing events for validators who included invalid attestations in the chain
	AttesterSlashings *solid.ListSSZ[*AttesterSlashing]
	// A list of attestations included in the block
	Attestations *solid.ListSSZ[*solid.Attestation]
	// A list of deposits made to the Ethereum 1.0 chain
	Deposits *solid.ListSSZ[*Deposit]
	// A list of validators who have voluntarily exited the beacon chain
	VoluntaryExits *solid.ListSSZ[*SignedVoluntaryExit]
	// A summary of the current state of the beacon chain
	SyncAggregate *SyncAggregate
	// Data related to crosslink records and executing operations on the Ethereum 2.0 chain
	ExecutionPayload *Eth1Block
	// Withdrawals Diffs for Execution Layer
	ExecutionChanges *solid.ListSSZ[*SignedBLSToExecutionChange]
	// The commitments for beacon chain blobs
	// With a max of 4 per block
	BlobKzgCommitments *solid.ListSSZ[*KZGCommitment]
	// The version of the beacon chain
	Version   clparams.StateVersion
	beaconCfg *clparams.BeaconChainConfig
}

// Getters

func NewSignedBeaconBlock(beaconCfg *clparams.BeaconChainConfig) *SignedBeaconBlock {
	return &SignedBeaconBlock{Block: NewBeaconBlock(beaconCfg)}
}

func NewBeaconBlock(beaconCfg *clparams.BeaconChainConfig) *BeaconBlock {
	return &BeaconBlock{Body: NewBeaconBody(beaconCfg)}
}

func NewBeaconBody(beaconCfg *clparams.BeaconChainConfig) *BeaconBody {
	return &BeaconBody{beaconCfg: beaconCfg}
}

// Version returns beacon block version.
func (b *SignedBeaconBlock) Version() clparams.StateVersion {
	return b.Block.Body.Version
}

// Version returns beacon block version.
func (b *SignedBeaconBlock) EncodeForStorage(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.getSchemaForStorage()...)
}

func (b *SignedBeaconBlock) DecodeForStorage(buf []byte, s int) error {
	b.Block.Body.Version = clparams.StateVersion(s)
	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[BeaconBody] err: %s", ssz.ErrLowBufferSize)
	}
	return ssz2.UnmarshalSSZ(buf, s, b.getSchemaForStorage()...)
}

// Version returns beacon block version.
func (b *SignedBeaconBlock) getSchemaForStorage() []interface{} {
	return append([]interface{}{b.Signature[:], &b.Block.Slot, &b.Block.ProposerIndex, b.Block.StateRoot[:], b.Block.ParentRoot[:]},
		b.Block.Body.getSchema(true)...)
}

// Version returns beacon block version.
func (b *BeaconBlock) Version() clparams.StateVersion {
	return b.Body.Version
}

func (b *BeaconBody) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.getSchema(false)...)
}

func (b *BeaconBody) EncodingSizeSSZ() (size int) {

	if b.Eth1Data == nil {
		b.Eth1Data = &Eth1Data{}
	}
	if b.SyncAggregate == nil {
		b.SyncAggregate = &SyncAggregate{}
	}
	if b.ExecutionPayload == nil {
		b.ExecutionPayload = NewEth1Block(b.Version, b.beaconCfg)
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
		b.ExecutionPayload = NewEth1Block(b.Version, b.beaconCfg)
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

func (b *BeaconBody) DecodeSSZ(buf []byte, version int) error {
	b.Version = clparams.StateVersion(version)

	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[BeaconBody] err: %s", ssz.ErrLowBufferSize)
	}

	err := ssz2.UnmarshalSSZ(buf, version, b.getSchema(false)...)
	return err
}

func (b *BeaconBody) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema(false)...)
}

func (b *BeaconBody) getSchema(storage bool) []interface{} {
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

func (b *BeaconBlock) EncodeSSZ(buf []byte) (dst []byte, err error) {
	return ssz2.MarshalSSZ(buf, b.Slot, b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (b *BeaconBlock) EncodingSizeSSZ() int {
	if b.Body == nil {
		return 80
	}
	return 80 + b.Body.EncodingSizeSSZ()
}

func (b *BeaconBlock) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &b.Slot, &b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (b *BeaconBlock) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Slot, b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (b *SignedBeaconBlock) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.Block, b.Signature[:])
}

func (b *SignedBeaconBlock) EncodingSizeSSZ() int {
	if b.Block == nil {
		return 100
	}
	return 100 + b.Block.EncodingSizeSSZ()
}

func (b *SignedBeaconBlock) DecodeSSZ(buf []byte, s int) error {
	return ssz2.UnmarshalSSZ(buf, s, b.Block, b.Signature[:])
}

func (b *SignedBeaconBlock) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Block, b.Signature[:])
}

func (*BeaconBody) Static() bool {
	return false
}

func (*BeaconBlock) Static() bool {
	return false
}
