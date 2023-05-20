package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

const (
	MaxAttesterSlashings = 2
	MaxProposerSlashings = 16
	MaxAttestations      = 128
	MaxDeposits          = 16
	MaxVoluntaryExits    = 16
	MaxExecutionChanges  = 16
	MaxBlobsPerBlock     = 4
)

func getBeaconBlockMinimumSize(v clparams.StateVersion) (size uint32) {
	switch v {
	case clparams.DenebVersion:
		size = 392
	case clparams.CapellaVersion:
		size = 388
	case clparams.BellatrixVersion:
		size = 384
	case clparams.AltairVersion:
		size = 380
	case clparams.Phase0Version:
		size = 220
	default:
		panic("unimplemented version")
	}
	return
}

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
	Version clparams.StateVersion
}

// Getters

// Version returns beacon block version.
func (b *SignedBeaconBlock) Version() clparams.StateVersion {
	return b.Block.Body.Version
}

// Version returns beacon block version.
func (b *BeaconBlock) Version() clparams.StateVersion {
	return b.Body.Version
}

func (b *BeaconBody) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	var err error
	//start := len(buf)
	offset := getBeaconBlockMinimumSize(b.Version)
	// Write "easy" fields
	buf = append(buf, b.RandaoReveal[:]...)
	if buf, err = b.Eth1Data.EncodeSSZ(buf); err != nil {
		return nil, err
	}
	if len(b.Graffiti) != 32 {
		return nil, fmt.Errorf("bad graffiti length")
	}
	buf = append(buf, b.Graffiti[:]...)
	// Write offsets for proposer slashings
	buf = append(buf, ssz.OffsetSSZ(offset)...)
	offset += uint32(b.ProposerSlashings.Len()) * 416
	// Attester slashings offset
	buf = append(buf, ssz.OffsetSSZ(offset)...)
	offset += uint32(b.AttesterSlashings.EncodingSizeSSZ())
	// Attestation offset
	buf = append(buf, ssz.OffsetSSZ(offset)...)
	offset += uint32(b.Attestations.EncodingSizeSSZ())
	// Deposits offset
	buf = append(buf, ssz.OffsetSSZ(offset)...)
	offset += uint32(b.Deposits.EncodingSizeSSZ())
	// Voluntary Exit offset
	buf = append(buf, ssz.OffsetSSZ(offset)...)
	offset += uint32(b.VoluntaryExits.EncodingSizeSSZ())
	// Encode Sync Aggregate
	if b.Version >= clparams.AltairVersion {
		if buf, err = b.SyncAggregate.EncodeSSZ(buf); err != nil {
			return nil, err
		}
	}
	if b.Version >= clparams.BellatrixVersion {
		buf = append(buf, ssz.OffsetSSZ(offset)...)
		offset += uint32(b.ExecutionPayload.EncodingSizeSSZ())
	}
	if b.Version >= clparams.CapellaVersion {
		buf = append(buf, ssz.OffsetSSZ(offset)...)
		offset += uint32(b.ExecutionChanges.EncodingSizeSSZ())
	}

	if b.Version >= clparams.DenebVersion {
		buf = append(buf, ssz.OffsetSSZ(offset)...)
	}

	// Now start encoding the rest of the fields.
	if b.AttesterSlashings.Len() > MaxAttesterSlashings {
		return nil, fmt.Errorf("Encode(SSZ): too many attester slashings")
	}
	if b.ProposerSlashings.Len() > MaxProposerSlashings {
		return nil, fmt.Errorf("Encode(SSZ): too many proposer slashings")
	}
	if b.Attestations.Len() > MaxAttestations {
		return nil, fmt.Errorf("Encode(SSZ): too many attestations")
	}
	if b.Deposits.Len() > MaxDeposits {
		return nil, fmt.Errorf("Encode(SSZ): too many attestations")
	}
	if b.VoluntaryExits.Len() > MaxVoluntaryExits {
		return nil, fmt.Errorf("Encode(SSZ): too many attestations")
	}
	if b.ExecutionChanges.Len() > MaxExecutionChanges {
		return nil, fmt.Errorf("Encode(SSZ): too many changes")
	}
	if b.BlobKzgCommitments.Len() > MaxBlobsPerBlock {
		return nil, fmt.Errorf("Encode(SSZ): too many blob kzg commitments in the block")
	}
	// Write proposer slashings
	if buf, err = b.ProposerSlashings.EncodeSSZ(buf); err != nil {
		return nil, err
	}
	// Write attester slashings as a dynamic list.
	if buf, err = b.AttesterSlashings.EncodeSSZ(buf); err != nil {
		return nil, err
	}

	if buf, err = b.Attestations.EncodeSSZ(buf); err != nil {
		return nil, err
	}

	if buf, err = b.Deposits.EncodeSSZ(buf); err != nil {
		return nil, err
	}

	if buf, err = b.VoluntaryExits.EncodeSSZ(buf); err != nil {
		return nil, err
	}

	if b.Version >= clparams.BellatrixVersion {
		buf, err = b.ExecutionPayload.EncodeSSZ(buf)
		if err != nil {
			return nil, err
		}
	}

	if b.Version >= clparams.CapellaVersion {
		if buf, err = b.ExecutionChanges.EncodeSSZ(buf); err != nil {
			return nil, err
		}
	}

	if b.Version >= clparams.DenebVersion {
		if buf, err = b.BlobKzgCommitments.EncodeSSZ(buf); err != nil {
			return nil, err
		}
	}

	return buf, nil
}

func (b *BeaconBody) EncodingSizeSSZ() (size int) {
	size = int(getBeaconBlockMinimumSize(b.Version))

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
		b.ExecutionPayload = new(Eth1Block)
	}
	if b.ExecutionChanges == nil {
		b.ExecutionChanges = solid.NewStaticListSSZ[*SignedBLSToExecutionChange](MaxExecutionChanges, 172)
	}
	if b.BlobKzgCommitments == nil {
		b.BlobKzgCommitments = solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsPerBlock, 48)
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
	var err error

	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[BeaconBody] err: %s", ssz.ErrLowBufferSize)
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
		b.ExecutionPayload = new(Eth1Block)
	}
	if b.ExecutionChanges == nil {
		b.ExecutionChanges = solid.NewStaticListSSZ[*SignedBLSToExecutionChange](MaxExecutionChanges, 172)
	}
	if b.BlobKzgCommitments == nil {
		b.BlobKzgCommitments = solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsPerBlock, 48)
	}

	// Start wildly decoding this thing
	copy(b.RandaoReveal[:], buf)
	// Decode ethereum 1 data.
	b.Eth1Data = new(Eth1Data)
	if err := b.Eth1Data.DecodeSSZ(buf[96:168], version); err != nil {
		return err
	}
	// Decode graffiti.
	copy(b.Graffiti[:], buf[168:200])

	// Decode offsets
	offSetProposerSlashings := ssz.DecodeOffset(buf[200:])
	offsetAttesterSlashings := ssz.DecodeOffset(buf[204:])
	offsetAttestations := ssz.DecodeOffset(buf[208:])
	offsetDeposits := ssz.DecodeOffset(buf[212:])
	offsetExits := ssz.DecodeOffset(buf[216:])
	// Decode sync aggregate if we are past altair.
	if b.Version >= clparams.AltairVersion {
		if len(buf) < 380 {
			return fmt.Errorf("[BeaconBody] altair version err: %s", ssz.ErrLowBufferSize)
		}
		b.SyncAggregate = new(SyncAggregate)
		if err := b.SyncAggregate.DecodeSSZ(buf[220:380], version); err != nil {
			return fmt.Errorf("[BeaconBody] err: %s", err)
		}
	}

	// Execution Payload offset if past bellatrix.
	var offsetExecution uint32
	if b.Version >= clparams.BellatrixVersion {
		offsetExecution = ssz.DecodeOffset(buf[380:])
	}
	// Execution to BLS changes
	var blsChangesOffset uint32
	if b.Version >= clparams.CapellaVersion {
		blsChangesOffset = ssz.DecodeOffset(buf[384:])
	}

	var blobKzgCommitmentOffset uint32
	if b.Version >= clparams.DenebVersion {
		blobKzgCommitmentOffset = ssz.DecodeOffset(buf[388:])
	}

	// Decode Proposer slashings
	if err = b.ProposerSlashings.DecodeSSZ(buf[offSetProposerSlashings:offsetAttesterSlashings], version); err != nil {
		return err
	}
	if err = b.AttesterSlashings.DecodeSSZ(buf[offsetAttesterSlashings:offsetAttestations], version); err != nil {
		return err
	}

	// Decode attestations
	if err = b.Attestations.DecodeSSZ(buf[offsetAttestations:offsetDeposits], version); err != nil {
		return err
	}
	// Decode deposits
	if err = b.Deposits.DecodeSSZ(buf[offsetDeposits:offsetExits], version); err != nil {
		return err
	}
	// Decode exits
	endOffset := len(buf)
	if b.Version >= clparams.BellatrixVersion {
		endOffset = int(offsetExecution)
	}
	if err = b.VoluntaryExits.DecodeSSZ(buf[offsetExits:endOffset], version); err != nil {
		return err
	}

	endOffset = len(buf)
	if b.Version >= clparams.CapellaVersion {
		endOffset = int(blsChangesOffset)
	}
	if b.Version >= clparams.BellatrixVersion {
		b.ExecutionPayload = new(Eth1Block)
		if offsetExecution > uint32(endOffset) || len(buf) < endOffset {
			return fmt.Errorf("[BeaconBody] err: %s", ssz.ErrBadOffset)
		}
		if err := b.ExecutionPayload.DecodeSSZ(buf[offsetExecution:endOffset], version); err != nil {
			return fmt.Errorf("[BeaconBody] err: %s", err)
		}
	}
	endOffset = len(buf)
	if b.Version >= clparams.DenebVersion {
		endOffset = int(blobKzgCommitmentOffset)
	}
	if b.Version >= clparams.CapellaVersion {
		if err = b.ExecutionChanges.DecodeSSZ(buf[blsChangesOffset:endOffset], version); err != nil {
			return err
		}
	}

	if b.Version >= clparams.DenebVersion {
		if err = b.BlobKzgCommitments.DecodeSSZ(buf[blobKzgCommitmentOffset:len(buf)], version); err != nil {
			return err
		}
	}

	return nil
}

func (b *BeaconBody) HashSSZ() ([32]byte, error) {
	switch b.Version {
	case clparams.Phase0Version:
		return merkle_tree.HashTreeRoot(b.RandaoReveal[:], b.Eth1Data, b.Graffiti[:], b.ProposerSlashings, b.AttesterSlashings,
			b.Attestations, b.Deposits, b.VoluntaryExits)
	case clparams.AltairVersion:
		return merkle_tree.HashTreeRoot(b.RandaoReveal[:], b.Eth1Data, b.Graffiti[:], b.ProposerSlashings, b.AttesterSlashings,
			b.Attestations, b.Deposits, b.VoluntaryExits, b.SyncAggregate)
	case clparams.BellatrixVersion:
		return merkle_tree.HashTreeRoot(b.RandaoReveal[:], b.Eth1Data, b.Graffiti[:], b.ProposerSlashings, b.AttesterSlashings,
			b.Attestations, b.Deposits, b.VoluntaryExits, b.SyncAggregate, b.ExecutionPayload)
	case clparams.CapellaVersion:
		return merkle_tree.HashTreeRoot(b.RandaoReveal[:], b.Eth1Data, b.Graffiti[:], b.ProposerSlashings, b.AttesterSlashings,
			b.Attestations, b.Deposits, b.VoluntaryExits, b.SyncAggregate, b.ExecutionPayload, b.ExecutionChanges)
	case clparams.DenebVersion:
		return merkle_tree.HashTreeRoot(b.RandaoReveal[:], b.Eth1Data, b.Graffiti[:], b.ProposerSlashings, b.AttesterSlashings,
			b.Attestations, b.Deposits, b.VoluntaryExits, b.SyncAggregate, b.ExecutionPayload, b.ExecutionChanges, b.BlobKzgCommitments)
	default:
		panic("rust is delusional")
	}
}

func (b *BeaconBlock) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	// Encode base params
	dst = append(dst, ssz.Uint64SSZ(b.Slot)...)
	dst = append(dst, ssz.Uint64SSZ(b.ProposerIndex)...)
	dst = append(dst, b.ParentRoot[:]...)
	dst = append(dst, b.StateRoot[:]...)
	// Encode body
	dst = append(dst, ssz.OffsetSSZ(84)...)
	if dst, err = b.Body.EncodeSSZ(dst); err != nil {
		return
	}

	return
}

func (b *BeaconBlock) EncodingSizeSSZ() int {
	if b.Body == nil {
		b.Body = new(BeaconBody)
	}
	return 80 + b.Body.EncodingSizeSSZ()
}

func (b *BeaconBlock) DecodeSSZ(buf []byte, version int) error {
	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[BeaconBlock] err: %s", ssz.ErrLowBufferSize)
	}
	b.Slot = ssz.UnmarshalUint64SSZ(buf)
	b.ProposerIndex = ssz.UnmarshalUint64SSZ(buf[8:])
	copy(b.ParentRoot[:], buf[16:])
	copy(b.StateRoot[:], buf[48:])
	b.Body = new(BeaconBody)
	return b.Body.DecodeSSZ(buf[84:], version)
}

func (b *BeaconBlock) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Slot, b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (b *SignedBeaconBlock) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	var err error
	dst = append(dst, ssz.OffsetSSZ(100)...)
	dst = append(dst, b.Signature[:]...)
	dst, err = b.Block.EncodeSSZ(dst)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func (b *SignedBeaconBlock) EncodingSizeSSZ() int {
	if b.Block == nil {
		b.Block = new(BeaconBlock)
	}
	return 100 + b.Block.EncodingSizeSSZ()
}

func (b *SignedBeaconBlock) DecodeSSZ(buf []byte, s int) error {
	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[SignedBeaconBlock] err: %s", ssz.ErrLowBufferSize)
	}
	copy(b.Signature[:], buf[4:100])
	return b.Block.DecodeSSZ(buf[100:], s)
}

func (b *SignedBeaconBlock) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Block, b.Signature[:])
}
