package p2p

//go:generate go run github.com/ferranbt/fastssz/sszgen -path generated.go -exclude-objs Bitvector4,Bitvector64,Bytea,Epoch,Root,Signature,Slot,Ignore

import (
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
)

type Bitvector4 uint8

type Bitvector64 uint64

type Bytea []byte

type Epoch uint64

type Root [32]byte

type Signature [96]byte

type Slot uint64

type BeaconBlockHeader struct {
	Slot Slot `json:"slot" `

	ProposerIndex Root `ssz-size:"32" json:"proposer_index" `

	ParentRoot Root `json:"parent_root" ssz-size:"32" `

	StateRoot Root `json:"state_root" ssz-size:"32" `

	BodyRoot Root `ssz-size:"32" json:"body_root" `
}

func (typ *BeaconBlockHeader) Clone() proto.Packet {
	return &BeaconBlockHeader{}
}

type ENRForkID struct {
	CurrentForkDigest Bytea `json:"current_fork_digest,omitempty" ssz-size:"4" `

	NextForkVersion Bytea `json:"next_fork_version,omitempty" ssz-size:"4" `

	NextForkEpoch Epoch `json:"next_fork_epoch,omitempty" `
}

func (typ *ENRForkID) Clone() proto.Packet {
	return &ENRForkID{}
}

type Goodbye struct {
	Reason uint64 `json:"reason" `
}

func (typ *Goodbye) Clone() proto.Packet {
	return &Goodbye{}
}

type LightClientBootstrap struct {
	Header BeaconBlockHeader `json:"header" `

	CurrentSyncCommittee SyncCommittee `json:"current_sync_committee" `

	CurrentSyncCommitteeBranch [][32]byte `json:"current_sync_committee" ssz-max:"5" ssz-size:",32" `
}

func (typ *LightClientBootstrap) Clone() proto.Packet {
	return &LightClientBootstrap{}
}

type LightClientFinalityUpdate struct {
	AttestedHeader BeaconBlockHeader `json:"attested_header" `

	FinalizedHeader BeaconBlockHeader `json:"finalized_header" `

	FinalityBranch [][32]byte `json:"finality_branch" ssz-max:"6" ssz-size:",32" `

	SyncAggregate SyncAggregate `json:"sync_aggregate" `

	SignatureSlot Slot `json:"signature_slot" `
}

func (typ *LightClientFinalityUpdate) Clone() proto.Packet {
	return &LightClientFinalityUpdate{}
}

type LightClientOptimisticUpdate struct {
	AttestedHeader BeaconBlockHeader `json:"attested_header" `

	SyncAggregate SyncAggregate `json:"sync_aggregate" `

	SignatureSlot Slot `json:"signature_slot" `
}

func (typ *LightClientOptimisticUpdate) Clone() proto.Packet {
	return &LightClientOptimisticUpdate{}
}

type LightClientUpdate struct {
	AttestedHeader BeaconBlockHeader `json:"attested_header" `

	NextSyncCommittee SyncCommittee `json:"next_sync_committee" `

	NextSyncCommitteeBranch [][32]byte `json:"next_sync_committee" ssz-max:"5" ssz-size:",32" `

	FinalizedHeader BeaconBlockHeader `json:"finalized_header" `

	FinalityBranch [][32]byte `json:"finality_branch" ssz-max:"6" ssz-size:",32" `

	SyncAggregate SyncAggregate `json:"sync_aggregate" `

	SignatureSlot Slot `json:"signature_slot" `
}

func (typ *LightClientUpdate) Clone() proto.Packet {
	return &LightClientUpdate{}
}

type MetaDataV1 struct {
	SeqNumber uint64 `json:"seq_number,omitempty" `

	Attnets Bitvector64 `json:"attnets,omitempty" ssz-size:"8" `

	Syncnets Bitvector64 `json:"syncnets,omitempty" ssz-size:"1" `
}

func (typ *MetaDataV1) Clone() proto.Packet {
	return &MetaDataV1{}
}

type MetadataV0 struct {
	SeqNumber uint64 `json:"seq_number,omitempty" `

	Attnets Bitvector64 `ssz-size:"8" json:"attnets,omitempty" `
}

func (typ *MetadataV0) Clone() proto.Packet {
	return &MetadataV0{}
}

type Ping struct {
	Id uint64 `json:"id" `
}

func (typ *Ping) Clone() proto.Packet {
	return &Ping{}
}

type SingleRoot struct {
	Root Root `json:"root" ssz-size:"32" `
}

func (typ *SingleRoot) Clone() proto.Packet {
	return &SingleRoot{}
}

type Status struct {
	ForkDigest Bytea `json:"fork_digest,omitempty" ssz-size:"4" `

	FinalizedRoot Bytea `json:"finalized_root,omitempty" ssz-size:"32" `

	FinalizedEpoch Epoch `json:"finalized_epoch,omitempty" `

	HeadRoot Bytea `json:"head_root,omitempty" ssz-size:"32" `

	HeadSlot Slot `json:"head_slot,omitempty" `
}

func (typ *Status) Clone() proto.Packet {
	return &Status{}
}

type SyncAggregate struct {
	SyncCommiteeBits [64]byte `json:"sync_committee_bits" ssz-size:"64" `

	SyncCommiteeSignature Signature `json:"sync_committee_signature" ssz-size:"96" `
}

func (typ *SyncAggregate) Clone() proto.Packet {
	return &SyncAggregate{}
}

type SyncCommittee struct {
	PubKeys [512][48]byte `json:"pubkeys" ssz-size:"512,48" `

	AggregatePubKey [48]byte `json:"aggregate_pubkey" ssz-size:"48" `
}

func (typ *SyncCommittee) Clone() proto.Packet {
	return &SyncCommittee{}
}
