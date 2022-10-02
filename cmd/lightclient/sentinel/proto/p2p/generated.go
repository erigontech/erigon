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

	ProposerIndex uint64 `json:"proposer_index" `

	ParentRoot Root `json:"parent_root" ssz-size:"32" `

	StateRoot Root `ssz-size:"32" json:"state_root" `

	BodyRoot Root `json:"body_root" ssz-size:"32" `
}

func (typ *BeaconBlockHeader) Clone() proto.Packet {
	return &BeaconBlockHeader{}
}

type Checkpoint struct {
	Epoch uint64 `json:"epoch" `

	Root Root `ssz-size:"32" json:"root" `
}

func (typ *Checkpoint) Clone() proto.Packet {
	return &Checkpoint{}
}

type ENRForkID struct {
	CurrentForkDigest Bytea `json:"current_fork_digest,omitempty" ssz-size:"4" `

	NextForkVersion Bytea `json:"next_fork_version,omitempty" ssz-size:"4" `

	NextForkEpoch Epoch `json:"next_fork_epoch,omitempty" `
}

func (typ *ENRForkID) Clone() proto.Packet {
	return &ENRForkID{}
}

type ForkData struct {
	CurrentVersion [4]byte `json:"current_version" ssz-size:"4" `

	GenesisValidatorsRoot Root `json:"genesis_validators_root" ssz-size:"32" `
}

func (typ *ForkData) Clone() proto.Packet {
	return &ForkData{}
}

type Goodbye struct {
	Reason uint64 `json:"reason" `
}

func (typ *Goodbye) Clone() proto.Packet {
	return &Goodbye{}
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
	ForkDigest Bytea `ssz-size:"4" json:"fork_digest,omitempty" `

	FinalizedRoot Bytea `json:"finalized_root,omitempty" ssz-size:"32" `

	FinalizedEpoch Epoch `json:"finalized_epoch,omitempty" `

	HeadRoot Bytea `json:"head_root,omitempty" ssz-size:"32" `

	HeadSlot Slot `json:"head_slot,omitempty" `
}

func (typ *Status) Clone() proto.Packet {
	return &Status{}
}
