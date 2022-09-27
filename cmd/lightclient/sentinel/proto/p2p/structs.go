package p2p

//req resp

type Slot uint64
type Epoch uint64
type Bitvector64 uint64
type Bitvector4 uint8

type Status struct {
	ForkDigest     []byte ` json:"fork_digest,omitempty" ssz-size:"4"`
	FinalizedRoot  []byte `json:"finalized_root,omitempty" ssz-size:"32"`
	FinalizedEpoch Epoch  `json:"finalized_epoch,omitempty"`
	HeadRoot       []byte `json:"head_root,omitempty" ssz-size:"32"`
	HeadSlot       Slot   `json:"head_slot,omitempty"`
}

type Goodbye struct {
	Reason uint64 `json:"reason"`
}

type Ping struct {
	Id uint64 `json:"id"`
}

type MetadataV0 struct {
	SeqNumber uint64      `json:"seq_number,omitempty"`
	Attnets   Bitvector64 `protobuf:"bytes,2,opt,name=attnets,proto3" json:"attnets,omitempty" cast-type:"github.com/prysmaticlabs/go-bitfield.Bitvector64" ssz-size:"8"`
}

type MetaDataV1 struct {
	SeqNumber uint64      `json:"seq_number,omitempty"`
	Attnets   Bitvector64 `json:"attnets,omitempty" ssz-size:"8"`
	Syncnets  Bitvector64 `json:"syncnets,omitempty" ssz-size:"1"`
}

type ENRForkID struct {
	CurrentForkDigest []byte `json:"current_fork_digest,omitempty" ssz-size:"4"`
	NextForkVersion   []byte `json:"next_fork_version,omitempty" ssz-size:"4"`
	NextForkEpoch     Epoch  `json:"next_fork_epoch,omitempty"`
}
