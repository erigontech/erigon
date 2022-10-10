package cltypes

type MetadataV1 struct {
	SeqNumber uint64
	Attnets   uint64
}

type MetadataV2 struct {
	SeqNumber uint64 `protobuf:"varint,1,opt,name=SeqNumber,json=seq_number,proto3" json:"SeqNumber,omitempty"`
	Attnets   uint64 `protobuf:"varint,2,opt,name=Attnets,json=attnets,proto3" json:"Attnets,omitempty"`
	Syncnets  uint64 `protobuf:"varint,3,opt,name=Syncnets,json=syncnets,proto3" json:"Syncnets,omitempty"`
}

type Checkpoint struct {
	Epoch uint64
	Root  [32]byte `ssz-size:"32" `
}

type ENRForkID struct {
	CurrentForkDigest [4]byte `ssz-size:"4" `
	NextForkVersion   [4]byte `ssz-size:"4" `
	NextForkEpoch     uint64
}

type ForkData struct {
	CurrentVersion        [4]byte  `ssz-size:"4" `
	GenesisValidatorsRoot [32]byte `ssz-size:"32" `
}

type Ping struct {
	Id uint64 `json:"id" `
}

type SingleRoot struct {
	Root     [32]byte `ssz-size:"32" `
	BodyRoot [32]byte `ssz-size:"32" `
}

type Status struct {
	ForkDigest     [4]byte  `ssz-size:"4" `
	FinalizedRoot  [32]byte `ssz-size:"32" `
	FinalizedEpoch uint64
	HeadRoot       [32]byte `ssz-size:"32" `
	HeadSlot       uint64
}
