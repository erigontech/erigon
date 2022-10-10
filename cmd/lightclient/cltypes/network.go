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
