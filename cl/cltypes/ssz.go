package cltypes

import ssz "github.com/ferranbt/fastssz"

type ObjectSSZ interface {
	ssz.Marshaler
	ssz.Unmarshaler

	HashTreeRoot() ([32]byte, error)
}

type EncodableSSZ interface {
	Marshaler
	Unmarshaler
}

type Marshaler interface {
	MarshalSSZ() ([]byte, error)
	SizeSSZ() int
}

type Unmarshaler interface {
	UnmarshalSSZ(buf []byte) error
}
