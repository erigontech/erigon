package ssz_utils

import (
	"encoding/binary"

	ssz "github.com/ferranbt/fastssz"
)

var (
	BaseExtraDataSSZOffsetHeader = 536
	BaseExtraDataSSZOffsetBlock  = 508
)

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

func MarshalUint64SSZ(buf []byte, x uint64) {
	binary.LittleEndian.PutUint64(buf, x)
}

func Uint64SSZ(x uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, x)
	return b
}

func OffsetSSZ(x uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, x)
	return b
}

// EncodeOffset marshals a little endian uint32 to buf
func EncodeOffset(buf []byte, offset uint32) {
	binary.LittleEndian.PutUint32(buf, offset)
}

// ReadOffset unmarshals a little endian uint32 to dst
func DecodeOffset(x []byte) uint32 {
	return binary.LittleEndian.Uint32(x)
}

func UnmarshalUint64SSZ(x []byte) uint64 {
	return binary.LittleEndian.Uint64(x)
}
