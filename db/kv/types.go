package kv

import (
	"encoding/binary"
)

// canonical sequence number of entity (in context)
type Num uint64

// canonical sequence number of the root entity (or secondary key)
type RootNum uint64

type EncToBytesI interface {
	EncToBytes(enc8Bytes bool) []byte
}

func (n Num) Uint64() uint64 {
	return uint64(n)
}

func (x Num) EncToBytes(x8Bytes bool) (out []byte) {
	return EncToBytes(x, x8Bytes)
}
func (x Num) EncTo8Bytes() (out []byte) {
	return EncToBytes(x, true)
}
func (x RootNum) EncTo8Bytes() (out []byte) {
	return EncToBytes(x, true)
}

func (x RootNum) Uint64() uint64 {
	return uint64(x)
}

func EncToBytes[T ~uint64](x T, x8Bytes bool) (out []byte) {
	if x8Bytes {
		out = make([]byte, 8)
		binary.BigEndian.PutUint64(out, uint64(x))
	} else {
		out = make([]byte, 4)
		binary.BigEndian.PutUint32(out, uint32(x))
	}
	return
}

func Decode64FromBytes(buf []byte, x8Bytes bool) (x uint64) {
	if x8Bytes {
		x = binary.BigEndian.Uint64(buf)
	} else {
		x = uint64(binary.BigEndian.Uint32(buf))
	}
	return
}
