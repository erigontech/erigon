package solid

import (
	"github.com/ledgerwatch/erigon-lib/types/clonable"
)

type uint64ListSSZ struct {
	u *byteBasedUint64Slice
}

func NewUint64ListSSZ(limit int) Uint64ListSSZ {
	return &uint64ListSSZ{
		u: NewUint64Slice(limit),
	}
}

func (arr *uint64ListSSZ) Clear() {
	arr.u.Clear()
}

func (arr *uint64ListSSZ) CopyTo(target IterableSSZ[uint64]) {
	if c, ok := target.(*uint64ListSSZ); ok {
		arr.u.CopyTo(c.u)
		return
	}
	panic("incompatible type")
}

func (arr *uint64ListSSZ) Range(fn func(index int, value uint64, length int) bool) {
	arr.u.Range(fn)
}

func (arr *uint64ListSSZ) Get(index int) uint64 {
	return arr.u.Get(index)
}

func (arr *uint64ListSSZ) Set(index int, v uint64) {
	arr.u.Set(index, v)
}

func (arr *uint64ListSSZ) Length() int {
	return arr.u.Length()
}

func (arr *uint64ListSSZ) Cap() int {
	return arr.u.Cap()
}

func (arr *uint64ListSSZ) HashSSZ() ([32]byte, error) {
	return arr.u.HashListSSZ()
}

func (arr *uint64ListSSZ) Clone() clonable.Clonable {
	return NewUint64ListSSZ(arr.Cap())
}

func (arr *uint64ListSSZ) EncodeSSZ(buf []byte) (dst []byte, err error) {
	return arr.u.EncodeSSZ(buf)
}

func (arr *uint64ListSSZ) DecodeSSZ(buf []byte, version int) error {
	return arr.u.DecodeSSZ(buf, version)
}

func (arr *uint64ListSSZ) EncodingSizeSSZ() int {
	return arr.u.EncodingSizeSSZ()
}

func (arr *uint64ListSSZ) Pop() uint64 {
	return arr.u.Pop()
}

func (arr *uint64ListSSZ) Append(v uint64) {
	arr.u.Append(v)
}
