package solid

import (
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/types/clonable"
)

type uint64VectorSSZ struct {
	u *byteBasedUint64Slice
}

func NewUint64VectorSSZ(size int) Uint64VectorSSZ {
	o := &byteBasedUint64Slice{
		c: size,
		l: size,
		u: make([]byte, size*8),
	}
	return &uint64VectorSSZ{
		u: o,
	}
}

func (h uint64VectorSSZ) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.u)
}

func (h *uint64VectorSSZ) UnmarshalJSON(buf []byte) error {
	return json.Unmarshal(buf, h.u)
}

func (arr *uint64VectorSSZ) Clear() {
	arr.u.Clear()
}

func (*uint64VectorSSZ) Static() bool {
	return true
}

func (arr *uint64VectorSSZ) CopyTo(target IterableSSZ[uint64]) {
	c := target.(*uint64VectorSSZ)
	arr.u.CopyTo(c.u)
}

func (arr *uint64VectorSSZ) Range(fn func(index int, value uint64, length int) bool) {
	arr.u.Range(fn)
}

func (arr *uint64VectorSSZ) Get(index int) uint64 {
	return arr.u.Get(index)
}

func (arr *uint64VectorSSZ) Set(index int, v uint64) {
	arr.u.Set(index, v)
}

func (arr *uint64VectorSSZ) Length() int {
	return arr.u.Length()
}

func (arr *uint64VectorSSZ) Cap() int {
	return arr.u.Cap()
}

func (arr *uint64VectorSSZ) HashSSZ() ([32]byte, error) {
	return arr.u.HashVectorSSZ()
}

func (arr *uint64VectorSSZ) Clone() clonable.Clonable {
	return NewUint64VectorSSZ(arr.Length())
}

func (arr *uint64VectorSSZ) EncodeSSZ(buf []byte) (dst []byte, err error) {
	return arr.u.EncodeSSZ(buf)
}

func (arr *uint64VectorSSZ) DecodeSSZ(buf []byte, version int) error {
	return arr.u.DecodeSSZ(buf[:arr.Length()*8], version)
}

func (arr *uint64VectorSSZ) EncodingSizeSSZ() int {
	return arr.u.EncodingSizeSSZ()
}

func (arr *uint64VectorSSZ) Pop() uint64 {
	panic("not implemented")
}

func (arr *uint64VectorSSZ) Append(uint64) {
	panic("not implemented")
}
