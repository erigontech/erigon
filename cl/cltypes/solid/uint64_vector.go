// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package solid

import (
	"encoding/json"

	"github.com/erigontech/erigon-lib/types/clonable"
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

func (arr *uint64VectorSSZ) Bytes() []byte {
	return arr.u.u[:arr.u.l*8]
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
