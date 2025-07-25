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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

type hashVector struct {
	u *hashList
}

func NewHashVector(s int) HashVectorSSZ {
	return &hashVector{
		u: &hashList{
			u: make([]byte, s*length.Hash),
			c: int(merkle_tree.NextPowerOfTwo(uint64(s))),
			l: s,
		},
	}
}

func (arr *hashVector) Bytes() []byte {
	return arr.u.u[:arr.u.l*length.Hash]
}

func (h *hashVector) Append(val common.Hash) {
	panic("not implemented")
}

func (h hashVector) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.u)
}

func (h *hashVector) UnmarshalJSON(buf []byte) error {
	return json.Unmarshal(buf, h.u)
}

func (h *hashVector) Cap() int {
	return h.u.l
}

func (h *hashVector) Length() int {
	return h.u.l
}

func (h *hashVector) Clear() {
	panic("not implemented")
}

func (h *hashVector) Clone() clonable.Clonable {
	return NewHashVector(h.u.l)
}

func (h *hashVector) CopyTo(t IterableSSZ[common.Hash]) {
	tu := t.(*hashVector)
	h.u.CopyTo(tu.u)
}

func (h *hashVector) Static() bool {
	return true
}

func (h *hashVector) DecodeSSZ(buf []byte, version int) error {
	if len(buf) < h.Length()*length.Hash {
		return ssz.ErrBadDynamicLength
	}
	h.u.MerkleTree = nil
	copy(h.u.u, buf)
	return nil
}

func (h *hashVector) EncodeSSZ(buf []byte) ([]byte, error) {
	return h.u.EncodeSSZ(buf)
}

func (h *hashVector) EncodingSizeSSZ() int {
	return h.u.EncodingSizeSSZ()
}

func (h *hashVector) Get(index int) (out common.Hash) {
	return h.u.Get(index)
}

func (h *hashVector) Set(index int, newValue common.Hash) {
	h.u.Set(index, newValue)
}

func (h *hashVector) HashSSZ() ([32]byte, error) {
	return h.u.hashVectorSSZ()
}

func (h *hashVector) Range(fn func(int, common.Hash, int) bool) {
	h.u.Range(fn)
}

func (h *hashVector) Pop() common.Hash {
	panic("didnt ask, dont need it, go fuck yourself")
}
