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
	"github.com/erigontech/erigon/cl/utils"
)

type hashList struct {
	u    []byte
	l, c int

	*merkle_tree.MerkleTree
}

func NewHashList(c int) HashListSSZ {
	return &hashList{
		c: c,
	}
}

func (arr *hashList) Bytes() []byte {
	return arr.u[:arr.l*length.Hash]
}

func (arr *hashList) MarshalJSON() ([]byte, error) {
	list := make([]common.Hash, arr.l)
	for i := 0; i < arr.l; i++ {
		list[i] = arr.Get(i)
	}
	return json.Marshal(list)
}

func (arr *hashList) UnmarshalJSON(buf []byte) error {
	var list []common.Hash

	if err := json.Unmarshal(buf, &list); err != nil {
		return err
	}
	arr.Clear()
	for _, elem := range list {
		arr.Append(elem)
	}
	return nil
}

func (h *hashList) Append(val common.Hash) {
	offset := h.l * length.Hash
	if h.MerkleTree != nil {
		h.MerkleTree.AppendLeaf()
	}
	if offset == len(h.u) {
		h.u = append(h.u, val[:]...)
		h.l++
		return
	}
	copy(h.u[offset:offset+length.Hash], val[:])
	h.l++
}

func (h *hashList) Cap() int {
	return h.c
}

func (h *hashList) Length() int {
	return h.l
}

func (h *hashList) Clear() {
	h.l = 0
	h.MerkleTree = nil
}

func (h *hashList) Clone() clonable.Clonable {
	return NewHashList(h.c)
}

func (h *hashList) CopyTo(t IterableSSZ[common.Hash]) {
	tu := t.(*hashList)
	tu.c = h.c
	tu.l = h.l
	if len(h.u) > len(tu.u) {
		tu.u = make([]byte, len(h.u))
	}
	if h.MerkleTree != nil {
		if tu.MerkleTree == nil {
			tu.MerkleTree = &merkle_tree.MerkleTree{}
		}
		h.MerkleTree.CopyInto(tu.MerkleTree)
		// make the leaf function on the new buffer
		tu.MerkleTree.SetComputeLeafFn(func(idx int, out []byte) {
			copy(out, tu.u[idx*length.Hash:])
		})
	} else {
		tu.MerkleTree = nil
	}
	copy(tu.u, h.u)
}

func (h *hashList) Static() bool {
	return false
}

func (h *hashList) DecodeSSZ(buf []byte, _ int) error {
	if len(buf)%length.Hash > 0 {
		return ssz.ErrBadDynamicLength
	}
	h.MerkleTree = nil
	h.u = common.Copy(buf)
	h.l = len(h.u) / length.Hash
	return nil
}

func (h *hashList) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, h.u[:h.l*length.Hash]...), nil
}

func (h *hashList) EncodingSizeSSZ() int {
	return h.l * length.Hash
}

func (h *hashList) Get(index int) (out common.Hash) {
	if index >= h.l {
		panic("too big bruh")
	}
	copy(out[:], h.u[index*length.Hash:])
	return
}

func (h *hashList) Set(index int, newValue common.Hash) {
	if index >= h.l {
		panic("too big bruh")
	}
	if h.MerkleTree != nil {
		h.MerkleTree.MarkLeafAsDirty(index)
	}
	copy(h.u[index*length.Hash:], newValue[:])
}

func (h *hashList) hashVectorSSZ() ([32]byte, error) {
	if h.MerkleTree == nil {
		cap := uint64(h.c)
		h.MerkleTree = &merkle_tree.MerkleTree{}
		h.MerkleTree.Initialize(h.l, merkle_tree.OptimalMaxTreeCacheDepth, func(idx int, out []byte) {
			copy(out, h.u[idx*length.Hash:(idx+1)*length.Hash])
		}, /*limit=*/ &cap)
	}
	return h.MerkleTree.ComputeRoot(), nil
}

func (h *hashList) HashSSZ() ([32]byte, error) {
	lengthRoot := merkle_tree.Uint64Root(uint64(h.l))
	coreRoot, err := h.hashVectorSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return utils.Sha256(coreRoot[:], lengthRoot[:]), nil
}

func (h *hashList) Range(fn func(int, common.Hash, int) bool) {
	for i := 0; i < h.l; i++ {
		if !fn(i, h.Get(i), h.l) {
			return
		}
	}
}

func (h *hashList) Pop() common.Hash {
	panic("didnt ask, dont need it, go fuck yourself")
}
