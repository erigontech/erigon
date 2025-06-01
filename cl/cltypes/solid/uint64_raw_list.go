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
	"encoding/binary"
	"encoding/json"
	"strconv"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/utils"
)

type RawUint64List struct {
	u []uint64
	c int

	hahsBuffer []byte
	cachedHash common.Hash
}

func NewRawUint64List(limit int, u []uint64) *RawUint64List {
	return &RawUint64List{
		c: limit,
		u: u,
	}
}

func (arr *RawUint64List) SetCap(c int) {
	arr.c = c
}

func (arr *RawUint64List) Clear() {
	arr.cachedHash = common.Hash{}
	arr.u = arr.u[:0]
}

func (arr *RawUint64List) Append(value uint64) {
	arr.cachedHash = common.Hash{}
	arr.u = append(arr.u, value)
}

func (arr *RawUint64List) Get(index int) uint64 {
	return arr.u[index]
}

func (arr *RawUint64List) Set(index int, v uint64) {
	arr.u[index] = v
}

func (arr *RawUint64List) CopyTo(target IterableSSZ[uint64]) {
	if c, ok := target.(*RawUint64List); ok {
		c.u = append(c.u[:0], arr.u...)
		c.cachedHash = arr.cachedHash
		return
	}
	panic("incompatible type")
}

func (arr *RawUint64List) Range(fn func(index int, value uint64, length int) bool) {
	for i, v := range arr.u {
		cont := fn(i, v, len(arr.u))
		if !cont {
			break
		}
	}
}

func (arr *RawUint64List) Static() bool {
	return false
}

func (arr *RawUint64List) Bytes() []byte {
	out, _ := arr.EncodeSSZ(nil)
	return out
}

func (arr *RawUint64List) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	for _, v := range arr.u {
		dst = binary.LittleEndian.AppendUint64(dst, v)
	}
	return dst, nil
}

func (arr *RawUint64List) DecodeSSZ(buf []byte, _ int) error {
	arr.cachedHash = common.Hash{}
	arr.u = make([]uint64, len(buf)/8)
	for i := range arr.u {
		arr.u[i] = binary.LittleEndian.Uint64(buf[i*8:])
	}
	return nil
}

func (arr *RawUint64List) Length() int {
	return len(arr.u)
}

func (arr *RawUint64List) Cap() int {
	return arr.c
}

func (arr *RawUint64List) Clone() clonable.Clonable {
	return &RawUint64List{}
}

func (arr *RawUint64List) EncodingSizeSSZ() int {
	return len(arr.u) * 8
}

func (arr *RawUint64List) SetReusableHashBuffer(buf []byte) {
	arr.hahsBuffer = buf
}

func (arr *RawUint64List) hashBufLength() int {
	return ((len(arr.u) + 3) / 4) * length.Hash
}

func (arr *RawUint64List) HashSSZ() ([32]byte, error) {
	if arr.cachedHash != (common.Hash{}) {
		return arr.cachedHash, nil
	}
	if cap(arr.hahsBuffer) < arr.hashBufLength() {
		arr.hahsBuffer = make([]byte, 0, arr.hashBufLength())
	}
	depth := GetDepth((uint64(arr.c)*8 + 31) / 32)

	lnRoot := merkle_tree.Uint64Root(uint64(len(arr.u)))
	if len(arr.u) == 0 {
		arr.cachedHash = utils.Sha256(merkle_tree.ZeroHashes[depth][:], lnRoot[:])
		return arr.cachedHash, nil
	}

	arr.hahsBuffer = arr.hahsBuffer[:arr.hashBufLength()]
	for i, v := range arr.u {
		binary.LittleEndian.PutUint64(arr.hahsBuffer[i*8:], v)
	}

	elements := arr.hahsBuffer
	for i := 0; i < int(depth); i++ {
		layerLen := len(elements)
		if layerLen%64 == 32 {
			elements = append(elements, merkle_tree.ZeroHashes[i][:]...)
		}
		outputLen := len(elements) / 2
		if err := merkle_tree.HashByteSlice(elements, elements); err != nil {
			return [32]byte{}, err
		}
		elements = elements[:outputLen]
	}

	arr.cachedHash = utils.Sha256(elements[:32], lnRoot[:])
	return arr.cachedHash, nil
}

func (arr *RawUint64List) Pop() uint64 {
	panic("k")
}

func (arr *RawUint64List) MarshalJSON() ([]byte, error) {
	// convert it to a list of strings
	strs := make([]string, len(arr.u))
	for i, v := range arr.u {
		strs[i] = strconv.FormatUint(v, 10)
	}
	return json.Marshal(strs)
}

func (arr *RawUint64List) UnmarshalJSON(data []byte) error {
	var strs []string
	if err := json.Unmarshal(data, &strs); err != nil {
		return err
	}
	arr.u = make([]uint64, len(strs))
	for i, s := range strs {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		arr.u[i] = v
	}
	return nil
}
