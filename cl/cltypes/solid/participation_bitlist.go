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
	"math/bits"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/utils"
)

// ParticipationBitList is like a dynamic binary string. It's like a flipbook of 1s and 0s!
// And just like a flipbook, we can add (Append), remove (Pop), or look at any bit (Get) we want.
type ParticipationBitList struct {
	// the underlying bytes that store the data
	u []byte
	// cap, or max size of the bitlist
	c int
	// current length of the bitlist
	l int

	hashBuf
}

// NewBitList creates a brand new BitList, just like when Zordon created the Power Rangers!
// We make sure to set its length and capacity first.
func NewParticipationBitList(l int, c int) *ParticipationBitList {
	return &ParticipationBitList{
		u: make([]byte, l+32),
		l: l,
		c: c,
	}
}

// BitlistFromBytes is like getting a new Power Ranger from a civilian - we already have the bits!
func ParticipationBitListFromBytes(xs []byte, c int) *ParticipationBitList {
	return &ParticipationBitList{
		u: xs,
		l: len(xs),
		c: c,
	}
}

func (u *ParticipationBitList) Bytes() []byte {
	return u.u[:u.l]
}

// Clear wipes the BitList clean, just like the memory wipe spell from a particularly forgetful wizard.
func (u *ParticipationBitList) Clear() {
	u.u = u.u[:0]
	u.l = 0
}

// Static returns false, because BitLists, like Power Rangers, are dynamic!
func (*ParticipationBitList) Static() bool {
	return false
}

// CopyTo is like a Power Rangers team up episode - we get the bits from another list!
func (u *ParticipationBitList) CopyTo(target IterableSSZ[byte]) {
	target.Clear()
	for i := 0; i < u.l; i++ {
		target.Append(u.u[i])
	}
}

func (u *ParticipationBitList) Copy() *ParticipationBitList {
	n := NewParticipationBitList(u.l, u.c)
	n.u = make([]byte, len(u.u), cap(u.u))
	copy(n.u, u.u)
	return n
}

// Range allows us to do something to each bit in the list, just like a Power Rangers roll call.
func (u *ParticipationBitList) Range(fn func(index int, value byte, length int) bool) {
	for i, v := range u.u {
		fn(i, v, len(u.u))
	}
}

// Pop removes the first bit from the list, like when the Red Ranger takes the first hit.
func (u *ParticipationBitList) Pop() (x byte) {
	x, u.u = u.u[0], u.u[1:]
	u.l = u.l - 1
	return x
}

// Append is like adding a new Power Ranger to the team - the bitlist gets bigger!
func (u *ParticipationBitList) Append(v byte) {
	if len(u.u) <= u.l {
		u.u = append(u.u, 0)
	}
	u.u[u.l] = v
	u.l = u.l + 1
}

// Get lets us peek at a bit in the list, like when the team uses their sensors to spot the monster.
func (u *ParticipationBitList) Get(index int) byte {
	return u.u[index]
}

// Set is like the Red Ranger giving an order - we set a bit to a certain value.
func (u *ParticipationBitList) Set(index int, v byte) {
	u.u[index] = v
}

// Length gives us the length of the bitlist, just like a roll call tells us how many Rangers there are.
func (u *ParticipationBitList) Length() int {
	return u.l
}

// Cap gives capacity of the bitlist
func (u *ParticipationBitList) Cap() int {
	return u.c
}

func (u *ParticipationBitList) HashSSZ() ([32]byte, error) {
	depth := GetDepth((uint64(u.c) + 31) / 32)
	baseRoot := [32]byte{}
	if u.l == 0 {
		copy(baseRoot[:], merkle_tree.ZeroHashes[depth][:])
	} else {
		err := u.getBaseHash(baseRoot[:], depth)
		if err != nil {
			return baseRoot, err
		}
	}
	lengthRoot := merkle_tree.Uint64Root(uint64(u.l))
	return utils.Sha256(baseRoot[:], lengthRoot[:]), nil
}

func (arr *ParticipationBitList) getBaseHash(xs []byte, depth uint8) error {
	elements := arr.u
	offset := 32*(arr.l/32) + 32
	if len(arr.u) <= offset {
		elements = append(elements, make([]byte, offset-len(arr.u)+1)...)
	}
	elements = elements[:offset]
	for i := uint8(0); i < depth; i++ {
		// Sequential
		layerLen := len(elements)
		if layerLen%64 == 32 {
			elements = append(elements, merkle_tree.ZeroHashes[i][:]...)
		}
		outputLen := len(elements) / 2
		arr.makeBuf(outputLen)
		if err := merkle_tree.HashByteSlice(arr.buf, elements); err != nil {
			return err
		}
		elements = arr.buf
	}
	copy(xs, elements[:32])
	return nil
}

// EncodeSSZ appends the underlying byte slice of the BitList to the destination byte slice.
// It returns the resulting byte slice.
func (u *ParticipationBitList) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, u.u[:u.l]...), nil
}

// DecodeSSZ replaces the underlying byte slice of the BitList with a copy of the input byte slice.
// It then updates the length of the BitList to match the length of the new byte slice.
func (u *ParticipationBitList) DecodeSSZ(dst []byte, _ int) error {
	u.u = make([]byte, len(dst))
	copy(u.u, dst)
	u.l = len(dst)
	return nil
}

// EncodingSizeSSZ returns the current length of the BitList.
// This is the number of bytes that would be written out when EncodeSSZ is called.
func (u *ParticipationBitList) EncodingSizeSSZ() int {
	return u.l
}

// Clone creates a new BitList with the same length and capacity as the original.
// Note that the underlying byte slice is not copied.
func (u *ParticipationBitList) Clone() clonable.Clonable {
	return NewParticipationBitList(u.l, u.c)
}

// getBitlistLength return the amount of bits in given bitlist.
func (u *ParticipationBitList) Bits() int {
	if len(u.u) == 0 {
		return 0
	}
	// The most significant bit is present in the last byte in the array.
	last := u.u[u.l-1]

	// Determine the position of the most significant bit.
	msb := bits.Len8(last)
	if msb == 0 {
		return 0
	}

	// The absolute position of the most significant bit will be the number of
	// bits in the preceding bytes plus the position of the most significant
	// bit. Subtract this value by 1 to determine the length of the bitlist.
	return 8*(u.l-1) + msb - 1
}

func (u *ParticipationBitList) MarshalJSON() ([]byte, error) {
	enc, err := u.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	return json.Marshal(hexutil.Bytes(enc))
}

func (u *ParticipationBitList) UnmarshalJSON(input []byte) error {
	var hex hexutil.Bytes
	if err := json.Unmarshal(input, &hex); err != nil {
		return err
	}
	return u.DecodeSSZ(hex, 0)
}
