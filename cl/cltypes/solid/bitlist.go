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
	"errors"
	"math/bits"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

// BitList is like a dynamic binary string. It's like a flipbook of 1s and 0s!
// And just like a flipbook, we can add (Append), remove (Pop), or look at any bit (Get) we want.
type BitList struct {
	// the underlying bytes that store the data
	u []byte
	// cap, or max size of the bitlist
	c int
	// current length of the bitlist
	l int
}

// NewBitList creates a brand new BitList, just like when Zordon created the Power Rangers!
// We make sure to set its length and capacity first.
func NewBitList(l int, c int) *BitList {
	return &BitList{
		u: make([]byte, l+32),
		l: l,
		c: c,
	}
}

// BitlistFromBytes is like getting a new Power Ranger from a civilian - we already have the bits!
func BitlistFromBytes(xs []byte, c int) *BitList {
	return &BitList{
		u: xs,
		l: len(xs),
		c: c,
	}
}

func (u *BitList) GetBitAt(i int) bool {
	if i >= u.Bits() {
		return false
	}
	return u.u[i/8]&(1<<(uint(i)%8)) != 0
}

func (u *BitList) Bytes() []byte {
	return u.u[:u.l]
}

// Clear wipes the BitList clean, just like the memory wipe spell from a particularly forgetful wizard.
func (u *BitList) Clear() {
	u.u = u.u[:0]
	u.l = 0
}

// Static returns false, because BitLists, like Power Rangers, are dynamic!
func (*BitList) Static() bool {
	return false
}

// CopyTo is like a Power Rangers team up episode - we get the bits from another list!
func (u *BitList) CopyTo(target IterableSSZ[byte]) {
	target.Clear()
	for i := 0; i < u.l; i++ {
		target.Append(u.u[i])
	}
}

func (u *BitList) Copy() *BitList {
	n := NewBitList(u.l, u.c)
	n.u = make([]byte, len(u.u), cap(u.u))
	copy(n.u, u.u)
	return n
}

// Range allows us to do something to each bit in the list, just like a Power Rangers roll call.
func (u *BitList) Range(fn func(index int, value byte, length int) bool) {
	for i, v := range u.u {
		fn(i, v, len(u.u))
	}
}

// Pop removes the first bit from the list, like when the Red Ranger takes the first hit.
func (u *BitList) Pop() (x byte) {
	x, u.u = u.u[0], u.u[1:]
	u.l = u.l - 1
	return x
}

// Append is like adding a new Power Ranger to the team - the bitlist gets bigger!
func (u *BitList) Append(v byte) {
	if len(u.u) <= u.l {
		u.u = append(u.u, 0)
	}
	u.u[u.l] = v
	u.l = u.l + 1
}

// Get lets us peek at a bit in the list, like when the team uses their sensors to spot the monster.
func (u *BitList) Get(index int) byte {
	return u.u[index]
}

// Set is like the Red Ranger giving an order - we set a bit to a certain value.
func (u *BitList) Set(index int, v byte) {
	u.u[index] = v
}

// removeMsb removes the most significant bit from the list, but doesn't change the length l.
func (u *BitList) removeMsb() {
	for i := len(u.u) - 1; i >= 0; i-- {
		if u.u[i] != 0 {
			// find last bit, make a mask and clear it
			u.u[i] &= ^(1 << uint(bits.Len8(u.u[i])-1))
			break
		}
	}
}

// addMsb adds a most significant bit to the list, but doesn't change the length l.
func (u *BitList) addMsb() int {
	byteLen := len(u.u)
	found := false
	for i := len(u.u) - 1; i >= 0; i-- {
		if u.u[i] != 0 {
			msb := bits.Len8(u.u[i])
			if msb == 8 {
				if i == len(u.u)-1 {
					u.u = append(u.u, 0)
				}
				byteLen++
				u.u[i+1] |= 1
			} else {
				u.u[i] |= 1 << uint(msb)
			}
			found = true
			break
		}
		byteLen--
	}
	if !found {
		u.u[0] = 1
		byteLen = 1
	}
	u.l = byteLen
	return byteLen
}

// Length gives us the length of the bitlist, just like a roll call tells us how many Rangers there are.
func (u *BitList) Length() int {
	return u.l
}

// Cap gives capacity of the bitlist
func (u *BitList) Cap() int {
	return u.c
}

func (u *BitList) HashSSZ() ([32]byte, error) {
	return merkle_tree.BitlistRootWithLimit(u.u[:u.l], uint64(u.c))
}

// EncodeSSZ appends the underlying byte slice of the BitList to the destination byte slice.
// It returns the resulting byte slice.
func (u *BitList) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, u.u[:u.l]...), nil
}

// DecodeSSZ replaces the underlying byte slice of the BitList with a copy of the input byte slice.
// It then updates the length of the BitList to match the length of the new byte slice.
func (u *BitList) DecodeSSZ(dst []byte, _ int) error {
	u.u = make([]byte, len(dst))
	copy(u.u, dst)
	u.l = len(dst)
	return nil
}

// EncodingSizeSSZ returns the current length of the BitList.
// This is the number of bytes that would be written out when EncodeSSZ is called.
func (u *BitList) EncodingSizeSSZ() int {
	return u.l
}

// Clone creates a new BitList with the same length and capacity as the original.
// Note that the underlying byte slice is not copied.
func (u *BitList) Clone() clonable.Clonable {
	return NewBitList(u.l, u.c)
}

// getBitlistLength return the amount of bits in given bitlist.
func (u *BitList) Bits() int {
	if len(u.u) == 0 {
		return 0
	}
	// The most significant bit is present in the last byte in the array.
	var last byte
	var byteLen int
	for i := len(u.u) - 1; i >= 0; i-- {
		if u.u[i] != 0 {
			last = u.u[i]
			byteLen = i + 1
			break
		}
	}

	// Determine the position of the most significant bit.
	msb := bits.Len8(last)
	if msb == 0 {
		return 0
	}

	// The absolute position of the most significant bit will be the number of
	// bits in the preceding bytes plus the position of the most significant
	// bit. Subtract this value by 1 to determine the length of the bitlist.
	return 8*(byteLen-1) + msb - 1
}

func (u *BitList) MarshalJSON() ([]byte, error) {
	enc, err := u.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	return json.Marshal(hexutil.Bytes(enc))
}

func (u *BitList) UnmarshalJSON(input []byte) error {
	var hex hexutil.Bytes
	if err := json.Unmarshal(input, &hex); err != nil {
		return err
	}
	return u.DecodeSSZ(hex, 0)
}

func (u *BitList) Merge(other *BitList) (*BitList, error) {
	if u.Bits() != other.Bits() {
		log.Warn("bitlist union: different length", "u", u.Bits(), "other", other.Bits())
		return nil, errors.New("bitlist union: different length")
	}
	// copy by the longer one
	var ret, unionFrom *BitList
	ret = other.Copy()
	unionFrom = u
	for i := 0; i < len(unionFrom.u); i++ {
		ret.u[i] |= unionFrom.u[i]
	}
	return ret, nil
}

// BitSlice maintains a slice of bits with underlying byte slice.
// This is just a auxiliary struct for merging BitList.
type BitSlice struct {
	container []byte
	length    int
}

func NewBitSlice() *BitSlice {
	return &BitSlice{
		container: make([]byte, 0),
		length:    0,
	}
}

// AppendBit appends one bit to the BitSlice.
func (b *BitSlice) AppendBit(bit bool) {
	if b.length%8 == 0 {
		b.container = append(b.container, 0)
	}
	if bit {
		b.container[b.length/8] |= 1 << uint(b.length%8)
	}
	b.length++
}

// Bytes returns the underlying byte slice of the BitSlice.
func (b *BitSlice) Bytes() []byte {
	return b.container
}

// Length returns the length of the BitSlice.
func (b *BitSlice) Length() int {
	return b.length
}
