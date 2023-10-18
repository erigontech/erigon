package solid

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"math/bits"

	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
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

	hashBuf
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

// Length gives us the length of the bitlist, just like a roll call tells us how many Rangers there are.
func (u *BitList) Length() int {
	return u.l
}

// Cap gives capacity of the bitlist
func (u *BitList) Cap() int {
	return u.c
}

func (u *BitList) HashSSZ() ([32]byte, error) {
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
	return utils.Keccak256(baseRoot[:], lengthRoot[:]), nil
}

func (arr *BitList) getBaseHash(xs []byte, depth uint8) error {
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
func (u *BitList) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, u.u[:u.l]...), nil
}

// DecodeSSZ replaces the underlying byte slice of the BitList with a copy of the input byte slice.
// It then updates the length of the BitList to match the length of the new byte slice.
func (u *BitList) DecodeSSZ(dst []byte, _ int) error {
	u.u = common.CopyBytes(dst)
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
