package solid

import (
	"math/bits"

	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
)

type BitList struct {
	// the underlying bytes that store the data
	u []byte
	// cap, or max size of the bitlist
	c int
	// current length of the bitlist
	l int

	hashBuf
}

func NewBitList(l int, c int) *BitList {
	return &BitList{
		u: make([]byte, l+32),
		l: l,
		c: c,
	}
}
func BitlistFromBytes(xs []byte, c int) *BitList {
	return &BitList{
		u: xs,
		l: len(xs),
		c: c,
	}
}

func (u *BitList) Clear() {
	u.u = u.u[:0]
	u.l = 0
}

func (u *BitList) CopyTo(target IterableSSZ[byte]) {
	target.Clear()
	for i := 0; i < u.l; i++ {
		target.Append(u.u[i])
	}
}

func (u *BitList) Range(fn func(index int, value byte, length int) bool) {
	for i, v := range u.u {
		fn(i, v, len(u.u))
	}
}

func (u *BitList) Pop() (x byte) {
	x, u.u = u.u[0], u.u[1:]
	u.l = u.l - 1
	return x
}

func (u *BitList) Append(v byte) {
	if len(u.u) <= u.l {
		u.u = append(u.u, 0)
	}
	u.u[u.l] = v
	u.l = u.l + 1
}

func (u *BitList) Get(index int) byte {
	return u.u[index]
}

func (u *BitList) Set(index int, v byte) {
	u.u[index] = v
}

func (u *BitList) Length() int {
	return u.l
}

func (u *BitList) Cap() int {
	return u.c
}

func (u *BitList) HashSSZ() ([32]byte, error) {
	depth := getDepth((uint64(u.c) + 31) / 32)
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

func (u *BitList) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, u.u[:u.l]...)
	return buf, nil
}

func (u *BitList) DecodeSSZ(dst []byte, _ int) error {
	u.u = common.CopyBytes(u.buf)
	u.l = len(u.buf)
	return nil
}

func (u *BitList) EncodingSizeSSZ() int {
	return u.l
}

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
