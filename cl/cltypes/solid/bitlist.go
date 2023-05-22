package solid

import (
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type bitlist struct {
	// the underlying bytes that store the data
	u []byte
	// cap, or max size of the bitlist
	c int
	// current length of the bitlist
	l int

	hashBuf
}

func NewBitList(l int, c int) BitList {
	return &bitlist{
		u: make([]byte, l+32),
		l: l,
		c: c,
	}
}
func BitlistFromBytes(xs []byte, c int) BitList {
	return &bitlist{
		u: xs,
		l: len(xs),
		c: c,
	}
}

func (u *bitlist) Clear() {
	u.u = u.u[:0]
	u.l = 0
}

func (u *bitlist) CopyTo(target BitList) {
	target.Clear()
	for i := 0; i < u.l; i++ {
		target.Append(u.u[i])
	}
}

func (u *bitlist) Range(fn func(index int, value byte, length int) bool) {
	for i, v := range u.u {
		fn(i, v, len(u.u))
	}
}

func (u *bitlist) Pop() (x byte) {
	x, u.u = u.u[0], u.u[1:]
	u.l = u.l - 1
	return x
}

func (u *bitlist) Append(v byte) {
	if len(u.u) <= u.l {
		u.u = append(u.u, 0)
	}
	u.u[u.l] = v
	u.l = u.l + 1
}

func (u *bitlist) Get(index int) byte {
	return u.u[index]
}

func (u *bitlist) Set(index int, v byte) {
	u.u[index] = v
}

func (u *bitlist) Length() int {
	return u.l
}

func (u *bitlist) Cap() int {
	return u.c
}

func (u *bitlist) HashSSZTo(xs []byte) error {
	depth := getDepth((uint64(u.c) + 31) / 32)
	baseRoot := [32]byte{}
	if u.l == 0 {
		copy(baseRoot[:], merkle_tree.ZeroHashes[depth][:])
	} else {
		err := u.getBaseHash(baseRoot[:], depth)
		if err != nil {
			return err
		}
	}
	lengthRoot := merkle_tree.Uint64Root(uint64(u.l))
	ans := utils.Keccak256(baseRoot[:], lengthRoot[:])
	copy(xs, ans[:])
	return nil
}

func (arr *bitlist) getBaseHash(xs []byte, depth uint8) error {
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

func (u *bitlist) EncodeSSZ(dst []byte) []byte {
	buf := dst
	buf = append(buf, u.u[:u.l]...)
	return buf
}
