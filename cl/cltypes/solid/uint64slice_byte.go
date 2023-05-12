package solid

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type byteBasedUint64Slice struct {
	// the bytes that back the slice
	u []byte

	// length of slice
	l int // len
	// cap of slize
	c int // cap

	hashBuf
}

func NewUint64Slice(limit int) Uint64Slice {
	o := &byteBasedUint64Slice{
		c: limit,
	}
	o.u = make([]byte, 0)
	return o
}

func (arr *byteBasedUint64Slice) Clear() {
	arr.l = 0
	for i := range arr.u {
		arr.u[i] = 0
	}
}

func (arr *byteBasedUint64Slice) CopyTo(target Uint64Slice) {
	target.Clear()
	switch c := target.(type) {
	case *byteBasedUint64Slice:
		c.c = arr.c
		c.l = arr.l
		if len(c.u) < len(arr.u) {
			c.u = make([]byte, len(arr.u))
		}
		c.u = c.u[:len(arr.u)]
		copy(c.u, arr.u)
		return
	default:
	}
	target.Range(func(index int, value uint64, length int) bool {
		target.Append(value)
		return true
	})
}

func (arr *byteBasedUint64Slice) depth() int {
	return int(getDepth(uint64(arr.c) / 4))
}

func (arr *byteBasedUint64Slice) Range(fn func(index int, value uint64, length int) bool) {
	for i := 0; i < arr.l; i++ {
		cont := fn(i, arr.Get(i), arr.l)
		if !cont {
			break
		}
	}
}

func (arr *byteBasedUint64Slice) Pop() uint64 {
	offset := (arr.l - 1) * 8
	val := binary.LittleEndian.Uint64(arr.u[offset : offset+8])
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], 0)
	arr.l = arr.l - 1
	return val
}

func (arr *byteBasedUint64Slice) Append(v uint64) {
	if len(arr.u) <= arr.l*8 {
		arr.u = append(arr.u, make([]byte, 32)...)
	}
	offset := arr.l * 8
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], v)
	arr.l = arr.l + 1
}

func (arr *byteBasedUint64Slice) Get(index int) uint64 {
	if index >= arr.l {
		panic("index out of range")
	}
	offset := index * 8
	return binary.LittleEndian.Uint64(arr.u[offset : offset+8])
}

func (arr *byteBasedUint64Slice) Set(index int, v uint64) {
	if index >= arr.l {
		panic("index out of range")
	}
	offset := index * 8
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], v)
}

func (arr *byteBasedUint64Slice) Length() int {
	return arr.l
}

func (arr *byteBasedUint64Slice) Cap() int {
	return arr.c
}

func (arr *byteBasedUint64Slice) HashSSZTo(xs []byte) error {
	depth := getDepth((uint64(arr.c)*8 + 31) / 32)
	baseRoot := [32]byte{}
	if arr.l == 0 {
		copy(baseRoot[:], merkle_tree.ZeroHashes[depth][:])
	} else {
		err := arr.getBaseHash(baseRoot[:])
		if err != nil {
			return err
		}
	}
	lengthRoot := merkle_tree.Uint64Root(uint64(arr.l))
	ans := utils.Keccak256(baseRoot[:], lengthRoot[:])
	copy(xs, ans[:])
	return nil
}

func (arr *byteBasedUint64Slice) getBaseHash(xs []byte) error {
	depth := getDepth((uint64(arr.c)*8 + 31) / 32)
	offset := 32*((arr.l-1)/4) + 32
	elements := arr.u[:offset]
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
