package solid

import (
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type hashList struct {
	u    []byte
	l, c int

	hashBuf
}

func NewHashList(c int) HashListSSZ {
	return &hashList{
		c: c,
	}
}

func (h *hashList) Append(val libcommon.Hash) {
	offset := h.l * length.Hash
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
}

func (h *hashList) Clone() clonable.Clonable {
	return NewHashList(h.c)
}

func (h *hashList) CopyTo(t IterableSSZ[libcommon.Hash]) {
	tu := t.(*hashList)
	tu.c = h.c
	tu.l = h.l
	if len(h.u) > len(tu.u) {
		tu.u = make([]byte, len(h.u))
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
	h.u = libcommon.Copy(buf)
	h.l = len(h.u) / length.Hash
	return nil
}

func (h *hashList) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, h.u[:h.l*length.Hash]...), nil
}

func (h *hashList) EncodingSizeSSZ() int {
	return h.l * length.Hash
}

func (h *hashList) Get(index int) (out libcommon.Hash) {
	if index >= h.l {
		panic("too big bruh")
	}
	copy(out[:], h.u[index*length.Hash:])
	return
}

func (h *hashList) Set(index int, newValue libcommon.Hash) {
	if index >= h.l {
		panic("too big bruh")
	}
	copy(h.u[index*length.Hash:], newValue[:])
}

func (h *hashList) hashVectorSSZ() ([32]byte, error) {
	depth := GetDepth(uint64(h.c))
	offset := length.Hash * h.l
	elements := common.Copy(h.u[:offset])
	for i := uint8(0); i < depth; i++ {
		// Sequential
		if len(elements)%64 != 0 {
			elements = append(elements, merkle_tree.ZeroHashes[i][:]...)
		}
		outputLen := len(elements) / 2
		h.makeBuf(outputLen)
		if err := merkle_tree.HashByteSlice(h.buf, elements); err != nil {
			return [32]byte{}, err
		}
		elements = h.buf
	}

	return common.BytesToHash(elements[:32]), nil
}

func (h *hashList) HashSSZ() ([32]byte, error) {
	depth := GetDepth(uint64(h.c))
	baseRoot := [32]byte{}
	var err error
	if h.l == 0 {
		copy(baseRoot[:], merkle_tree.ZeroHashes[depth][:])
	} else {
		baseRoot, err = h.hashVectorSSZ()
		if err != nil {
			return [32]byte{}, err
		}
	}
	lengthRoot := merkle_tree.Uint64Root(uint64(h.l))
	return utils.Keccak256(baseRoot[:], lengthRoot[:]), nil
}

func (h *hashList) Range(fn func(int, libcommon.Hash, int) bool) {
	for i := 0; i < h.l; i++ {
		if !fn(i, h.Get(i), h.l) {
			return
		}
	}
}

func (h *hashList) Pop() libcommon.Hash {
	panic("didnt ask, dont need it, go fuck yourself")
}
