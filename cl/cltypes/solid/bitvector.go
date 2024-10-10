package solid

import (
	"encoding/json"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

var (
	_ ssz2.SizedObjectSSZ = (*BitVector)(nil)
	_ ssz.HashableSSZ     = (*BitVector)(nil)
	_ json.Marshaler      = (*BitVector)(nil)
	_ json.Unmarshaler    = (*BitVector)(nil)
)

type BitVector struct {
	bitLen        int
	bitCap        int
	elemContainer []byte
}

func NewBitVector(c int) *BitVector {
	return &BitVector{
		bitLen:        0,
		bitCap:        c,
		elemContainer: make([]byte, 0),
	}
}

func (b *BitVector) Len() int {
	return b.bitLen
}

func (b *BitVector) Cap() int {
	return b.bitCap
}

func (b *BitVector) Static() bool {
	return false
}

func (b *BitVector) GetBitAt(i int) bool {
	if i < 0 || i >= b.bitLen {
		return false
	}
	return b.elemContainer[i/8]&(1<<(uint(i)%8)) != 0
}

func (b *BitVector) SetBitAt(i int, v bool) bool {
	if i < 0 || i >= b.bitCap {
		return false
	}
	if i >= b.bitLen {
		for j := b.bitLen/8 + 1; j < i/8+1; j++ {
			b.elemContainer = append(b.elemContainer, 0)
		}
		b.bitLen = i + 1
	}
	if v {
		b.elemContainer[i/8] |= 1 << (uint(i) % 8)
	} else {
		b.elemContainer[i/8] &= ^(1 << (uint(i) % 8))
	}
	return true
}

func (b *BitVector) GetOnIndices() []int {
	indices := make([]int, 0)
	for i := 0; i < b.bitLen; i++ {
		if b.GetBitAt(i) {
			indices = append(indices, i)
		}
	}
	return indices
}

func (b *BitVector) Copy() *BitVector {
	if b == nil {
		return nil
	}
	new := &BitVector{}
	new.bitLen = b.bitLen
	new.bitCap = b.bitCap
	new.elemContainer = make([]byte, len(b.elemContainer))
	copy(new.elemContainer, b.elemContainer)
	return new
}

func (b *BitVector) Clone() clonable.Clonable {
	return b.Copy()
}

func (b *BitVector) CopyTo(dst *BitVector) {
	if b == nil {
		return
	}
	dst.bitLen = b.bitLen
	dst.bitCap = b.bitCap
	dst.elemContainer = make([]byte, len(b.elemContainer))
	copy(dst.elemContainer, b.elemContainer)
}

func (b *BitVector) EncodingSizeSSZ() int {
	return b.bitCap
}

func (b *BitVector) DecodeSSZ(buf []byte, version int) error {
	b.bitLen = len(buf)
	b.elemContainer = make([]byte, len(buf))
	copy(b.elemContainer, buf)
	return nil
}

func (b *BitVector) EncodeSSZ(dst []byte) ([]byte, error) {
	dst = append(dst, b.elemContainer...)
	// zero padding until cap
	for i := b.bitLen; i < b.bitCap; i++ {
		dst = append(dst, 0)
	}
	return dst, nil
}

func (b *BitVector) HashSSZ() ([32]byte, error) {
	return merkle_tree.BitvectorRootWithLimit(b.elemContainer, uint64(b.bitCap))
}

func (b *BitVector) MarshalJSON() ([]byte, error) {
	return json.Marshal(hexutility.Bytes(b.elemContainer))
}

func (b *BitVector) UnmarshalJSON(data []byte) error {
	var hex hexutility.Bytes
	if err := json.Unmarshal(data, &hex); err != nil {
		return err
	}
	b.elemContainer = hex
	b.bitLen = len(hex)
	return nil
}
