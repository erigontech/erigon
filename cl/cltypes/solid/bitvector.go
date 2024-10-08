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
	len      int
	cap      int
	elements []byte
}

func NewBitVector(c int) *BitVector {
	return &BitVector{
		len:      0,
		cap:      c,
		elements: []byte{},
	}
}

func (b *BitVector) Len() int {
	return b.len
}

func (b *BitVector) Cap() int {
	return b.cap
}

func (b *BitVector) Static() bool {
	return false
}

func (b *BitVector) Copy() *BitVector {
	if b == nil {
		return nil
	}
	new := &BitVector{}
	new.len = b.len
	new.cap = b.cap
	new.elements = make([]byte, len(b.elements))
	copy(new.elements, b.elements)
	return new
}

func (b *BitVector) Clone() clonable.Clonable {
	return b.Copy()
}

func (b *BitVector) CopyTo(dst *BitVector) {
	if b == nil {
		return
	}
	dst.len = b.len
	dst.cap = b.cap
	dst.elements = make([]byte, len(b.elements))
	copy(dst.elements, b.elements)
}

func (b *BitVector) EncodingSizeSSZ() int {
	return b.cap
}

func (b *BitVector) DecodeSSZ(buf []byte, version int) error {
	b.len = len(buf)
	b.elements = make([]byte, len(buf))
	copy(b.elements, buf)
	return nil
}

func (b *BitVector) EncodeSSZ(dst []byte) ([]byte, error) {
	dst = append(dst, b.elements...)
	// zero padding until cap
	for i := b.len; i < b.cap; i++ {
		dst = append(dst, 0)
	}
	return dst, nil
}

func (b *BitVector) HashSSZ() ([32]byte, error) {
	return merkle_tree.BitvectorRootWithLimit(b.elements, uint64(b.cap))
}

func (b *BitVector) MarshalJSON() ([]byte, error) {
	return json.Marshal(hexutility.Bytes(b.elements))
}

func (b *BitVector) UnmarshalJSON(data []byte) error {
	var hex hexutility.Bytes
	if err := json.Unmarshal(data, &hex); err != nil {
		return err
	}
	b.elements = hex
	b.len = len(hex)
	return nil
}
