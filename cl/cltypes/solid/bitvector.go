package solid

import (
	"encoding/json"
	"errors"
	"fmt"

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
	bitLen    int
	bitCap    int
	container []byte
}

func NewBitVector(c int) *BitVector {
	return &BitVector{
		bitLen:    0,
		bitCap:    c,
		container: make([]byte, 0),
	}
}

func (b *BitVector) BitLen() int {
	return b.bitLen
}

func (b *BitVector) BitCap() int {
	return b.bitCap
}

func (b *BitVector) Static() bool {
	return false
}

func (b *BitVector) GetBitAt(i int) bool {
	if i < 0 || i >= b.bitLen {
		return false
	}
	return b.container[i/8]&(1<<(uint(i)%8)) != 0
}

func (b *BitVector) SetBitAt(i int, v bool) error {
	if i < 0 || i >= b.bitCap {
		return fmt.Errorf("index %v out of bitvector cap range %v", i, b.bitCap)
	}
	if i >= b.bitLen {
		for j := b.bitLen/8 + 1; j < i/8+1; j++ {
			b.container = append(b.container, 0)
		}
		b.bitLen = i + 1
	}
	if v {
		b.container[i/8] |= 1 << (uint(i) % 8)
	} else {
		b.container[i/8] &= ^(1 << (uint(i) % 8))
	}
	return nil
}

func (b *BitVector) GetOnIndices() []int {
	if b == nil {
		return nil
	}
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
	new.container = make([]byte, len(b.container))
	copy(new.container, b.container)
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
	dst.container = make([]byte, len(b.container))
	copy(dst.container, b.container)
}

func (b *BitVector) EncodingSizeSSZ() int {
	// quite different from bitlist
	return (b.bitCap + 7) / 8 // ceil(bitCap / 8) bytes
}

func (b *BitVector) DecodeSSZ(buf []byte, version int) error {
	b.bitLen = len(buf)
	b.container = make([]byte, len(buf))
	copy(b.container, buf)
	return nil
}

func (b *BitVector) EncodeSSZ(dst []byte) ([]byte, error) {
	dst = append(dst, b.container...)
	// zero padding until cap
	for i := b.bitLen; i < b.bitCap; i++ {
		dst = append(dst, 0)
	}
	return dst, nil
}

func (b *BitVector) HashSSZ() ([32]byte, error) {
	return merkle_tree.BitvectorRootWithLimit(b.container, uint64(b.bitCap))
}

func (b *BitVector) MarshalJSON() ([]byte, error) {
	return json.Marshal(hexutility.Bytes(b.container))
}

func (b *BitVector) UnmarshalJSON(data []byte) error {
	var hex hexutility.Bytes
	if err := json.Unmarshal(data, &hex); err != nil {
		return err
	}
	b.container = hex
	b.bitLen = len(hex)
	return nil
}

func (b *BitVector) Union(other *BitVector) (*BitVector, error) {
	if b.bitCap != other.bitCap {
		return nil, errors.New("bitvector size mismatch")
	}
	new := b.Copy()
	for i := 0; i < other.bitLen; i++ {
		if other.GetBitAt(i) {
			if err := new.SetBitAt(i, true); err != nil {
				return nil, err
			}
		}
	}
	return new, nil
}
