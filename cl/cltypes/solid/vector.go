package solid

import (
	"encoding/binary"

	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/ssz"
)

type Vector[T ssz2.HashableSizedObjectSSZ] interface {
	IterableSSZ[T]
	ssz2.HashableSizedObjectSSZ
}

type VectorSSZ[T ssz2.HashableSizedObjectSSZ] struct {
	items []T
	// whether items are static or dynamic
	isStatic bool
	// cached encoding size for static items
	staticItemSize int
	// Merkle tree for efficient incremental hashing
	merkleTree *merkle_tree.MerkleTree
}

func NewVectorSSZ[T ssz2.HashableSizedObjectSSZ](size int) *VectorSSZ[T] {
	items := make([]T, size)
	// Initialize each item using Clone to get proper zero values
	for i := range items {
		items[i] = items[i].Clone().(T)
	}

	// Determine if items are static and their size
	var isStatic bool
	var staticItemSize int
	if size > 0 {
		isStatic = items[0].Static()
		if isStatic {
			staticItemSize = items[0].EncodingSizeSSZ()
		}
	}

	return &VectorSSZ[T]{
		items:          items,
		isStatic:       isStatic,
		staticItemSize: staticItemSize,
	}
}

func (v *VectorSSZ[T]) Clear() {
	// Reset all items to zero values
	for i := range v.items {
		v.items[i] = v.items[i].Clone().(T)
	}
	v.merkleTree = nil
}

func (v *VectorSSZ[T]) CopyTo(target IterableSSZ[T]) {
	targetVec, ok := target.(*VectorSSZ[T])
	if !ok || len(v.items) != targetVec.Length() {
		panic("VectorSSZ: CopyTo requires same length vectors of same type")
	}

	for i := range v.items {
		target.Set(i, v.items[i])
	}
}

func (v *VectorSSZ[T]) Range(fn func(index int, value T, length int) bool) {
	for i, item := range v.items {
		if !fn(i, item, len(v.items)) {
			break
		}
	}
}

func (v *VectorSSZ[T]) Get(index int) T {
	return v.items[index]
}

func (v *VectorSSZ[T]) Set(index int, value T) {
	v.items[index] = value
	if v.merkleTree != nil {
		v.merkleTree.MarkLeafAsDirty(index)
	}
}

func (v *VectorSSZ[T]) Length() int {
	return len(v.items)
}

func (v *VectorSSZ[T]) Cap() int {
	return len(v.items)
}

func (v *VectorSSZ[T]) Bytes() []byte {
	buf, _ := v.EncodeSSZ(nil)
	return buf
}

func (v *VectorSSZ[T]) Pop() T {
	panic("VectorSSZ: cannot pop from fixed-size vector")
}

func (v *VectorSSZ[T]) Append(value T) {
	panic("VectorSSZ: cannot append to fixed-size vector")
}

func (v *VectorSSZ[T]) Static() bool {
	// Vector is static if all its elements are static
	return v.isStatic
}

func (v *VectorSSZ[T]) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf

	if v.isStatic {
		// For static items, encode them sequentially
		for _, item := range v.items {
			var err error
			dst, err = item.EncodeSSZ(dst)
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	}

	// For dynamic items, use offset encoding
	// First, write offsets for all items
	currentOffset := len(v.items) * 4
	for _, item := range v.items {
		offsetBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(offsetBuf, uint32(currentOffset))
		dst = append(dst, offsetBuf...)
		currentOffset += item.EncodingSizeSSZ()
	}

	// Then, write the actual data
	for _, item := range v.items {
		var err error
		dst, err = item.EncodeSSZ(dst)
		if err != nil {
			return nil, err
		}
	}

	return dst, nil
}

func (v *VectorSSZ[T]) DecodeSSZ(buf []byte, version int) error {
	v.merkleTree = nil

	if v.isStatic {
		// For static items, decode them sequentially
		expectedSize := len(v.items) * v.staticItemSize
		if len(buf) < expectedSize {
			return ssz.ErrBadDynamicLength
		}

		for i := range v.items {
			start := i * v.staticItemSize
			end := start + v.staticItemSize
			v.items[i] = v.items[i].Clone().(T)
			if err := v.items[i].DecodeSSZ(buf[start:end], version); err != nil {
				return err
			}
		}
		return nil
	}

	// For dynamic items, decode using offsets
	if len(buf) < len(v.items)*4 {
		return ssz.ErrBadOffset
	}

	// Read offsets
	offsets := make([]uint32, len(v.items))
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(buf[i*4:])
	}

	// Decode each item
	for i := range v.items {
		start := offsets[i]
		var end uint32
		if i == len(v.items)-1 {
			end = uint32(len(buf))
		} else {
			end = offsets[i+1]
		}

		if start > end || int(end) > len(buf) {
			return ssz.ErrBadOffset
		}

		v.items[i] = v.items[i].Clone().(T)
		if err := v.items[i].DecodeSSZ(buf[start:end], version); err != nil {
			return err
		}
	}

	return nil
}

func (v *VectorSSZ[T]) EncodingSizeSSZ() int {
	if v.isStatic {
		return len(v.items) * v.staticItemSize
	}

	// For dynamic items, calculate total size
	size := len(v.items) * 4 // offsets
	for _, item := range v.items {
		size += item.EncodingSizeSSZ()
	}
	return size
}

func (v *VectorSSZ[T]) HashSSZ() ([32]byte, error) {
	// Initialize MerkleTree if not already done
	if v.merkleTree == nil {
		// For vectors, the limit should be the next power of 2 for proper merkleization
		limit := merkle_tree.NextPowerOfTwo(uint64(len(v.items)))
		v.merkleTree = &merkle_tree.MerkleTree{}
		v.merkleTree.Initialize(len(v.items), merkle_tree.OptimalMaxTreeCacheDepth, func(idx int, out []byte) {
			hash, err := v.items[idx].HashSSZ()
			if err != nil {
				// Note: this shouldn't happen in normal operation
				// If it does, we'll get a zero hash for this leaf
				copy(out, make([]byte, 32))
				return
			}
			copy(out, hash[:])
		}, &limit)
	}

	// Compute root using the MerkleTree (only recalculates dirty leaves)
	return v.merkleTree.ComputeRoot(), nil
}

func (v *VectorSSZ[T]) Clone() clonable.Clonable {
	return NewVectorSSZ[T](len(v.items))
}
