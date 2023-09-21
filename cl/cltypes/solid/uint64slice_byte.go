package solid

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
)

const treeCacheDepthUint64Slice = 0

func convertDepthToChunkSize(d int) int {
	return (1 << d) // just power of 2
}

func getTreeCacheSize(listLen int, cacheDepth int) int {
	treeChunks := convertDepthToChunkSize(cacheDepth)
	return (listLen + treeChunks - 1) / treeChunks

}

// byteBasedUint64Slice represents a dynamic Uint64Slice data type that is byte-backed.
// The underlying storage for the slice is a byte array. This approach allows for efficient
// memory usage, especially when dealing with large slices.
type byteBasedUint64Slice struct {
	// The bytes that back the slice
	u               []byte
	treeCacheBuffer []byte

	// Length of the slice
	l int

	// Capacity of the slice
	c int

	hashBuf
}

// NewUint64Slice creates a new instance of byteBasedUint64Slice with a specified capacity limit.
func NewUint64Slice(limit int) *byteBasedUint64Slice {
	o := &byteBasedUint64Slice{
		c: limit,
	}
	return o
}

// Clear clears the slice by setting its length to 0 and zeroing out its backing array.
func (arr *byteBasedUint64Slice) Clear() {
	arr.l = 0
	for i := range arr.u {
		arr.u[i] = 0
	}
	for i := range arr.treeCacheBuffer {
		arr.treeCacheBuffer[i] = 0
	}
}

// CopyTo copies the slice to a target slice.
func (arr *byteBasedUint64Slice) CopyTo(target *byteBasedUint64Slice) {
	target.Clear()

	target.c = arr.c
	target.l = arr.l
	if len(target.u) < len(arr.u) {
		target.u = make([]byte, len(arr.u))
	}
	if len(target.treeCacheBuffer) < len(arr.treeCacheBuffer) {
		target.treeCacheBuffer = make([]byte, len(arr.treeCacheBuffer))
	}
	target.treeCacheBuffer = target.treeCacheBuffer[:len(arr.treeCacheBuffer)]
	target.u = target.u[:len(arr.u)]
	copy(target.u, arr.u)
	copy(target.treeCacheBuffer, arr.treeCacheBuffer)
}

func (arr *byteBasedUint64Slice) MarshalJSON() ([]byte, error) {
	list := make([]uint64, arr.l)
	for i := 0; i < arr.l; i++ {
		list[0] = arr.Get(i)
	}
	return json.Marshal(list)
}

func (arr *byteBasedUint64Slice) UnmarshalJSON(buf []byte) error {
	var list []uint64

	if err := json.Unmarshal(buf, &list); err != nil {
		return err
	}
	arr.Clear()
	arr.l = len(list)
	for _, elem := range list {
		arr.Append(elem)
	}
	return nil
}

// Range iterates over the slice and applies a provided function to each element.
func (arr *byteBasedUint64Slice) Range(fn func(index int, value uint64, length int) bool) {
	for i := 0; i < arr.l; i++ {
		cont := fn(i, arr.Get(i), arr.l)
		if !cont {
			break
		}
	}
}

// Pop removes and returns the last element of the slice.
func (arr *byteBasedUint64Slice) Pop() uint64 {
	offset := (arr.l - 1) * 8
	val := binary.LittleEndian.Uint64(arr.u[offset : offset+8])
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], 0)
	arr.l = arr.l - 1
	arr.treeCacheBuffer = arr.treeCacheBuffer[:getTreeCacheSize((arr.l+3)/4, treeCacheDepthUint64Slice)*length.Hash]
	return val
}

// Append adds a new element to the end of the slice.
func (arr *byteBasedUint64Slice) Append(v uint64) {
	if len(arr.u) <= arr.l*8 {
		arr.u = append(arr.u, make([]byte, 32)...)
	}

	offset := arr.l * 8
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], v)
	arr.l = arr.l + 1
	treeBufferExpectCache := getTreeCacheSize((arr.l+3)/4, treeCacheDepthUint64Slice) * length.Hash
	if len(arr.treeCacheBuffer) < treeBufferExpectCache {
		arr.treeCacheBuffer = append(arr.treeCacheBuffer, make([]byte, treeBufferExpectCache-len(arr.treeCacheBuffer))...)
	}
	ihIdx := (((arr.l - 1) / 4) / convertDepthToChunkSize(treeCacheDepthUint64Slice)) * length.Hash
	for i := ihIdx; i < ihIdx+length.Hash; i++ {
		arr.treeCacheBuffer[i] = 0
	}
}

// Get returns the element at the given index.
func (arr *byteBasedUint64Slice) Get(index int) uint64 {
	if index >= arr.l {
		panic("index out of range")
	}
	offset := index * 8
	return binary.LittleEndian.Uint64(arr.u[offset : offset+8])
}

// Set replaces the element at the given index with a new value.
func (arr *byteBasedUint64Slice) Set(index int, v uint64) {
	offset := index * 8
	ihIdx := ((index / 4) / convertDepthToChunkSize(treeCacheDepthUint64Slice)) * length.Hash
	for i := ihIdx; i < ihIdx+length.Hash; i++ {
		arr.treeCacheBuffer[i] = 0
	}
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], v)
}

// Length returns the current length (number of elements) of the slice.
func (arr *byteBasedUint64Slice) Length() int {
	return arr.l
}

// Cap returns the capacity (maximum number of elements) of the slice.
func (arr *byteBasedUint64Slice) Cap() int {
	return arr.c
}

// HashListSSZ computes the SSZ hash of the slice as a list. It returns the hash and any error encountered.
func (arr *byteBasedUint64Slice) HashListSSZ() ([32]byte, error) {
	depth := GetDepth((uint64(arr.c)*8 + 31) / 32)
	baseRoot := [32]byte{}
	var err error
	if arr.l == 0 {
		copy(baseRoot[:], merkle_tree.ZeroHashes[depth][:])
	} else {
		baseRoot, err = arr.HashVectorSSZ()
		if err != nil {
			return [32]byte{}, err
		}
	}
	lengthRoot := merkle_tree.Uint64Root(uint64(arr.l))
	return utils.Keccak256(baseRoot[:], lengthRoot[:]), nil
}

// HashVectorSSZ computes the SSZ hash of the slice as a vector. It returns the hash and any error encountered.
func (arr *byteBasedUint64Slice) HashVectorSSZ() ([32]byte, error) {
	chunkSize := convertDepthToChunkSize(treeCacheDepthUint64Slice) * length.Hash
	depth := GetDepth((uint64(arr.c)*8 + length.Hash - 1) / length.Hash)
	emptyHashBytes := make([]byte, length.Hash)

	layerBuffer := make([]byte, chunkSize)
	maxTo := length.Hash*((arr.l-1)/4) + length.Hash

	offset := 0
	for i := 0; i < maxTo; i += chunkSize {
		offset = (i / chunkSize) * length.Hash
		from := i
		to := int(utils.Min64(uint64(from+chunkSize), uint64(maxTo)))

		if !bytes.Equal(arr.treeCacheBuffer[offset:offset+length.Hash], emptyHashBytes) {
			continue
		}
		layerBuffer = layerBuffer[:to-from]
		copy(layerBuffer, arr.u[from:to])
		if err := computeFlatRootsToBuffer(uint8(utils.Min64(treeCacheDepthUint64Slice, uint64(depth))), layerBuffer, arr.treeCacheBuffer[offset:]); err != nil {
			return [32]byte{}, err
		}
	}
	if treeCacheDepthUint64Slice >= depth {
		return common.BytesToHash(arr.treeCacheBuffer[:32]), nil
	}

	arr.makeBuf(offset + length.Hash)
	copy(arr.buf, arr.treeCacheBuffer[:offset+length.Hash])
	elements := arr.buf
	for i := uint8(treeCacheDepthUint64Slice); i < depth; i++ {
		layerLen := len(elements)
		if layerLen%64 == 32 {
			elements = append(elements, merkle_tree.ZeroHashes[i][:]...)
		}
		outputLen := len(elements) / 2
		arr.makeBuf(outputLen)
		if err := merkle_tree.HashByteSlice(arr.buf, elements); err != nil {
			return [32]byte{}, err
		}
		elements = arr.buf
	}

	return common.BytesToHash(elements[:32]), nil
}

// EncodeSSZ encodes the slice in SSZ format. It appends the encoded data to the provided buffer and returns the result.
func (arr *byteBasedUint64Slice) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, arr.u[:arr.l*8]...), nil
}

// DecodeSSZ decodes the slice from SSZ format. It takes a byte slice as input and updates the current slice.
func (arr *byteBasedUint64Slice) DecodeSSZ(buf []byte, _ int) error {
	if len(buf)%8 > 0 {
		return ssz.ErrBadDynamicLength
	}
	arr.l = len(buf) / 8
	bufferLength := length.Hash*((arr.l-1)/4) + length.Hash
	arr.u = make([]byte, bufferLength)
	copy(arr.u, buf)
	arr.treeCacheBuffer = make([]byte, getTreeCacheSize((arr.l+3)/4, treeCacheDepthUint64Slice)*length.Hash)
	return nil
}

// EncodingSizeSSZ returns the size in bytes that the slice would occupy when encoded in SSZ format.
func (arr *byteBasedUint64Slice) EncodingSizeSSZ() int {
	return arr.l * 8
}
