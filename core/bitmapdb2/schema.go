package bitmapdb2

import (
	"encoding/binary"
	"io"
	"math/bits"
	"reflect"
	"sort"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/ledgerwatch/erigon-lib/common"
)

// Logical structure of the database:
// AccountsHistory:
//   key: plain unhashed address
//   value: bitmap
// StorageHistory
//   key: plain unhashed address + plain unhashed storage key
//   value: bitmap
// CallFromIndex
//   key: plain unhashed address
//   value: bitmap
// CallToIndex
//   key: plain unhashed address
//   value: bitmap
// LogTopicIndex
//   key: plain unhashed topic
//   value: bitmap
// LogAddressIndex
//   key: plain unhashed address
//   value: bitmap

var (
	BucketKeyPrefix = []byte("B")
)

func containerRowKey(bucket string, key []byte, hi uint16) []byte {
	rowKey := make([]byte, len(BucketKeyPrefix)+len(bucket)+len(key)+2)
	copy(rowKey, BucketKeyPrefix)
	copy(rowKey[len(BucketKeyPrefix):], bucket)
	copy(rowKey[len(BucketKeyPrefix)+len(bucket):], key)
	binary.BigEndian.PutUint16(rowKey[len(BucketKeyPrefix)+len(bucket)+len(key):], hi)
	return rowKey
}

func containerRowPrefix(bucket string, key []byte) []byte {
	prefix := make([]byte, len(BucketKeyPrefix)+len(bucket)+len(key))
	copy(prefix, BucketKeyPrefix)
	copy(prefix[len(BucketKeyPrefix):], bucket)
	copy(prefix[len(BucketKeyPrefix)+len(bucket):], key)
	return prefix
}

func getHiFromRowKey(rowKey []byte) uint16 {
	return binary.BigEndian.Uint16(rowKey[len(rowKey)-2:])
}

const (
	ContainerTypeBitmap = 1
	ContainerTypeArray  = 2

	ContainerArrayMaxSize = 4096
)

type container struct {
	Hi     uint16
	Buffer []byte
}

func ContainerNoCopy(hi uint16, b []byte) *container {
	if len(b) < 1 {
		panic("invalid container")
	}
	return &container{Hi: hi, Buffer: b}
}

func ContainerFromArray(vals []uint32) *container {
	if len(vals) == 0 {
		return nil
	}
	hi := uint16(vals[0] >> 16)
	if len(vals) <= ContainerArrayMaxSize {
		b := make([]byte, 8+len(vals)*2)
		binary.LittleEndian.PutUint64(b, ContainerTypeArray)
		for i, v := range vals {
			binary.LittleEndian.PutUint16(b[8+i*2:], uint16(v))
		}
		return &container{Hi: hi, Buffer: b}
	} else {
		b := make([]byte, 8+1024*8)
		binary.LittleEndian.PutUint64(b, ContainerTypeBitmap)
		bitmap := toUint64Slice(b[8:])
		for _, v := range vals {
			lo := uint16(v)
			bitmap[lo>>6] |= uint64(1) << (lo & 63)
		}
		return &container{Hi: hi, Buffer: b}
	}
}

func NewEmptyContainer(hi uint16) *container {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, ContainerTypeArray)
	return &container{Hi: hi, Buffer: b}
}

// Use 64bit for type to make sure of memory alignment.
func (c *container) Type() uint64 {
	return binary.LittleEndian.Uint64(c.Buffer)
}

func (c *container) Copy() *container {
	return &container{Buffer: append([]byte(nil), c.Buffer...)}
}

func (c *container) ForEach(fn func(v uint32) error) error {
	switch c.Type() {
	case ContainerTypeBitmap:
		bitmap := toUint64Slice(c.Buffer[8:])
		base := uint32(c.Hi) << 16
		for k := 0; k < len(bitmap); k++ {
			bitset := bitmap[k]
			for bitset != 0 {
				t := bitset & -bitset
				if err := fn(base + uint32(popcount(t-1))); err != nil {
					return err
				}
				bitset ^= t
			}
			base += 64
		}
	case ContainerTypeArray:
		array := toUint16Slice(c.Buffer[8:])
		base := uint32(c.Hi) << 16
		for _, v := range array {
			if err := fn(base + uint32(v)); err != nil {
				return err
			}
		}
	default:
		panic("unknown container type")
	}
	return nil
}

func (c *container) Add(v uint32) {
	hi := uint16(v >> 16)
	if hi != c.Hi {
		panic("invalid high bits")
	}
	lo := uint16(v)
	switch c.Type() {
	case ContainerTypeBitmap:
		bitmap := toUint64Slice(c.Buffer[8:])
		bitmap[lo>>6] |= uint64(1) << (lo & 63)
	case ContainerTypeArray:
		if len(c.Buffer) == 8 {
			// Array not yet initialized.
			c.Buffer = append(c.Buffer, 0, 0)
			binary.LittleEndian.PutUint16(c.Buffer[8:], lo)
			return
		}
		array := toUint16Slice(c.Buffer[8:])
		// Fast path for appending to end of array.
		if len(array) > 0 && array[len(array)-1] < lo {
			c.Buffer = append(c.Buffer, 0, 0)
			binary.LittleEndian.PutUint16(c.Buffer[len(c.Buffer)-2:], lo)
			return
		}
		idx := sort.Search(len(array), func(i int) bool { return array[i] >= lo })
		if idx < len(array) && array[idx] == lo {
			return
		}
		if len(array) < ContainerArrayMaxSize {
			// Insert into existing array.
			c.Buffer = append(c.Buffer, 0, 0)
			// Buffer may have been reallocated, so slice again.
			array := toUint16Slice(c.Buffer[8:])
			copy(array[idx+1:], array[idx:])
			array[idx] = lo
		} else {
			// Convert to bitmap.
			newBuffer := make([]byte, 8+1024*8)
			binary.LittleEndian.PutUint64(newBuffer, ContainerTypeBitmap)

			bitmap := toUint64Slice(newBuffer[8:])
			bitmap[lo>>6] |= uint64(1) << (lo & 63)
			for _, lo2 := range array {
				bitmap[lo2>>6] |= uint64(1) << (lo2 & 63)
			}
			c.Buffer = newBuffer
		}
	}
}

func (c *container) Contains(v uint32) bool {
	hi := uint16(v >> 16)
	if hi != c.Hi {
		panic("invalid high bits")
	}
	lo := uint16(v)
	switch c.Type() {
	case ContainerTypeBitmap:
		bitmap := toUint64Slice(c.Buffer[8:])
		return bitmap[lo>>6]&(uint64(1)<<(lo&63)) != 0
	case ContainerTypeArray:
		if len(c.Buffer) == 8 {
			return false
		}
		array := toUint16Slice(c.Buffer[8:])
		idx := sort.Search(len(array), func(i int) bool { return array[i] >= lo })
		if idx < len(array) && array[idx] == lo {
			return true
		} else {
			return false
		}
	default:
		panic("unknown container type")
	}
}

func toUint64Slice(b []byte) []uint64 {
	var u64s []uint64
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u64s))
	hdr.Len = len(b) / 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u64s
}

func toUint16Slice(b []byte) []uint16 {
	var u16s []uint16
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u16s))
	hdr.Len = len(b) / 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u16s
}

func popcount(x uint64) uint64 {
	return uint64(bits.OnesCount64(x))
}

type containerValueMerger struct {
	c *container
}

func (m *containerValueMerger) merge(value []byte) error {
	c2 := ContainerNoCopy(m.c.Hi, value)
	if c2.Type() == ContainerTypeBitmap && m.c.Type() == ContainerTypeBitmap {
		// Merge bitmaps.
		bitmap1 := toUint64Slice(m.c.Buffer[8:])
		bitmap2 := toUint64Slice(c2.Buffer[8:])
		if len(bitmap1) != len(bitmap2) {
			panic("bitmap has different length")
		}
		for i := range bitmap1 {
			bitmap1[i] |= bitmap2[i]
		}
	} else {
		c2.ForEach(func(v uint32) error {
			m.c.Add(v)
			return nil
		})
	}
	return nil
}

func (m *containerValueMerger) MergeNewer(value []byte) error {
	return m.merge(value)
}

func (m *containerValueMerger) MergeOlder(value []byte) error {
	return m.merge(value)
}

func (m *containerValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return m.c.Buffer, nil, nil
}

func containerMerger(key, value []byte) (pebble.ValueMerger, error) {
	hi := getHiFromRowKey(key)
	return &containerValueMerger{
		c: ContainerNoCopy(hi, common.Copy(value)),
	}, nil
}
