package fusefilter

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/FastFilter/xorfilter"
	"github.com/c2h5oh/datasize"
	"github.com/edsrzf/mmap-go"

	"github.com/erigontech/erigon/common/dbg"
	mm "github.com/erigontech/erigon/common/mmap"
)

type Features uint32

const (
	IsLittleEndianFeature Features = 0b1
)

type Reader struct {
	inner     *xorfilter.BinaryFuse[uint8]
	keepInMem bool // keep it in mem insted of mmap

	fileName string
	f        *os.File
	m        mmap.MMap
	features Features

	version uint8
}

var (
	MadvWillNeedByDefault = dbg.EnvBool("FUSE_MADV_WILLNEED", false)
	MadvNormalByDefault   = dbg.EnvBool("FUSE_MADV_NORMAL", false)
)

func NewReader(filePath string) (*Reader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close() //nolint
		return nil, err
	}
	sz := int(st.Size())
	var content []byte
	m, err := mmap.MapRegion(f, sz, mmap.RDONLY, 0, 0)
	if err != nil {
		_ = f.Close() //nolint
		return nil, err
	}
	content = m

	_, fileName := filepath.Split(filePath)
	r, _, err := NewReaderOnBytes(content, fileName)
	if err != nil {
		return nil, err
	}
	r.f = f
	r.m = m
	r.fileName = fileName
	return r, nil
}

func parseHeaderFeatures(header []byte, fName string) (version uint8, features Features, err error) {
	version = header[0]
	features = Features(binary.BigEndian.Uint32(header[:4]) & 0x00FFFFFF)
	if (features&IsLittleEndianFeature != 0) != IsLittleEndian {
		return 0, 0, fmt.Errorf("file %s is not compatible with your machine (different Endianness), but you can run `erigon snapshots index`", fName)
	}
	return version, features, nil
}

func NewReaderOnBytes(m []byte, fName string) (*Reader, int, error) {
	const headerSize = filterBlobHeaderSize
	if len(m) < headerSize {
		return nil, 0, fmt.Errorf("fusefilter %s: too small for header (%d < %d)", fName, len(m), headerSize)
	}
	header, data := m[:headerSize], m[headerSize:]

	v, features, err := parseHeaderFeatures(header, fName)
	if err != nil {
		return nil, 0, err
	}

	filter := &xorfilter.BinaryFuse[uint8]{}
	filter.SegmentCount = binary.BigEndian.Uint32(header[4:])
	filter.SegmentCountLength = binary.BigEndian.Uint32(header[4+4:])
	filter.Seed = binary.BigEndian.Uint64(header[4+4+4:])
	filter.SegmentLength = binary.BigEndian.Uint32(header[4+4+4+8:])
	filter.SegmentLengthMask = binary.BigEndian.Uint32(header[4+4+4+8+4:])
	fingerprintsLen64 := binary.BigEndian.Uint64(header[4+4+4+8+4+4:])
	if fingerprintsLen64 > math.MaxInt || uint64(len(data)) < fingerprintsLen64 {
		return nil, 0, fmt.Errorf("fusefilter %s: fingerprints length %d exceeds available bytes %d", fName, fingerprintsLen64, len(data))
	}
	fingerprintsLen := int(fingerprintsLen64)

	filter.Fingerprints = data[:fingerprintsLen]
	total := headerSize + fingerprintsLen
	return &Reader{inner: filter, version: v, features: features, m: m[:total]}, total, nil
}

func (r *Reader) ForceInMem() datasize.ByteSize {
	r.inner.Fingerprints = bytes.Clone(r.inner.Fingerprints)
	r.keepInMem = true
	return datasize.ByteSize(len(r.inner.Fingerprints))
}

func (r *Reader) MadvWillNeed() {
	if r == nil || r.m == nil || len(r.m) == 0 || r.keepInMem {
		return
	}
	if err := mm.MadviseWillNeed(r.m); err != nil {
		panic(err)
	}
}
func (r *Reader) MadvNormal() {
	if r == nil || r.m == nil || len(r.m) == 0 || r.keepInMem {
		return
	}
	if err := mm.MadviseNormal(r.m); err != nil {
		panic(err)
	}
}
func (r *Reader) FileName() string           { return r.fileName }
func (r *Reader) ContainsHash(v uint64) bool { return r.inner.Contains(v) }
func (r *Reader) Close() {
	if r == nil || r.f == nil {
		return
	}
	_ = r.m.Unmap() //nolint
	_ = r.f.Close() //nolint
	r.f = nil
}

// ReaderSharded reads a sharded fusefilter (outer header version=1).
// Only the shard matching keyHash >> 56 is checked on lookup.
// shards is a value array (not pointer array) so ContainsHash needs only one pointer dereference.
type ReaderSharded struct {
	m      mmap.MMap // outer slice spanning header + all shard blobs, used for madvise
	shards [256]Reader
}

func NewReaderShardedOnBytes(m []byte, fName string) (*ReaderSharded, int, error) {
	const headerSize = 4
	if len(m) < headerSize {
		return nil, 0, fmt.Errorf("fusefilter sharded %s: too small (%d bytes)", fName, len(m))
	}
	v, _, err := parseHeaderFeatures(m[:4], fName)
	if err != nil {
		return nil, 0, err
	}
	if v != 1 {
		return nil, 0, fmt.Errorf("fusefilter sharded %s: unsupported version %d", fName, v)
	}

	r := &ReaderSharded{}
	offset := headerSize
	for i := range 256 {
		if offset+8 > len(m) {
			return nil, 0, fmt.Errorf("fusefilter sharded %s: truncated at shard %d", fName, i)
		}
		sz64 := binary.BigEndian.Uint64(m[offset:])
		offset += 8
		if sz64 == 0 {
			continue
		}
		if sz64 > math.MaxInt || sz64 > uint64(len(m)-offset) {
			return nil, 0, fmt.Errorf("fusefilter sharded %s: shard %d blob overflows (offset=%d sz=%d total=%d)", fName, i, offset, sz64, len(m))
		}
		sz := int(sz64)
		if sz < filterBlobHeaderSize {
			return nil, 0, fmt.Errorf("fusefilter sharded %s: shard %d size %d < header %d", fName, i, sz, filterBlobHeaderSize)
		}
		shard, consumed, err := NewReaderOnBytes(m[offset:offset+sz], fName)
		if err != nil {
			return nil, 0, fmt.Errorf("shard %d of %s: %w", i, fName, err)
		}
		if consumed != sz {
			return nil, 0, fmt.Errorf("fusefilter sharded %s: shard %d consumed %d != declared size %d", fName, i, consumed, sz)
		}
		r.shards[i] = *shard
		offset += sz
	}
	r.m = m[:offset]
	return r, offset, nil
}

func (r *ReaderSharded) ContainsHash(v uint64) bool {
	s := &r.shards[v>>56]
	if s.inner == nil {
		return false
	}
	return s.ContainsHash(v)
}

func (r *ReaderSharded) ForceInMem() datasize.ByteSize {
	var total datasize.ByteSize
	for i := range r.shards {
		if r.shards[i].inner != nil {
			total += r.shards[i].ForceInMem()
		}
	}
	return total
}

// MadvWillNeed hints to the OS that all shard blobs will be accessed.
// One madvise on the outer mmap slice covers all shards in a single syscall,
// instead of 256 madvise calls on adjacent sub-slices of the same VMA.
func (r *ReaderSharded) MadvWillNeed() {
	if len(r.m) == 0 {
		return
	}
	if err := mm.MadviseWillNeed(r.m); err != nil {
		panic(err)
	}
}

func (r *ReaderSharded) MadvNormal() {
	if len(r.m) == 0 {
		return
	}
	if err := mm.MadviseNormal(r.m); err != nil {
		panic(err)
	}
}

var IsLittleEndian = isLittleEndian()

func isLittleEndian() bool {
	var x uint16 = 0x0102
	xb := *(*[2]byte)(unsafe.Pointer(&x))
	return (xb[0] == 0x02)
}
