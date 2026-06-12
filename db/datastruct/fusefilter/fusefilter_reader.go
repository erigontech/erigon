package fusefilter

import (
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

// headerSize is the fixed byte layout of the fusefilter on-disk header:
//
//	1B version + 3B features + 4B SegmentCount + 4B SegmentCountLength +
//	8B Seed + 4B SegmentLength + 4B SegmentLengthMask + 8B fingerprints-len.
const headerSize = 1 + 3 + 4 + 4 + 8 + 4 + 4 + 8

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

func NewReader(filePath string) (_ *Reader, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = f.Close() //nolint
		}
	}()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	sz := int(st.Size())
	m, err := mmap.MapRegion(f, sz, mmap.RDONLY, 0, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = m.Unmap() //nolint
		}
	}()
	_, fileName := filepath.Split(filePath)
	r, _, err := NewReaderOnBytes(m, fileName)
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
	if len(m) < headerSize {
		return nil, 0, fmt.Errorf("fusefilter %s: too small for header (%d < %d)", fName, len(m), headerSize)
	}
	filter := &xorfilter.BinaryFuse[uint8]{}

	header, data := m[:headerSize], m[headerSize:]

	v, features, err := parseHeaderFeatures(header, fName)
	if err != nil {
		return nil, 0, err
	}

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

	if err := validateFilterGeometry(filter, fingerprintsLen, fName); err != nil {
		return nil, 0, err
	}

	filter.Fingerprints = data[:fingerprintsLen]
	total := headerSize + fingerprintsLen
	return &Reader{inner: filter, version: v, features: features, m: m[:total]}, total, nil
}

// validateFilterGeometry rejects on-disk header values that would let
// xorfilter.Contains panic with an out-of-bounds Fingerprints index. The fuse
// filter encodes h_i = uint32(Mul64(hash, SegmentCountLength).hi) + i*SegmentLength
// for i in {0,1,2}; the largest index it can produce is SegmentCountLength-1 +
// 2*SegmentLength, so we need len(Fingerprints) >= (SegmentCount+2)*SegmentLength.
func validateFilterGeometry(filter *xorfilter.BinaryFuse[uint8], fingerprintsLen int, fName string) error {
	if filter.SegmentLength == 0 || filter.SegmentCount == 0 {
		return fmt.Errorf("fusefilter %s: zero SegmentLength=%d SegmentCount=%d", fName, filter.SegmentLength, filter.SegmentCount)
	}
	// SegmentLength is a power of two by construction (calculateSegmentLength
	// returns 1<<k). The mask must equal SegmentLength-1; otherwise hi-bit
	// shifts in getHashFromHash compute indices beyond SegmentLengthMask+1,
	// breaking the implicit bound used below.
	if filter.SegmentLength&(filter.SegmentLength-1) != 0 {
		return fmt.Errorf("fusefilter %s: SegmentLength=%d is not a power of two", fName, filter.SegmentLength)
	}
	if filter.SegmentLengthMask != filter.SegmentLength-1 {
		return fmt.Errorf("fusefilter %s: SegmentLengthMask=%d != SegmentLength-1=%d", fName, filter.SegmentLengthMask, filter.SegmentLength-1)
	}
	wantSCL := uint64(filter.SegmentCount) * uint64(filter.SegmentLength)
	if wantSCL > math.MaxUint32 {
		return fmt.Errorf("fusefilter %s: SegmentCount*SegmentLength=%d overflows uint32", fName, wantSCL)
	}
	if uint64(filter.SegmentCountLength) != wantSCL {
		return fmt.Errorf("fusefilter %s: SegmentCountLength=%d != SegmentCount*SegmentLength=%d", fName, filter.SegmentCountLength, wantSCL)
	}
	wantFP := (uint64(filter.SegmentCount) + 2) * uint64(filter.SegmentLength)
	if wantFP > math.MaxInt {
		return fmt.Errorf("fusefilter %s: required fingerprints length %d overflows int", fName, wantFP)
	}
	if uint64(fingerprintsLen) < wantFP {
		return fmt.Errorf("fusefilter %s: fingerprints length %d < required %d for SegmentCount=%d SegmentLength=%d", fName, fingerprintsLen, wantFP, filter.SegmentCount, filter.SegmentLength)
	}
	return nil
}

func (r *Reader) ForceInMem() datasize.ByteSize {
	if r.m == nil || r.inner == nil {
		return 0
	}
	cpy := make([]byte, len(r.inner.Fingerprints)) //don't use bytes.Clone - to see ram owner on heap profiler
	copy(cpy, r.inner.Fingerprints)
	r.inner.Fingerprints = cpy
	r.keepInMem = true
	return datasize.ByteSize(len(r.inner.Fingerprints))
}

func (r *Reader) MadvWillNeed() {
	if r == nil || r.f == nil || r.m == nil || len(r.m) == 0 || r.keepInMem {
		return
	}
	if err := mm.MadviseWillNeed(r.m); err != nil {
		panic(err)
	}
}
func (r *Reader) MadvNormal() {
	if r == nil || r.f == nil || r.m == nil || len(r.m) == 0 || r.keepInMem {
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
	m         mmap.MMap // outer slice spanning header + all shard blobs, used for madvise
	f         *os.File  // non-nil only when opened via NewReaderSharded(filePath)
	fileName  string
	keepInMem bool // ForceInMem replaced m with an anonymous heap copy; skip madvise/munmap
	shards    [256]Reader
}

func NewReaderSharded(filePath string) (_ *ReaderSharded, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = f.Close() //nolint
		}
	}()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	sz := int(st.Size())
	m, err := mmap.MapRegion(f, sz, mmap.RDONLY, 0, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = m.Unmap() //nolint
		}
	}()
	_, fileName := filepath.Split(filePath)
	r, _, err := NewReaderShardedOnBytes(m, fileName)
	if err != nil {
		return nil, err
	}
	r.f = f
	r.m = m
	r.fileName = fileName
	return r, nil
}

func (r *ReaderSharded) FileName() string { return r.fileName }

func (r *ReaderSharded) Close() {
	if r == nil || r.f == nil {
		return
	}
	if !r.keepInMem {
		_ = r.m.Unmap() //nolint
	}
	_ = r.f.Close() //nolint
	r.f = nil
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

// ForceInMem clones each shard's fingerprints into anonymous heap memory.
func (r *ReaderSharded) ForceInMem() datasize.ByteSize {
	var res datasize.ByteSize
	for i := range r.shards {
		res += r.shards[i].ForceInMem()
	}
	return res
}

// MadvWillNeed hints to the OS that all shard blobs will be accessed.
// One madvise on the outer mmap slice covers all shards in a single syscall,
// instead of 256 madvise calls on adjacent sub-slices of the same VMA.
func (r *ReaderSharded) MadvWillNeed() {
	if r == nil || len(r.m) == 0 || r.keepInMem {
		return
	}
	if err := mm.MadviseWillNeed(r.m); err != nil {
		panic(err)
	}
}

func (r *ReaderSharded) MadvNormal() {
	if r == nil || len(r.m) == 0 || r.keepInMem {
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
