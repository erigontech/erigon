package fusefilter

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

var (
	MadvWillNeedByDefault = dbg.EnvBool("FUSE_MADV_WILLNEED", false)
	MadvNormalByDefault   = dbg.EnvBool("FUSE_MADV_NORMAL", false)
)

// ── monolithic filter ────────────────────────────────────────────────────────

type Reader struct {
	inner     *xorfilter.BinaryFuse[uint8]
	keepInMem bool // keep it in mem insted of mmap

	fileName string
	f        *os.File
	m        mmap.MMap
	features Features

	version uint8
}

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

func NewReaderOnBytes(m []byte, fName string) (*Reader, int, error) {
	filter := &xorfilter.BinaryFuse[uint8]{}

	const headerSize = 1 + 3 + 4 + 4 + 8 + 4 + 4 + 8
	header, data := m[:headerSize], m[headerSize:]
	v := header[0]

	// 1 byte - version, 3 bytes - `Features`
	featuresBytes := bytes.Clone(header[:4])
	featuresBytes[0] = 0 // mask version byte
	features := Features(binary.BigEndian.Uint32(featuresBytes))
	fileIsLittleEndian := features&IsLittleEndianFeature != 0
	if fileIsLittleEndian != IsLittleEndian {
		return nil, 0, fmt.Errorf("file %s %s", fName, endianMismatchErr)
	}

	filter.SegmentCount = binary.BigEndian.Uint32(header[4+4:])
	filter.SegmentCountLength = binary.BigEndian.Uint32(header[4+4:])
	filter.Seed = binary.BigEndian.Uint64(header[4+4+4:])
	filter.SegmentLength = binary.BigEndian.Uint32(header[4+4+4+8:])
	filter.SegmentLengthMask = binary.BigEndian.Uint32(header[4+4+4+8+4:])
	fingerprintsLen := int(binary.BigEndian.Uint64(header[4+4+4+8+4+4:]))

	filter.Fingerprints = data[:fingerprintsLen]
	return &Reader{inner: filter, version: v, features: features, m: m}, headerSize + fingerprintsLen, nil
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

var (
	IsLittleEndian = isLittleEndian()
	// endianMismatchErr is returned when a filter file was built on a machine
	// with a different byte order than the current one.
	endianMismatchErr = "is not compatible with your machine (different Endianness), but you can run `erigon snapshots index`"
)

func isLittleEndian() bool {
	var x uint16 = 0x0102
	xb := *(*[2]byte)(unsafe.Pointer(&x))
	return (xb[0] == 0x02)
}

// ── sharded filter (256 shards by low byte of hash) ─────────────────────────

// ShardedReader is the read side of the 256-shard fuse filter. It holds 256
// BinaryFuse[uint8] value-structs inline; each shard's Fingerprints slice
// points zero-copy into the caller-provided mmap region (or into the cloned
// region after ForceInMem).
type ShardedReader struct {
	// Empty shards have SegmentCount == 0 and Fingerprints == nil; ContainsHash
	// short-circuits on the former before dispatching to xorfilter's Contains.
	shards [shardCount]xorfilter.BinaryFuse[uint8]

	fpRegion  []byte // slice into mmap covering all shards' fingerprints
	fpOffsets [shardCount + 1]uint64

	keepInMem bool
	fileName  string
	features  Features

	f *os.File
	m mmap.MMap // backing byte region for madvise; set by both constructors
}

func NewShardedReader(filePath string) (*ShardedReader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	sz := int(st.Size())
	m, err := mmap.MapRegion(f, sz, mmap.RDONLY, 0, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	_, fileName := filepath.Split(filePath)
	r, _, err := NewShardedReaderOnBytes(m, fileName)
	if err != nil {
		_ = m.Unmap()
		_ = f.Close()
		return nil, err
	}
	r.f = f
	r.m = m
	r.fileName = fileName
	return r, nil
}

// NewShardedReaderOnBytes parses the sharded-filter layout from `m` (which
// must remain valid for the lifetime of the returned reader). Returns the
// reader, the number of bytes consumed from `m`, and an error.
func NewShardedReaderOnBytes(m []byte, fName string) (*ShardedReader, int, error) {
	if len(m) < shardedFixedHeader {
		return nil, 0, fmt.Errorf("file %s: sharded fuse filter truncated: have %d bytes, need at least %d", fName, len(m), shardedFixedHeader)
	}
	features := Features(binary.BigEndian.Uint32(m[:4]))
	fileIsLittleEndian := features&IsLittleEndianFeature != 0
	if fileIsLittleEndian != IsLittleEndian {
		return nil, 0, fmt.Errorf("file %s %s", fName, endianMismatchErr)
	}

	r := &ShardedReader{features: features, fileName: fName, m: m}
	for s := 0; s < shardCount; s++ {
		off := shardedHeaderSize + s*shardDescriptorSize
		sc := binary.BigEndian.Uint32(m[off:])
		if sc == 0 {
			continue // empty shard; leave r.shards[s] zero-valued
		}
		sl := binary.BigEndian.Uint32(m[off+4:])
		if sl == 0 || sl&(sl-1) != 0 {
			return nil, 0, fmt.Errorf("file %s: shard %d invalid SegmentLength %d (must be non-zero power of two)", fName, s, sl)
		}
		filter := &r.shards[s]
		filter.SegmentCount = sc
		filter.SegmentLength = sl
		filter.SegmentCountLength = sc * sl
		filter.SegmentLengthMask = sl - 1
		filter.Seed = binary.BigEndian.Uint64(m[off+8:])
	}
	for i := 0; i <= shardCount; i++ {
		r.fpOffsets[i] = binary.BigEndian.Uint64(m[shardDescriptorsEnd+i*8:])
	}
	// Validate offsets are monotonic and within bounds.
	total := r.fpOffsets[shardCount]
	if uint64(len(m)) < uint64(shardedFixedHeader)+total {
		return nil, 0, fmt.Errorf("file %s: sharded fuse filter fingerprints truncated: have %d bytes, need %d", fName, len(m), uint64(shardedFixedHeader)+total)
	}
	for i := 0; i < shardCount; i++ {
		if r.fpOffsets[i] > r.fpOffsets[i+1] {
			return nil, 0, fmt.Errorf("file %s: sharded fuse filter offsets not monotonic at shard %d", fName, i)
		}
	}
	r.fpRegion = m[shardedFixedHeader : shardedFixedHeader+int(total)]
	for s := 0; s < shardCount; s++ {
		if r.shards[s].SegmentCount == 0 {
			continue
		}
		r.shards[s].Fingerprints = r.fpRegion[r.fpOffsets[s]:r.fpOffsets[s+1]]
	}
	return r, shardedFixedHeader + int(total), nil
}

// ContainsHash routes the query to the shard identified by the low byte of v
// and delegates to that shard's BinaryFuse[uint8].Contains. Empty shards are
// identified by SegmentCount == 0 (xorfilter would panic on Contains).
func (r *ShardedReader) ContainsHash(v uint64) bool {
	shard := &r.shards[byte(v)]
	if shard.SegmentCount == 0 {
		return false
	}
	return shard.Contains(v)
}

// ForceInMem copies the entire fingerprints region out of the mmap into a
// heap allocation so the reader no longer depends on the underlying mmap.
// Returns the number of bytes now held in RAM.
func (r *ShardedReader) ForceInMem() datasize.ByteSize {
	cloned := bytes.Clone(r.fpRegion)
	r.fpRegion = cloned
	for s := 0; s < shardCount; s++ {
		if r.shards[s].SegmentCount == 0 {
			continue
		}
		r.shards[s].Fingerprints = cloned[r.fpOffsets[s]:r.fpOffsets[s+1]]
	}
	r.keepInMem = true
	return datasize.ByteSize(len(cloned))
}

func (r *ShardedReader) MadvWillNeed() {
	if r == nil || len(r.m) == 0 || r.keepInMem {
		return
	}
	if err := mm.MadviseWillNeed(r.m); err != nil {
		panic(err)
	}
}

func (r *ShardedReader) MadvNormal() {
	if r == nil || len(r.m) == 0 || r.keepInMem {
		return
	}
	if err := mm.MadviseNormal(r.m); err != nil {
		panic(err)
	}
}

func (r *ShardedReader) FileName() string { return r.fileName }

func (r *ShardedReader) Close() {
	if r == nil || r.f == nil {
		return
	}
	_ = r.m.Unmap()
	_ = r.f.Close()
	r.f = nil
}
