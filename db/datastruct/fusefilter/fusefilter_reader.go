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

// parseHeaderFeatures reads the 4-byte [version | features] prefix, validates endianness, and returns both.
func parseHeaderFeatures(header []byte, fName string) (version uint8, features Features, err error) {
	version = header[0]
	featuresBytes := bytes.Clone(header[:4])
	featuresBytes[0] = 0
	features = Features(binary.BigEndian.Uint32(featuresBytes))
	if (features&IsLittleEndianFeature != 0) != IsLittleEndian {
		return 0, 0, fmt.Errorf("file %s is not compatible with your machine (different Endianness), but you can run `erigon snapshots index`", fName)
	}
	return version, features, nil
}

func NewReaderOnBytes(m []byte, fName string) (*Reader, int, error) {
	filter := &xorfilter.BinaryFuse[uint8]{}

	const headerSize = 1 + 3 + 4 + 4 + 8 + 4 + 4 + 8
	header, data := m[:headerSize], m[headerSize:]

	v, features, err := parseHeaderFeatures(header, fName)
	if err != nil {
		return nil, 0, err
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

// ReaderSharded reads a sharded fusefilter (file version 1).
// Only the shard matching keyHash >> 56 is checked on lookup.
type ReaderSharded struct {
	shards [256]*Reader
}

// NewReaderShardedOnBytes parses a sharded fusefilter blob (version 1) from m.
// Returns the reader and the number of bytes consumed.
func NewReaderShardedOnBytes(m []byte, fName string) (*ReaderSharded, int, error) {
	const headerSize = 4
	const sizeTableSize = 256 * 8
	if len(m) < headerSize+sizeTableSize {
		return nil, 0, fmt.Errorf("fusefilter sharded %s: too small (%d bytes)", fName, len(m))
	}
	if _, _, err := parseHeaderFeatures(m[:4], fName); err != nil {
		return nil, 0, err
	}

	r := &ReaderSharded{}
	offset := headerSize + sizeTableSize
	for i := range 256 {
		sz := int(binary.BigEndian.Uint64(m[headerSize+i*8:]))
		if sz == 0 {
			continue
		}
		shard, _, err := NewReaderOnBytes(m[offset:offset+sz], fName)
		if err != nil {
			return nil, 0, fmt.Errorf("shard %d of %s: %w", i, fName, err)
		}
		r.shards[i] = shard
		offset += sz
	}
	return r, offset, nil
}

func (r *ReaderSharded) ContainsHash(v uint64) bool {
	s := r.shards[v>>56]
	if s == nil {
		return false
	}
	return s.ContainsHash(v)
}

func (r *ReaderSharded) ForceInMem() datasize.ByteSize {
	var total datasize.ByteSize
	for _, s := range r.shards {
		if s != nil {
			total += s.ForceInMem()
		}
	}
	return total
}

var IsLittleEndian = isLittleEndian()

func isLittleEndian() bool {
	var x uint16 = 0x0102
	xb := *(*[2]byte)(unsafe.Pointer(&x))
	return (xb[0] == 0x02)
}
