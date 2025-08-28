package fusefilter

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/FastFilter/xorfilter"
	"github.com/edsrzf/mmap-go"

	mm "github.com/erigontech/erigon-lib/mmap"
)

type Features uint32

const (
	IsLittleEndianFeature Features = 0b1
)

type Reader struct {
	inner    *xorfilter.BinaryFuse[uint8]
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
	m, err := mmap.MapRegion(f, sz, mmap.RDONLY, 0, 0)
	if err != nil {
		_ = f.Close() //nolint
		return nil, err
	}
	_, fileName := filepath.Split(filePath)
	r, _, err := NewReaderOnBytes(m, fileName)
	if err != nil {
		return nil, err
	}
	r.f = f
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
		return nil, 0, fmt.Errorf("file %s is not compatible with your machine (different Endianness), but you can run `erigon snapshots index`", fName)
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

func (r *Reader) MadvWillNeed() {
	if r == nil || r.m == nil || len(r.m) == 0 {
		return
	}
	if err := mm.MadviseWillNeed(r.m); err != nil {
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

var IsLittleEndian = isLittleEndian()

func isLittleEndian() bool {
	var x uint16 = 0x0102
	xb := *(*[2]byte)(unsafe.Pointer(&x))
	return (xb[0] == 0x02)
}
