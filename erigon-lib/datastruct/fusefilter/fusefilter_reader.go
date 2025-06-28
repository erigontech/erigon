package fusefilter

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/FastFilter/xorfilter"
	"github.com/edsrzf/mmap-go"
)

type Reader struct {
	inner    *xorfilter.BinaryFuse[uint8]
	filePath string
	fileName string
	f        *os.File
	m        mmap.MMap

	version uint8
}

func NewReader(filePath string) (*Reader, error) {
	if !IsLittleEndian {
		return nil, fmt.Errorf("TODO: On reader side convert `data` to BigEndian and store to new `.be` file - and mmap it instead of original file")
	}

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
	filter := &xorfilter.BinaryFuse[uint8]{}

	v := m[0]
	headerSize := 8 + 4 + 4 + 4 + 4 + 4
	header, data := m[1:1+headerSize], m[1+headerSize:]
	filter.Seed = binary.BigEndian.Uint64(header[:])
	filter.SegmentLength = binary.BigEndian.Uint32(header[8:])
	filter.SegmentLengthMask = binary.BigEndian.Uint32(header[8+4:])
	filter.SegmentCount = binary.BigEndian.Uint32(header[8+4+4:])
	filter.SegmentCountLength = binary.BigEndian.Uint32(header[8+4+4+4:])
	fingerprintsLen := binary.BigEndian.Uint32(header[8+4+4+4+4:])
	filter.Fingerprints = data[:fingerprintsLen]

	_, fileName := filepath.Split(filePath)
	return &Reader{inner: filter, version: v, f: f, m: m, filePath: filePath, fileName: fileName}, nil
}

func (r *Reader) FileName() string { return r.fileName }
func (r *Reader) ContainsHash(v uint64) bool {
	if r.f == nil {
		panic("closed: " + r.fileName)
	}
	return r.inner.Contains(v)
}
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
