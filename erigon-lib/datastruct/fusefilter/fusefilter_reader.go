package fusefilter

import (
	"encoding/binary"
	"os"

	"github.com/FastFilter/xorfilter"
	"github.com/edsrzf/mmap-go"
)

type Reader struct {
	inner    *xorfilter.BinaryFuse[uint8]
	filePath string
	f        *os.File
	m        mmap.MMap

	version uint8
}

func NewReader(filePath string) (*Reader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		panic(err)
		return nil, err
	}
	sz := int(st.Size())
	m, err := mmap.MapRegion(f, sz, mmap.RDONLY, 0, 0)
	if err != nil {
		panic(err)
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

	return &Reader{inner: filter, version: v, f: f, m: m, filePath: filePath}, nil
}

func (r *Reader) ContainsHash(v uint64) bool { return r.inner.Contains(v) }
func (r *Reader) Close() {
	if r == nil || r.f == nil {
		return
	}
	r.f.Close()
	r.f = nil
}
