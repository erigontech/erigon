package fusefilter

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/FastFilter/xorfilter"
	"github.com/c2h5oh/datasize"
	"github.com/edsrzf/mmap-go"

	"github.com/erigontech/erigon-lib/common/dbg"
	mm "github.com/erigontech/erigon-lib/mmap"
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

var fuseMem = dbg.EnvBool("FUSE_MEM", true)

var fuseMadvWillNeed = dbg.EnvBool("FUSE_MADV_WILLNEED", false)
var fuseMadvNormal = dbg.EnvBool("FUSE_MADV_NORMAL", false)

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
	var m mmap.MMap
	var content []byte
	if fuseMem {
		content, err = io.ReadAll(bufio.NewReaderSize(f, int(128*datasize.KB)))
		if err != nil {
			_ = f.Close() //nolint
			return nil, err
		}
	} else {
		m, err = mmap.MapRegion(f, sz, mmap.RDONLY, 0, 0)
		if err != nil {
			_ = f.Close() //nolint
			return nil, err
		}
		content = m
	}

	_, fileName := filepath.Split(filePath)
	r, _, err := NewReaderOnBytes(content, fileName)
	if err != nil {
		return nil, err
	}
	r.f = f
	r.m = m
	r.keepInMem = fuseMem
	r.fileName = fileName

	if fuseMadvWillNeed {
		r.MadvWillNeed()
	}
	if fuseMadvNormal {
		r.MadvNormal()
	}
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

var IsLittleEndian = isLittleEndian()

func isLittleEndian() bool {
	var x uint16 = 0x0102
	xb := *(*[2]byte)(unsafe.Pointer(&x))
	return (xb[0] == 0x02)
}
