package fusefilter

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/FastFilter/xorfilter"
	"github.com/edsrzf/mmap-go"
)

type WriterStateless struct {
	count    int
	page     [512]uint64
	features Features

	tmpFile *os.File
	fPath   string
}

func NewWriterStateless(filePath string) (*WriterStateless, error) {
	tmpFilePath := filePath + ".existence.tmp"
	f, err := os.Create(tmpFilePath)
	if err != nil {
		return nil, err
	}
	var features Features
	if IsLittleEndian {
		features |= IsLittleEndianFeature
	}
	return &WriterStateless{tmpFile: f, features: features, fPath: tmpFilePath}, nil
}

func (w *WriterStateless) Close() {
	if w.tmpFile != nil {
		w.tmpFile.Close()
		os.Remove(w.fPath)
	}
}

func (w *WriterStateless) build() (*xorfilter.BinaryFuse[uint8], error) {
	defer os.Remove(w.fPath)
	if w.count%len(w.page) != 0 {
		_, err := w.tmpFile.Write(castToBytes(w.page[:w.count%len(w.page)]))
		if err != nil {
			return nil, err
		}
	}

	st, err := w.tmpFile.Stat()
	if err != nil {
		return nil, err
	}
	sz := int(st.Size())
	m, err := mmap.MapRegion(w.tmpFile, sz, mmap.RDONLY, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("%s %w", w.fPath, err)
	}
	defer m.Unmap()

	keysHashes := castToArrU64(m[:sz])

	filter, err := xorfilter.NewBinaryFuse[uint8](keysHashes)
	if err != nil {
		return nil, fmt.Errorf("%s %w", w.fPath, err)
	}
	if w.count%len(w.page) != 0 {
		_, err := w.tmpFile.Write(castToBytes(w.page[:w.count%len(w.page)]))
		if err != nil {
			return nil, err
		}
	}
	return filter, nil
}

func (w *WriterStateless) write(filter *xorfilter.BinaryFuse[uint8], fw io.Writer) (int, error) {
	if filter.SegmentCount > math.MaxUint32/2 {
		return 0, fmt.Errorf("SegmentCount=%d cannot be greater than u32/2", filter.SegmentCount)
	}
	const headerSize = 1 + 3 + 4 + 4 + 8 + 4 + 4 + 8
	const version uint8 = 0
	var header [headerSize]byte

	// 1 byte - version, 3 bytes `Features`
	binary.BigEndian.PutUint32(header[:], uint32(w.features)) //nolint:gocritic
	header[0] = version

	binary.BigEndian.PutUint32(header[4:], filter.SegmentCount)
	binary.BigEndian.PutUint32(header[4+4:], filter.SegmentCountLength)
	binary.BigEndian.PutUint64(header[4+4+4:], filter.Seed)
	binary.BigEndian.PutUint32(header[4+4+4+8:], filter.SegmentLength)
	binary.BigEndian.PutUint32(header[4+4+4+8+4:], filter.SegmentLengthMask)
	binary.BigEndian.PutUint64(header[4+4+4+8+4+4:], uint64(len(filter.Fingerprints)))
	_, err := fw.Write(header[:]) //nolint:gocritic
	if err != nil {
		return 0, err
	}
	_, err = fw.Write(filter.Fingerprints)
	if err != nil {
		return 0, err
	}
	return headerSize + len(filter.Fingerprints), nil
}

func (w *WriterStateless) AddHash(k uint64) error {
	w.page[w.count%len(w.page)] = k
	w.count++
	if w.count%len(w.page) == 0 {
		_, err := w.tmpFile.Write(castToBytes(w.page[:]))
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *WriterStateless) BuildTo(to io.Writer) (int, error) {
	filter, err := w.build()
	if err != nil {
		return 0, fmt.Errorf("%s %w", w.fPath, err)
	}
	return w.write(filter, to)
}

type Writer struct {
	filePath string
	fileName string
	noFsync  bool

	data *WriterStateless
}

func NewWriter(filePath string) (*Writer, error) {
	if !IsLittleEndian {
		return nil, fmt.Errorf("TODO: on BigEndian machine - store in file LittleEndian-encoded `data`. Then on reader side convert it to BigEndian and store to new `.be` file - and mmap it instead of original file")
	}

	_, fileName := filepath.Split(filePath)
	w, err := NewWriterStateless(filePath)
	if err != nil {
		return nil, err
	}
	return &Writer{filePath: filePath, fileName: fileName, data: w}, nil
}

func (w *Writer) DisableFsync()          { w.noFsync = true }
func (w *Writer) FileName() string       { return w.fileName }
func (w *Writer) AddHash(k uint64) error { return w.data.AddHash(k) }

func (w *Writer) Build() error {
	defer os.Remove(w.filePath + ".tmp")

	f, err := os.Create(w.filePath + ".tmp")
	if err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}
	defer f.Close()

	fw := bufio.NewWriter(f)
	defer fw.Flush()

	_, err = w.data.BuildTo(fw)
	if err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}

	err = fw.Flush()
	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}
	err = os.Rename(w.filePath+".tmp", w.filePath)
	if err != nil {
		return err
	}

	return nil
}

func (w *Writer) Close() {
	if w.data != nil {
		w.data.Close()
		w.data = nil
		os.Remove(w.filePath + ".tmp")
	}
}

// castToBytes converts []uint64 to []byte without copying data
func castToBytes(in []uint64) []byte {
	if len(in) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&in[0])), len(in)*8) // each uint64 is 8 bytes
}

// castToArrU64 converts []byte to []uint64 without copying data on little endian machines
func castToArrU64(b []byte) []uint64 {
	if len(b) == 0 {
		return nil
	}
	if len(b)%8 != 0 {
		panic("byte slice length must be a multiple of 8")
	}

	return unsafe.Slice((*uint64)(unsafe.Pointer(&b[0])), len(b)/8)
}
