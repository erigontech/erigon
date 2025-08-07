package fusefilter

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/erigontech/erigon-lib/common/dir"
	"io"
	"math"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/FastFilter/xorfilter"
	"github.com/edsrzf/mmap-go"
)

// WriterOffHeap does write all keys to temporary mmap file - and using it as a source for `fusefilter` building
type WriterOffHeap struct {
	count    int
	page     [512]uint64
	features Features

	tmpFile     *os.File
	tmpFilePath string
}

func NewWriterOffHeap(filePath string) (*WriterOffHeap, error) {
	tmpFilePath := filePath + ".existence.tmp"
	f, err := os.Create(tmpFilePath)
	if err != nil {
		return nil, err
	}
	var features Features
	if IsLittleEndian {
		features |= IsLittleEndianFeature
	}
	return &WriterOffHeap{tmpFile: f, features: features, tmpFilePath: tmpFilePath}, nil
}

func (w *WriterOffHeap) Close() {
	if w.tmpFile != nil {
		w.tmpFile.Close()
		dir.RemoveFile(w.tmpFilePath)
	}
}

func (w *WriterOffHeap) build() (*xorfilter.BinaryFuse[uint8], error) {
	defer dir.RemoveFile(w.tmpFilePath)
	if w.count%len(w.page) != 0 {
		if _, err := w.tmpFile.Write(castToBytes(w.page[:w.count%len(w.page)])); err != nil {
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
		return nil, fmt.Errorf("%s %w", w.tmpFilePath, err)
	}
	defer m.Unmap()

	keysHashes := castToArrU64(m[:sz])

	filter, err := xorfilter.NewBinaryFuse[uint8](keysHashes)
	if err != nil {
		return nil, fmt.Errorf("%s %w", w.tmpFilePath, err)
	}
	return filter, nil
}

func (w *WriterOffHeap) write(filter *xorfilter.BinaryFuse[uint8], fw io.Writer) (int, error) {
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

	if _, err := fw.Write(header[:]); err != nil { //nolint:gocritic
		return 0, err
	}
	if _, err := fw.Write(filter.Fingerprints); err != nil {
		return 0, err
	}
	return headerSize + len(filter.Fingerprints), nil
}

func (w *WriterOffHeap) AddHash(k uint64) error {
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

func (w *WriterOffHeap) BuildTo(to io.Writer) (int, error) {
	filter, err := w.build()
	if err != nil {
		return 0, fmt.Errorf("%s %w", w.tmpFilePath, err)
	}
	return w.write(filter, to)
}

type Writer struct {
	filePath string
	fileName string
	noFsync  bool

	data *WriterOffHeap
}

func NewWriter(filePath string) (*Writer, error) {
	_, fileName := filepath.Split(filePath)
	w, err := NewWriterOffHeap(filePath)
	if err != nil {
		return nil, err
	}
	return &Writer{filePath: filePath, fileName: fileName, data: w}, nil
}

func (w *Writer) DisableFsync()          { w.noFsync = true }
func (w *Writer) FileName() string       { return w.fileName }
func (w *Writer) AddHash(k uint64) error { return w.data.AddHash(k) }

func (w *Writer) Build() error {
	tmpResultFilePath := w.filePath + ".tmp"
	defer dir.RemoveFile(tmpResultFilePath)
	f, err := os.Create(tmpResultFilePath)
	if err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}
	defer f.Close()

	fw := bufio.NewWriter(f)
	defer fw.Flush()

	if _, err = w.data.BuildTo(fw); err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}
	if err = fw.Flush(); err != nil {
		return err
	}
	if !w.noFsync {
		if err = f.Sync(); err != nil {
			return err
		}
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpResultFilePath, w.filePath); err != nil {
		return err
	}
	return nil
}

func (w *Writer) Close() {
	if w.data != nil {
		w.data.Close()
		w.data = nil
		dir.RemoveFile(w.filePath + ".tmp")
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
