package fusefilter

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/FastFilter/xorfilter"
	"github.com/edsrzf/mmap-go"
)

type Writer struct {
	filePath string
	fileName string
	noFsync  bool
	tmpFile  *os.File

	count int
	page  [512]uint64
}

func NewWriter(filePath string) (*Writer, error) {
	if !IsLittleEndian {
		return nil, fmt.Errorf("TODO: on BigEndian machine - store in file LittleEndian-encoded `data`. Then on reader side convert it to BigEndian and store to new `.be` file - and mmap it instead of original file")
	}

	f, err := os.Create(filePath + ".tmp.tmp")
	if err != nil {
		return nil, err
	}
	_, fileName := filepath.Split(filePath)
	return &Writer{tmpFile: f, filePath: filePath, fileName: fileName}, nil
}

func (w *Writer) DisableFsync()    { w.noFsync = true }
func (w *Writer) FileName() string { return w.fileName }
func (w *Writer) AddHash(k uint64) error {
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

func (w *Writer) Build() error {
	defer os.Remove(w.filePath + ".tmp.tmp")
	defer os.Remove(w.filePath + ".tmp")

	if w.count%len(w.page) != 0 {
		_, err := w.tmpFile.Write(castToBytes(w.page[:w.count%len(w.page)]))
		if err != nil {
			return err
		}
	}

	st, err := w.tmpFile.Stat()
	if err != nil {
		return err
	}
	sz := int(st.Size())
	m, err := mmap.MapRegion(w.tmpFile, sz, mmap.RDONLY, 0, 0)
	if err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}
	defer m.Unmap()

	keysHashes := castToArrU64(m[:sz])

	filter, err := xorfilter.NewBinaryFuse[uint8](keysHashes)
	if err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}

	f, err := os.Create(w.filePath + ".tmp")
	if err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}
	defer f.Close()

	fw := bufio.NewWriter(f)
	defer fw.Flush()

	var _header [1 + 8 + 4 + 4 + 4 + 4 + 4]byte
	_header[0] = 1 //version
	header := _header[1:]
	binary.BigEndian.PutUint64(header[:], filter.Seed) //nolint:gocritic
	binary.BigEndian.PutUint32(header[8:], filter.SegmentLength)
	binary.BigEndian.PutUint32(header[8+4:], filter.SegmentLengthMask)
	binary.BigEndian.PutUint32(header[8+4+4:], filter.SegmentCount)
	binary.BigEndian.PutUint32(header[8+4+4+4:], filter.SegmentCountLength)
	binary.BigEndian.PutUint32(header[8+4+4+4+4:], uint32(len(filter.Fingerprints)))
	_, err = fw.Write(_header[:]) //nolint:gocritic
	if err != nil {
		return err
	}

	_, err = fw.Write(filter.Fingerprints)
	if err != nil {
		return err
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
	if w.tmpFile == nil {
		return
	}
	w.tmpFile.Close()
	os.Remove(w.filePath + ".tmp.tmp")
	os.Remove(w.filePath + ".tmp")
	w.tmpFile = nil
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
