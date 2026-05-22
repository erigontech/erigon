package fusefilter

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"unsafe"

	"github.com/erigontech/erigon/common/dir"

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

func initFeatures() Features {
	var f Features
	if IsLittleEndian {
		f |= IsLittleEndianFeature
	}
	return f
}

func NewWriterOffHeap(filePath string) (*WriterOffHeap, error) {
	f, err := dir.CreateTempWithExtension(filePath, "existence.tmp")
	if err != nil {
		return nil, err
	}
	return &WriterOffHeap{tmpFile: f, features: initFeatures(), tmpFilePath: f.Name()}, nil
}

func (w *WriterOffHeap) Close() {
	if w.tmpFile != nil {
		w.tmpFile.Close()
		dir.RemoveFile(w.tmpFilePath)
		w.tmpFile = nil
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

// filterBlobHeaderSize is the fixed header size of a serialised BinaryFuse[uint8] blob.
const filterBlobHeaderSize = 1 + 3 + 4 + 4 + 8 + 4 + 4 + 8

// writeFilter serialises a built BinaryFuse filter as a version-0 fusefilter blob.
func writeFilter(features Features, filter *xorfilter.BinaryFuse[uint8], fw io.Writer) (int, error) {
	if filter.SegmentCount > math.MaxUint32/2 {
		return 0, fmt.Errorf("SegmentCount=%d cannot be greater than u32/2", filter.SegmentCount)
	}
	const version uint8 = 0
	var header [headerSize]byte

	binary.BigEndian.PutUint32(header[:], uint32(features)) //nolint:gocritic
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
	return writeFilter(w.features, filter, to)
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
	return &Writer{
		filePath: filePath,
		fileName: fileName,
		data:     w,
	}, nil
}

func (w *Writer) DisableFsync()          { w.noFsync = true }
func (w *Writer) FileName() string       { return w.fileName }
func (w *Writer) AddHash(k uint64) error { return w.data.AddHash(k) }

func (w *Writer) Build() error {
	f, err := dir.CreateTemp(w.filePath)
	if err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}
	defer dir.RemoveFile(f.Name())
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
	if err = os.Rename(f.Name(), w.filePath); err != nil {
		return err
	}
	return nil
}

func (w *Writer) Close() {
	if w.data != nil {
		w.data.Close()
		w.data = nil
	}
}

// WriterSharded shards keys into 256 sub-filters by first byte of keyHash (keyHash >> 56).
// Produces fusefilter file version 1. Lookup checks only the one relevant shard.
// Embeds WriterOffHeap for the shared page-buffer + temp-file machinery.
type WriterSharded struct {
	WriterOffHeap
	filePath string
	noFsync  bool
}

func NewWriterSharded(filePath string) (*WriterSharded, error) {
	f, err := dir.CreateTempWithExtension(filePath, "existence-sharded.tmp")
	if err != nil {
		return nil, err
	}
	return &WriterSharded{
		WriterOffHeap: WriterOffHeap{tmpFile: f, tmpFilePath: f.Name(), features: initFeatures()},
		filePath:      filePath,
	}, nil
}

func (w *WriterSharded) DisableFsync() { w.noFsync = true }

// Build writes the sharded fusefilter to w.filePath using the same atomic
// temp-then-rename pattern as Writer.Build.
func (w *WriterSharded) Build() error {
	f, err := dir.CreateTemp(w.filePath)
	if err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}
	defer dir.RemoveFile(f.Name())
	defer f.Close()

	fw := bufio.NewWriter(f)
	if _, err = w.BuildTo(fw); err != nil {
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
	return os.Rename(f.Name(), w.filePath)
}

// BuildTo writes sharded fusefilter (file version 1) to fw.
// Format: [4 bytes header] then 256 × [8 bytes size | blob] pairs (size==0 means absent).
// Fully streaming: no intermediate files, one shard's fingerprints in RAM at a time.
func (w *WriterSharded) BuildTo(fw io.Writer) (int, error) {
	defer dir.RemoveFile(w.tmpFilePath)

	if rem := w.count % len(w.page); rem != 0 {
		if _, err := w.tmpFile.Write(castToBytes(w.page[:rem])); err != nil {
			return 0, err
		}
	}

	st, err := w.tmpFile.Stat()
	if err != nil {
		return 0, err
	}
	sz := int(st.Size())
	if sz == 0 {
		return 0, fmt.Errorf("WriterSharded: no keys added")
	}

	m, err := mmap.MapRegion(w.tmpFile, sz, mmap.RDWR, 0, 0)
	if err != nil {
		return 0, fmt.Errorf("%s %w", w.tmpFilePath, err)
	}
	defer m.Unmap()

	all := castToArrU64(m[:sz])
	slices.Sort(all) // ascending sort groups hashes by top byte = shard index

	// Find the largest shard so MakeBinaryFuseBuilder preallocates buffers that
	// fit every shard. Without this, sizing the builder to the average shard
	// (len(all)/256) leaves xorfilter's reuseBuffer to grow buffers on the first
	// above-average shard via `*buf = append((*buf)[:0], make([]T, size)...)`,
	// which holds the old AND new buffers live until GC — doubling peak heap
	// briefly. This prescan is one sequential walk over the just-sorted (and
	// thus cache-warm) mmap, so it's essentially free.
	var maxShardSize int
	{
		i := 0
		for shard := range 256 {
			start := i
			for i < len(all) && all[i]>>56 == uint64(shard) {
				i++
			}
			if sz := i - start; sz > maxShardSize {
				maxShardSize = sz
			}
		}
	}

	const version1 uint8 = 1
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(w.features))
	header[0] = version1
	if _, err := fw.Write(header[:]); err != nil {
		return 0, err
	}
	total := 4

	builder := xorfilter.MakeBinaryFuseBuilder[uint8](maxShardSize)
	var sizeBuf [8]byte
	i := 0
	for shard := range 256 {
		start := i
		for i < len(all) && all[i]>>56 == uint64(shard) {
			i++
		}
		if start == i {
			binary.BigEndian.PutUint64(sizeBuf[:], 0)
			if _, err := fw.Write(sizeBuf[:]); err != nil {
				return 0, err
			}
			total += 8
			continue
		}
		filter, err := xorfilter.BuildBinaryFuse[uint8](&builder, all[start:i])
		if err != nil {
			return 0, fmt.Errorf("shard %d: %w", shard, err)
		}
		blobSize := filterSize(&filter)
		binary.BigEndian.PutUint64(sizeBuf[:], uint64(blobSize))
		if _, err := fw.Write(sizeBuf[:]); err != nil {
			return 0, err
		}
		n, err := writeFilter(w.features, &filter, fw)
		if err != nil {
			return 0, fmt.Errorf("shard %d: %w", shard, err)
		}
		total += 8 + n
	}
	return total, nil
}

func filterSize(f *xorfilter.BinaryFuse[uint8]) int {
	return filterBlobHeaderSize + len(f.Fingerprints)
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
