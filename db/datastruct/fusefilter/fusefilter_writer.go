package fusefilter

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"unsafe"

	"github.com/FastFilter/xorfilter"
	"github.com/edsrzf/mmap-go"

	"github.com/erigontech/erigon/common/dir"
)

// ── monolithic filter ────────────────────────────────────────────────────────

// WriterOffHeap does write all keys to temporary mmap file - and using it as a source for `fusefilter` building
type WriterOffHeap struct {
	count    int
	page     [512]uint64
	features Features

	tmpFile     *os.File
	tmpFilePath string
}

func NewWriterOffHeap(filePath string) (*WriterOffHeap, error) {
	f, err := dir.CreateTempWithExtension(filePath, "existence.tmp")
	if err != nil {
		return nil, err
	}
	var features Features
	if IsLittleEndian {
		features |= IsLittleEndianFeature
	}
	return &WriterOffHeap{tmpFile: f, features: features, tmpFilePath: f.Name()}, nil
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
	return &Writer{
		filePath: filePath,
		fileName: fileName,
		data:     w,
	}, nil
}

func (w *Writer) DisableFsync()          { w.noFsync = true }
func (w *Writer) FileName() string       { return w.fileName }
func (w *Writer) AddHash(k uint64) error { return w.data.AddHash(k) }
func (w *Writer) Build() error           { return buildToFile(w.filePath, w.noFsync, w.data.BuildTo) }

func (w *Writer) Close() {
	if w.data != nil {
		w.data.Close()
		w.data = nil
	}
}

// ── sharded filter (256 shards by low byte of hash) ─────────────────────────

// Sharded fuse filter: 256 independent BinaryFuse[uint8] filters, routed by
// the low byte of the hash. Enables building/reading at very large key counts
// by keeping each per-shard fingerprint region small enough for good query
// locality, and each per-shard xorfilter build small enough to fit in RAM.
//
// On-disk layout written by BuildTo and parsed by ShardedReader:
//
//	[0..3]       features (u32 BE, same encoding as monolithic filter)
//	[4..4099]    256 × 16-byte descriptors: SegmentCount (u32) + SegmentLength
//	             (u32) + Seed (u64). SegmentCount == 0 signals an empty shard.
//	             BinaryFuse[uint8]'s SegmentCountLength and SegmentLengthMask
//	             are derivable (SC×SL and SL-1 — SegmentLength is always a
//	             power of two) and recomputed at read time.
//	[4100..6155] 257 × 8-byte cumulative fingerprint offsets (u64 BE)
//	[6156..]     concatenated shard fingerprints (empty shards: 0 bytes)
const (
	shardCount          = 256
	shardedHeaderSize   = 4
	shardDescriptorSize = 16
	shardDescriptorsEnd = shardedHeaderSize + shardCount*shardDescriptorSize // 4100
	shardOffsetTableEnd = shardDescriptorsEnd + (shardCount+1)*8             // 6156
	shardedFixedHeader  = shardOffsetTableEnd
)

// ShardedWriterOffHeap appends all keys into a single tmp file. At BuildTo
// the file is mmap'd RDWR and sorted in place by byte(k), after which each
// shard's keys occupy a contiguous byte range and are handed directly to
// xorfilter.BuildBinaryFuse without a copy. Sorting the mmap in place trades
// a couple of passes over the tmp file for determinism, zero-copy to
// xorfilter, and a single file descriptor.
type ShardedWriterOffHeap struct {
	page     [512]uint64
	pageFill uint16
	keyCount uint64
	tmpFile  *os.File
	filePath string
	features Features
}

func NewShardedWriterOffHeap(filePath string) (*ShardedWriterOffHeap, error) {
	f, err := dir.CreateTempWithExtension(filePath, "existence.tmp")
	if err != nil {
		return nil, err
	}
	w := &ShardedWriterOffHeap{
		tmpFile:  f,
		filePath: filePath,
	}
	if IsLittleEndian {
		w.features |= IsLittleEndianFeature
	}
	return w, nil
}

func (w *ShardedWriterOffHeap) Close() {
	if w.tmpFile == nil {
		return
	}
	path := w.tmpFile.Name()
	_ = w.tmpFile.Close()
	dir.RemoveFile(path)
	w.tmpFile = nil
}

func (w *ShardedWriterOffHeap) AddHash(k uint64) error {
	w.page[w.pageFill] = k
	w.pageFill++
	w.keyCount++
	if w.pageFill == 512 {
		if _, err := w.tmpFile.Write(castToBytes(w.page[:])); err != nil {
			return err
		}
		w.pageFill = 0
	}
	return nil
}

// BuildTo partitions all previously-added keys in place by byte(k), builds
// 256 independent BinaryFuse[uint8] filters (one per shard), and emits the
// sharded layout to `to`. Returns the total number of bytes written.
func (w *ShardedWriterOffHeap) BuildTo(to io.Writer) (int, error) {
	defer w.Close()

	if w.pageFill > 0 {
		if _, err := w.tmpFile.Write(castToBytes(w.page[:w.pageFill])); err != nil {
			return 0, err
		}
		w.pageFill = 0
	}

	var header [shardOffsetTableEnd]byte
	binary.BigEndian.PutUint32(header[:4], uint32(w.features))

	if w.keyCount == 0 {
		return to.Write(header[:])
	}

	total := int(w.keyCount) * 8
	m, err := mmap.MapRegion(w.tmpFile, total, mmap.RDWR, 0, 0)
	if err != nil {
		return 0, fmt.Errorf("mmap: %w", err)
	}
	defer m.Unmap()
	keysHashes := castToArrU64(m[:total])

	var keyOffsets [shardCount + 1]int
	for _, k := range keysHashes {
		keyOffsets[int(byte(k))+1]++
	}
	largest := 0
	for s := 0; s < shardCount; s++ {
		if keyOffsets[s+1] > largest {
			largest = keyOffsets[s+1]
		}
		keyOffsets[s+1] += keyOffsets[s]
	}

	slices.SortFunc(keysHashes, func(a, b uint64) int { return cmp.Compare(byte(a), byte(b)) })

	builder := xorfilter.MakeBinaryFuseBuilder[uint8](max(1, largest))

	// xorfilter returns f.Fingerprints aliased to its builder buffer, so each
	// shard's fingerprints must be persisted before the next BuildBinaryFuse
	// reuses that buffer. Reusing the keys mmap isn't safe: for tiny shards
	// xorfilter's sizeFactor drives fp bytes larger than the shard's 8-bytes-
	// per-key region and would overwrite later shards' keys. A disposable
	// scratch file is the simplest safe home for the fingerprints.
	fpScratch, err := dir.CreateTempWithExtension(w.filePath, "existence.fp.tmp")
	if err != nil {
		return 0, err
	}
	defer dir.RemoveFile(fpScratch.Name())
	defer fpScratch.Close()

	var fingerprintBytes uint64
	for s := 0; s < shardCount; s++ {
		binary.BigEndian.PutUint64(header[shardDescriptorsEnd+s*8:], fingerprintBytes)
		shardKeys := keysHashes[keyOffsets[s]:keyOffsets[s+1]]
		if len(shardKeys) == 0 {
			continue // SegmentCount left at 0 signals empty.
		}
		f, err := xorfilter.BuildBinaryFuse[uint8](&builder, shardKeys)
		if err != nil {
			return 0, fmt.Errorf("shard %d build: %w", s, err)
		}
		if f.SegmentCount > math.MaxUint32/2 {
			return 0, fmt.Errorf("shard %d SegmentCount=%d cannot be greater than u32/2", s, f.SegmentCount)
		}
		descOff := shardedHeaderSize + s*shardDescriptorSize
		binary.BigEndian.PutUint32(header[descOff:], f.SegmentCount)
		binary.BigEndian.PutUint32(header[descOff+4:], f.SegmentLength)
		binary.BigEndian.PutUint64(header[descOff+8:], f.Seed)
		if _, err := fpScratch.Write(f.Fingerprints); err != nil {
			return 0, err
		}
		fingerprintBytes += uint64(len(f.Fingerprints))
	}
	binary.BigEndian.PutUint64(header[shardDescriptorsEnd+shardCount*8:], fingerprintBytes)
	if _, err := fpScratch.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	headerBytes, err := to.Write(header[:])
	if err != nil {
		return 0, err
	}
	if _, err := io.Copy(to, fpScratch); err != nil {
		return 0, err
	}
	return headerBytes + int(fingerprintBytes), nil
}

// ShardedWriter wraps ShardedWriterOffHeap with standalone file creation, for
// callers that want to build a fuse filter file independent of a recsplit
// index.
type ShardedWriter struct {
	filePath string
	fileName string
	noFsync  bool

	data *ShardedWriterOffHeap
}

func NewShardedWriter(filePath string) (*ShardedWriter, error) {
	_, fileName := filepath.Split(filePath)
	w, err := NewShardedWriterOffHeap(filePath)
	if err != nil {
		return nil, err
	}
	return &ShardedWriter{
		filePath: filePath,
		fileName: fileName,
		data:     w,
	}, nil
}

func (w *ShardedWriter) DisableFsync()          { w.noFsync = true }
func (w *ShardedWriter) FileName() string       { return w.fileName }
func (w *ShardedWriter) AddHash(k uint64) error { return w.data.AddHash(k) }
func (w *ShardedWriter) Build() error           { return buildToFile(w.filePath, w.noFsync, w.data.BuildTo) }

func (w *ShardedWriter) Close() {
	if w.data != nil {
		w.data.Close()
		w.data = nil
	}
}

// ── shared helpers ────────────────────────────────────────────────────────────

// buildToFile writes a filter to a temp file via build, then atomically renames it to filePath.
func buildToFile(filePath string, noFsync bool, build func(io.Writer) (int, error)) error {
	f, err := dir.CreateTemp(filePath)
	if err != nil {
		return fmt.Errorf("%s %w", filePath, err)
	}
	defer dir.RemoveFile(f.Name())
	defer f.Close()

	fw := bufio.NewWriter(f)
	if _, err = build(fw); err != nil {
		return fmt.Errorf("%s %w", filePath, err)
	}
	if err = fw.Flush(); err != nil {
		return err
	}
	if !noFsync {
		if err = f.Sync(); err != nil {
			return err
		}
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filePath)
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
