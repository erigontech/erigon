package fusefilter

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/FastFilter/xorfilter"
	"github.com/edsrzf/mmap-go"

	"github.com/erigontech/erigon/common/dir"
)

// Sharded fuse filter: 256 independent BinaryFuse[uint8] filters, routed by
// the low byte of the hash. Enables building/reading at very large key counts
// by keeping each per-shard fingerprint region small enough for good query
// locality, and each per-shard xorfilter build small enough to fit in RAM.
//
// On-disk layout written by BuildTo and parsed by ShardedReader:
//
//	[0..3]       features (u32 BE, same encoding as monolithic filter)
//	[4..7171]    256 × 28-byte descriptors
//	[7172..9227] 257 × 8-byte cumulative fingerprint offsets (u64 BE)
//	[9228..]     concatenated shard fingerprints (empty shards: 0 bytes)
const (
	shardCount          = 256
	shardedHeaderSize   = 4
	shardDescriptorSize = 28
	shardDescriptorsEnd = shardedHeaderSize + shardCount*shardDescriptorSize // 7172
	shardOffsetTableEnd = shardDescriptorsEnd + (shardCount+1)*8             // 9228
	shardedFixedHeader  = shardOffsetTableEnd                                // bytes before fingerprints
)

const (
	shardFlagEmpty uint32 = 0b1
)

type shardDescriptor struct {
	SegmentCount       uint32
	SegmentCountLength uint32
	Seed               uint64
	SegmentLength      uint32
	SegmentLengthMask  uint32
	flags              uint32
}

// ShardedWriterOffHeap buffers keys per-shard in 256 small page buffers and
// spills each page to a dedicated per-shard temp file. At BuildTo time, each
// shard's tmp file is mmap'd RDWR and handed directly to
// xorfilter.BuildBinaryFuse (which may mutate its input to dedup).
//
// Requires RLIMIT_NOFILE high enough for 256 concurrent temp files; Erigon
// already bumps this for MDBX so it is satisfied in all deployments.
type ShardedWriterOffHeap struct {
	pages     *[shardCount][512]uint64 // heap; 1 MiB
	counts    [shardCount]uint16       // 0..512 in-flight fill level per shard
	keyCounts [shardCount]uint64       // total keys routed to each shard
	tmpFiles  [shardCount]*os.File
	filePath  string
	features  Features
}

func NewShardedWriterOffHeap(filePath string) (*ShardedWriterOffHeap, error) {
	w := &ShardedWriterOffHeap{
		pages:    new([shardCount][512]uint64),
		filePath: filePath,
	}
	if IsLittleEndian {
		w.features |= IsLittleEndianFeature
	}
	for s := 0; s < shardCount; s++ {
		f, err := dir.CreateTempWithExtension(filePath, fmt.Sprintf("existence.shard%03d.tmp", s))
		if err != nil {
			w.closeFiles()
			return nil, err
		}
		w.tmpFiles[s] = f
	}
	return w, nil
}

func (w *ShardedWriterOffHeap) closeFiles() {
	for s := 0; s < shardCount; s++ {
		if w.tmpFiles[s] == nil {
			continue
		}
		path := w.tmpFiles[s].Name()
		_ = w.tmpFiles[s].Close()
		dir.RemoveFile(path)
		w.tmpFiles[s] = nil
	}
}

func (w *ShardedWriterOffHeap) Close() {
	w.closeFiles()
}

func (w *ShardedWriterOffHeap) AddHash(k uint64) error {
	s := byte(k)
	pc := w.counts[s]
	w.pages[s][pc] = k
	w.counts[s] = pc + 1
	w.keyCounts[s]++
	if pc+1 == 512 {
		if _, err := w.tmpFiles[s].Write(castToBytes(w.pages[s][:])); err != nil {
			return err
		}
		w.counts[s] = 0
	}
	return nil
}

// BuildTo builds 256 independent BinaryFuse[uint8] filters and emits the
// sharded layout to `to`. Returns the total number of bytes written (including
// fixed header, descriptors, offset table, and concatenated fingerprints).
func (w *ShardedWriterOffHeap) BuildTo(to io.Writer) (int, error) {
	defer w.closeFiles()

	// Phase 1: flush partial pages for each shard.
	for s := 0; s < shardCount; s++ {
		if pc := w.counts[s]; pc > 0 {
			if _, err := w.tmpFiles[s].Write(castToBytes(w.pages[s][:pc])); err != nil {
				return 0, err
			}
			w.counts[s] = 0
		}
	}

	// Phase 2: find the largest shard to pre-size the shared builder.
	largest := uint64(0)
	for s := 0; s < shardCount; s++ {
		if w.keyCounts[s] > largest {
			largest = w.keyCounts[s]
		}
	}
	builder := xorfilter.MakeBinaryFuseBuilder[uint8](max(1, int(largest)))

	// Phase 3: per-shard build, stream fingerprints to a scratch file.
	fpScratch, err := dir.CreateTempWithExtension(w.filePath, "existence.fp.tmp")
	if err != nil {
		return 0, err
	}
	defer dir.RemoveFile(fpScratch.Name())
	fpBuf := bufio.NewWriter(fpScratch)
	defer fpScratch.Close()

	descriptors := make([]shardDescriptor, shardCount)
	offsets := make([]uint64, shardCount+1)
	var written uint64
	for s := 0; s < shardCount; s++ {
		offsets[s] = written
		if w.keyCounts[s] == 0 {
			descriptors[s].flags |= shardFlagEmpty
			continue
		}
		sz := int(w.keyCounts[s]) * 8
		// RDWR: BuildBinaryFuse may mutate `keys` via pruneDuplicates. The tmp
		// file is disposable and under our control, so in-place mutation is
		// fine and avoids a per-shard u64 copy.
		m, err := mmap.MapRegion(w.tmpFiles[s], sz, mmap.RDWR, 0, 0)
		if err != nil {
			return 0, fmt.Errorf("shard %d mmap: %w", s, err)
		}
		keys := castToArrU64(m[:sz])
		f, err := xorfilter.BuildBinaryFuse[uint8](&builder, keys)
		if err != nil {
			_ = m.Unmap()
			return 0, fmt.Errorf("shard %d build: %w", s, err)
		}
		if f.SegmentCount > math.MaxUint32/2 {
			_ = m.Unmap()
			return 0, fmt.Errorf("shard %d SegmentCount=%d cannot be greater than u32/2", s, f.SegmentCount)
		}
		descriptors[s] = shardDescriptor{
			SegmentCount:       f.SegmentCount,
			SegmentCountLength: f.SegmentCountLength,
			Seed:               f.Seed,
			SegmentLength:      f.SegmentLength,
			SegmentLengthMask:  f.SegmentLengthMask,
		}
		// f.Fingerprints aliases the builder — write it out now, before the
		// next BuildBinaryFuse call reuses the builder buffer.
		if _, err := fpBuf.Write(f.Fingerprints); err != nil {
			_ = m.Unmap()
			return 0, err
		}
		written += uint64(len(f.Fingerprints))
		if err := m.Unmap(); err != nil {
			return 0, err
		}
	}
	offsets[shardCount] = written
	if err := fpBuf.Flush(); err != nil {
		return 0, err
	}
	if _, err := fpScratch.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	// Phase 4: emit header + descriptors + offsets + fingerprints.
	var header [shardOffsetTableEnd]byte
	binary.BigEndian.PutUint32(header[:4], uint32(w.features))
	for s := 0; s < shardCount; s++ {
		off := shardedHeaderSize + s*shardDescriptorSize
		d := &descriptors[s]
		binary.BigEndian.PutUint32(header[off:], d.SegmentCount)
		binary.BigEndian.PutUint32(header[off+4:], d.SegmentCountLength)
		binary.BigEndian.PutUint64(header[off+8:], d.Seed)
		binary.BigEndian.PutUint32(header[off+16:], d.SegmentLength)
		binary.BigEndian.PutUint32(header[off+20:], d.SegmentLengthMask)
		binary.BigEndian.PutUint32(header[off+24:], d.flags)
	}
	for i := 0; i <= shardCount; i++ {
		binary.BigEndian.PutUint64(header[shardDescriptorsEnd+i*8:], offsets[i])
	}
	if _, err := to.Write(header[:]); err != nil {
		return 0, err
	}
	if _, err := io.Copy(to, fpScratch); err != nil {
		return 0, err
	}
	return shardedFixedHeader + int(written), nil
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

func (w *ShardedWriter) Build() error {
	f, err := dir.CreateTemp(w.filePath)
	if err != nil {
		return fmt.Errorf("%s %w", w.filePath, err)
	}
	defer dir.RemoveFile(f.Name())
	defer f.Close()

	fw := bufio.NewWriter(f)
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
	return os.Rename(f.Name(), w.filePath)
}

func (w *ShardedWriter) Close() {
	if w.data != nil {
		w.data.Close()
		w.data = nil
	}
}
