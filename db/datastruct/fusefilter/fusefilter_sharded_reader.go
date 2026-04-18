package fusefilter

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/FastFilter/xorfilter"
	"github.com/c2h5oh/datasize"
	"github.com/edsrzf/mmap-go"

	mm "github.com/erigontech/erigon/common/mmap"
)

// ShardedReader is the read side of the 256-shard fuse filter. It holds 256
// BinaryFuse[uint8] value-structs inline; each shard's Fingerprints slice
// points zero-copy into the caller-provided mmap region (or into the cloned
// region after ForceInMem).
type ShardedReader struct {
	// Empty shards have SegmentCount == 0 and Fingerprints == nil; ContainsHash
	// short-circuits on the former before dispatching to xorfilter's Contains.
	shards [shardCount]xorfilter.BinaryFuse[uint8]

	fpRegion  []byte // slice into mmap covering all shards' fingerprints
	fpOffsets [shardCount + 1]uint64

	keepInMem bool
	fileName  string
	features  Features

	f *os.File
	m mmap.MMap // non-nil only when opened via NewShardedReader(filePath)
}

func NewShardedReader(filePath string) (*ShardedReader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	sz := int(st.Size())
	m, err := mmap.MapRegion(f, sz, mmap.RDONLY, 0, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	_, fileName := filepath.Split(filePath)
	r, _, err := NewShardedReaderOnBytes(m, fileName)
	if err != nil {
		_ = m.Unmap()
		_ = f.Close()
		return nil, err
	}
	r.f = f
	r.m = m
	r.fileName = fileName
	return r, nil
}

// NewShardedReaderOnBytes parses the sharded-filter layout from `m` (which
// must remain valid for the lifetime of the returned reader). Returns the
// reader, the number of bytes consumed from `m`, and an error.
func NewShardedReaderOnBytes(m []byte, fName string) (*ShardedReader, int, error) {
	if len(m) < shardedFixedHeader {
		return nil, 0, fmt.Errorf("file %s: sharded fuse filter truncated: have %d bytes, need at least %d", fName, len(m), shardedFixedHeader)
	}
	features := Features(binary.BigEndian.Uint32(m[:4]))
	fileIsLittleEndian := features&IsLittleEndianFeature != 0
	if fileIsLittleEndian != IsLittleEndian {
		return nil, 0, fmt.Errorf("file %s is not compatible with your machine (different Endianness), but you can run `erigon snapshots index`", fName)
	}

	r := &ShardedReader{features: features, fileName: fName}
	for s := 0; s < shardCount; s++ {
		off := shardedHeaderSize + s*shardDescriptorSize
		sc := binary.BigEndian.Uint32(m[off:])
		if sc == 0 {
			continue // empty shard; leave r.shards[s] zero-valued
		}
		sl := binary.BigEndian.Uint32(m[off+4:])
		filter := &r.shards[s]
		filter.SegmentCount = sc
		filter.SegmentLength = sl
		filter.SegmentCountLength = sc * sl
		filter.SegmentLengthMask = sl - 1 // SegmentLength is always a power of two
		filter.Seed = binary.BigEndian.Uint64(m[off+8:])
	}
	for i := 0; i <= shardCount; i++ {
		r.fpOffsets[i] = binary.BigEndian.Uint64(m[shardDescriptorsEnd+i*8:])
	}
	// Validate offsets are monotonic and within bounds.
	total := r.fpOffsets[shardCount]
	if uint64(len(m)) < uint64(shardedFixedHeader)+total {
		return nil, 0, fmt.Errorf("file %s: sharded fuse filter fingerprints truncated: have %d bytes, need %d", fName, len(m), uint64(shardedFixedHeader)+total)
	}
	for i := 0; i < shardCount; i++ {
		if r.fpOffsets[i] > r.fpOffsets[i+1] {
			return nil, 0, fmt.Errorf("file %s: sharded fuse filter offsets not monotonic at shard %d", fName, i)
		}
	}
	r.fpRegion = m[shardedFixedHeader : shardedFixedHeader+int(total)]
	for s := 0; s < shardCount; s++ {
		if r.shards[s].SegmentCount == 0 {
			continue
		}
		r.shards[s].Fingerprints = r.fpRegion[r.fpOffsets[s]:r.fpOffsets[s+1]]
	}
	return r, shardedFixedHeader + int(total), nil
}

// ContainsHash routes the query to the shard identified by the low byte of v
// and delegates to that shard's BinaryFuse[uint8].Contains. Empty shards are
// identified by SegmentCount == 0 (xorfilter would panic on Contains).
func (r *ShardedReader) ContainsHash(v uint64) bool {
	shard := &r.shards[byte(v)]
	if shard.SegmentCount == 0 {
		return false
	}
	return shard.Contains(v)
}

// ForceInMem copies the entire fingerprints region out of the mmap into a
// heap allocation so the reader no longer depends on the underlying mmap.
// Returns the number of bytes now held in RAM.
func (r *ShardedReader) ForceInMem() datasize.ByteSize {
	cloned := bytes.Clone(r.fpRegion)
	r.fpRegion = cloned
	for s := 0; s < shardCount; s++ {
		if r.shards[s].SegmentCount == 0 {
			continue
		}
		r.shards[s].Fingerprints = cloned[r.fpOffsets[s]:r.fpOffsets[s+1]]
	}
	r.keepInMem = true
	return datasize.ByteSize(len(cloned))
}

func (r *ShardedReader) MadvWillNeed() {
	if r == nil || r.m == nil || len(r.m) == 0 || r.keepInMem {
		return
	}
	if err := mm.MadviseWillNeed(r.m); err != nil {
		panic(err)
	}
}

func (r *ShardedReader) MadvNormal() {
	if r == nil || r.m == nil || len(r.m) == 0 || r.keepInMem {
		return
	}
	if err := mm.MadviseNormal(r.m); err != nil {
		panic(err)
	}
}

func (r *ShardedReader) FileName() string { return r.fileName }

func (r *ShardedReader) Close() {
	if r == nil || r.f == nil {
		return
	}
	_ = r.m.Unmap()
	_ = r.f.Close()
	r.f = nil
}
