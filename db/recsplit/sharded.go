// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package recsplit

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/common/murmur3"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/version"
)

// RecSplit partitions keys into RecSplitShards inner RecSplitShard indexes by the low
// byte of the bucket hash (the high bits drive each shard's remap, so low-byte routing keeps
// shards uniform). For enums each shard stores the global arrival ordinal, and one monotonic
// arrival-order offset-EF appended after the shards backs OrdinalLookup.
const (
	RecSplitShards = 256

	// shardedRSVersion is byte 0 of a sharded file: the next DataStructureVersion after the
	// monolithic range (0..2), so OpenIndexShard dispatches versions >= this to the sharded reader.
	shardedRSVersion version.DataStructureVersion = 3

	// shardInnerVersion is the existence-filter version of each shard: monolithic
	// (non-sharded) FuseFilter.
	shardInnerVersion version.DataStructureVersion = 1

	// shardRecordSize is the size of one buffered (hi, lo, offset) record.
	shardRecordSize = 24

	// shardedHeaderSize: version(1) + reserved(3) + salt(4) + baseDataID(8) +
	// keyCount(8) + enums(1) + lessFalsePositives(1) + reserved(6).
	shardedHeaderSize = 32
)

// RecSplit builds a sharded RecSplitShard index. Its API mirrors RecSplitShard so it
// can be driven by the same add-then-build-with-collision-retry loop.
type RecSplit struct {
	salt               uint32
	enums              bool
	lessFalsePositives bool
	bucketSize         int
	leafSize           uint16
	baseDataID         uint64

	filePath string
	fileName string
	tmpDir   string
	noFsync  bool
	logger   log.Logger

	shardFiles   [RecSplitShards]*os.File
	shardWriters [RecSplitShards]*bufio.Writer
	shardCounts  [RecSplitShards]uint64

	// global arrival-order offset stream, used to build the OrdinalLookup EF (enums only)
	globalOffsetFile   *os.File
	globalOffsetWriter *bufio.Writer
	maxOffset          uint64
	prevOffset         uint64

	keyExpectedCount uint64
	keysAdded        uint64
	collision        bool
	built            bool
}

func NewRecSplit(args RecSplitArgs, logger log.Logger) (*RecSplit, error) {
	if args.BaseDataID >= math.MaxUint64/2 {
		return nil, fmt.Errorf("baseDataID %d is too large, must be less than %d", args.BaseDataID, math.MaxUint64/2)
	}
	if args.LeafSize > MaxLeafSize {
		return nil, fmt.Errorf("exceeded max leaf size %d: %d", MaxLeafSize, args.LeafSize)
	}
	_, fileName := filepath.Split(args.IndexFile)
	rs := &RecSplit{
		enums:              args.Enums,
		lessFalsePositives: args.LessFalsePositives,
		bucketSize:         args.BucketSize,
		leafSize:           args.LeafSize,
		baseDataID:         args.BaseDataID,
		filePath:           args.IndexFile,
		fileName:           fileName,
		tmpDir:             args.TmpDir,
		noFsync:            args.NoFsync,
		logger:             logger,
		keyExpectedCount:   uint64(args.KeyCount),
	}
	if args.Salt == nil {
		seedBytes := make([]byte, 4)
		if _, err := rand.Read(seedBytes); err != nil {
			return nil, err
		}
		rs.salt = binary.BigEndian.Uint32(seedBytes)
	} else {
		rs.salt = *args.Salt
	}
	if rs.enums {
		f, err := os.CreateTemp(rs.tmpDir, fmt.Sprintf("%s.goffsets.", fileName))
		if err != nil {
			return nil, err
		}
		rs.globalOffsetFile = f
		rs.globalOffsetWriter = bufio.NewWriter(f)
	}
	return rs, nil
}

func (rs *RecSplit) FileName() string { return rs.fileName }
func (rs *RecSplit) Salt() uint32     { return rs.salt }
func (rs *RecSplit) Collision() bool  { return rs.collision }
func (rs *RecSplit) DisableFsync()    { rs.noFsync = true }

func (rs *RecSplit) AddKey(key []byte, offset uint64) error {
	if rs.built {
		return errors.New("cannot add keys after perfect hash function had been built")
	}
	hi, lo := murmur3.Sum128WithSeed(key, rs.salt)
	shard := byte(hi)
	w, err := rs.shardWriter(shard)
	if err != nil {
		return err
	}

	// enums stores the global arrival ordinal per shard plus the raw offset in the global EF
	// stream; non-enums stores the raw offset directly in the shard.
	val := offset
	if rs.enums {
		if rs.keysAdded > 0 && offset < rs.prevOffset {
			return fmt.Errorf("sharded recsplit %s: offsets must be monotonically increasing: prev=%d cur=%d", rs.fileName, rs.prevOffset, offset)
		}
		var ob [8]byte
		binary.BigEndian.PutUint64(ob[:], offset)
		if _, err := rs.globalOffsetWriter.Write(ob[:]); err != nil {
			return err
		}
		val = rs.keysAdded
		rs.prevOffset = offset
		if offset > rs.maxOffset {
			rs.maxOffset = offset
		}
	}

	var buf [shardRecordSize]byte
	binary.BigEndian.PutUint64(buf[0:], hi)
	binary.BigEndian.PutUint64(buf[8:], lo)
	binary.BigEndian.PutUint64(buf[16:], val)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	rs.shardCounts[shard]++
	rs.keysAdded++
	return nil
}

func (rs *RecSplit) shardWriter(shard byte) (*bufio.Writer, error) {
	if rs.shardWriters[shard] != nil {
		return rs.shardWriters[shard], nil
	}
	f, err := os.CreateTemp(rs.tmpDir, fmt.Sprintf("%s.shardbuf.%03d.", rs.fileName, shard))
	if err != nil {
		return nil, err
	}
	rs.shardFiles[shard] = f
	rs.shardWriters[shard] = bufio.NewWriter(f)
	return rs.shardWriters[shard], nil
}

// ResetNextSalt bumps the shared routing salt and clears buffered keys, so a
// collision in any shard forces the caller to re-add and rebuild all shards.
func (rs *RecSplit) ResetNextSalt() {
	rs.salt++
	rs.built = false
	rs.collision = false
	rs.keysAdded = 0
	rs.maxOffset = 0
	rs.prevOffset = 0
	for i := range rs.shardFiles {
		rs.shardCounts[i] = 0
		f := rs.shardFiles[i]
		if f == nil {
			continue
		}
		_ = f.Truncate(0)
		_, _ = f.Seek(0, io.SeekStart)
		rs.shardWriters[i].Reset(f)
	}
	if rs.globalOffsetFile != nil {
		_ = rs.globalOffsetFile.Truncate(0)
		_, _ = rs.globalOffsetFile.Seek(0, io.SeekStart)
		rs.globalOffsetWriter.Reset(rs.globalOffsetFile)
	}
}

func (rs *RecSplit) Build(ctx context.Context) error {
	if rs.built {
		return errors.New("already built")
	}
	if rs.keysAdded != rs.keyExpectedCount {
		return fmt.Errorf("rs %s expected keys %d, got %d", rs.fileName, rs.keyExpectedCount, rs.keysAdded)
	}

	f, err := dir.CreateTemp(rs.filePath)
	if err != nil {
		return fmt.Errorf("create index file %s: %w", rs.filePath, err)
	}
	defer dir.RemoveFile(f.Name())
	defer f.Close()

	w := bufio.NewWriter(f)
	if err := rs.writeHeader(w); err != nil {
		return err
	}

	var sizeBuf [8]byte
	for shard := 0; shard < RecSplitShards; shard++ {
		n := rs.shardCounts[shard]
		if n == 0 {
			binary.BigEndian.PutUint64(sizeBuf[:], 0)
			if _, err := w.Write(sizeBuf[:]); err != nil {
				return err
			}
			continue
		}
		shardIdxPath, err := rs.buildShard(ctx, shard, n)
		if err != nil {
			if shardIdxPath != "" {
				_ = dir.RemoveFile(shardIdxPath)
			}
			if errors.Is(err, ErrCollision) {
				rs.collision = true
			}
			return err
		}
		if err := appendShardBlob(w, shardIdxPath, sizeBuf[:]); err != nil {
			_ = dir.RemoveFile(shardIdxPath)
			return err
		}
		_ = dir.RemoveFile(shardIdxPath)
	}

	if rs.enums && rs.keysAdded > 0 {
		if err := rs.appendGlobalEF(w); err != nil {
			return err
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}
	if !rs.noFsync {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), rs.filePath); err != nil {
		return err
	}
	rs.built = true
	rs.logger.Debug("[index] created sharded", "file", rs.fileName)
	return nil
}

func (rs *RecSplit) writeHeader(w *bufio.Writer) error {
	var header [shardedHeaderSize]byte
	header[0] = byte(shardedRSVersion)
	binary.BigEndian.PutUint32(header[4:], rs.salt)
	binary.BigEndian.PutUint64(header[8:], rs.baseDataID)
	binary.BigEndian.PutUint64(header[16:], rs.keysAdded)
	if rs.enums {
		header[24] = 1
	}
	if rs.lessFalsePositives {
		header[25] = 1
	}
	_, err := w.Write(header[:])
	return err
}

func (rs *RecSplit) buildShard(ctx context.Context, shard int, n uint64) (string, error) {
	if err := rs.shardWriters[shard].Flush(); err != nil {
		return "", err
	}
	if _, err := rs.shardFiles[shard].Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	shardIdxPath := filepath.Join(rs.tmpDir, fmt.Sprintf("%s.shard.%03d", rs.fileName, shard))
	inner, err := NewRecSplitShard(RecSplitArgs{
		KeyCount:           int(n),
		Enums:              rs.enums,
		LessFalsePositives: rs.lessFalsePositives,
		Version:            shardInnerVersion,
		BucketSize:         rs.bucketSize,
		LeafSize:           rs.leafSize,
		Salt:               &rs.salt,
		BaseDataID:         rs.baseDataID,
		IndexFile:          shardIdxPath,
		TmpDir:             rs.tmpDir,
		NoFsync:            true,
	}, rs.logger)
	if err != nil {
		return "", err
	}
	defer inner.Close()
	inner.LogLvl(log.LvlTrace)

	r := bufio.NewReader(rs.shardFiles[shard])
	var buf [shardRecordSize]byte
	for i := uint64(0); i < n; i++ {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return "", fmt.Errorf("shard %d read: %w", shard, err)
		}
		hi := binary.BigEndian.Uint64(buf[0:])
		lo := binary.BigEndian.Uint64(buf[8:])
		offset := binary.BigEndian.Uint64(buf[16:])
		if err := inner.addHashedKey(hi, lo, offset); err != nil {
			return "", err
		}
	}
	if err := inner.Build(ctx); err != nil {
		return shardIdxPath, err
	}
	return shardIdxPath, nil
}

func appendShardBlob(w *bufio.Writer, shardIdxPath string, sizeBuf []byte) error {
	sf, err := os.Open(shardIdxPath)
	if err != nil {
		return err
	}
	defer sf.Close()
	st, err := sf.Stat()
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(sizeBuf, uint64(st.Size()))
	if _, err := w.Write(sizeBuf); err != nil {
		return err
	}
	if _, err := io.Copy(w, sf); err != nil {
		return err
	}
	return nil
}

// appendGlobalEF builds the arrival-order offset EF from the buffered global offset
// stream and writes it after all shard blobs. It backs OrdinalLookup.
func (rs *RecSplit) appendGlobalEF(w *bufio.Writer) error {
	if err := rs.globalOffsetWriter.Flush(); err != nil {
		return err
	}
	if _, err := rs.globalOffsetFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	ef, err := eliasfano32.NewEliasFanoOffHeap(rs.keysAdded, rs.maxOffset, filepath.Join(rs.tmpDir, rs.fileName))
	if err != nil {
		return err
	}
	defer ef.Close()
	r := bufio.NewReader(rs.globalOffsetFile)
	var buf [8]byte
	for i := uint64(0); i < rs.keysAdded; i++ {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return fmt.Errorf("read global offset %d: %w", i, err)
		}
		ef.AddOffset(binary.BigEndian.Uint64(buf[:]))
	}
	ef.Build()
	return ef.Write(w)
}

func (rs *RecSplit) Close() {
	for i := range rs.shardFiles {
		f := rs.shardFiles[i]
		if f == nil {
			continue
		}
		_ = f.Close()
		_ = dir.RemoveFile(f.Name())
		rs.shardFiles[i] = nil
		rs.shardWriters[i] = nil
	}
	if rs.globalOffsetFile != nil {
		_ = rs.globalOffsetFile.Close()
		_ = dir.RemoveFile(rs.globalOffsetFile.Name())
		rs.globalOffsetFile = nil
		rs.globalOffsetWriter = nil
	}
}

// Index reads a file produced by RecSplit. The shards are backed by
// sub-slices of one mmap, so their IndexShard.Close is a no-op and Munmap frees them all.
type Index struct {
	f           *os.File
	mmapHandle1 []byte
	mmapHandle2 *[mmap.MaxMapSize]byte
	data        []byte

	filePath, fileName string
	size               int64
	modTime            time.Time

	salt       uint32
	baseDataID uint64
	keyCount   uint64
	enums      bool

	shards       [RecSplitShards]*IndexShard
	globalEf     *eliasfano32.EliasFano // arrival-order offsets, backs OrdinalLookup (enums only)
	sharedReader *IndexReader

	// mono is non-nil for legacy monolithic files (version < shardedRSVersion): the whole
	// file is one IndexShard and all lookups delegate to it without shard routing.
	mono *IndexShard
}

func MustOpen(indexFile string) *Index {
	idx, err := OpenIndex(indexFile)
	if err != nil {
		panic(err)
	}
	return idx
}

func OpenIndex(indexFilePath string) (*Index, error) {
	_, fName := filepath.Split(indexFilePath)
	idx := &Index{filePath: indexFilePath, fileName: fName}

	var err error
	idx.f, err = os.Open(indexFilePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			idx.Close()
		}
	}()
	var stat os.FileInfo
	if stat, err = idx.f.Stat(); err != nil {
		return nil, err
	}
	idx.size = stat.Size()
	idx.modTime = stat.ModTime()
	if idx.mmapHandle1, idx.mmapHandle2, err = mmap.Mmap(idx.f, int(idx.size)); err != nil {
		return nil, err
	}
	idx.data = idx.mmapHandle1[:idx.size]
	if err = idx.init(); err != nil {
		return nil, err
	}
	idx.sharedReader = NewIndexReader(idx)
	return idx, nil
}

func (idx *Index) init() error {
	if len(idx.data) == 0 {
		return fmt.Errorf("index %s: empty file", idx.fileName)
	}
	if version.DataStructureVersion(idx.data[0]) < shardedRSVersion {
		// legacy monolithic file: parse the whole file as one shard and delegate to it.
		mono, err := newIndexFromMemory(idx.data, idx.fileName)
		if err != nil {
			return err
		}
		idx.mono = mono
		idx.salt = mono.salt
		idx.baseDataID = mono.baseDataID
		idx.keyCount = mono.keyCount
		idx.enums = mono.enums
		return nil
	}
	if len(idx.data) < shardedHeaderSize {
		return fmt.Errorf("sharded index %s: too small for header (%d < %d)", idx.fileName, len(idx.data), shardedHeaderSize)
	}
	idx.salt = binary.BigEndian.Uint32(idx.data[4:])
	idx.baseDataID = binary.BigEndian.Uint64(idx.data[8:])
	idx.keyCount = binary.BigEndian.Uint64(idx.data[16:])
	idx.enums = idx.data[24] != 0

	offset := shardedHeaderSize
	for i := 0; i < RecSplitShards; i++ {
		if offset+8 > len(idx.data) {
			return fmt.Errorf("sharded index %s: truncated at shard %d", idx.fileName, i)
		}
		sz64 := binary.BigEndian.Uint64(idx.data[offset:])
		offset += 8
		if sz64 == 0 {
			continue
		}
		if sz64 > math.MaxInt || sz64 > uint64(len(idx.data)-offset) {
			return fmt.Errorf("sharded index %s: shard %d blob overflows (offset=%d sz=%d total=%d)", idx.fileName, i, offset, sz64, len(idx.data))
		}
		sz := int(sz64)
		shard, err := newIndexFromMemory(idx.data[offset:offset+sz:offset+sz], idx.fileName)
		if err != nil {
			return fmt.Errorf("shard %d of %s: %w", i, idx.fileName, err)
		}
		idx.shards[i] = shard
		offset += sz
	}

	if idx.enums && idx.keyCount > 0 {
		if offset+16 > len(idx.data) {
			return fmt.Errorf("%w. sharded index %s: missing/truncated global offset EF", IncompatibleErr, idx.fileName)
		}
		idx.globalEf, _ = eliasfano32.ReadEliasFano(idx.data[offset:])
		if idx.globalEf.Count() != idx.keyCount {
			return fmt.Errorf("%w. sharded index %s: global EF count %d != keyCount %d", IncompatibleErr, idx.fileName, idx.globalEf.Count(), idx.keyCount)
		}
	}
	return nil
}

func (idx *Index) Close() {
	if idx == nil || idx.f == nil {
		return
	}
	if err := mmap.Munmap(idx.mmapHandle1, idx.mmapHandle2); err != nil {
		log.Log(dbg.FileCloseLogLevel, "unmap", "err", err, "file", idx.FileName())
	}
	if err := idx.f.Close(); err != nil {
		log.Log(dbg.FileCloseLogLevel, "close", "err", err, "file", idx.FileName())
	}
	idx.f = nil
}

func (idx *Index) Empty() bool          { return idx.keyCount == 0 }
func (idx *Index) KeyCount() uint64     { return idx.keyCount }
func (idx *Index) BaseDataID() uint64   { return idx.baseDataID }
func (idx *Index) Enums() bool          { return idx.enums }
func (idx *Index) Salt() uint32         { return idx.salt }
func (idx *Index) Size() int64          { return idx.size }
func (idx *Index) ModTime() time.Time   { return idx.modTime }
func (idx *Index) FilePath() string     { return idx.filePath }
func (idx *Index) FileName() string     { return idx.fileName }
func (idx *Index) Reader() *IndexReader { return idx.sharedReader }

func (idx *Index) MadvNormal() *Index {
	if idx == nil || idx.mmapHandle1 == nil {
		return idx
	}
	_ = mmap.MadviseNormal(idx.mmapHandle1)
	return idx
}

func (idx *Index) MadvWillNeed() *Index {
	if idx == nil || idx.mmapHandle1 == nil {
		return idx
	}
	_ = mmap.MadviseWillNeed(idx.mmapHandle1)
	return idx
}

func (idx *Index) lookup(hi, lo uint64) (uint64, bool) {
	if idx.mono != nil {
		return idx.mono.Lookup(hi, lo)
	}
	shard := idx.shards[byte(hi)]
	if shard == nil {
		return 0, false
	}
	return shard.Lookup(hi, lo)
}

func (idx *Index) twoLayerLookupByHash(hi, lo uint64) (uint64, bool) {
	if idx.mono != nil {
		id, ok := idx.mono.Lookup(hi, lo)
		if !ok || !idx.enums {
			return id, ok
		}
		return idx.mono.OrdinalLookup(id), true
	}
	shard := idx.shards[byte(hi)]
	if shard == nil {
		return 0, false
	}
	if !idx.enums {
		return shard.Lookup(hi, lo)
	}
	id, ok := shard.Lookup(hi, lo)
	if !ok {
		return 0, false
	}
	return idx.globalEf.Get(shard.OrdinalLookup(id)), true
}

// OrdinalLookup returns the offset of the i-th key in arrival order. The result is
// monotonically increasing in i (required by BinarySearch). Only valid for enums=true.
func (idx *Index) OrdinalLookup(i uint64) uint64 {
	if idx.mono != nil {
		return idx.mono.OrdinalLookup(i)
	}
	if !idx.enums {
		panic("OrdinalLookup should not be used for indices without enums: " + idx.fileName)
	}
	return idx.globalEf.Get(i)
}

// IndexReader is the concurrency-safe lookup front-end for Index.
type IndexReader struct {
	index *Index
	salt  uint32
}

func NewIndexReader(index *Index) *IndexReader {
	return &IndexReader{index: index, salt: index.salt}
}

func (r *IndexReader) Sum(key []byte) (uint64, uint64) {
	return murmur3.Sum128WithSeed(key, r.salt)
}

func (r *IndexReader) Lookup(key []byte) (uint64, bool) {
	hi, lo := r.Sum(key)
	return r.index.lookup(hi, lo)
}

func (r *IndexReader) Lookup2(key1, key2 []byte) (uint64, bool) {
	hi, lo := murmur3.Sum128PairWithSeed(key1, key2, r.salt)
	return r.index.lookup(hi, lo)
}

func (r *IndexReader) Empty() bool { return r.index.Empty() }

func (r *IndexReader) BaseDataID() uint64 { return r.index.BaseDataID() }

func (r *IndexReader) OrdinalLookup(i uint64) uint64 { return r.index.OrdinalLookup(i) }

func (r *IndexReader) TwoLayerLookup(key []byte) (uint64, bool) {
	if r.index.Empty() {
		return 0, false
	}
	hi, lo := r.Sum(key)
	return r.index.twoLayerLookupByHash(hi, lo)
}

func (r *IndexReader) TwoLayerLookupByHash(hi, lo uint64) (uint64, bool) {
	if r.index.Empty() {
		return 0, false
	}
	return r.index.twoLayerLookupByHash(hi, lo)
}
