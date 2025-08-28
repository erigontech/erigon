// Copyright 2022 The Erigon Authors
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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/mmap"
	"github.com/erigontech/erigon/db/datastruct/fusefilter"
	"github.com/erigontech/erigon/db/recsplit/eliasfano16"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
)

type Features byte

const (
	No Features = 0b0

	// Enums - To build 2-lvl index with perfect hash table pointing to enumeration and enumeration pointing to offsets
	Enums Features = 0b1
	// LessFalsePositives - Reduce false-positives to 1/256=0.4% in cost of 1byte per key
	// Implementation:
	//   PerfectHashMap - does false-positives if unknown key is requested. But "false-positives itself" is not a problem.
	//   Problem is "nature of false-positives" - they are randomly/smashed across .seg files.
	//   It makes .seg files "warm" - which is bad because they are big and
	//      data-locality of touches is bad (and maybe need visit a lot of shards to find key).
	//   Can add a built-in "existence filter" (like bloom/cuckoo/ribbon/xor-filter/fuse-filter); it will improve
	//      data-locality - filters are small-enough and existance-chekcs will be co-located on disk.
	//   But there are 2 additional properties we have in our data:
	//      "keys are known", "keys are hashed" (.idx works on murmur3), ".idx can calc key-number by key".
	//   It means: if we rely on this properties then we can do better than general-purpose-existance-filter.
	//   Seems just an "array of 1-st bytes of key-hashes" is great alternative:
	//      general-purpose-filter: 9bits/key, 0.3% false-positives, 3 mem access
	//      first-bytes-array: 8bits/key, 1/256=0.4% false-positives, 1 mem access
	//
	// See also: https://github.com/erigontech/erigon/issues/9486
	LessFalsePositives Features = 0b10 //
)

// SupportedFeaturs - if see feature not from this list (likely after downgrade) - return IncompatibleErr and recommend for user manually delete file
var SupportedFeatures = []Features{Enums, LessFalsePositives}
var IncompatibleErr = errors.New("incompatible. can re-build such files by command 'erigon snapshots index'")

// Index implements index lookup from the file created by the RecSplit
type Index struct {
	offsetEf           *eliasfano32.EliasFano
	f                  *os.File
	mmapHandle2        *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	filePath, fileName string

	grData      []uint64
	data        []byte // slice of correct size for the index to work with
	mmapHandle1 []byte // mmap handle for unix (this is used to close mmap)
	golombRice  []uint32

	version            uint8
	startSeed          []uint64
	ef                 eliasfano16.DoubleEliasFano
	bucketSize         int
	size               int64
	modTime            time.Time
	baseDataID         uint64 // Index internally organized as [0,N) array. Use this field to map EntityID=[M;M+N) to [0,N)
	bucketCount        uint64 // Number of buckets
	keyCount           uint64
	recMask            uint64
	bytesPerRec        int
	salt               uint32
	leafSize           uint16 // Leaf size for recursive split algorithms
	secondaryAggrBound uint16 // The lower bound for secondary key aggregation (computed from leadSize)
	primaryAggrBound   uint16 // The lower bound for primary key aggregation (computed from leafSize)
	enums              bool

	lessFalsePositives bool
	existenceV0        []byte
	existenceV1        *fusefilter.Reader

	readers         *sync.Pool
	readAheadRefcnt atomic.Int32 // ref-counter: allow enable/disable read-ahead from goroutines. only when refcnt=0 - disable read-ahead once
}

func MustOpen(indexFile string) *Index {
	idx, err := OpenIndex(indexFile)
	if err != nil {
		panic(err)
	}
	return idx
}

func OpenIndex(indexFilePath string) (idx *Index, err error) {
	_, fName := filepath.Split(indexFilePath)
	idx = &Index{
		filePath: indexFilePath,
		fileName: fName,
	}

	idx.f, err = os.Open(indexFilePath)
	if err != nil {
		return nil, err
	}
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

	if err := idx.init(); err != nil {
		return nil, err
	}

	// dontt know how to madv part of file in golang yet
	//if idx.version == 0 && idx.lessFalsePositives && idx.enums && idx.keyCount > 0 {
	//	if len(idx.existence) > 0 {
	//		if err := mmap.MadviseWillNeed(idx.existence); err != nil {
	//			panic(err)
	//		}
	//	}
	//	pos := 1 + 8 + idx.bytesPerRec*int(idx.keyCount)
	//	if err := mmap.MadviseWillNeed(idx.data[:pos]); err != nil {
	//		panic(err)
	//	}
	//}

	idx.readers = &sync.Pool{
		New: func() interface{} {
			return NewIndexReader(idx)
		},
	}
	return idx, nil
}

func (idx *Index) init() (err error) {
	var validationPassed = false
	defer func() {
		// recover from panic if one occurred. Set err to nil if no panic
		if rec := recover(); rec != nil {
			// do r with only the stack trace
			err = fmt.Errorf("incomplete file: %s, %+v, trace: %s", idx.fileName, rec, dbg.Stack())
		}
		if err != nil || !validationPassed {
			idx.Close()
			idx = nil
		}
	}()

	defer idx.MadvSequential().DisableReadAhead()

	// 1 byte: version, 7 bytes: app-specific minimal dataID (of current shard)
	idx.version = idx.data[0]
	baseDataBytes := bytes.Clone(idx.data[:8])
	baseDataBytes[0] = 0
	idx.baseDataID = binary.BigEndian.Uint64(baseDataBytes)

	idx.keyCount = binary.BigEndian.Uint64(idx.data[8:])
	idx.bytesPerRec = int(idx.data[16])
	idx.recMask = (uint64(1) << (8 * idx.bytesPerRec)) - 1
	offset := 16 + 1 + int(idx.keyCount)*idx.bytesPerRec

	if offset < 0 {
		return fmt.Errorf("file %s %w. offset is: %d which is below zero", idx.fileName, IncompatibleErr, offset)
	}

	// Bucket count, bucketSize, leafSize
	idx.bucketCount = binary.BigEndian.Uint64(idx.data[offset:])
	offset += 8
	idx.bucketSize = int(binary.BigEndian.Uint16(idx.data[offset:]))
	offset += 2
	idx.leafSize = binary.BigEndian.Uint16(idx.data[offset:])
	offset += 2
	idx.primaryAggrBound = idx.leafSize * uint16(math.Max(2, math.Ceil(0.35*float64(idx.leafSize)+1./2.)))
	if idx.leafSize < 7 {
		idx.secondaryAggrBound = idx.primaryAggrBound * 2
	} else {
		idx.secondaryAggrBound = idx.primaryAggrBound * uint16(math.Ceil(0.21*float64(idx.leafSize)+9./10.))
	}
	// Salt
	idx.salt = binary.BigEndian.Uint32(idx.data[offset:])
	offset += 4
	// Start seed
	startSeedLen := int(idx.data[offset])
	offset++
	idx.startSeed = make([]uint64, startSeedLen)
	for i := 0; i < startSeedLen; i++ {
		idx.startSeed[i] = binary.BigEndian.Uint64(idx.data[offset:])
		offset += 8
	}
	features := Features(idx.data[offset])
	if err := onlyKnownFeatures(features); err != nil {
		return fmt.Errorf("file %s %w", idx.fileName, err)
	}

	idx.enums = features&Enums != No
	idx.lessFalsePositives = features&LessFalsePositives != No
	offset++
	if idx.enums && idx.keyCount > 0 {
		var size int
		idx.offsetEf, size = eliasfano32.ReadEliasFano(idx.data[offset:])
		offset += size
	}
	if idx.version == 0 && idx.lessFalsePositives && idx.enums && idx.keyCount > 0 {
		arrSz := binary.BigEndian.Uint64(idx.data[offset:])
		offset += 8
		if arrSz != idx.keyCount {
			return fmt.Errorf("%w. size of existence filter %d != keys count %d", IncompatibleErr, arrSz, idx.keyCount)
		}
		idx.existenceV0 = idx.data[offset : offset+int(arrSz)]
		offset += int(arrSz)
	}

	if idx.version >= 1 && idx.lessFalsePositives && idx.keyCount > 0 {
		var sz int
		idx.existenceV1, sz, err = fusefilter.NewReaderOnBytes(idx.data[offset:], idx.fileName)
		if err != nil {
			return fmt.Errorf("NewReaderOnBytes: %w, %s", err, idx.fileName)
		}
		offset += sz
	}

	// Size of golomb rice params
	golombParamSize := binary.BigEndian.Uint16(idx.data[offset:])
	offset += 4
	idx.golombRice = make([]uint32, golombParamSize)
	for i := uint16(0); i < golombParamSize; i++ {
		if i == 0 {
			idx.golombRice[i] = (bijMemo[i] << 27) | bijMemo[i]
		} else if i <= idx.leafSize {
			idx.golombRice[i] = (bijMemo[i] << 27) | (uint32(1) << 16) | bijMemo[i]
		} else {
			computeGolombRice(i, idx.golombRice, idx.leafSize, idx.primaryAggrBound, idx.secondaryAggrBound)
		}
	}

	l := binary.BigEndian.Uint64(idx.data[offset:])
	offset += 8
	p := (*[maxDataSize / 8]uint64)(unsafe.Pointer(&idx.data[offset]))
	idx.grData = p[:l]
	offset += 8 * int(l)
	idx.ef.Read(idx.data[offset:])
	validationPassed = true
	return nil
}

func onlyKnownFeatures(features Features) error {
	for _, f := range SupportedFeatures {
		features = features &^ f
	}
	if features != No {
		return fmt.Errorf("%w. unknown features bitmap: %b", IncompatibleErr, features)
	}
	return nil
}

func (idx *Index) DataHandle() unsafe.Pointer {
	return unsafe.Pointer(&idx.data[0])
}

func (idx *Index) Size() int64 { return idx.size }
func (idx *Index) Enums() bool { return idx.enums }
func (idx *Index) Sizes() (total, offsets, ef, golombRice, existence, layer1 datasize.ByteSize) {
	total = datasize.ByteSize(idx.size)
	if idx.offsetEf != nil {
		offsets = idx.offsetEf.Size()
	}
	ef = idx.ef.Size()
	golombRice = datasize.ByteSize(len(idx.grData) * 8)
	existence = datasize.ByteSize(len(idx.existenceV0))
	layer1 = total - offsets - golombRice - existence
	return
}
func (idx *Index) ModTime() time.Time { return idx.modTime }
func (idx *Index) BaseDataID() uint64 { return idx.baseDataID }
func (idx *Index) FilePath() string   { return idx.filePath }
func (idx *Index) FileName() string   { return idx.fileName }
func (idx *Index) IsOpen() bool       { return idx != nil && idx.f != nil }

func (idx *Index) Close() {
	if idx == nil || idx.f == nil {
		return
	}
	if err := mmap.Munmap(idx.mmapHandle1, idx.mmapHandle2); err != nil {
		log.Log(dbg.FileCloseLogLevel, "unmap", "err", err, "file", idx.FileName(), "stack", dbg.Stack())
	}
	if err := idx.f.Close(); err != nil {
		log.Log(dbg.FileCloseLogLevel, "close", "err", err, "file", idx.FileName(), "stack", dbg.Stack())
	}
	idx.f = nil
}

func (idx *Index) skipBits(m uint16) int {
	return int(idx.golombRice[m] & 0xffff)
}

func (idx *Index) skipNodes(m uint16) int {
	return int(idx.golombRice[m]>>16) & 0x7FF
}

// golombParam returns the optimal Golomb parameter to use for encoding
// salt for the part of the hash function separating m elements. It is based on
// calculations with assumptions that we draw hash functions at random
func (idx *Index) golombParam(m uint16) int {
	return int(idx.golombRice[m] >> 27)
}

func (idx *Index) Empty() bool {
	return idx.keyCount == 0
}

func (idx *Index) KeyCount() uint64 { return idx.keyCount }
func (idx *Index) LeafSize() uint16 { return idx.leafSize }
func (idx *Index) BucketSize() int  { return idx.bucketSize }

// Lookup is not thread-safe because it used id.hasher
func (idx *Index) Lookup(bucketHash, fingerprint uint64) (uint64, bool) {
	if idx.keyCount == 0 {
		_, fName := filepath.Split(idx.filePath)
		panic("no Lookup should be done when keyCount==0, please use Empty function to guard " + fName)
	}
	if idx.keyCount == 1 {
		return 0, true
	}
	if idx.version == 1 && idx.lessFalsePositives {
		if ok := idx.existenceV1.ContainsHash(bucketHash); !ok {
			return 0, false
		}
	}

	var gr GolombRiceReader
	gr.data = idx.grData

	bucket := remap(bucketHash, idx.bucketCount)
	cumKeys, cumKeysNext, bitPos := idx.ef.Get3(bucket)
	m := uint16(cumKeysNext - cumKeys) // Number of keys in this bucket
	gr.ReadReset(int(bitPos), idx.skipBits(m))
	var level int
	for m > idx.secondaryAggrBound { // fanout = 2
		d := gr.ReadNext(idx.golombParam(m))
		hmod := remap16(remix(fingerprint+idx.startSeed[level]+d), m)
		split := (((m+1)/2 + idx.secondaryAggrBound - 1) / idx.secondaryAggrBound) * idx.secondaryAggrBound
		if hmod < split {
			m = split
		} else {
			gr.SkipSubtree(idx.skipNodes(split), idx.skipBits(split))
			m -= split
			cumKeys += uint64(split)
		}
		level++
	}
	if m > idx.primaryAggrBound {
		d := gr.ReadNext(idx.golombParam(m))
		hmod := remap16(remix(fingerprint+idx.startSeed[level]+d), m)
		part := hmod / idx.primaryAggrBound
		if idx.primaryAggrBound < m-part*idx.primaryAggrBound {
			m = idx.primaryAggrBound
		} else {
			m = m - part*idx.primaryAggrBound
		}
		cumKeys += uint64(idx.primaryAggrBound * part)
		if part != 0 {
			gr.SkipSubtree(idx.skipNodes(idx.primaryAggrBound)*int(part), idx.skipBits(idx.primaryAggrBound)*int(part))
		}
		level++
	}

	if m > idx.leafSize {
		d := gr.ReadNext(idx.golombParam(m))
		hmod := remap16(remix(fingerprint+idx.startSeed[level]+d), m)
		part := hmod / idx.leafSize
		if idx.leafSize < m-part*idx.leafSize {
			m = idx.leafSize
		} else {
			m = m - part*idx.leafSize
		}
		cumKeys += uint64(idx.leafSize * part)
		if part != 0 {
			gr.SkipSubtree(int(part), idx.skipBits(idx.leafSize)*int(part))
		}
		level++
	}
	b := gr.ReadNext(idx.golombParam(m))
	rec := int(cumKeys) + int(remap16(remix(fingerprint+idx.startSeed[level]+b), m))

	pos := 1 + 8 + idx.bytesPerRec*(rec+1)

	found := binary.BigEndian.Uint64(idx.data[pos:]) & idx.recMask
	if idx.version == 0 && idx.lessFalsePositives && idx.enums && idx.keyCount > 0 {
		return found, idx.existenceV0[found] == byte(bucketHash)
	}
	return found, true
}

// OrdinalLookup returns the offset of i-th element in the index
// Perfect hash table lookup is not performed, only access to the
// Elias-Fano structure containing all offsets.
func (idx *Index) OrdinalLookup(i uint64) uint64 {
	if !idx.enums {
		panic("OrdinalLookup should not be used for indices without enums: " + idx.fileName)
	}
	return idx.offsetEf.Get(i)
}

func (idx *Index) Has(bucketHash, i uint64) bool {
	if idx.lessFalsePositives {
		return idx.existenceV0[i] == byte(bucketHash)
	}
	return true
}

func (idx *Index) ExtractOffsets() map[uint64]uint64 {
	m := map[uint64]uint64{}
	pos := 1 + 8 + idx.bytesPerRec
	for rec := uint64(0); rec < idx.keyCount; rec++ {
		offset := binary.BigEndian.Uint64(idx.data[pos:]) & idx.recMask
		m[offset] = 0
		pos += idx.bytesPerRec
	}
	return m
}

func (idx *Index) RewriteWithOffsets(w *bufio.Writer, m map[uint64]uint64) error {
	// New max offset
	var maxOffset uint64
	for _, offset := range m {
		if offset > maxOffset {
			maxOffset = offset
		}
	}
	bytesPerRec := common.BitLenToByteLen(bits.Len64(maxOffset))
	var numBuf [8]byte
	// Write baseDataID
	binary.BigEndian.PutUint64(numBuf[:], idx.baseDataID)
	if _, err := w.Write(numBuf[:]); err != nil {
		return fmt.Errorf("write number of keys: %w", err)
	}

	// Write number of keys
	binary.BigEndian.PutUint64(numBuf[:], idx.keyCount)
	if _, err := w.Write(numBuf[:]); err != nil {
		return fmt.Errorf("write number of keys: %w", err)
	}
	// Write number of bytes per index record
	if err := w.WriteByte(byte(bytesPerRec)); err != nil {
		return fmt.Errorf("write bytes per record: %w", err)
	}
	pos := 1 + 8 + idx.bytesPerRec
	for rec := uint64(0); rec < idx.keyCount; rec++ {
		offset := binary.BigEndian.Uint64(idx.data[pos:]) & idx.recMask
		pos += idx.bytesPerRec
		binary.BigEndian.PutUint64(numBuf[:], m[offset])
		if _, err := w.Write(numBuf[8-bytesPerRec:]); err != nil {
			return err
		}
	}
	// Write the rest as it is (TODO - wrong for indices with enums)
	if _, err := w.Write(idx.data[16+1+int(idx.keyCount)*idx.bytesPerRec:]); err != nil {
		return err
	}
	return nil
}

// DisableReadAhead - usage: `defer d.EnableReadAhead().DisableReadAhead()`. Please don't use this funcs without `defer` to avoid leak.
func (idx *Index) DisableReadAhead() {
	if idx == nil || idx.mmapHandle1 == nil {
		return
	}
	leftReaders := idx.readAheadRefcnt.Add(-1)
	if leftReaders == 0 {
		_ = mmap.MadviseRandom(idx.mmapHandle1)
	} else if leftReaders < 0 {
		log.Warn("read-ahead negative counter", "file", idx.FileName())
	}
}
func (idx *Index) MadvSequential() *Index {
	if idx == nil || idx.mmapHandle1 == nil {
		return idx
	}
	idx.readAheadRefcnt.Add(1)
	_ = mmap.MadviseSequential(idx.mmapHandle1)
	return idx
}
func (idx *Index) MadvNormal() *Index {
	if idx == nil || idx.mmapHandle1 == nil {
		return idx
	}
	idx.readAheadRefcnt.Add(1)
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

func (idx *Index) GetReaderFromPool() *IndexReader {
	return idx.readers.Get().(*IndexReader)
}
