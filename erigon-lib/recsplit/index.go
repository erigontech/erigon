/*
   Copyright 2022 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package recsplit

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/mmap"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano16"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

// Index implements index lookup from the file created by the RecSplit
type Index struct {
	offsetEf           *eliasfano32.EliasFano
	f                  *os.File
	mmapHandle2        *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	filePath, fileName string

	grData             []uint64
	data               []byte // slice of correct size for the index to work with
	startSeed          []uint64
	golombRice         []uint32
	mmapHandle1        []byte // mmap handle for unix (this is used to close mmap)
	ef                 eliasfano16.DoubleEliasFano
	bucketSize         int
	size               int64
	modTime            time.Time
	baseDataID         uint64 // Index internaly organized as [0,N) array. Use this field to map EntityID=[M;M+N) to [0,N)
	bucketCount        uint64 // Number of buckets
	keyCount           uint64
	recMask            uint64
	bytesPerRec        int
	salt               uint32
	leafSize           uint16 // Leaf size for recursive split algorithms
	secondaryAggrBound uint16 // The lower bound for secondary key aggregation (computed from leadSize)
	primaryAggrBound   uint16 // The lower bound for primary key aggregation (computed from leafSize)
	enums              bool

	readers *sync.Pool
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
	idx := &Index{
		filePath: indexFilePath,
		fileName: fName,
	}
	var err error
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
	defer idx.EnableReadAhead().DisableReadAhead()

	// Read number of keys and bytes per record
	idx.baseDataID = binary.BigEndian.Uint64(idx.data[:8])
	idx.keyCount = binary.BigEndian.Uint64(idx.data[8:16])
	idx.bytesPerRec = int(idx.data[16])
	idx.recMask = (uint64(1) << (8 * idx.bytesPerRec)) - 1
	offset := 16 + 1 + int(idx.keyCount)*idx.bytesPerRec

	if offset < 0 {
		return nil, fmt.Errorf("offset is: %d which is below zero, the file: %s is broken", offset, indexFilePath)
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
	idx.enums = idx.data[offset] != 0
	offset++
	if idx.enums {
		var size int
		idx.offsetEf, size = eliasfano32.ReadEliasFano(idx.data[offset:])
		offset += size
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

	idx.readers = &sync.Pool{
		New: func() interface{} {
			return NewIndexReader(idx)
		},
	}
	return idx, nil
}

func (idx *Index) DataHandle() unsafe.Pointer {
	return unsafe.Pointer(&idx.data[0])
}

func (idx *Index) Size() int64        { return idx.size }
func (idx *Index) ModTime() time.Time { return idx.modTime }
func (idx *Index) BaseDataID() uint64 { return idx.baseDataID }
func (idx *Index) FilePath() string   { return idx.filePath }
func (idx *Index) FileName() string   { return idx.fileName }

func (idx *Index) Close() {
	if idx == nil {
		return
	}
	if idx.f != nil {
		if err := mmap.Munmap(idx.mmapHandle1, idx.mmapHandle2); err != nil {
			log.Log(dbg.FileCloseLogLevel, "unmap", "err", err, "file", idx.FileName(), "stack", dbg.Stack())
		}
		if err := idx.f.Close(); err != nil {
			log.Log(dbg.FileCloseLogLevel, "close", "err", err, "file", idx.FileName(), "stack", dbg.Stack())
		}
		idx.f = nil
	}
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

func (idx *Index) KeyCount() uint64 {
	return idx.keyCount
}

// Lookup is not thread-safe because it used id.hasher
func (idx *Index) Lookup(bucketHash, fingerprint uint64) uint64 {
	if idx.keyCount == 0 {
		_, fName := filepath.Split(idx.filePath)
		panic("no Lookup should be done when keyCount==0, please use Empty function to guard " + fName)
	}
	if idx.keyCount == 1 {
		return 0
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

	return binary.BigEndian.Uint64(idx.data[pos:]) & idx.recMask
}

// OrdinalLookup returns the offset of i-th element in the index
// Perfect hash table lookup is not performed, only access to the
// Elias-Fano structure containing all offsets.
func (idx *Index) OrdinalLookup(i uint64) uint64 {
	return idx.offsetEf.Get(i)
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
	_ = mmap.MadviseRandom(idx.mmapHandle1)
}
func (idx *Index) EnableReadAhead() *Index {
	_ = mmap.MadviseSequential(idx.mmapHandle1)
	return idx
}
func (idx *Index) EnableMadvNormal() *Index {
	_ = mmap.MadviseNormal(idx.mmapHandle1)
	return idx
}
func (idx *Index) EnableWillNeed() *Index {
	_ = mmap.MadviseWillNeed(idx.mmapHandle1)
	return idx
}

func (idx *Index) GetReaderFromPool() *IndexReader {
	return idx.readers.Get().(*IndexReader)
}
