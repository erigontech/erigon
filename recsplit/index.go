/*
   Copyright 2021 Erigon contributors

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
	"encoding/binary"
	"math"
	"os"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/mmap"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano16"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/spaolacci/murmur3"
)

// Index implements index lookup from the file created by the RecSplit
type Index struct {
	indexFile          string
	f                  *os.File
	mmapHandle1        []byte                 // mmap handle for unix (this is used to close mmap)
	mmapHandle2        *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	data               []byte                 // slice of correct size for the index to work with
	keyCount           uint64
	bytesPerRec        int
	recMask            uint64
	grData             []uint64
	ef                 eliasfano16.DoubleEliasFano
	enums              bool
	offsetEf           *eliasfano32.EliasFano
	bucketCount        uint64          // Number of buckets
	hasher             murmur3.Hash128 // Salted hash function to use for splitting into initial buckets and mapping to 64-bit fingerprints
	bucketSize         int
	leafSize           uint16 // Leaf size for recursive split algorithms
	primaryAggrBound   uint16 // The lower bound for primary key aggregation (computed from leafSize)
	secondaryAggrBound uint16 // The lower bound for secondary key aggregation (computed from leadSize)
	salt               uint32
	startSeed          []uint64
	golombRice         []uint32
}

func MustOpen(indexFile string) *Index {
	idx, err := NewIndex(indexFile)
	if err != nil {
		panic(err)
	}
	return idx
}

func NewIndex(indexFile string) (*Index, error) {
	idx := &Index{
		indexFile: indexFile,
	}
	var err error
	idx.f, err = os.Open(indexFile)
	if err != nil {
		return nil, err
	}
	var stat os.FileInfo
	if stat, err = idx.f.Stat(); err != nil {
		return nil, err
	}
	size := int(stat.Size())
	if idx.mmapHandle1, idx.mmapHandle2, err = mmap.Mmap(idx.f, size); err != nil {
		return nil, err
	}
	idx.data = idx.mmapHandle1[:size]
	// Read number of keys and bytes per record
	idx.keyCount = binary.BigEndian.Uint64(idx.data[:8])
	idx.bytesPerRec = int(idx.data[8])
	idx.recMask = (uint64(1) << (8 * idx.bytesPerRec)) - 1
	offset := 9 + int(idx.keyCount)*idx.bytesPerRec
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
	idx.hasher = murmur3.New128WithSeed(idx.salt)
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
	return idx, nil
}

func (idx *Index) Close() error {
	if err := mmap.Munmap(idx.mmapHandle1, idx.mmapHandle2); err != nil {
		return err
	}
	if err := idx.f.Close(); err != nil {
		return err
	}
	return nil
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

func (idx Index) Empty() bool {
	return idx.keyCount == 0
}

func (idx Index) Lookup(key []byte) uint64 {
	if idx.keyCount == 0 {
		panic("no Lookup should be done when keyCount==0, please use Empty function to guard")
	}
	if idx.keyCount == 1 {
		return 0
	}
	var gr GolombRiceReader
	gr.data = idx.grData
	idx.hasher.Reset()
	idx.hasher.Write(key) //nolint:errcheck
	bucketHash, fingerprint := idx.hasher.Sum128()
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
	return binary.BigEndian.Uint64(idx.data[1+idx.bytesPerRec*(rec+1):]) & idx.recMask
}

func (idx Index) Lookup2(i uint64) uint64 {
	return idx.offsetEf.Get(i)
}
