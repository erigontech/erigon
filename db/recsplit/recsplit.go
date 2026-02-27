// Copyright 2021 The Erigon Authors
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
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/spaolacci/murmur3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/assert"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/datastruct/fusefilter"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/recsplit/eliasfano16"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/version"
)

var ErrCollision = errors.New("duplicate key")

const RecSplitLogPrefix = "recsplit"

const MaxLeafSize = 24

/** David Stafford's (http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html)
 * 13th variant of the 64-bit finalizer function in Austin Appleby's
 * MurmurHash3 (https://github.com/aappleby/smhasher).
 *
 * @param z a 64-bit integer.
 * @return a 64-bit integer obtained by mixing the bits of `z`.
 */

func remix(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// bucketTask represents a work unit to be processed by a worker goroutine.
type bucketTask struct {
	order   uint64   // Bucket index (for ordering results)
	keys    []uint64 // Fingerprints
	offsets []uint64 // Index offsets
}

// bucketResult contains the output of processing a single bucket.
type bucketResult struct {
	order      uint64     // Bucket index (for ordering results)
	offsetData []byte     // Serialized leaf-offset bytes
	gr         GolombRice // Per-bucket Golomb-Rice encoding
	bucketSize int        // Number of keys in this bucket
}

// bucketResultQueue is a min-heap of *bucketResult ordered by .order
type bucketResultQueue []*bucketResult

func (q bucketResultQueue) Len() int            { return len(q) }
func (q bucketResultQueue) Less(i, j int) bool  { return q[i].order < q[j].order }
func (q bucketResultQueue) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *bucketResultQueue) Push(x interface{}) { *q = append(*q, x.(*bucketResult)) }
func (q *bucketResultQueue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return x
}

// recsplitScratch holds per-execution scratch buffers and configuration (shared by sequential and worker paths).
type recsplitScratch struct {
	count        []uint16 // Size = secondaryAggrBound (for findSplit 8-way)
	buffer       []uint64
	offsetBuffer []uint64
	numBuf       [8]byte
	trace        bool     // Enable tracing output
	startSeed    []uint64 // Hash seeds for each level of recursive split
}

// workerState carries per-goroutine state for parallel execution.
type workerState struct {
	scratch    *recsplitScratch // Scratch buffers (per-worker copy)
	unaryBuf   []uint64
	gr         GolombRice // Local output Golomb-Rice for this bucket
	offsetData []byte     // Local output: serialized offset bytes

	// Read-only shared fields (set once before workers start)
	golombRice         []uint32
	leafSize           uint16
	primaryAggrBound   uint16
	secondaryAggrBound uint16
	bytesPerRec        int
}

// RecSplit is the implementation of Recursive Split algorithm for constructing perfect hash mapping, described in
// https://arxiv.org/pdf/1910.06416.pdf Emmanuel Esposito, Thomas Mueller Graf, and Sebastiano Vigna.
// Recsplit: Minimal perfect hashing via recursive splitting. In 2020 Proceedings of the Symposium on Algorithm Engineering and Experiments (ALENEX),
// pages 175−185. SIAM, 2020.
type RecSplit struct {
	// v=0 falsePositeves=true - as array of hashedKeys[0]. Requires `enum=true`. Problem: requires key number - which recsplit has but expensive to encode (~5bytes/key)
	// v=1 falsePositeves=true - as fuse filter (%9 bits/key). Doesn't require `enum=true`
	dataStructureVersion version.DataStructureVersion

	//v0 fields
	existenceFV0 *os.File
	existenceWV0 *bufio.Writer

	//v1 fields
	existenceFV1 *fusefilter.WriterOffHeap

	offsetFile   *os.File      // Temp file for offsets (already sorted, no need for etl.Collector)
	offsetWriter *bufio.Writer // Buffered writer for offset file

	indexW          *bufio.Writer
	indexF          *os.File
	offsetEf        *eliasfano32.EliasFano // Elias Fano instance for encoding the offsets
	bucketCollector *etl.Collector         // Collector that sorts by buckets

	fileName string
	filePath string

	tmpDir            string
	scratch           *recsplitScratch // Working buffers
	gr                GolombRice       // Helper object to encode the tree of hash function salts using Golomb-Rice code.
	bucketPosAcc      []uint64         // Accumulator for position of every bucket in the encoding of the hash function
	startSeed         []uint64
	currentBucket     []uint64 // 64-bit fingerprints of keys in the current bucket accumulated before the recsplit is performed for that bucket
	currentBucketOffs []uint64 // Index offsets for the current bucket
	golombRice        []uint32
	bucketSizeAcc     []uint64 // Bucket size accumulator
	// Helper object to encode the sequence of cumulative number of keys in the buckets
	// and the sequence of cumulative bit offsets of buckets in the Golomb-Rice code.
	ef                 eliasfano16.DoubleEliasFano
	lvl                log.Lvl
	bytesPerRec        int
	minDelta           uint64 // minDelta for Elias Fano encoding of "enum -> offset" index
	prevOffset         uint64 // Previously added offset (for calculating minDelta for Elias Fano encoding of "enum -> offset" index)
	bucketSize         int
	keyExpectedCount   uint64 // Number of keys in the hash table
	keysAdded          uint64 // Number of keys actually added to the recSplit (to check the match with keyExpectedCount)
	maxOffset          uint64 // Maximum value of index offset to later decide how many bytes to use for the encoding
	currentBucketIdx   uint64 // Current bucket being accumulated
	baseDataID         uint64 // Minimal app-specific ID of entries of this index - helps app understand what data stored in given shard - persistent field
	bucketCount        uint64 // Number of buckets
	salt               uint32 // Murmur3 hash used for converting keys to 64-bit values and assigning to buckets
	leafSize           uint16 // Leaf size for recursive split algorithm
	secondaryAggrBound uint16 // The lower bound for secondary key aggregation (computed from leadSize)
	primaryAggrBound   uint16 // The lower bound for primary key aggregation (computed from leafSize)
	bucketKeyBuf       [16]byte
	numBuf             [8]byte
	collision          bool
	enums              bool // Whether to build two level index with perfect hash table pointing to enumeration and enumeration pointing to offsets
	lessFalsePositives bool
	built              bool // Flag indicating that the hash function has been built and no more keys can be added
	trace              bool
	workers            int // Number of worker goroutines for parallel bucket processing
	logger             log.Logger

	noFsync bool // fsync is enabled by default, but tests can manually disable
	timings Timings

	unaryBuf []uint64 // `recsplit` func returning `unary` array. re-using it between buckets
}

type RecSplitArgs struct {
	// Whether two level index needs to be built, where perfect hash map points to an enumeration, and enumeration points to offsets
	// if Enum=false: can have unsorted and duplicated values
	// if Enum=true:  must have sorted values (can have duplicates) - monotonically growing sequence
	Enums              bool
	LessFalsePositives bool
	Version            uint8

	IndexFile  string // File name where the index and the minimal perfect hash function will be written to
	TmpDir     string
	StartSeed  []uint64 // For each level of recursive split, the hash seed (salt) used for that level - need to be generated randomly and be large enough to accommodate all the levels
	KeyCount   int
	BucketSize int
	BaseDataID uint64
	Salt       *uint32 // Hash seed (salt) for the hash function used for allocating the initial buckets - need to be generated randomly
	LeafSize   uint16
	Workers    int // Number of worker goroutines for parallel bucket processing (0 or 1 = single-threaded)

	NoFsync bool // fsync is enabled by default, but tests can manually disable
}

type Timings struct {
	Enabled    bool
	AddStart   time.Time
	AddTook    time.Duration
	BuildStart time.Time
	BuildTook  time.Duration
}

// DefaultLeafSize - LeafSize=8 and BucketSize=100, use about 1.8 bits per key. Increasing the leaf and bucket
// sizes gives more compact structures (1.56 bits per key), at the	price of a slower construction time
const DefaultLeafSize = 8
const DefaultBucketSize = 100 // typical from 100 to 2000, with smaller buckets giving slightly larger but faster function

// NewRecSplit creates a new RecSplit instance with given number of keys and given bucket size
// Typical bucket size is 100 - 2000, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
// salt parameters is used to randomise the hash function construction, to ensure that different Erigon instances (nodes)
// are likely to use different hash function, to collision attacks are unlikely to slow down any meaningful number of nodes at the same time
func NewRecSplit(args RecSplitArgs, logger log.Logger) (*RecSplit, error) {
	if args.BaseDataID >= math.MaxUint64/2 {
		return nil, fmt.Errorf("baseDataID %d is too large, must be less than %d", args.BaseDataID, math.MaxUint64/2)
	}

	if len(args.StartSeed) == 0 {
		args.StartSeed = []uint64{0x106393c187cae2a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a}
	}
	bucketCount := (args.KeyCount + args.BucketSize - 1) / args.BucketSize
	workers := args.Workers
	if workers <= 0 {
		workers = 1
	}
	rs := &RecSplit{
		dataStructureVersion: version.DataStructureVersion(args.Version),
		bucketSize:           args.BucketSize, keyExpectedCount: uint64(args.KeyCount), bucketCount: uint64(bucketCount),
		tmpDir: args.TmpDir, filePath: args.IndexFile,
		enums:              args.Enums,
		baseDataID:         args.BaseDataID,
		lessFalsePositives: args.LessFalsePositives,
		startSeed:          args.StartSeed,
		workers:            workers,
		lvl:                log.LvlDebug, logger: logger,
	}
	closeFiles := true
	defer func() {
		if closeFiles {
			rs.Close()
		}
	}()
	_, fname := filepath.Split(rs.filePath)
	rs.fileName = fname
	if args.Salt == nil {
		seedBytes := make([]byte, 4)
		if _, err := rand.Read(seedBytes); err != nil {
			return nil, err
		}
		rs.salt = binary.BigEndian.Uint32(seedBytes)
	} else {
		rs.salt = *args.Salt
	}
	rs.bucketCollector = etl.NewCollectorWithAllocator(RecSplitLogPrefix+" "+fname, rs.tmpDir, etl.SmallSortableBuffers, logger)
	rs.bucketCollector.SortAndFlushInBackground(false)
	rs.bucketCollector.LogLvl(log.LvlDebug)
	var err error
	if args.Enums {
		rs.offsetFile, err = os.CreateTemp(rs.tmpDir, "recsplit-offsets-")
		if err != nil {
			return nil, err
		}
		rs.offsetWriter = getBufioWriter(rs.offsetFile)
	}
	if rs.enums && args.KeyCount > 0 && rs.lessFalsePositives {
		if rs.dataStructureVersion == 0 {
			rs.existenceFV0, err = os.CreateTemp(rs.tmpDir, "erigon-lfp-buf-")
			if err != nil {
				return nil, err
			}
			rs.existenceWV0 = getBufioWriter(rs.existenceFV0)
		}

	}
	if args.KeyCount > 0 && rs.lessFalsePositives && rs.dataStructureVersion >= 1 {
		rs.existenceFV1, err = fusefilter.NewWriterOffHeap(rs.filePath)
		if err != nil {
			return nil, err
		}
	}

	rs.currentBucket = make([]uint64, 0, args.BucketSize)
	rs.currentBucketOffs = make([]uint64, 0, args.BucketSize)
	rs.bucketSizeAcc = make([]uint64, 1, bucketCount+1)
	rs.bucketPosAcc = make([]uint64, 1, bucketCount+1)
	if args.LeafSize > MaxLeafSize {
		return nil, fmt.Errorf("exceeded max leaf size %d: %d", MaxLeafSize, args.LeafSize)
	}
	rs.leafSize = args.LeafSize
	rs.primaryAggrBound = rs.leafSize * uint16(math.Max(2, math.Ceil(0.35*float64(rs.leafSize)+1./2.)))
	if rs.leafSize < 7 {
		rs.secondaryAggrBound = rs.primaryAggrBound * 2
	} else {
		rs.secondaryAggrBound = rs.primaryAggrBound * uint16(math.Ceil(0.21*float64(rs.leafSize)+9./10.))
	}
	rs.scratch = &recsplitScratch{
		count:     make([]uint16, rs.secondaryAggrBound),
		trace:     rs.trace,
		startSeed: args.StartSeed,
	}
	if args.NoFsync {
		rs.DisableFsync()
	}
	closeFiles = false
	return rs, nil
}

func (rs *RecSplit) FileName() string                           { return rs.fileName }
func (rs *RecSplit) MajorVersion() version.DataStructureVersion { return rs.dataStructureVersion }
func (rs *RecSplit) Salt() uint32                               { return rs.salt }

// golombParamValue extracts the golomb parameter from pre-computed table
func golombParamValue(golombRice []uint32, m uint16) int {
	return int(golombRice[m] >> 27)
}
func (rs *RecSplit) Close() {
	if rs.indexF != nil {
		_ = rs.indexF.Close()
		_ = dir.RemoveFile(rs.indexF.Name())
		rs.indexF = nil
	}
	if rs.existenceFV0 != nil {
		_ = rs.existenceFV0.Close()
		_ = dir.RemoveFile(rs.existenceFV0.Name())
		rs.existenceFV0 = nil
	}
	if rs.existenceWV0 != nil {
		putBufioWriter(rs.existenceWV0)
		rs.existenceWV0 = nil
	}
	if rs.existenceFV1 != nil {
		rs.existenceFV1.Close()
		rs.existenceFV1 = nil
	}
	if rs.bucketCollector != nil {
		rs.bucketCollector.Close()
	}
	if rs.offsetFile != nil {
		_ = rs.offsetFile.Close()
		_ = dir.RemoveFile(rs.offsetFile.Name())
		rs.offsetFile = nil
	}
	if rs.offsetWriter != nil {
		putBufioWriter(rs.offsetWriter)
		rs.offsetWriter = nil
	}
}

func (rs *RecSplit) LogLvl(lvl log.Lvl) { rs.lvl = lvl }

func (rs *RecSplit) SetTrace(trace bool) {
	rs.trace = trace
}

// remap converts the number x which is assumed to be uniformly distributed over the range [0..2^64) to the number that is uniformly
// distributed over the range [0..n)
func remap(x uint64, n uint64) (hi uint64) {
	hi, _ = bits.Mul64(x, n)
	return hi
}

const mask48 uint64 = (1 << 48) - 1

// remap converts the number x which is assumed to be uniformly distributed over the range [0..2^64) to the number that is uniformly
// distributed over the range [0..n), under assumption that n is less than 2^16
func remap16(x uint64, n uint16) uint16 {
	return uint16(((x & mask48) * uint64(n)) >> 48)
}

// ResetNextSalt resets the RecSplit and uses the next salt value to try to avoid collisions
// when mapping keys to 64-bit values
func (rs *RecSplit) ResetNextSalt() {
	rs.built = false
	rs.collision = false
	rs.keysAdded = 0
	rs.salt++
	if rs.bucketCollector != nil {
		rs.bucketCollector.Close()
	}
	rs.bucketCollector = etl.NewCollectorWithAllocator(RecSplitLogPrefix+" "+rs.fileName, rs.tmpDir, etl.SmallSortableBuffers, rs.logger)
	rs.bucketCollector.SortAndFlushInBackground(false)
	rs.bucketCollector.LogLvl(log.LvlDebug)
	if rs.offsetFile != nil {
		_ = rs.offsetFile.Truncate(0)
		_, _ = rs.offsetFile.Seek(0, 0)
		rs.offsetWriter.Reset(rs.offsetFile)
	}
	rs.currentBucket = rs.currentBucket[:0]
	rs.currentBucketOffs = rs.currentBucketOffs[:0]
	rs.maxOffset = 0
	rs.bucketSizeAcc = rs.bucketSizeAcc[:1] // First entry is always zero
	rs.bucketPosAcc = rs.bucketPosAcc[:1]   // First entry is always zero
}

func splitParams(m, leafSize, primaryAggrBound, secondaryAggrBound uint16) (fanout, unit uint16) {
	if m > secondaryAggrBound { // High-level aggregation (fanout 2)
		unit = secondaryAggrBound * (((m+1)/2 + secondaryAggrBound - 1) / secondaryAggrBound)
		fanout = 2
	} else if m > primaryAggrBound { // Second-level aggregation
		unit = primaryAggrBound
		fanout = (m + primaryAggrBound - 1) / primaryAggrBound
	} else { // First-level aggregation
		unit = leafSize
		fanout = (m + leafSize - 1) / leafSize
	}
	return
}

var golombBaseLog2 = -math.Log((math.Sqrt(5) + 1.0) / 2.0)

func computeGolombRice(m uint16, table []uint32, leafSize, primaryAggrBound, secondaryAggrBound uint16) {
	fanout, unit := splitParams(m, leafSize, primaryAggrBound, secondaryAggrBound)
	k := make([]uint16, fanout)
	k[fanout-1] = m
	for i := uint16(0); i < fanout-1; i++ {
		k[i] = unit
		k[fanout-1] -= k[i]
	}
	sqrtProd := float64(1)
	for i := uint16(0); i < fanout; i++ {
		sqrtProd *= math.Sqrt(float64(k[i]))
	}
	p := math.Sqrt(float64(m)) / (math.Pow(2*math.Pi, (float64(fanout)-1.)/2.0) * sqrtProd)
	golombRiceLength := uint32(math.Ceil(math.Log2(golombBaseLog2 / math.Log1p(-p)))) // log2 Golomb modulus
	if golombRiceLength > 0x1F {
		panic("golombRiceLength > 0x1F")
	}
	table[m] = golombRiceLength << 27
	for i := uint16(0); i < fanout; i++ {
		golombRiceLength += table[k[i]] & 0xFFFF
	}
	if golombRiceLength > 0xFFFF {
		panic("golombRiceLength > 0xFFFF")
	}
	table[m] |= golombRiceLength // Sum of Golomb-Rice codeslengths in the subtree, stored in the lower 16 bits
	nodes := uint32(1)
	for i := uint16(0); i < fanout; i++ {
		nodes += (table[k[i]] >> 16) & 0x7FF
	}
	if leafSize >= 3 && nodes > 0x7FF {
		panic("rs.leafSize >= 3 && nodes > 0x7FF")
	}
	table[m] |= nodes << 16
}

// golombParam returns the optimal Golomb parameter to use for encoding
// salt for the part of the hash function separating m elements. It is based on
// calculations with assumptions that we draw hash functions at random
func (rs *RecSplit) golombParam(m uint16) int {
	for s := uint16(len(rs.golombRice)); m >= s; s++ {
		rs.golombRice = append(rs.golombRice, 0)
		// For the case where bucket is larger than planned
		if s == 0 {
			rs.golombRice[0] = (bijMemo[0] << 27) | bijMemo[0]
		} else if s <= rs.leafSize {
			rs.golombRice[s] = (bijMemo[s] << 27) | (uint32(1) << 16) | bijMemo[s]
		} else {
			computeGolombRice(s, rs.golombRice, rs.leafSize, rs.primaryAggrBound, rs.secondaryAggrBound)
		}
	}
	return int(rs.golombRice[m] >> 27)
}

// Add key to the RecSplit. There can be many more keys than what fits in RAM, and RecSplit
// spills data onto disk to accommodate that. The key gets copied by the collector, therefore
// the slice underlying key is not getting accessed by RecSplit after this invocation.
func (rs *RecSplit) AddKey(key []byte, offset uint64) error {
	if rs.built {
		return errors.New("cannot add keys after perfect hash function had been built")
	}
	hi, lo := murmur3.Sum128WithSeed(key, rs.salt)
	binary.BigEndian.PutUint64(rs.bucketKeyBuf[:], remap(hi, rs.bucketCount))
	binary.BigEndian.PutUint64(rs.bucketKeyBuf[8:], lo)
	binary.BigEndian.PutUint64(rs.numBuf[:], offset)
	if offset > rs.maxOffset {
		rs.maxOffset = offset
	}
	if rs.keysAdded > 0 {
		delta := offset - rs.prevOffset
		if rs.keysAdded == 1 || delta < rs.minDelta {
			rs.minDelta = delta
		}
	}

	if rs.enums {
		if rs.keysAdded > 0 && offset < rs.prevOffset {
			panic(fmt.Sprintf("recsplit: AddKey offsets must be monotonically increasing: prev=%d, cur=%d", rs.prevOffset, offset))
		}
		if _, err := rs.offsetWriter.Write(rs.numBuf[:]); err != nil {
			return err
		}
		binary.BigEndian.PutUint64(rs.numBuf[:], rs.keysAdded)
		if err := rs.bucketCollector.Collect(rs.bucketKeyBuf[:], rs.numBuf[:]); err != nil {
			return err
		}
		if rs.lessFalsePositives {
			if rs.dataStructureVersion == 0 {
				//1 byte from each hashed key
				if err := rs.existenceWV0.WriteByte(byte(hi)); err != nil {
					return err
				}
			}
		}
	} else {
		if err := rs.bucketCollector.Collect(rs.bucketKeyBuf[:], rs.numBuf[:]); err != nil {
			return err
		}
	}

	if rs.lessFalsePositives && rs.dataStructureVersion >= 1 {
		if err := rs.existenceFV1.AddHash(hi); err != nil {
			return err
		}
	}

	rs.keysAdded++
	rs.prevOffset = offset
	return nil
}

func (rs *RecSplit) AddOffset(offset uint64) error {
	if rs.enums {
		if rs.keysAdded > 0 && offset < rs.prevOffset {
			panic(fmt.Sprintf("recsplit: AddOffset offsets must be monotonically increasing: prev=%d, cur=%d", rs.prevOffset, offset))
		}
		binary.BigEndian.PutUint64(rs.numBuf[:], offset)
		if _, err := rs.offsetWriter.Write(rs.numBuf[:]); err != nil {
			return err
		}
	}
	return nil
}

func (rs *RecSplit) recsplitCurrentBucket() error {
	// Extend rs.bucketSizeAcc to accommodate the current bucket index + 1
	for len(rs.bucketSizeAcc) <= int(rs.currentBucketIdx)+1 {
		rs.bucketSizeAcc = append(rs.bucketSizeAcc, rs.bucketSizeAcc[len(rs.bucketSizeAcc)-1])
	}
	rs.bucketSizeAcc[int(rs.currentBucketIdx)+1] += uint64(len(rs.currentBucket))
	// Sets of size 0 and 1 are not further processed, just write them to index
	if len(rs.currentBucket) > 1 {
		for i, key := range rs.currentBucket[1:] {
			if key == rs.currentBucket[i] {
				rs.collision = true
				return fmt.Errorf("%w: %x", ErrCollision, key)
			}
		}
		bitPos := rs.gr.bitCount
		if rs.scratch.buffer == nil {
			rs.scratch.buffer = make([]uint64, len(rs.currentBucket))
			rs.scratch.offsetBuffer = make([]uint64, len(rs.currentBucketOffs))
		} else {
			for len(rs.scratch.buffer) < len(rs.currentBucket) {
				rs.scratch.buffer = append(rs.scratch.buffer, 0)
				rs.scratch.offsetBuffer = append(rs.scratch.offsetBuffer, 0)
			}
		}
		var err error
		rs.unaryBuf, err = rs.recsplit(0 /* level */, rs.currentBucket, rs.currentBucketOffs, rs.unaryBuf[:0], rs.scratch)
		if err != nil {
			return err
		}
		rs.gr.appendUnaryAll(rs.unaryBuf)
		if rs.trace {
			fmt.Printf("recsplitBucket(%d, %d, bitsize = %d)\n", rs.currentBucketIdx, len(rs.currentBucket), rs.gr.bitCount-bitPos)
		}
	} else {
		for _, offset := range rs.currentBucketOffs {
			binary.BigEndian.PutUint64(rs.numBuf[:], offset)
			if _, err := rs.indexW.Write(rs.numBuf[8-rs.bytesPerRec:]); err != nil {
				return err
			}
		}
	}
	// Extend rs.bucketPosAcc to accommodate the current bucket index + 1
	for len(rs.bucketPosAcc) <= int(rs.currentBucketIdx)+1 {
		rs.bucketPosAcc = append(rs.bucketPosAcc, rs.bucketPosAcc[len(rs.bucketPosAcc)-1])
	}
	rs.bucketPosAcc[int(rs.currentBucketIdx)+1] = uint64(rs.gr.Bits())
	// clear for the next buckey
	rs.currentBucket = rs.currentBucket[:0]
	rs.currentBucketOffs = rs.currentBucketOffs[:0]
	return nil
}

// findSplit finds a salt value such that keys in bucket are evenly distributed
// into fanout partitions of size unit each (based on remap16(remix(key+salt), m) / unit).
// Uses 8-way salt parallelism with 8 independent count arrays carved from the
// count slice (which must have len >= 8*fanout).
func findSplit(bucket []uint64, salt uint64, fanout, unit uint16, count []uint16) uint64 {
	m := uint16(len(bucket))
	c0 := count[0*fanout : 1*fanout : 1*fanout]
	c1 := count[1*fanout : 2*fanout : 2*fanout]
	c2 := count[2*fanout : 3*fanout : 3*fanout]
	c3 := count[3*fanout : 4*fanout : 4*fanout]
	c4 := count[4*fanout : 5*fanout : 5*fanout]
	c5 := count[5*fanout : 6*fanout : 6*fanout]
	c6 := count[6*fanout : 7*fanout : 7*fanout]
	c7 := count[7*fanout : 8*fanout : 8*fanout]
	for {
		clear(count[:8*fanout])
		for i := uint16(0); i < m; i++ {
			key := bucket[i]
			c0[remap16(remix(key+salt), m)/unit]++
			c1[remap16(remix(key+salt+1), m)/unit]++
			c2[remap16(remix(key+salt+2), m)/unit]++
			c3[remap16(remix(key+salt+3), m)/unit]++
			c4[remap16(remix(key+salt+4), m)/unit]++
			c5[remap16(remix(key+salt+5), m)/unit]++
			c6[remap16(remix(key+salt+6), m)/unit]++
			c7[remap16(remix(key+salt+7), m)/unit]++
		}
		// Branchless validation: XOR each count with expected value,
		// OR-accumulate to detect any mismatch.
		var bad0, bad1, bad2, bad3, bad4, bad5, bad6, bad7 uint16
		for i := uint16(0); i < fanout-1; i++ {
			bad0 |= c0[i] ^ unit
			bad1 |= c1[i] ^ unit
			bad2 |= c2[i] ^ unit
			bad3 |= c3[i] ^ unit
			bad4 |= c4[i] ^ unit
			bad5 |= c5[i] ^ unit
			bad6 |= c6[i] ^ unit
			bad7 |= c7[i] ^ unit
		}
		if bad0 == 0 {
			return salt
		}
		if bad1 == 0 {
			return salt + 1
		}
		if bad2 == 0 {
			return salt + 2
		}
		if bad3 == 0 {
			return salt + 3
		}
		if bad4 == 0 {
			return salt + 4
		}
		if bad5 == 0 {
			return salt + 5
		}
		if bad6 == 0 {
			return salt + 6
		}
		if bad7 == 0 {
			return salt + 7
		}
		salt += 8
	}
}

// findBijection finds a salt value such that all keys in bucket hash to distinct
// positions in [0, m).
// Uses 8-way salt parallelism with branchless OR-accumulate
// to exploit CPU instruction-level parallelism and avoid branch mispredictions.
func findBijection(bucket []uint64, salt uint64) uint64 {
	m := uint16(len(bucket))
	fullMask := uint32((1 << m) - 1)
	for {
		var mask0, mask1, mask2, mask3, mask4, mask5, mask6, mask7 uint32
		for i := uint16(0); i < m; i++ {
			key := bucket[i]
			// adding `& 31` - it doesn't have runtime overhead, but it tells for compiler that shift can't overflow
			// and compiler generating less assembly checks: ~10% perf.
			// it's safe because: len(bucket) <= leafSize <= 24
			mask0 |= uint32(1) << remap16(remix(key+salt), m&31)
			mask1 |= uint32(1) << remap16(remix(key+salt+1), m&31)
			mask2 |= uint32(1) << remap16(remix(key+salt+2), m&31)
			mask3 |= uint32(1) << remap16(remix(key+salt+3), m&31)
			mask4 |= uint32(1) << remap16(remix(key+salt+4), m&31)
			mask5 |= uint32(1) << remap16(remix(key+salt+5), m&31)
			mask6 |= uint32(1) << remap16(remix(key+salt+6), m&31)
			mask7 |= uint32(1) << remap16(remix(key+salt+7), m&31)
		}
		if mask0 == fullMask {
			return salt
		}
		if mask1 == fullMask {
			return salt + 1
		}
		if mask2 == fullMask {
			return salt + 2
		}
		if mask3 == fullMask {
			return salt + 3
		}
		if mask4 == fullMask {
			return salt + 4
		}
		if mask5 == fullMask {
			return salt + 5
		}
		if mask6 == fullMask {
			return salt + 6
		}
		if mask7 == fullMask {
			return salt + 7
		}
		salt += 8
	}
}

// newWorkerState creates a new workerState for a worker goroutine.
func (rs *RecSplit) newWorkerState() *workerState {
	return &workerState{
		scratch: &recsplitScratch{
			count:        make([]uint16, rs.secondaryAggrBound),
			buffer:       make([]uint64, rs.secondaryAggrBound),
			offsetBuffer: make([]uint64, rs.secondaryAggrBound),
			trace:        rs.trace,
			startSeed:    rs.startSeed,
		},
		unaryBuf:           make([]uint64, 0, rs.secondaryAggrBound),
		golombRice:         rs.golombRice,
		leafSize:           rs.leafSize,
		primaryAggrBound:   rs.primaryAggrBound,
		secondaryAggrBound: rs.secondaryAggrBound,
		bytesPerRec:        rs.bytesPerRec,
	}
}

// recsplitBucketWorker processes buckets from input channel and writes results to output channel.
func recsplitBucketWorker(inCh <-chan *bucketTask, outCh chan<- *bucketResult, ws *workerState, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range inCh {
		// Reset local state for this bucket
		ws.gr = GolombRice{}
		ws.offsetData = ws.offsetData[:0]
		ws.unaryBuf = ws.unaryBuf[:0]

		if len(task.keys) > 1 {
			// Check for collisions
			for i, key := range task.keys[1:] {
				if key == task.keys[i] {
					// Collision detected - send error via special result (order = MaxUint64)
					outCh <- &bucketResult{
						order: 0xffffffffffffffff, // Special marker for error
					}
					return
				}
			}

			// Process the bucket recursively
			var err error
			ws.unaryBuf, err = recsplitWorkerFunc(ws, 0, task.keys, task.offsets, ws.unaryBuf[:0], ws.scratch)
			if err != nil {
				outCh <- &bucketResult{
					order: 0xffffffffffffffff, // Special marker for error
				}
				return
			}
			ws.gr.appendUnaryAll(ws.unaryBuf)
		} else {
			// Size 0 or 1: write offsets directly
			for _, off := range task.offsets {
				binary.BigEndian.PutUint64(ws.scratch.numBuf[:], off)
				ws.offsetData = append(ws.offsetData, ws.scratch.numBuf[8-ws.bytesPerRec:]...)
			}
		}

		// Send result (copy data to avoid use-after-free)
		result := &bucketResult{
			order:      task.order,
			offsetData: append([]byte{}, ws.offsetData...), // Copy out
			gr:         ws.gr,                              // Copy out (value type)
			bucketSize: len(task.keys),
		}
		outCh <- result
	}
}

// recsplitWorkerFunc applies recsplit algorithm to the given bucket using per-worker state.
// This is the worker-safe version that accumulates results without writing to indexW.
func recsplitWorkerFunc(ws *workerState, level int, bucket []uint64, offsets []uint64, unary []uint64, scratch *recsplitScratch) ([]uint64, error) {
	if scratch.trace {
		fmt.Printf("recsplit(%d, %d, %x)\n", level, len(bucket), bucket)
	}
	// Pick initial salt for this level of recursive split
	salt := scratch.startSeed[level]
	m := uint16(len(bucket))
	if m <= ws.leafSize {
		salt = findBijection(bucket, salt)
		for i := uint16(0); i < m; i++ {
			j := remap16(remix(bucket[i]+salt), m)
			scratch.offsetBuffer[j] = offsets[i]
		}
		// Write offsets to local offsetData buffer instead of to file
		for _, offset := range scratch.offsetBuffer[:m] {
			binary.BigEndian.PutUint64(scratch.numBuf[:], offset)
			ws.offsetData = append(ws.offsetData, scratch.numBuf[8-ws.bytesPerRec:]...)
		}
		salt -= scratch.startSeed[level]
		log2golomb := golombParamValue(ws.golombRice, m)
		if scratch.trace {
			fmt.Printf("encode bij %d with log2golomn %d at p = %d\n", salt, log2golomb, ws.gr.Bits())
		}
		ws.gr.appendFixed(salt, log2golomb)
		unary = append(unary, salt>>log2golomb)
	} else {
		fanout, unit := splitParams(m, ws.leafSize, ws.primaryAggrBound, ws.secondaryAggrBound)
		count := scratch.count
		salt = findSplit(bucket, salt, fanout, unit, count)
		for i, c := uint16(0), uint16(0); i < fanout; i++ {
			count[i] = c
			c += unit
		}
		for i := uint16(0); i < m; i++ {
			j := remap16(remix(bucket[i]+salt), m) / unit
			scratch.buffer[count[j]] = bucket[i]
			scratch.offsetBuffer[count[j]] = offsets[i]
			count[j]++
		}
		copy(bucket, scratch.buffer)
		copy(offsets, scratch.offsetBuffer)
		salt -= scratch.startSeed[level]
		log2golomb := golombParamValue(ws.golombRice, m)
		if scratch.trace {
			fmt.Printf("encode fanout %d: %d with log2golomn %d at p = %d\n", fanout, salt, log2golomb, ws.gr.Bits())
		}
		ws.gr.appendFixed(salt, log2golomb)
		unary = append(unary, salt>>log2golomb)
		var err error
		var i uint16
		for i = 0; i < m-unit; i += unit {
			if unary, err = recsplitWorkerFunc(ws, level+1, bucket[i:i+unit], offsets[i:i+unit], unary, scratch); err != nil {
				return nil, err
			}
		}
		if m-i > 1 {
			if unary, err = recsplitWorkerFunc(ws, level+1, bucket[i:], offsets[i:], unary, scratch); err != nil {
				return nil, err
			}
		} else if m-i == 1 {
			binary.BigEndian.PutUint64(scratch.numBuf[:], offsets[i])
			ws.offsetData = append(ws.offsetData, scratch.numBuf[8-ws.bytesPerRec:]...)
		}
	}
	return unary, nil
}

// recsplit applies recSplit algorithm to the given bucket
func (rs *RecSplit) recsplit(level int, bucket []uint64, offsets []uint64, unary []uint64, scratch *recsplitScratch) ([]uint64, error) {
	if scratch.trace {
		fmt.Printf("recsplit(%d, %d, %x)\n", level, len(bucket), bucket)
	}
	// Pick initial salt for this level of recursive split
	salt := scratch.startSeed[level]
	m := uint16(len(bucket))
	if m <= rs.leafSize {
		salt = findBijection(bucket, salt)
		for i := uint16(0); i < m; i++ {
			j := remap16(remix(bucket[i]+salt), m)
			scratch.offsetBuffer[j] = offsets[i]
		}
		for _, offset := range scratch.offsetBuffer[:m] {
			binary.BigEndian.PutUint64(scratch.numBuf[:], offset)
			if _, err := rs.indexW.Write(scratch.numBuf[8-rs.bytesPerRec:]); err != nil {
				return nil, err
			}
		}
		salt -= scratch.startSeed[level]
		log2golomb := golombParamValue(rs.golombRice, m)
		if scratch.trace {
			fmt.Printf("encode bij %d with log2golomn %d at p = %d\n", salt, log2golomb, rs.gr.bitCount)
		}
		rs.gr.appendFixed(salt, log2golomb)
		unary = append(unary, salt>>log2golomb)
	} else {
		fanout, unit := splitParams(m, rs.leafSize, rs.primaryAggrBound, rs.secondaryAggrBound)
		count := scratch.count
		salt = findSplit(bucket, salt, fanout, unit, count)
		for i, c := uint16(0), uint16(0); i < fanout; i++ {
			count[i] = c
			c += unit
		}
		for i := uint16(0); i < m; i++ {
			j := remap16(remix(bucket[i]+salt), m) / unit
			scratch.buffer[count[j]] = bucket[i]
			scratch.offsetBuffer[count[j]] = offsets[i]
			count[j]++
		}
		copy(bucket, scratch.buffer)
		copy(offsets, scratch.offsetBuffer)
		salt -= scratch.startSeed[level]
		log2golomb := golombParamValue(rs.golombRice, m)
		if scratch.trace {
			fmt.Printf("encode fanout %d: %d with log2golomn %d at p = %d\n", fanout, salt, log2golomb, rs.gr.bitCount)
		}
		rs.gr.appendFixed(salt, log2golomb)
		unary = append(unary, salt>>log2golomb)
		var err error
		var i uint16
		for i = 0; i < m-unit; i += unit {
			if unary, err = rs.recsplit(level+1, bucket[i:i+unit], offsets[i:i+unit], unary, scratch); err != nil {
				return nil, err
			}
		}
		if m-i > 1 {
			if unary, err = rs.recsplit(level+1, bucket[i:], offsets[i:], unary, scratch); err != nil {
				return nil, err
			}
		} else if m-i == 1 {
			binary.BigEndian.PutUint64(scratch.numBuf[:], offsets[i])
			if _, err := rs.indexW.Write(scratch.numBuf[8-rs.bytesPerRec:]); err != nil {
				return nil, err
			}
		}
	}
	return unary, nil
}

// loadFuncBucket is required to satisfy the type etl.LoadFunc type, to use with collector.Load
func (rs *RecSplit) loadFuncBucket(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
	// k is the BigEndian encoding of the bucket number, and the v is the key that is assigned into that bucket
	bucketIdx := binary.BigEndian.Uint64(k)
	if rs.currentBucketIdx != bucketIdx {
		if rs.currentBucketIdx != math.MaxUint64 {
			if err := rs.recsplitCurrentBucket(); err != nil {
				return err
			}
		}
		rs.currentBucketIdx = bucketIdx
	}
	rs.currentBucket = append(rs.currentBucket, binary.BigEndian.Uint64(k[8:]))
	rs.currentBucketOffs = append(rs.currentBucketOffs, binary.BigEndian.Uint64(v))
	return nil
}

// buildOffsetEf mmaps the offset temp file and builds the Elias-Fano encoding.
func (rs *RecSplit) buildOffsetEf() error {
	rs.offsetEf = eliasfano32.NewEliasFano(rs.keysAdded, rs.maxOffset)
	if err := rs.offsetWriter.Flush(); err != nil {
		return fmt.Errorf("flush offset writer: %w", err)
	}

	mmapSize := int(rs.keysAdded * 8)
	mmapHandle1, mmapHandle2, err := mmap.Mmap(rs.offsetFile, mmapSize)
	if err != nil {
		return fmt.Errorf("mmap offset file: %w", err)
	}
	defer mmap.Munmap(mmapHandle1, mmapHandle2)

	data := mmapHandle1[:mmapSize]
	for i := uint64(0); i < rs.keysAdded; i++ {
		rs.offsetEf.AddOffset(binary.BigEndian.Uint64(data[i*8:]))
	}
	rs.offsetEf.Build()
	return nil
}

// buildWithWorkers implements the producer-consumer pattern for parallel bucket processing.
func (rs *RecSplit) buildWithWorkers(ctx context.Context) error {
	// Create channels for task distribution and result collection
	taskCh := make(chan *bucketTask, rs.workers*2)
	resultCh := make(chan *bucketResult, rs.workers*2)

	// Spawn worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < rs.workers; i++ {
		ws := rs.newWorkerState()
		wg.Add(1)
		go recsplitBucketWorker(taskCh, resultCh, ws, &wg)
	}

	// Producer goroutine that loads buckets and sends tasks
	var producerErr error
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		producerErr = rs.bucketCollector.Load(nil, "", func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			bucketIdx := binary.BigEndian.Uint64(k)
			if rs.currentBucketIdx != bucketIdx {
				if rs.currentBucketIdx != math.MaxUint64 {
					// Send current bucket as a task
					task := &bucketTask{
						order:   rs.currentBucketIdx,
						keys:    rs.currentBucket,
						offsets: rs.currentBucketOffs,
					}
					select {
					case taskCh <- task:
					case <-ctx.Done():
						return ctx.Err()
					}
					// Prepare for next bucket
					rs.currentBucket = make([]uint64, 0, rs.bucketSize)
					rs.currentBucketOffs = make([]uint64, 0, rs.bucketSize)
				}
				rs.currentBucketIdx = bucketIdx
			}
			rs.currentBucket = append(rs.currentBucket, binary.BigEndian.Uint64(k[8:]))
			rs.currentBucketOffs = append(rs.currentBucketOffs, binary.BigEndian.Uint64(v))
			return nil
		}, etl.TransformArgs{Quit: ctx.Done()})
	}()

	// Wait for producer to finish and send last bucket
	<-producerDone
	if producerErr != nil {
		close(taskCh)
		return producerErr
	}

	// Send last bucket if any
	if len(rs.currentBucket) > 0 {
		task := &bucketTask{
			order:   rs.currentBucketIdx,
			keys:    rs.currentBucket,
			offsets: rs.currentBucketOffs,
		}
		taskCh <- task
	}
	close(taskCh)

	// Collector goroutine that receives results and writes them in order
	var resultErr error
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		resultErr = rs.collectAndWriteResults(resultCh)
	}()

	// Wait for workers to finish
	wg.Wait()
	close(resultCh)

	// Wait for collector to finish
	<-collectorDone
	return resultErr
}

// collectAndWriteResults collects results from workers and writes them to indexW in order.
func (rs *RecSplit) collectAndWriteResults(resultCh <-chan *bucketResult) error {
	var queue bucketResultQueue
	heap.Init(&queue)
	var nextOrder uint64 = 0

	for result := range resultCh {
		// Check for error result (order = MaxUint64)
		if result.order == 0xffffffffffffffff {
			return fmt.Errorf("worker error processing bucket")
		}

		// Push result to heap if out of order
		if result.order != nextOrder {
			heap.Push(&queue, result)
			continue
		}

		// Write this result and any in-order results from queue
		if err := rs.writeResult(result); err != nil {
			return err
		}
		nextOrder++

		// Drain queue while results are in order
		for len(queue) > 0 && queue[0].order == nextOrder {
			result := heap.Pop(&queue).(*bucketResult)
			if err := rs.writeResult(result); err != nil {
				return err
			}
			nextOrder++
		}
	}

	// Write any remaining queued results
	for len(queue) > 0 {
		result := heap.Pop(&queue).(*bucketResult)
		if err := rs.writeResult(result); err != nil {
			return err
		}
	}

	return nil
}

// writeResult writes a single bucket result to the output.
func (rs *RecSplit) writeResult(result *bucketResult) error {
	// Extend bucketSizeAcc and bucketPosAcc
	for len(rs.bucketSizeAcc) <= int(result.order)+1 {
		rs.bucketSizeAcc = append(rs.bucketSizeAcc, rs.bucketSizeAcc[len(rs.bucketSizeAcc)-1])
	}
	rs.bucketSizeAcc[int(result.order)+1] += uint64(result.bucketSize)

	for len(rs.bucketPosAcc) <= int(result.order)+1 {
		rs.bucketPosAcc = append(rs.bucketPosAcc, rs.bucketPosAcc[len(rs.bucketPosAcc)-1])
	}

	// Write offset data
	if _, err := rs.indexW.Write(result.offsetData); err != nil {
		return err
	}

	// Append Golomb-Rice encoding
	rs.gr.Append(&result.gr)

	// Set bucket position accumulator after appending
	rs.bucketPosAcc[int(result.order)+1] = uint64(rs.gr.Bits())

	return nil
}

// Build has to be called after all the keys have been added, and it initiates the process
// of building the perfect hash function and writing index into a file
func (rs *RecSplit) Build(ctx context.Context) error {
	if rs.built {
		return errors.New("already built")
	}
	if rs.keysAdded != rs.keyExpectedCount {
		return fmt.Errorf("rs %s expected keys %d, got %d", rs.fileName, rs.keyExpectedCount, rs.keysAdded)
	}
	if rs.timings.Enabled {
		rs.timings.AddTook = time.Since(rs.timings.AddStart) // assume Adding data into compressor complete
		rs.timings.BuildStart = time.Now()
		defer func() { rs.timings.BuildTook = time.Since(rs.timings.BuildStart) }()
	}

	var err error
	if rs.indexF, err = dir.CreateTemp(rs.filePath); err != nil {
		return fmt.Errorf("create index file %s: %w", rs.filePath, err)
	}

	defer rs.indexF.Close()
	rs.indexW = getBufioWriter(rs.indexF)
	defer putBufioWriter(rs.indexW)
	// 1 byte: dataStructureVersion, 7 bytes: app-specific minimal dataID (of current shard)
	binary.BigEndian.PutUint64(rs.numBuf[:], rs.baseDataID)
	rs.numBuf[0] = uint8(rs.dataStructureVersion)
	if _, err = rs.indexW.Write(rs.numBuf[:]); err != nil {
		return fmt.Errorf("write number of keys: %w", err)
	}

	// Write number of keys
	binary.BigEndian.PutUint64(rs.numBuf[:], rs.keysAdded)
	if _, err = rs.indexW.Write(rs.numBuf[:]); err != nil {
		return fmt.Errorf("write number of keys: %w", err)
	}
	// Write number of bytes per index record
	if rs.enums {
		rs.bytesPerRec = common.BitLenToByteLen(bits.Len64(rs.keysAdded + 1))
	} else {
		rs.bytesPerRec = common.BitLenToByteLen(bits.Len64(rs.maxOffset))
	}
	if err = rs.indexW.WriteByte(byte(rs.bytesPerRec)); err != nil {
		return fmt.Errorf("write bytes per record: %w", err)
	}

	rs.currentBucketIdx = math.MaxUint64 // To make sure 0 bucket is detected
	defer rs.bucketCollector.Close()

	// Pre-compute golombRice table up to max bucket size (for workers to use without locking)
	maxM := uint16(rs.bucketSize)
	if rs.secondaryAggrBound > maxM {
		maxM = rs.secondaryAggrBound
	}
	for m := uint16(0); m <= maxM; m++ {
		rs.golombParam(m)
	}

	if rs.lvl < log.LvlTrace {
		log.Log(rs.lvl, "[index] calculating", "file", rs.fileName)
	}

	if rs.workers <= 1 {
		// Single-threaded path (original implementation)
		if err := rs.bucketCollector.Load(nil, "", rs.loadFuncBucket, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
			return err
		}
		if len(rs.currentBucket) > 0 {
			if err := rs.recsplitCurrentBucket(); err != nil {
				return err
			}
		}
	} else {
		// Multi-worker producer-consumer path
		if err := rs.buildWithWorkers(ctx); err != nil {
			return err
		}
	}

	if assert.Enable {
		_ = rs.indexW.Flush()
		rs.indexF.Seek(0, 0)
		b, _ := io.ReadAll(rs.indexF)
		if len(b) != 9+int(rs.keysAdded)*rs.bytesPerRec {
			panic(fmt.Errorf("expected: %d, got: %d; rs.keysAdded=%d, rs.bytesPerRec=%d, %s", 9+int(rs.keysAdded)*rs.bytesPerRec, len(b), rs.keysAdded, rs.bytesPerRec, rs.filePath))
		}
	}
	if rs.lvl < log.LvlTrace {
		log.Log(rs.lvl, "[index] write", "file", rs.fileName)
	}
	if rs.enums && rs.keysAdded > 0 {
		if err := rs.buildOffsetEf(); err != nil {
			return err
		}
	}
	rs.gr.appendFixed(1, 1) // Sentinel (avoids checking for parts of size 1)
	// Construct Elias Fano index
	rs.ef.Build(rs.bucketSizeAcc, rs.bucketPosAcc)
	rs.built = true

	// Write out bucket count, bucketSize, leafSize
	binary.BigEndian.PutUint64(rs.numBuf[:], rs.bucketCount)
	if _, err := rs.indexW.Write(rs.numBuf[:8]); err != nil {
		return fmt.Errorf("writing bucketCount: %w", err)
	}
	binary.BigEndian.PutUint16(rs.numBuf[:], uint16(rs.bucketSize))
	if _, err := rs.indexW.Write(rs.numBuf[:2]); err != nil {
		return fmt.Errorf("writing bucketSize: %w", err)
	}
	binary.BigEndian.PutUint16(rs.numBuf[:], rs.leafSize)
	if _, err := rs.indexW.Write(rs.numBuf[:2]); err != nil {
		return fmt.Errorf("writing leafSize: %w", err)
	}
	// Write out salt
	binary.BigEndian.PutUint32(rs.numBuf[:], rs.salt)
	if _, err := rs.indexW.Write(rs.numBuf[:4]); err != nil {
		return fmt.Errorf("writing salt: %w", err)
	}
	// Write out start seeds
	if err := rs.indexW.WriteByte(byte(len(rs.startSeed))); err != nil {
		return fmt.Errorf("writing len of start seeds: %w", err)
	}
	for _, s := range rs.startSeed {
		binary.BigEndian.PutUint64(rs.numBuf[:], s)
		if _, err := rs.indexW.Write(rs.numBuf[:8]); err != nil {
			return fmt.Errorf("writing start seed: %w", err)
		}
	}

	var features Features
	if rs.enums {
		features |= Enums
	}
	if rs.lessFalsePositives {
		features |= LessFalsePositives
	}
	if err := rs.indexW.WriteByte(byte(features)); err != nil {
		return fmt.Errorf("writing enums = true: %w", err)
	}
	if rs.enums && rs.keysAdded > 0 {
		// Write out elias fano for offsets
		if err := rs.offsetEf.Write(rs.indexW); err != nil {
			return fmt.Errorf("writing elias fano for offsets: %w", err)
		}
	}
	if err := rs.flushExistenceFilter(); err != nil {
		return err
	}
	// Write out the size of golomb rice params
	binary.BigEndian.PutUint16(rs.numBuf[:], uint16(len(rs.golombRice)))
	if _, err := rs.indexW.Write(rs.numBuf[:4]); err != nil {
		return fmt.Errorf("writing golomb rice param size: %w", err)
	}
	// Write out golomb rice
	if err := rs.gr.Write(rs.indexW); err != nil {
		return fmt.Errorf("writing golomb rice: %w", err)
	}
	// Write out elias fano
	if err := rs.ef.Write(rs.indexW); err != nil {
		return fmt.Errorf("writing elias fano: %w", err)
	}

	if err = rs.indexW.Flush(); err != nil {
		return err
	}
	if err = rs.fsync(); err != nil {
		return err
	}
	if err = rs.indexF.Close(); err != nil {
		return err
	}

	if err = os.Rename(rs.indexF.Name(), rs.filePath); err != nil {
		rs.logger.Warn("[index] rename", "file", rs.indexF.Name(), "err", err)
		return err
	}
	rs.logger.Debug("[index] created", "file", rs.fileName)

	return nil
}

func (rs *RecSplit) flushExistenceFilter() error {
	if rs.dataStructureVersion == 0 && rs.enums && rs.keysAdded > 0 && rs.lessFalsePositives {
		defer rs.existenceFV0.Close()

		//Write len of array
		binary.BigEndian.PutUint64(rs.numBuf[:], rs.keysAdded)
		if _, err := rs.indexW.Write(rs.numBuf[:]); err != nil {
			return err
		}

		// flush bufio and rewind before io.Copy, but no reason to fsync the file - it temporary
		if err := rs.existenceWV0.Flush(); err != nil {
			return err
		}
		if _, err := rs.existenceFV0.Seek(0, io.SeekStart); err != nil {
			return err
		}
		r := getBufioReader(rs.existenceFV0)
		_, copyErr := io.CopyN(rs.indexW, r, int64(rs.keysAdded))
		putBufioReader(r)
		if copyErr != nil {
			return copyErr
		}
	}

	if rs.dataStructureVersion >= 1 && rs.keysAdded > 0 && rs.lessFalsePositives {
		_, err := rs.existenceFV1.BuildTo(rs.indexW)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *RecSplit) DisableFsync()    { rs.noFsync = true }
func (rs *RecSplit) Timings() Timings { return rs.timings }

func (rs *RecSplit) CollectTimings() {
	rs.timings.Enabled, rs.timings.AddStart = true, time.Now() // assume Adding data into compressor starting
}

// Fsync - other processes/goroutines must see only "fully-complete" (valid) files. No partial-writes.
// To achieve it: write to .tmp file then `rename` when file is ready.
// Machine may power-off right after `rename` - it means `fsync` must be before `rename`
func (rs *RecSplit) fsync() error {
	if rs.noFsync {
		return nil
	}
	if err := rs.indexF.Sync(); err != nil {
		rs.logger.Warn("couldn't fsync", "err", err, "file", rs.indexF.Name())
		return err
	}
	return nil
}

// Stats returns the size of golomb rice encoding and ellias fano encoding
func (rs *RecSplit) Stats() (int, int) {
	return len(rs.gr.Data()), len(rs.ef.Data())
}

// Collision returns true if there was a collision detected during mapping of keys
// into 64-bit values
// RecSplit needs to be reset, re-populated with keys, and rebuilt
func (rs *RecSplit) Collision() bool {
	return rs.collision
}

// Erigon doesn't create tons of bufio readers/writers, but it has tons of
// parallel small unit-tests which each create many small files and bufio
// readers/writers — pooling avoids the allocation pressure in that scenario.
var (
	bufioWriterPool = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, int(512*datasize.KB)) }}
	bufioReaderPool = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, int(512*datasize.KB)) }}
)

func getBufioWriter(w io.Writer) *bufio.Writer {
	bw := bufioWriterPool.Get().(*bufio.Writer)
	bw.Reset(w)
	return bw
}

// Reset(nil) before Put is required: without it the pool entry retains a
// reference to the underlying io.Writer/io.Reader, keeping it alive until the
// next GC cycle or until the entry is reused — whichever comes first.
func putBufioWriter(w *bufio.Writer) { w.Reset(nil); bufioWriterPool.Put(w) }

func getBufioReader(r io.Reader) *bufio.Reader {
	br := bufioReaderPool.Get().(*bufio.Reader)
	br.Reset(r)
	return br
}
func putBufioReader(r *bufio.Reader) { r.Reset(nil); bufioReaderPool.Put(r) }
