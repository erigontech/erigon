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
	"fmt"
	"hash"
	"math"
	"math/bits"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/spaolacci/murmur3"
)

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

// RecSplit is the implementation of Recursive Split algorithm for constructing perfect hash mapping, described in
// https://arxiv.org/pdf/1910.06416.pdf Emmanuel Esposito, Thomas Mueller Graf, and Sebastiano Vigna.
// Recsplit: Minimal perfect hashing via recursive splitting. In 2020 Proceedings of the Symposium on Algorithm Engineering and Experiments (ALENEX),
// pages 175−185. SIAM, 2020.
type RecSplit struct {
	bucketSize       int
	keyExpectedCount uint64      // Number of keys in the hash table
	keysAdded        uint64      // Number of keys actually added to the recSplit (to check the match with keyExpectedCount)
	bucketCount      uint64      // Number of buckets
	hasher           hash.Hash64 // Salted hash function to use for splitting into initial buckets
	collector        *etl.Collector
	built            bool       // Flag indicating that the hash function has been built and no more keys can be added
	currentBucketIdx uint64     // Current bucket being accumulated
	currentBucket    []uint64   // 64-bit fingerprints of keys in the current bucket accumulated before the recsplit is performed for that bucket
	gr               GolombRice // Helper object to encode the tree of hash function salts using Golomb-Rice code.
	// Helper object to encode the sequence of cumulative number of keys in the buckets
	// and the sequence of of cumulative bit offsets of buckets in the Golomb-Rice code.
	ef                 DoubleEliasFano
	bucketSizeAcc      []uint64 // Bucket size accumulator
	bucketPosAcc       []uint64 // Accumulator for position of every bucket in the encoding of the hash function
	leafSize           int      // Leaf size for recursive split algorithm
	primaryAggrBound   int      // The lower bound for primary key aggregation (computed from leafSize)
	secondaryAggrBound int      // The lower bound for secondary key aggregation (computed from leadSize)
	startSeed          []uint64
	golombRice         []uint32
	buffer             []uint64
	salt               uint32 // Murmur3 hash used for converting keys to 64-bit values and assigning to buckets
	collision          bool
	tmpDir             string
	trace              bool
}

type RecSplitArgs struct {
	KeyCount   int
	BucketSize int
	Salt       uint32 // Hash seed (salt) for the hash function used for allocating the initial buckets - need to be generated randomly
	LeafSize   int
	TmpDir     string
	StartSeed  []uint64 // For each level of recursive split, the hash seed (salt) used for that level - need to be generated randomly and be large enough to accomodate all the levels
}

// NewRecSplit creates a new RecSplit instance with given number of keys and given bucket size
// Typical bucket size is 100 - 2000, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
// salt parameters is used to randomise the hash function construction, to ensure that different Erigon instances (nodes)
// are likely to use different hash function, to collision attacks are unlikely to slow down any meaningful number of nodes at the same time
func NewRecSplit(args RecSplitArgs) (*RecSplit, error) {
	bucketCount := (args.KeyCount + args.BucketSize - 1) / args.BucketSize
	rs := &RecSplit{bucketSize: args.BucketSize, keyExpectedCount: uint64(args.KeyCount), bucketCount: uint64(bucketCount)}
	rs.salt = args.Salt
	rs.hasher = murmur3.New64WithSeed(rs.salt)
	rs.tmpDir = args.TmpDir
	rs.collector = etl.NewCollector(rs.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	rs.currentBucket = make([]uint64, 0, args.BucketSize)
	rs.bucketSizeAcc = make([]uint64, 1, bucketCount+1)
	rs.bucketPosAcc = make([]uint64, 1, bucketCount+1)
	if args.LeafSize > MaxLeafSize {
		return nil, fmt.Errorf("exceeded max leaf size %d: %d", MaxLeafSize, args.LeafSize)
	}
	rs.leafSize = args.LeafSize
	rs.primaryAggrBound = rs.leafSize * int(math.Max(2, math.Ceil(0.35*float64(rs.leafSize)+1./2.)))
	if rs.leafSize < 7 {
		rs.secondaryAggrBound = rs.primaryAggrBound * 2
	} else {
		rs.secondaryAggrBound = rs.primaryAggrBound * int(math.Ceil(0.21*float64(rs.leafSize)+9./10.))
	}
	rs.startSeed = args.StartSeed
	return rs, nil
}

func (rs *RecSplit) SetTrace(trace bool) {
	rs.trace = trace
}

// remap converts the number x which is assumed to be uniformly distributed over the range [0..2^64) to the number that is uniformly
// distributed over the range [0..n)
func remap(x uint64, n uint64) uint64 {
	hi, _ := bits.Mul64(x, n)
	return hi
}

const mask48 uint64 = (1 << 48) - 1

// remap converts the number x which is assumed to be uniformly distributed over the range [0..2^64) to the number that is uniformly
// distributed over the range [0..n), under assumption that n is less than 2^16
func remap16(x uint64, n int) int {
	return int(((x & mask48) * uint64(n)) >> 48)
}

// ResetNextSalt resets the RecSplit and uses the next salt value to try to avoid collisions
// when mapping keys to 64-bit values
func (rs *RecSplit) ResetNextSalt() {
	rs.collision = false
	rs.keysAdded = 0
	rs.salt++
	rs.hasher = murmur3.New64WithSeed(rs.salt)
	rs.collector = etl.NewCollector(rs.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	rs.currentBucket = rs.currentBucket[:0]
	rs.bucketSizeAcc = rs.bucketSizeAcc[:1] // First entry is always zero
	rs.bucketPosAcc = rs.bucketPosAcc[:0]   // First entry is always zero
}

func (rs *RecSplit) splitParams(m int) (fanout, unit int) {
	if m > rs.secondaryAggrBound { // High-level aggregation (fanout 2)
		unit = rs.secondaryAggrBound * (((m+1)/2 + rs.secondaryAggrBound - 1) / rs.secondaryAggrBound)
		fanout = 2
	} else if m > rs.primaryAggrBound { // Second-level aggregation
		unit = rs.primaryAggrBound
		fanout = (m + rs.primaryAggrBound - 1) / rs.primaryAggrBound
	} else { // First-level aggregation
		unit = rs.leafSize
		fanout = (m + rs.leafSize - 1) / rs.leafSize
	}
	return
}

func (rs *RecSplit) computeGolombRice(m int, table []uint32) {
	fanout, unit := rs.splitParams(m)
	k := make([]int, fanout)
	k[fanout-1] = m
	for i := 0; i < fanout-1; i++ {
		k[i] = unit
		k[fanout-1] -= k[i]
	}
	sqrt_prod := float64(1)
	for i := 0; i < fanout; i++ {
		sqrt_prod *= math.Sqrt(float64(k[i]))
	}
	p := math.Sqrt(float64(m)) / (math.Pow(2*math.Pi, (float64(fanout)-1.)/2.0) * sqrt_prod)
	golombRiceLength := uint32(math.Ceil(math.Log2(-math.Log((math.Sqrt(5)+1.0)/2.0) / math.Log1p(-p)))) // log2 Golomb modulus
	if golombRiceLength > 0x1F {
		panic("golombRiceLength > 0x1F")
	}
	table[m] = golombRiceLength << 27
	for i := 0; i < fanout; i++ {
		golombRiceLength += table[k[i]] & 0xFFFF
	}
	if golombRiceLength > 0xFFFF {
		panic("golombRiceLength > 0xFFFF")
	}
	table[m] |= golombRiceLength // Sum of Golomb-Rice codeslengths in the subtree, stored in the lower 16 bits
	nodes := uint32(1)
	for i := 0; i < fanout; i++ {
		nodes += (table[k[i]] >> 16) & 0x7FF
	}
	if rs.leafSize >= 3 && nodes > 0x7FF {
		panic("rs.leafSize >= 3 && nodes > 0x7FF")
	}
	table[m] |= nodes << 16
}

// golombParam returns the optimal Golomb parameter to use for encoding
// salt for the part of the hash function separating m elements. It is based on
// calculations with assumptions that we draw hash functions at random
func (rs *RecSplit) golombParam(m int) int {
	s := len(rs.golombRice)
	for m >= s {
		rs.golombRice = append(rs.golombRice, 0)
		// For the case where bucket is larger than planned
		if s == 0 {
			rs.golombRice[0] = (bijMemo[0] << 27) | bijMemo[0]
		} else if s <= rs.leafSize {
			rs.golombRice[s] = (bijMemo[s] << 27) | (uint32(1) << 16) | bijMemo[s]
		} else {
			rs.computeGolombRice(s, rs.golombRice)
		}
		s++
	}
	return int(rs.golombRice[m] >> 27)
}

// Add key to the RecSplit. There can be many more keys than what fits in RAM, and RecSplit
// spills data onto disk to accomodate that. The key gets copied by the collector, therefore
// the slice underlying key is not getting accessed by RecSplit after this invocation.
func (rs *RecSplit) AddKey(key []byte) error {
	if rs.built {
		return fmt.Errorf("cannot add keys after perfect hash function had been built")
	}
	rs.hasher.Reset()
	rs.hasher.Write(key) //nolint:errcheck
	hash := rs.hasher.Sum64()
	var bucketKey [16]byte
	binary.BigEndian.PutUint64(bucketKey[:], remap(hash, rs.bucketCount))
	binary.BigEndian.PutUint64(bucketKey[8:], hash)
	rs.keysAdded++
	return rs.collector.Collect(bucketKey[:], []byte{})
}

func (rs *RecSplit) recsplitCurrentBucket() error {
	// Extend rs.bucketSizeAcc to accomodate current bucket index + 1
	for len(rs.bucketSizeAcc) <= int(rs.currentBucketIdx)+1 {
		rs.bucketSizeAcc = append(rs.bucketSizeAcc, rs.bucketSizeAcc[len(rs.bucketSizeAcc)-1])
	}
	rs.bucketSizeAcc[int(rs.currentBucketIdx)+1] += uint64(len(rs.currentBucket))
	if len(rs.currentBucket) > 1 {
		for i, key := range rs.currentBucket[1:] {
			if key == rs.currentBucket[i] {
				rs.collision = true
				return fmt.Errorf("duplicate key %x", key)
			}
		}
		bitPos := rs.gr.bitCount
		if rs.buffer == nil {
			rs.buffer = make([]uint64, len(rs.currentBucket))
		} else {
			for len(rs.buffer) < len(rs.currentBucket) {
				rs.buffer = append(rs.buffer, 0)
			}
		}
		unary := rs.recsplit(0 /* level */, rs.currentBucket, nil /* unary */)
		rs.gr.appendUnaryAll(unary)
		if rs.trace {
			fmt.Printf("recsplitBucket(%d, %d, bitsize = %d)\n", rs.currentBucketIdx, len(rs.currentBucket), rs.gr.bitCount-bitPos)
		}
	}
	// Extend rs.bucketPosAcc to accomodate current bucket index + 1
	for len(rs.bucketPosAcc) <= int(rs.currentBucketIdx)+1 {
		rs.bucketPosAcc = append(rs.bucketPosAcc, rs.bucketPosAcc[len(rs.bucketPosAcc)-1])
	}
	rs.bucketPosAcc[int(rs.currentBucketIdx)+1] = uint64(rs.gr.Bits())
	// clear for the next buckey
	rs.currentBucket = rs.currentBucket[:0]
	return nil
}

// recsplit applies recSplit algorithm to the given bucket
func (rs *RecSplit) recsplit(level int, bucket []uint64, unary []uint64) []uint64 {
	if rs.trace {
		fmt.Printf("recsplit(%d, %d, %x)\n", level, len(bucket), bucket)
	}
	// Pick initial salt for this level of recursive split
	salt := rs.startSeed[level]
	m := len(bucket)
	if m <= rs.leafSize {
		// No need to build aggregation levels - just find find bijection
		var mask uint32
		for {
			mask = 0
			var fail bool
			for i := 0; !fail && i < m; i++ {
				bit := uint32(1) << remap16(remix(bucket[i]+salt), m)
				if mask&bit != 0 {
					fail = true
				} else {
					mask |= bit
				}
			}
			if !fail {
				break
			}
			salt++
		}
		salt -= rs.startSeed[level]
		log2golomb := rs.golombParam(m)
		if rs.trace {
			fmt.Printf("encode bij %d with log2golomn %d at p = %d\n", salt, log2golomb, rs.gr.bitCount)
		}
		rs.gr.appendFixed(salt, log2golomb)
		unary = append(unary, salt>>log2golomb)
	} else {
		fanout, unit := rs.splitParams(m)
		count := make([]int, fanout)
		for {
			var fail bool
			for i := 0; !fail && i < m; i++ {
				j := remap16(remix(bucket[i]+salt), m) / unit
				if j >= len(count) {
					fmt.Printf("rs.primaryAggrBound = %d, s.secondaryAggrBound = %d\n", rs.primaryAggrBound, rs.secondaryAggrBound)
					fmt.Printf("m = %d, len(count) = %d, unit = %d, remap16(remix(bucket[i]+salt), m) = %d\n", m, len(count), unit, remap16(remix(bucket[i]+salt), m))
				}
				if count[j] == unit {
					fail = true
				} else {
					count[j]++
				}
			}
			if !fail && count[fanout-1] == m-(fanout-1)*unit {
				break
			}
			salt++
			for i := 0; i < fanout; i++ {
				count[i] = 0
			}
		}
		for i, c := 0, 0; i < fanout; i++ {
			count[i] = c
			c += unit
		}
		for _, fingerprint := range bucket {
			j := remap16(remix(fingerprint+salt), m) / unit
			rs.buffer[count[j]] = fingerprint
			count[j]++
		}
		copy(bucket, rs.buffer)
		salt -= rs.startSeed[level]
		log2golomb := rs.golombParam(m)
		if rs.trace {
			fmt.Printf("encode fanout %d: %d with log2golomn %d at p = %d\n", fanout, salt, log2golomb, rs.gr.bitCount)
		}
		rs.gr.appendFixed(salt, log2golomb)
		unary = append(unary, salt>>log2golomb)
		var i int
		for i = 0; i < m-unit; i += unit {
			unary = rs.recsplit(level+1, bucket[i:i+unit], unary)
		}
		if m-i > 1 {
			unary = rs.recsplit(level+1, bucket[i:], unary)
		}
	}
	return unary
}

// loadFunc is required to satisfy the type etl.LoadFunc type, to use with collector.Load
func (rs *RecSplit) loadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
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
	return nil
}

// Build has to be called after all the keys have been added, and it initiates the process
// of building the perfect hash function.
func (rs *RecSplit) Build() error {
	if rs.built {
		return fmt.Errorf("already built")
	}
	if rs.keysAdded != rs.keyExpectedCount {
		return fmt.Errorf("expected keys %d, got %d", rs.keyExpectedCount, rs.keysAdded)
	}
	rs.currentBucketIdx = math.MaxUint64 // To make sure 0 bucket is detected
	defer rs.collector.Close(RecSplitLogPrefix)
	if err := rs.collector.Load(RecSplitLogPrefix, nil /* db */, "" /* toBucket */, rs.loadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	if len(rs.currentBucket) > 0 {
		if err := rs.recsplitCurrentBucket(); err != nil {
			return err
		}
	}
	rs.gr.appendFixed(1, 1) // Sentinel (avoids checking for parts of size 1)
	// Construct Elias Fano index
	rs.ef.Build(rs.bucketSizeAcc, rs.bucketPosAcc)
	rs.built = true
	return nil
}

func (rs *RecSplit) skipBits(m int) int {
	return int(rs.golombRice[m] & 0xffff)
}

func (rs *RecSplit) skipNodes(m int) int {
	return int(rs.golombRice[m]>>16) & 0x7FF
}

func (rs *RecSplit) Lookup(key []byte, trace bool) int {
	rs.hasher.Reset()
	rs.hasher.Write(key) //nolint:errcheck
	fingerprint := rs.hasher.Sum64()
	if trace {
		fmt.Printf("lookup key %x, fingerprint %x\n", key, fingerprint)
	}
	bucket := remap(fingerprint, rs.bucketCount)
	cumKeys, cumKeysNext, bitPos := rs.ef.Get3(bucket)
	m := int(cumKeysNext - cumKeys) // Number of keys in this bucket
	if trace {
		fmt.Printf("bucket: %d, m = %d, bitPos = %d, unaryOffset = %d\n", bucket, m, bitPos, rs.skipBits(m))
	}
	rs.gr.ReadReset(int(bitPos), rs.skipBits(m))
	var level int
	var p int
	for m > rs.secondaryAggrBound { // fanout = 2
		if trace {
			p = rs.gr.currFixedOffset
		}
		d := rs.gr.ReadNext(rs.golombParam(m))
		if trace {
			fmt.Printf("level %d, p = %d, d = %d golomb %d\n", level, p, d, rs.golombParam(m))
		}
		hmod := remap16(remix(fingerprint+rs.startSeed[level]+d), m)
		split := (((m+1)/2 + rs.secondaryAggrBound - 1) / rs.secondaryAggrBound) * rs.secondaryAggrBound
		if hmod < split {
			m = split
		} else {
			rs.gr.SkipSubtree(rs.skipNodes(split), rs.skipBits(split))
			m -= split
			cumKeys += uint64(split)
		}
		level++
	}
	if m > rs.primaryAggrBound {
		if trace {
			p = rs.gr.currFixedOffset
		}
		d := rs.gr.ReadNext(rs.golombParam(m))
		if trace {
			fmt.Printf("level %d, p = %d, d = %d golomb %d\n", level, p, d, rs.golombParam(m))
		}
		hmod := remap16(remix(fingerprint+rs.startSeed[level]+d), m)
		part := hmod / rs.primaryAggrBound
		if rs.primaryAggrBound < m-part*rs.primaryAggrBound {
			m = rs.primaryAggrBound
		} else {
			m = m - part*rs.primaryAggrBound
		}
		cumKeys += uint64(rs.primaryAggrBound * part)
		if part != 0 {
			rs.gr.SkipSubtree(rs.skipNodes(rs.primaryAggrBound)*part, rs.skipBits(rs.primaryAggrBound)*part)
		}
		level++
	}
	if m > rs.leafSize {
		if trace {
			p = rs.gr.currFixedOffset
		}
		d := rs.gr.ReadNext(rs.golombParam(m))
		if trace {
			fmt.Printf("level %d, p = %d, d = %d, golomb %d\n", level, p, d, rs.golombParam(m))
		}
		hmod := remap16(remix(fingerprint+rs.startSeed[level]+d), m)
		part := hmod / rs.leafSize
		if rs.leafSize < m-part*rs.leafSize {
			m = rs.leafSize
		} else {
			m = m - part*rs.leafSize
		}
		cumKeys += uint64(rs.leafSize * part)
		if part != 0 {
			rs.gr.SkipSubtree(part, rs.skipBits(rs.leafSize)*part)
		}
		level++
	}
	if trace {
		p = rs.gr.currFixedOffset
	}
	b := rs.gr.ReadNext(rs.golombParam(m))
	if trace {
		fmt.Printf("level %d, p = %d, b = %d, golomn = %d\n", level, p, b, rs.golombParam(m))
	}
	return int(cumKeys) + remap16(remix(fingerprint+rs.startSeed[level]+b), m)
}

// Stats returns the size of golomb rice encoding and ellias fano encoding
func (rs RecSplit) Stats() (int, int) {
	return len(rs.gr.Data()), len(rs.ef.Data())
}

// Collision returns true if there was a collision detected during mapping of keys
// into 64-bit values
// RecSplit needs to be reset, re-populated with keys, and rebuilt
func (rs RecSplit) Collision() bool {
	return rs.collision
}
