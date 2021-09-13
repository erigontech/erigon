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
	"sort"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/spaolacci/murmur3"
)

const RecSplitLogPrefix = "recsplit"

const MaxLeafSize = 24

type Bucket struct {
	keys   []string
	hasher hash.Hash64
}

func (b Bucket) Len() int {
	return len(b.keys)
}

func (b *Bucket) Less(i, j int) bool {
	b.hasher.Reset()
	b.hasher.Write([]byte(b.keys[i]))
	iPos := remap16(b.hasher.Sum64(), len(b.keys))
	b.hasher.Reset()
	b.hasher.Write([]byte(b.keys[j]))
	jPos := remap16(b.hasher.Sum64(), len(b.keys))
	return iPos < jPos
}

func (b *Bucket) Swap(i, j int) {
	b.keys[i], b.keys[j] = b.keys[j], b.keys[i]
}

// RecSplit is the implementation of Recursive Split algorithm for constructing perfect hash mapping, described in
// https://arxiv.org/pdf/1910.06416.pdf Emmanuel Esposito, Thomas Mueller Graf, and Sebastiano Vigna.
// Recsplit: Minimal perfect hashing via recursive splitting. In 2020 Proceedings of the Symposium on Algorithm Engineering and Experiments (ALENEX),
// pages 175−185. SIAM, 2020.
type RecSplit struct {
	keyExpectedCount   uint64      // Number of keys in the hash table
	keysAdded          uint64      // Number of keys actually added to the recSplit (to check the match with keyExpectedCount)
	bucketCount        uint64      // Number of buckets
	hasher             hash.Hash64 // Salted hash function to use for splitting into initial buckets
	collector          *etl.Collector
	built              bool     // Flag indicating that the hash function has been built and no more keys can be added
	currentBucketIdx   uint64   // Current bucket being accumulated
	currentBucket      []string // Keys in the current bucket accumulated before the recsplit is performed for that bucket
	builder            Builder
	bucketSizeAcc      []int // Bucket size accumulator
	bucketPosAcc       []int // Accumulator for position of every bucket in the encoding of the hash function
	leafSize           int   // Leaf size for recursive split algorithm
	primaryAggrBound   int   // The lower bound for primary key aggregation (computed from leafSize)
	secondaryAggrBound int   // The lower bound for secondary key aggregation (computed from leadSize)
	startSeed          []uint32
}

type RecSplitArgs struct {
	KeyCount   int
	BucketSize int
	Salt       uint32 // Hash seed (salt) for the hash function used for allocating the initial buckets - need to be generated randomly
	LeafSize   int
	TmpDir     string
	StartSeed  []uint32 // For each level of recursive split, the hash seed (salt) used for that level - need to be generated randomly and be large enough to accomodate all the levels
}

// NewRecSplit creates a new RecSplit instance with given number of keys and given bucket size
// Typical bucket size is 100 - 2000, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
// salt parameters is used to randomise the hash function construction, to ensure that different Erigon instances (nodes)
// are likely to use different hash function, to collision attacks are unlikely to slow down any meaningful number of nodes at the same time
func NewRecSplit(args RecSplitArgs) (*RecSplit, error) {
	bucketCount := (args.KeyCount + args.BucketSize - 1) / args.BucketSize
	rs := &RecSplit{keyExpectedCount: uint64(args.KeyCount), bucketCount: uint64(bucketCount)}
	rs.hasher = murmur3.New64WithSeed(args.Salt)
	rs.collector = etl.NewCollector(args.TmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	rs.currentBucket = make([]string, 0, args.BucketSize)
	rs.bucketSizeAcc = make([]int, 1, bucketCount+1)
	rs.bucketPosAcc = make([]int, 1, bucketCount+1)
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

// Builder builds up the representation of the hash function and is capable of then outputting
// the compact encoding of this representation.
type Builder struct {
}

func (b *Builder) appendUnaryAll(unary []uint32) {

}

func (b Builder) appendFixed(x uint32) {

}

// bits returns currrent number of bits in the compact encoding of the hash function representation
func (b Builder) bits() int {
	return 0
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
	var bucket [8]byte
	binary.BigEndian.PutUint64(bucket[:], remap(hash, rs.bucketCount))
	rs.keysAdded++
	return rs.collector.Collect(bucket[:], key)
}

func (rs RecSplit) recsplitCurrentBucket() error {
	// Extend rs.bucketSizeAcc to accomodate current bucket index + 1
	for len(rs.bucketSizeAcc) <= int(rs.currentBucketIdx)+1 {
		rs.bucketSizeAcc = append(rs.bucketSizeAcc, rs.bucketSizeAcc[len(rs.bucketSizeAcc)-1])
	}
	rs.bucketSizeAcc[int(rs.currentBucketIdx)+1] += len(rs.currentBucket)
	if len(rs.currentBucket) > 1 {
		// First we check that the keys are distinct by sorting them
		sort.Strings(rs.currentBucket)
		for i, key := range rs.currentBucket[1:] {
			if key == rs.currentBucket[i] {
				return fmt.Errorf("duplicate key %x", key)
			}
		}
		unary := rs.recsplit(0 /* level */, rs.currentBucket, nil /* unary */)
		rs.builder.appendUnaryAll(unary)
	}
	// Extend rs.bucketPosAcc to accomodate current bucket index + 1
	for len(rs.bucketPosAcc) <= int(rs.currentBucketIdx)+1 {
		rs.bucketPosAcc = append(rs.bucketPosAcc, rs.bucketPosAcc[len(rs.bucketPosAcc)-1])
	}
	rs.bucketPosAcc[int(rs.currentBucketIdx)+1] = rs.builder.bits()
	// clear for the next buckey
	rs.currentBucket = rs.currentBucket[:0]
	return nil
}

// recsplit applies recSplit algorithm to the given bucket
func (rs *RecSplit) recsplit(level int, bucket []string, unary []uint32) []uint32 {
	//fmt.Printf("recsplit(%d, %v)\n", level, bucket)
	// Pick initial salt for this level of recursive split
	salt := rs.startSeed[level]
	hasher := murmur3.New64WithSeed(salt)
	m := len(bucket)
	if m <= rs.leafSize {
		// No need to build aggregation levels - just find find bijection
		var mask uint32
		for {
			mask = 0
			var fail bool
			for i := 0; !fail && i < m; i++ {
				hasher.Reset()
				hasher.Write([]byte(bucket[i]))
				bit := uint32(1) << (remap16(hasher.Sum64(), m))
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
			hasher = murmur3.New64WithSeed(salt)
		}
		salt -= rs.startSeed[level]
		// TODO: Add salt to unary and to rs.builder
		rs.builder.appendFixed(salt)
	} else {
		var fanout, split int
		if m > rs.secondaryAggrBound {
			fanout = 2
			split = ((m/2 + rs.secondaryAggrBound - 1) / rs.secondaryAggrBound) * rs.secondaryAggrBound
		} else if m > rs.primaryAggrBound {
			// 2nd aggregation level
			fanout = (m + rs.primaryAggrBound - 1) / rs.primaryAggrBound
			split = rs.primaryAggrBound
		} else {
			// 1st aggregation level
			fanout = (m + rs.leafSize - 1) / rs.leafSize
			split = rs.leafSize
		}
		count := make([]int, fanout)
		for {
			var fail bool
			for i := 0; !fail && i < m; i++ {
				hasher.Reset()
				hasher.Write([]byte(bucket[i]))
				j := remap16(hasher.Sum64(), m) / split
				if count[j] == split {
					fail = true
				} else {
					count[j]++
				}
			}
			if !fail && count[fanout-1] == m-(fanout-1)*split {
				break
			}
			salt++
			hasher = murmur3.New64WithSeed(salt)
			for i := 0; i < fanout; i++ {
				count[i] = 0
			}
		}
		b := Bucket{keys: bucket, hasher: hasher}
		sort.Sort(&b)
		salt -= rs.startSeed[level]
		// TODO: Add salt to unary and to rs.builder
		rs.builder.appendFixed(salt)
		var i int
		for i = 0; i < m-split; i += split {
			unary = rs.recsplit(level+1, b.keys[i:i+split], unary)
		}
		if m-i > 1 {
			unary = rs.recsplit(level+1, b.keys[i:], unary)
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
	rs.currentBucket = append(rs.currentBucket, string(v))
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
	rs.built = true
	return nil
}
