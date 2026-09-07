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
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestRecSplit2(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	salt := uint32(1)
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(tmpDir, "index"),
		LeafSize:   8,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if err = rs.AddKey([]byte("first_key"), 0); err != nil {
		t.Error(err)
	}
	if err = rs.Build(t.Context()); err == nil {
		t.Errorf("test is expected to fail, too few keys added")
	}
	if err = rs.AddKey([]byte("second_key"), 0); err != nil {
		t.Error(err)
	}
	if err = rs.Build(t.Context()); err != nil {
		t.Error(err)
	}
	if err = rs.Build(t.Context()); err == nil {
		t.Errorf("test is expected to fail, hash gunction was built already")
	}
	if err = rs.AddKey([]byte("key_to_fail"), 0); err == nil {
		t.Errorf("test is expected to fail, hash function was built")
	}
}

func TestRecSplitDuplicate(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	salt := uint32(1)
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(tmpDir, "index"),
		LeafSize:   8,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if err := rs.AddKey([]byte("first_key"), 0); err != nil {
		t.Error(err)
	}
	if err := rs.AddKey([]byte("first_key"), 0); err != nil {
		t.Error(err)
	}
	if err := rs.Build(t.Context()); err == nil {
		t.Errorf("test is expected to fail, duplicate key")
	}
}

func TestRecSplitLeafSizeTooLarge(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	salt := uint32(1)
	_, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(tmpDir, "index"),
		LeafSize:   64,
	}, logger)
	if err == nil {
		t.Errorf("test is expected to fail, leaf size too large")
	}
}

func TestIndexLookup(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index")
	salt := uint32(1)
	test := func(t *testing.T, cfg RecSplitArgs) {
		t.Helper()
		rs, err := NewRecSplit(cfg, logger)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		for i := range 100 {
			if err = rs.AddKey(fmt.Appendf(nil, "key %d", i), uint64(i*17)); err != nil {
				t.Fatal(err)
			}
		}
		if err := rs.Build(t.Context()); err != nil {
			t.Fatal(err)
		}
		idx := MustOpen(indexFile)
		defer idx.Close()
		for i := range 100 {
			reader := NewIndexReader(idx)
			offset, ok := reader.Lookup(fmt.Appendf(nil, "key %d", i))
			assert.True(t, ok)
			if offset != uint64(i*17) {
				t.Errorf("expected offset: %d, looked up: %d", i*17, offset)
			}
		}
	}
	cfg := RecSplitArgs{
		KeyCount:   100,
		BucketSize: 10,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  indexFile,
		LeafSize:   8,

		Enums:              false,
		LessFalsePositives: true, //must not impact index when `Enums: false`
	}
	t.Run("v0", func(t *testing.T) {
		test(t, cfg)
	})
	t.Run("v1", func(t *testing.T) {
		cfg := cfg
		cfg.Version = 1
		test(t, cfg)
	})
	t.Run("v2", func(t *testing.T) {
		cfg := cfg
		cfg.Version = 2
		test(t, cfg)
	})
}

func TestFindBijection(t *testing.T) {
	// Build a small bucket of murmur3-hashed keys
	bucket := make([]uint64, 8)
	for i := range bucket {
		key := fmt.Appendf(nil, "bij_key_%d", i)
		_, lo := murmur3.Sum128WithSeed(key, 1)
		bucket[i] = lo
	}

	salt := findBijection(bucket, 0)

	// Verify: every key maps to a distinct position in [0, m)
	m := uint16(len(bucket))
	seen := make(map[uint16]bool)
	for _, key := range bucket {
		pos := remap16(remix(key+salt), m)
		assert.Less(t, pos, m)
		assert.False(t, seen[pos], "duplicate position %d", pos)
		seen[pos] = true
	}
	assert.Equal(t, int(m), len(seen))
}

func TestFindBijectionSmallBuckets(t *testing.T) {
	for size := 1; size <= 8; size++ {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			bucket := make([]uint64, size)
			for i := range bucket {
				key := fmt.Appendf(nil, "small_%d_%d", size, i)
				_, lo := murmur3.Sum128WithSeed(key, 1)
				bucket[i] = lo
			}

			salt := findBijection(bucket, 0)

			m := uint16(size)
			seen := make(map[uint16]bool)
			for _, key := range bucket {
				pos := remap16(remix(key+salt), m)
				assert.False(t, seen[pos], "duplicate position %d for size %d", pos, size)
				seen[pos] = true
			}
			assert.Equal(t, size, len(seen))
		})
	}
}

// findSplit masks partition indexes with maxFanout-1 instead of bounds-checking them.
// That is only correct while every reachable (leafSize, m) keeps fanout - and the
// largest index a key can land in - below maxFanout.
func TestSplitParamsFanoutBound(t *testing.T) {
	for leafSize := uint16(1); leafSize <= MaxLeafSize; leafSize++ {
		primaryAggrBound := leafSize * uint16(math.Max(2, math.Ceil(0.35*float64(leafSize)+1./2.)))
		secondaryAggrBound := primaryAggrBound * 2
		if leafSize >= 7 {
			secondaryAggrBound = primaryAggrBound * uint16(math.Ceil(0.21*float64(leafSize)+9./10.))
		}
		// m == MaxUint16 is excluded: splitParams computes m+1, which wraps to 0 and
		// yields unit 0. Unreachable for real bucket sizes and unrelated to the mask.
		for m := leafSize + 1; m < math.MaxUint16; m++ {
			fanout, unit := splitParams(m, leafSize, primaryAggrBound, secondaryAggrBound)
			if fanout > maxFanout {
				t.Fatalf("fanout %d > maxFanout %d at leafSize=%d m=%d", fanout, maxFanout, leafSize, m)
			}
			if j := (m - 1) / unit; j >= fanout {
				t.Fatalf("index %d >= fanout %d at leafSize=%d m=%d unit=%d", j, fanout, leafSize, m, unit)
			}
		}
	}
}

func TestFindSplit(t *testing.T) {
	const (
		leafSize           = uint16(8)
		primaryAggrBound   = uint16(32)
		secondaryAggrBound = uint16(96)
	)

	// Build a bucket at the primary aggregation level (32 keys)
	const m = primaryAggrBound
	bucket := make([]uint64, m)
	for i := range bucket {
		key := fmt.Appendf(nil, "split_key_%d", i)
		_, lo := murmur3.Sum128WithSeed(key, 1)
		bucket[i] = lo
	}

	fanout, unit := splitParams(m, leafSize, primaryAggrBound, secondaryAggrBound)

	salt := findSplit(bucket, 0, fanout, unit)

	// Verify: each partition gets exactly 'unit' keys (except possibly the last)
	partitionCounts := make([]uint16, fanout)
	for _, key := range bucket {
		j := remap16(remix(key+salt), m) / unit
		partitionCounts[j]++
	}
	for i := uint16(0); i < fanout-1; i++ {
		assert.Equal(t, unit, partitionCounts[i], "partition %d should have %d keys", i, unit)
	}
	// Last partition gets the remainder
	remainder := m - unit*(fanout-1)
	assert.Equal(t, remainder, partitionCounts[fanout-1], "last partition should have %d keys", remainder)
}

func TestFindSplitSecondaryAggr(t *testing.T) {
	const (
		leafSize           = uint16(8)
		primaryAggrBound   = uint16(32)
		secondaryAggrBound = uint16(96)
	)

	// Bucket at secondary aggregation level (64 keys, between 32 and 96)
	const m = uint16(64)
	bucket := make([]uint64, m)
	for i := range bucket {
		key := fmt.Appendf(nil, "sec_split_%d", i)
		_, lo := murmur3.Sum128WithSeed(key, 1)
		bucket[i] = lo
	}

	fanout, unit := splitParams(m, leafSize, primaryAggrBound, secondaryAggrBound)

	salt := findSplit(bucket, 0, fanout, unit)

	partitionCounts := make([]uint16, fanout)
	for _, key := range bucket {
		j := remap16(remix(key+salt), m) / unit
		partitionCounts[j]++
	}
	for i := uint16(0); i < fanout-1; i++ {
		assert.Equal(t, unit, partitionCounts[i], "partition %d should have %d keys", i, unit)
	}
	remainder := m - unit*(fanout-1)
	assert.Equal(t, remainder, partitionCounts[fanout-1])
}

func TestTwoLayerIndex(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index")
	salt := uint32(1)
	N := 2571
	test := func(t *testing.T, cfg RecSplitArgs) {
		t.Helper()
		rs, err := NewRecSplit(cfg, logger)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		for i := range N {
			if err = rs.AddKey(fmt.Appendf(nil, "key %d", i), uint64(i*17)); err != nil {
				t.Fatal(err)
			}
		}
		if err := rs.Build(t.Context()); err != nil {
			t.Fatal(err)
		}

		idx := MustOpen(indexFile)
		defer idx.Close()
		for i := range N {
			reader := NewIndexReader(idx)
			e, _ := reader.Lookup(fmt.Appendf(nil, "key %d", i))
			if e != uint64(i) {
				t.Errorf("expected enumeration: %d, lookup up: %d", i, e)
			}
			offset := idx.OrdinalLookup(e)
			if offset != uint64(i*17) {
				t.Errorf("expected offset: %d, looked up: %d", i*17, offset)
			}
		}
	}
	cfg := RecSplitArgs{
		KeyCount:           N,
		BucketSize:         10,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          indexFile,
		LeafSize:           8,
		Enums:              true,
		LessFalsePositives: true,
	}
	t.Run("v0", func(t *testing.T) {
		test(t, cfg)
	})
	t.Run("v1", func(t *testing.T) {
		cfg := cfg
		cfg.Version = 1
		test(t, cfg)
	})
	t.Run("v2", func(t *testing.T) {
		cfg := cfg
		cfg.Version = 2
		test(t, cfg)
	})
}

func TestIndexLookupParallel(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	salt := uint32(1)
	const N = 1000

	for _, workers := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			indexFile := filepath.Join(tmpDir, fmt.Sprintf("index_w%d", workers))
			rs, err := NewRecSplit(RecSplitArgs{
				KeyCount:   N,
				BucketSize: 10,
				Salt:       &salt,
				TmpDir:     tmpDir,
				IndexFile:  indexFile,
				LeafSize:   8,
				Workers:    workers,
				NoFsync:    true,
			}, logger)
			if err != nil {
				t.Fatal(err)
			}
			defer rs.Close()
			for i := range N {
				if err = rs.AddKey(fmt.Appendf(nil, "key %d", i), uint64(i*17)); err != nil {
					t.Fatal(err)
				}
			}
			if err := rs.Build(t.Context()); err != nil {
				t.Fatal(err)
			}
			idx := MustOpen(indexFile)
			defer idx.Close()
			for i := range N {
				reader := NewIndexReader(idx)
				offset, ok := reader.Lookup(fmt.Appendf(nil, "key %d", i))
				assert.True(t, ok)
				if offset != uint64(i*17) {
					t.Errorf("workers=%d key %d: expected offset %d, got %d", workers, i, i*17, offset)
				}
			}
		})
	}
}

// TestParallelMatchesSequential checks that the index file produced by the parallel
// build path is byte-for-byte identical to the one produced by the sequential path.
func TestParallelMatchesSequential(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	salt := uint32(42)
	const N = 10_000

	keys := make([][]byte, N)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%d", i)
	}

	fileChecksum := func(path string) []byte {
		t.Helper()
		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()
		h := sha256.New()
		_, err = io.Copy(h, f)
		require.NoError(t, err)
		return h.Sum(nil)
	}

	build := func(workers int, indexFile string) {
		t.Helper()
		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount:   N,
			BucketSize: 100,
			Salt:       &salt,
			TmpDir:     tmpDir,
			IndexFile:  indexFile,
			LeafSize:   8,
			NoFsync:    true,
			Workers:    workers,
		}, logger)
		require.NoError(t, err)
		defer rs.Close()
		for i, k := range keys {
			require.NoError(t, rs.AddKey(k, uint64(i*17)))
		}
		require.NoError(t, rs.Build(t.Context()))
	}

	seqFile := filepath.Join(tmpDir, "seq.idx")
	build(1, seqFile)
	seqSum := fileChecksum(seqFile)

	for _, workers := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			parFile := filepath.Join(tmpDir, fmt.Sprintf("par_w%d.idx", workers))
			build(workers, parFile)
			assert.Equal(t, seqSum, fileChecksum(parFile),
				"parallel (workers=%d) index file differs from sequential", workers)
		})
	}
}
