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
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"

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
	if err = rs.Build(context.Background()); err == nil {
		t.Errorf("test is expected to fail, too few keys added")
	}
	if err = rs.AddKey([]byte("second_key"), 0); err != nil {
		t.Error(err)
	}
	if err = rs.Build(context.Background()); err != nil {
		t.Error(err)
	}
	if err = rs.Build(context.Background()); err == nil {
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
	if err := rs.Build(context.Background()); err == nil {
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
		for i := 0; i < 100; i++ {
			if err = rs.AddKey(fmt.Appendf(nil, "key %d", i), uint64(i*17)); err != nil {
				t.Fatal(err)
			}
		}
		if err := rs.Build(context.Background()); err != nil {
			t.Fatal(err)
		}
		idx := MustOpen(indexFile)
		defer idx.Close()
		for i := 0; i < 100; i++ {
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
	count := make([]uint16, secondaryAggrBound)

	salt := findSplit(bucket, 0, fanout, unit, count)

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
	count := make([]uint16, secondaryAggrBound)

	salt := findSplit(bucket, 0, fanout, unit, count)

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

func BenchmarkFindSplit(b *testing.B) {
	// Simulate realistic aggregation-level buckets.
	// With leafSize=8: primaryAggrBound=32, secondaryAggrBound=96.
	// A bucket of size 2000 first splits with fanout=2, unit=1056 (secondaryAggr level),
	// then recursively into primaryAggr and leaf levels.
	// The most common aggregation call is primaryAggr level: m~32, fanout=4, unit=8.
	const (
		leafSize           = uint16(8)
		primaryAggrBound   = uint16(32) // leafSize * max(2, ceil(0.35*8+0.5)) = 8*4
		secondaryAggrBound = uint16(96) // primaryAggrBound * ceil(0.21*8+0.9) = 32*3
	)
	// Generate buckets at the primary aggregation level (most frequent)
	const numBuckets = 1000
	const m = primaryAggrBound // 32 keys
	buckets := make([][m]uint64, numBuckets)
	for i := range buckets {
		for j := range buckets[i] {
			key := fmt.Appendf(nil, "split_%d_%d", i, j)
			hi, lo := murmur3.Sum128WithSeed(key, 1)
			_ = hi
			buckets[i][j] = lo
		}
	}
	fanout, unit := splitParams(m, leafSize, primaryAggrBound, secondaryAggrBound)
	salt := uint64(0x6453cec3f7376937) // startSeed[1]
	count := make([]uint16, secondaryAggrBound)

	b.ResetTimer()
	for b.Loop() {
		for i := range buckets {
			findSplit(buckets[i][:], salt, fanout, unit, count)
		}
	}
}

func BenchmarkFindBijection(b *testing.B) {
	// Simulate realistic leaf buckets: leafSize=8 keys with murmur3 hashes
	const leafSize = 8
	const numBuckets = 1000
	buckets := make([][leafSize]uint64, numBuckets)
	for i := range buckets {
		for j := range buckets[i] {
			key := fmt.Appendf(nil, "key_%d_%d", i, j)
			hi, lo := murmur3.Sum128WithSeed(key, 1)
			_ = hi
			buckets[i][j] = lo
		}
	}
	salt := uint64(0x106393c187cae2a) // startSeed[0]

	b.ResetTimer()
	for b.Loop() {
		for i := range buckets {
			findBijection(buckets[i][:], salt)
		}
	}
}

func BenchmarkBuild(b *testing.B) {
	b.ReportAllocs()
	logger := log.New()
	tmpDir := b.TempDir()
	salt := uint32(1)
	const KeysN = 1_000_000

	// Pre-allocate all keys outside the benchmark loop
	keys := make([][]byte, KeysN)
	for j := 0; j < KeysN; j++ {
		keys[j] = fmt.Appendf(nil, "key %d", j)
	}
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		b.StopTimer()
		indexFile := filepath.Join(tmpDir, fmt.Sprintf("index_%d", i))
		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount:   KeysN,
			BucketSize: 2000,
			Salt:       &salt,
			TmpDir:     tmpDir,
			IndexFile:  indexFile,
			LeafSize:   8,
			NoFsync:    true,
		}, logger)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < KeysN; j++ {
			if err = rs.AddKey(keys[j], uint64(j*17)); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		if err := rs.Build(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		rs.Close()
		b.StartTimer()
	}
}
func BenchmarkAddKeyAndBuild(b *testing.B) {
	b.ReportAllocs()
	logger := log.New()
	tmpDir := b.TempDir()
	salt := uint32(1)
	const KeysN = 1_000_000

	keys := make([][]byte, KeysN)
	for j := 0; j < KeysN; j++ {
		keys[j] = fmt.Appendf(nil, "key %d", j)
	}

	for _, enums := range []bool{false, true} {
		name := "noEnums"
		if enums {
			name = "enums"
		}
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				indexFile := filepath.Join(tmpDir, fmt.Sprintf("index_full_%s_%d", name, i))
				rs, err := NewRecSplit(RecSplitArgs{
					KeyCount:   KeysN,
					BucketSize: 2000,
					Salt:       &salt,
					TmpDir:     tmpDir,
					IndexFile:  indexFile,
					LeafSize:   8,
					Enums:      enums,
					NoFsync:    true,
				}, logger)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				for j := 0; j < KeysN; j++ {
					if err = rs.AddKey(keys[j], uint64(j*17)); err != nil {
						b.Fatal(err)
					}
				}
				if err := rs.Build(context.Background()); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				rs.Close()
			}
		})
	}
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
		for i := 0; i < N; i++ {
			if err = rs.AddKey(fmt.Appendf(nil, "key %d", i), uint64(i*17)); err != nil {
				t.Fatal(err)
			}
		}
		if err := rs.Build(context.Background()); err != nil {
			t.Fatal(err)
		}

		idx := MustOpen(indexFile)
		defer idx.Close()
		for i := 0; i < N; i++ {
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
}

// TestSequentialVsParallel verifies parallel and sequential processing produce identical output.
func TestSequentialVsParallel(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	salt := uint32(42)

	build := func(workers int) string {
		indexFile := filepath.Join(tmpDir, fmt.Sprintf("idx_%d", workers))
		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount: 100, BucketSize: 50, LeafSize: 8, Salt: &salt,
			TmpDir: tmpDir, IndexFile: indexFile, Workers: workers,
		}, logger)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			rs.AddKey(fmt.Appendf(nil, "k%d", i), uint64(i*17))
		}
		rs.Build(context.Background())
		rs.Close()

		data, _ := os.ReadFile(indexFile)
		return fmt.Sprintf("%x", md5.Sum(data))
	}

	h1 := build(1)
	h2 := build(2)
	h4 := build(4)
	assert.Equal(t, h1, h2)
	assert.Equal(t, h1, h4)
}
