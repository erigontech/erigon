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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/stretchr/testify/assert"
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

// BenchmarkRecSplitBuild_10K benchmarks building RecSplit index with 10,000 keys
func BenchmarkRecSplitBuild_10K(b *testing.B) {
	benchmarkRecSplitBuild(b, 10000, DefaultBucketSize, DefaultLeafSize)
}

// BenchmarkRecSplitBuild_100K benchmarks building RecSplit index with 100,000 keys
func BenchmarkRecSplitBuild_100K(b *testing.B) {
	benchmarkRecSplitBuild(b, 100000, DefaultBucketSize, DefaultLeafSize)
}

// BenchmarkRecSplitBuild_1M benchmarks building RecSplit index with 1,000,000 keys
func BenchmarkRecSplitBuild_1M(b *testing.B) {
	benchmarkRecSplitBuild(b, 1000000, DefaultBucketSize, DefaultLeafSize)
}

// BenchmarkRecSplitBuild_BucketSize100 benchmarks with bucket size 100
func BenchmarkRecSplitBuild_BucketSize100(b *testing.B) {
	benchmarkRecSplitBuild(b, 100000, 100, DefaultLeafSize)
}

// BenchmarkRecSplitBuild_BucketSize500 benchmarks with bucket size 500
func BenchmarkRecSplitBuild_BucketSize500(b *testing.B) {
	benchmarkRecSplitBuild(b, 100000, 500, DefaultLeafSize)
}

// BenchmarkRecSplitBuild_BucketSize1000 benchmarks with bucket size 1000
func BenchmarkRecSplitBuild_BucketSize1000(b *testing.B) {
	benchmarkRecSplitBuild(b, 100000, 1000, DefaultLeafSize)
}

// BenchmarkRecSplitBuild_LeafSize8 benchmarks with leaf size 8
func BenchmarkRecSplitBuild_LeafSize8(b *testing.B) {
	benchmarkRecSplitBuild(b, 100000, DefaultBucketSize, 8)
}

// BenchmarkRecSplitBuild_LeafSize12 benchmarks with leaf size 12
func BenchmarkRecSplitBuild_LeafSize12(b *testing.B) {
	benchmarkRecSplitBuild(b, 100000, DefaultBucketSize, 12)
}

// BenchmarkRecSplitBuild_LeafSize16 benchmarks with leaf size 16
func BenchmarkRecSplitBuild_LeafSize16(b *testing.B) {
	benchmarkRecSplitBuild(b, 100000, DefaultBucketSize, 16)
}

func benchmarkRecSplitBuild(b *testing.B, keyCount, bucketSize int, leafSize uint16) {
	logger := log.New()
	salt := uint32(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir := b.TempDir()
		indexFile := filepath.Join(tmpDir, "index")

		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount:   keyCount,
			BucketSize: bucketSize,
			Salt:       &salt,
			TmpDir:     tmpDir,
			IndexFile:  indexFile,
			LeafSize:   leafSize,
			NoFsync:    true, // Disable fsync for benchmarking
		}, logger)
		if err != nil {
			b.Fatal(err)
		}

		// Add keys
		for j := 0; j < keyCount; j++ {
			if err = rs.AddKey(fmt.Appendf(nil, "benchmark_key_%d", j), uint64(j)); err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		// Benchmark the Build operation
		if err := rs.Build(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		rs.Close()
	}

	// Report keys per second
	b.ReportMetric(float64(keyCount)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
}

// TestParallelBuild tests that parallel build produces correct results
func TestParallelBuild(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	salt := uint32(1)
	keyCount := 1000

	// Build sequential index
	indexFileSeq := filepath.Join(tmpDir, "index_seq")
	rsSeq, err := NewRecSplit(RecSplitArgs{
		KeyCount:      keyCount,
		BucketSize:    100,
		Salt:          &salt,
		TmpDir:        tmpDir,
		IndexFile:     indexFileSeq,
		LeafSize:      8,
		ParallelBuild: false,
		NoFsync:       true,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < keyCount; i++ {
		if err = rsSeq.AddKey(fmt.Appendf(nil, "test_key_%d", i), uint64(i*17)); err != nil {
			t.Fatal(err)
		}
	}

	if err := rsSeq.Build(context.Background()); err != nil {
		t.Fatal(err)
	}
	rsSeq.Close()

	// Build parallel index
	indexFilePar := filepath.Join(tmpDir, "index_par")
	rsPar, err := NewRecSplit(RecSplitArgs{
		KeyCount:      keyCount,
		BucketSize:    100,
		Salt:          &salt,
		TmpDir:        tmpDir,
		IndexFile:     indexFilePar,
		LeafSize:      8,
		ParallelBuild: true,
		Workers:       4,
		NoFsync:       true,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < keyCount; i++ {
		if err = rsPar.AddKey(fmt.Appendf(nil, "test_key_%d", i), uint64(i*17)); err != nil {
			t.Fatal(err)
		}
	}

	if err := rsPar.Build(context.Background()); err != nil {
		t.Fatal(err)
	}
	rsPar.Close()

	// Open both indexes and verify lookups match
	idxSeq := MustOpen(indexFileSeq)
	defer idxSeq.Close()
	idxPar := MustOpen(indexFilePar)
	defer idxPar.Close()

	for i := 0; i < keyCount; i++ {
		key := fmt.Appendf(nil, "test_key_%d", i)
		readerSeq := NewIndexReader(idxSeq)
		offsetSeq, okSeq := readerSeq.Lookup(key)

		readerPar := NewIndexReader(idxPar)
		offsetPar, okPar := readerPar.Lookup(key)

		if okSeq != okPar {
			t.Errorf("key %d: sequential found=%v, parallel found=%v", i, okSeq, okPar)
		}
		if offsetSeq != offsetPar {
			t.Errorf("key %d: sequential offset=%d, parallel offset=%d", i, offsetSeq, offsetPar)
		}
		if offsetSeq != uint64(i*17) {
			t.Errorf("key %d: expected offset %d, got %d", i, i*17, offsetSeq)
		}
	}
}

// BenchmarkRecSplitBuild_Parallel_100K benchmarks parallel build with 100K keys
func BenchmarkRecSplitBuild_Parallel_100K(b *testing.B) {
	benchmarkRecSplitBuildParallel(b, 100000, DefaultBucketSize, DefaultLeafSize, 0)
}

// BenchmarkRecSplitBuild_Parallel_1M benchmarks parallel build with 1M keys
func BenchmarkRecSplitBuild_Parallel_1M(b *testing.B) {
	benchmarkRecSplitBuildParallel(b, 1000000, DefaultBucketSize, DefaultLeafSize, 0)
}

// BenchmarkRecSplitBuild_Parallel_Workers2 benchmarks parallel build with 2 workers
func BenchmarkRecSplitBuild_Parallel_Workers2(b *testing.B) {
	benchmarkRecSplitBuildParallel(b, 100000, DefaultBucketSize, DefaultLeafSize, 2)
}

// BenchmarkRecSplitBuild_Parallel_Workers4 benchmarks parallel build with 4 workers
func BenchmarkRecSplitBuild_Parallel_Workers4(b *testing.B) {
	benchmarkRecSplitBuildParallel(b, 100000, DefaultBucketSize, DefaultLeafSize, 4)
}

// BenchmarkRecSplitBuild_Parallel_Workers8 benchmarks parallel build with 8 workers
func BenchmarkRecSplitBuild_Parallel_Workers8(b *testing.B) {
	benchmarkRecSplitBuildParallel(b, 100000, DefaultBucketSize, DefaultLeafSize, 8)
}

func benchmarkRecSplitBuildParallel(b *testing.B, keyCount, bucketSize int, leafSize uint16, workers int) {
	logger := log.New()
	salt := uint32(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir := b.TempDir()
		indexFile := filepath.Join(tmpDir, "index")

		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount:      keyCount,
			BucketSize:    bucketSize,
			Salt:          &salt,
			TmpDir:        tmpDir,
			IndexFile:     indexFile,
			LeafSize:      leafSize,
			NoFsync:       true,
			ParallelBuild: true,
			Workers:       workers,
		}, logger)
		if err != nil {
			b.Fatal(err)
		}

		// Add keys
		for j := 0; j < keyCount; j++ {
			if err = rs.AddKey(fmt.Appendf(nil, "benchmark_key_%d", j), uint64(j)); err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		// Benchmark the Build operation
		if err := rs.Build(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		rs.Close()
	}

	// Report keys per second
	b.ReportMetric(float64(keyCount)/b.Elapsed().Seconds()*float64(b.N), "keys/sec")
}
