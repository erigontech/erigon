package fusefilter

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/FastFilter/xorfilter"
	"github.com/stretchr/testify/require"
)

func TestBasicFunctionality(t *testing.T) {
	require := require.New(t)

	// Setup
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test_filter")
	testKeys := []uint64{1, 2, 3, 4, 5, 100, 1000, 10000, 100000}

	// Create and build the filter
	writer, err := NewWriter(filePath)
	require.NoError(err, "Failed to create writer")
	defer writer.Close()

	// Add keys
	for _, key := range testKeys {
		err := writer.AddHash(key)
		require.NoError(err, "Failed to add hash")
	}

	// Build and close
	require.NoError(writer.Build(), "Failed to build filter")
	writer.Close()

	// Verify file exists
	_, err = os.Stat(filePath)
	require.NoError(err, "Filter file was not created")

	// Read the filter
	reader, err := NewReader(filePath)
	require.NoError(err, "Failed to create reader")
	defer reader.Close()

	// Verify all keys exist
	for _, key := range testKeys {
		require.True(reader.ContainsHash(key), "Key %d not found in filter", key)
	}
	require.False(reader.ContainsHash(11))
	require.False(reader.ContainsHash(12))
	require.False(reader.ContainsHash(999))
	require.False(reader.ContainsHash(512))
}

func TestLargeDataSet(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "test_filter_large")

	// Create writer
	writer, err := NewWriter(filePath)
	require.NoError(err, "Failed to create writer")
	defer writer.Close()

	// Add 10,000 keys (exceeding page size)
	keyCount := 100_000
	for i := 0; i < keyCount; i++ {
		require.NoError(writer.AddHash(uint64(i)), "Failed to add hash %d", i)
	}

	// Build and close
	require.NoError(writer.Build(), "Failed to build filter")
	writer.Close()

	// Read the filter
	reader, err := NewReader(filePath)
	require.NoError(err, "Failed to create reader")
	defer reader.Close()

	// Verify all keys exist
	for i := 0; i < keyCount; i++ {
		require.True(reader.ContainsHash(uint64(i)), "Key %d not found in filter", i)
	}

	// Test some keys that shouldn't exist (though false positives can occur)
	falsePositives := 0
	nonExistentKeys := []uint64{
		uint64(keyCount + 1000),
		uint64(keyCount + 10000),
		uint64(keyCount + 100000),
	}

	for _, key := range nonExistentKeys {
		if reader.ContainsHash(key) {
			falsePositives++
		}
	}

	t.Logf("False positive rate: %d/%d (%.2f%%)",
		falsePositives, len(nonExistentKeys),
		float64(falsePositives)/float64(len(nonExistentKeys))*100)
}

func TestPartialPage(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "test_partial_page")

	// Create writer
	writer, err := NewWriter(filePath)
	require.NoError(err, "Failed to create writer")
	defer writer.Close()

	// Add keys to partially fill the last page (not a multiple of 512)
	keyCount := 600
	keys := make([]uint64, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = uint64(i * 100)
		require.NoError(writer.AddHash(keys[i]), "Failed to add hash %d", i)
	}

	// Build and close
	require.NoError(writer.Build(), "Failed to build filter")
	writer.Close()

	// Read the filter
	reader, err := NewReader(filePath)
	require.NoError(err, "Failed to create reader")
	defer reader.Close()

	// Verify all keys exist
	for _, key := range keys {
		require.True(reader.ContainsHash(key), "Key %d not found in filter", key)
	}
}

func TestCastFunctions(t *testing.T) {
	require := require.New(t)

	// Test castToBytes
	u64s := []uint64{1, 2, 3, 4, 5}
	bytes := castToBytes(u64s)
	require.Equal(len(u64s)*8, len(bytes), "Incorrect byte length after conversion")

	// Test castToArrU64
	u64sRoundTrip := castToArrU64(bytes)
	require.Equal(len(u64s), len(u64sRoundTrip), "Incorrect uint64 length after round trip")

	// Verify values are preserved
	for i, v := range u64s {
		require.Equal(v, u64sRoundTrip[i], "Value mismatch at index %d", i)
	}

	// Test with empty slices
	require.Nil(castToBytes(nil), "castToBytes should return nil for empty input")
	require.Nil(castToArrU64(nil), "castToArrU64 should return nil for empty input")

	// Test panic condition with non-multiple of 8
	require.Panics(func() {
		castToArrU64([]byte{1, 2, 3}) // Not a multiple of 8
	}, "Should panic when byte length is not a multiple of 8")
}

func TestEmptyBuild(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "test_empty")

	// Create writer
	writer, err := NewWriter(filePath)
	require.NoError(err, "Failed to create writer")
	defer writer.Close()

	// We need at least one key for the filter to build
	require.NoError(writer.AddHash(123), "Failed to add hash")

	// Build and close
	require.NoError(writer.Build(), "Failed to build filter")
	writer.Close()

	// Read the filter
	reader, err := NewReader(filePath)
	require.NoError(err, "Failed to create reader")
	defer reader.Close()

	// Verify the key exists
	require.True(reader.ContainsHash(123), "Key 123 not found in filter")
}

func TestWriterClose(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "test_close")

	// Create writer
	writer, err := NewWriter(filePath)
	require.NoError(err, "Failed to create writer")
	defer writer.Close()

	// Close without building
	writer.Close()

	// Close again (should be safe)
	writer.Close()
}

func TestWriterShardedBasic(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	w, err := NewWriterSharded(filepath.Join(dir, "sharded"))
	require.NoError(err)
	defer w.Close()

	keys := []uint64{1, 2, 3, 0xFF << 56, 0xAB<<56 | 12345, 0x01<<56 | 99}
	for _, k := range keys {
		require.NoError(w.AddHash(k))
	}

	var buf bytes.Buffer
	_, err = w.BuildTo(&buf)
	require.NoError(err)

	r, consumed, err := NewReaderShardedOnBytes(buf.Bytes(), "test")
	require.NoError(err)
	require.Equal(buf.Len(), consumed)

	for _, k := range keys {
		require.True(r.ContainsHash(k), "key %d missing", k)
	}
	// Keys in different shards are absent.
	require.False(r.ContainsHash(0xBB<<56 | 99999))
}

func TestWriterShardedLarge(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	w, err := NewWriterSharded(filepath.Join(dir, "sharded_large"))
	require.NoError(err)
	defer w.Close()

	const n = 100_000
	for i := range n {
		require.NoError(w.AddHash(uint64(i)))
	}

	var buf bytes.Buffer
	_, err = w.BuildTo(&buf)
	require.NoError(err)

	r, _, err := NewReaderShardedOnBytes(buf.Bytes(), "test")
	require.NoError(err)

	for i := range n {
		require.True(r.ContainsHash(uint64(i)), "key %d missing", i)
	}
}

func TestWriterShardedForceInMem(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	w, err := NewWriterSharded(filepath.Join(dir, "sharded_mem"))
	require.NoError(err)
	defer w.Close()
	require.NoError(w.AddHash(42))

	var buf bytes.Buffer
	_, err = w.BuildTo(&buf)
	require.NoError(err)

	r, _, err := NewReaderShardedOnBytes(buf.Bytes(), "test")
	require.NoError(err)

	sz := r.ForceInMem()
	require.Greater(uint64(sz), uint64(0))
	require.True(r.ContainsHash(42))
}

func TestWriterShardedSingleShard(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	w, err := NewWriterSharded(filepath.Join(dir, "single_shard"))
	require.NoError(err)
	defer w.Close()

	// All keys have high byte = 0x42 — only shard 0x42 is populated
	const shard = uint64(0x42)
	keys := []uint64{shard<<56 | 1, shard<<56 | 2, shard<<56 | 999, shard<<56 | 0xFFFF}
	for _, k := range keys {
		require.NoError(w.AddHash(k))
	}

	var buf bytes.Buffer
	_, err = w.BuildTo(&buf)
	require.NoError(err)

	r, consumed, err := NewReaderShardedOnBytes(buf.Bytes(), "test")
	require.NoError(err)
	require.Equal(buf.Len(), consumed)

	for _, k := range keys {
		require.True(r.ContainsHash(k), "key %#x missing", k)
	}
	// All other shards absent
	require.False(r.ContainsHash(uint64(0x00)<<56 | 1))
	require.False(r.ContainsHash(uint64(0xFF)<<56 | 1))
}

func TestWriterShardedTruncated(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	w, err := NewWriterSharded(filepath.Join(dir, "trunc"))
	require.NoError(err)
	defer w.Close()
	for i := range 10 {
		require.NoError(w.AddHash(uint64(i)))
	}

	var buf bytes.Buffer
	_, err = w.BuildTo(&buf)
	require.NoError(err)
	full := buf.Bytes()

	_, _, err = NewReaderShardedOnBytes(full[:2], "trunc") // shorter than 4-byte header
	require.Error(err)

	_, _, err = NewReaderShardedOnBytes(full[:5], "trunc") // header ok but truncated shard table
	require.Error(err)
}

func TestWriterShardedSegmentCountRoundTrip(t *testing.T) {
	// Regression: SegmentCount and SegmentCountLength were aliased (both read from offset 8).
	require := require.New(t)
	dir := t.TempDir()

	filePath := filepath.Join(dir, "seg_count")
	writer, err := NewWriter(filePath)
	require.NoError(err)
	defer writer.Close()

	const n = 1000
	for i := range n {
		require.NoError(writer.AddHash(uint64(i)))
	}
	require.NoError(writer.Build())
	writer.Close()

	f, err := os.Open(filePath)
	require.NoError(err)
	defer f.Close()
	raw, err := io.ReadAll(f)
	require.NoError(err)

	r, _, err := NewReaderOnBytes(raw, "seg_count")
	require.NoError(err)
	require.NotZero(r.inner.SegmentCount)
	require.NotZero(r.inner.SegmentCountLength)
	require.NotZero(r.inner.SegmentLength)
}

// 200K random keys, then probe 200K fresh randoms. Expects FP rate < 1%.
func TestWriterShardedUniformDistribution(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_uniform")

	rng := rand.New(rand.NewPCG(1, 2))
	const keyCount = 200_000
	keys := make(map[uint64]struct{}, keyCount)
	for len(keys) < keyCount {
		keys[rng.Uint64()] = struct{}{}
	}

	w, err := NewWriterSharded(filePath)
	require.NoError(err)
	w.DisableFsync()
	for k := range keys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewReaderSharded(filePath)
	require.NoError(err)
	defer r.Close()

	for k := range keys {
		require.True(r.ContainsHash(k))
	}

	falsePositives := 0
	const probes = 200_000
	for i := 0; i < probes; i++ {
		var k uint64
		for {
			k = rng.Uint64()
			if _, ok := keys[k]; !ok {
				break
			}
		}
		if r.ContainsHash(k) {
			falsePositives++
		}
	}
	fpRate := float64(falsePositives) / float64(probes)
	t.Logf("false positive rate: %d/%d = %.4f%%", falsePositives, probes, fpRate*100)
	require.Less(fpRate, 0.01, "false positive rate unexpectedly high")
}

// Per-shard fill counts at 511/512/513 stress the page-flush boundary in
// WriterOffHeap (page = 512 uint64s).
func TestWriterShardedPageBoundary(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_pageboundary")

	// v2 shards by high byte: pack shard id into the high byte.
	plan := map[uint64]int{
		0x01: 511,
		0x02: 512,
		0x03: 513,
		0x04: 500,
		0x05: 1024,
		0x07: 100,
	}
	var allKeys []uint64
	for shard, n := range plan {
		for i := 0; i < n; i++ {
			allKeys = append(allKeys, shard<<56|uint64(i))
		}
	}

	w, err := NewWriterSharded(filePath)
	require.NoError(err)
	w.DisableFsync()
	for _, k := range allKeys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewReaderSharded(filePath)
	require.NoError(err)
	defer r.Close()
	for _, k := range allKeys {
		require.True(r.ContainsHash(k))
	}
}

// Feed duplicated hashes to one shard to exercise xorfilter's in-place
// pruneDuplicates pass against the RDWR mmap of the temp file.
func TestWriterShardedMmapMutationOnDuplicates(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_dedup")

	const unique = 1000
	w, err := NewWriterSharded(filePath)
	require.NoError(err)
	w.DisableFsync()
	for i := 0; i < unique; i++ {
		require.NoError(w.AddHash(uint64(i)))
	}
	for i := 0; i < 100; i++ {
		require.NoError(w.AddHash(uint64(i))) // duplicates
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewReaderSharded(filePath)
	require.NoError(err)
	defer r.Close()
	for i := 0; i < unique; i++ {
		require.True(r.ContainsHash(uint64(i)))
	}
}

// requireFilterEqual asserts every header field and fingerprint count match between two filters.
func requireFilterEqual(t *testing.T, expected, got *xorfilter.BinaryFuse[uint8]) {
	t.Helper()
	require.Equal(t, expected.SegmentCount, got.SegmentCount)
	require.Equal(t, expected.SegmentCountLength, got.SegmentCountLength)
	require.Equal(t, expected.Seed, got.Seed)
	require.Equal(t, expected.SegmentLength, got.SegmentLength)
	require.Equal(t, expected.SegmentLengthMask, got.SegmentLengthMask)
	require.Equal(t, len(expected.Fingerprints), len(got.Fingerprints))
}

// Regression: reader previously decoded SegmentCount from offset 8 instead of 4, silently reading SegmentCountLength.
func TestHeaderRoundTrip(t *testing.T) {
	require := require.New(t)

	w, err := NewWriterOffHeap(filepath.Join(t.TempDir(), "hdr_rt"))
	require.NoError(err)
	defer w.Close()

	for i := uint64(0); i < 50; i++ {
		require.NoError(w.AddHash(i))
	}

	original, err := w.build()
	require.NoError(err)

	var buf bytes.Buffer
	_, err = writeFilter(w.features, original, &buf)
	require.NoError(err)

	r, _, err := NewReaderOnBytes(buf.Bytes(), "test")
	require.NoError(err)

	requireFilterEqual(t, original, r.inner)
}

func TestMultipleFilters(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()

	// Create multiple filters with different keys
	for i := 0; i < 3; i++ {
		filePath := filepath.Join(dir, fmt.Sprintf("filter_%d", i))
		baseKey := uint64(i * 1000)

		writer, err := NewWriter(filePath)
		require.NoError(err, "Failed to create writer %d", i)

		// Add some keys
		for j := 0; j < 100; j++ {
			key := baseKey + uint64(j)
			require.NoError(writer.AddHash(key), "Failed to add hash %d to filter %d", key, i)
		}

		require.NoError(writer.Build(), "Failed to build filter %d", i)
		writer.Close()

		// Read back and verify
		reader, err := NewReader(filePath)
		require.NoError(err, "Failed to create reader for filter %d", i)
		for j := 0; j < 100; j++ {
			key := baseKey + uint64(j)
			require.True(reader.ContainsHash(key), "Key %d not found in filter %d", key, i)
		}
		reader.Close()
	}
}
