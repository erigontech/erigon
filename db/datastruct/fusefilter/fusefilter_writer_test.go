package fusefilter

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// ── monolithic filter tests ───────────────────────────────────────────────────

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

// ── sharded filter tests ──────────────────────────────────────────────────────

func TestSharded_BasicRoundTrip(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_basic.tmp")
	testKeys := []uint64{1, 2, 3, 4, 5, 100, 1000, 10000, 100000}

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for _, k := range testKeys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()
	for _, k := range testKeys {
		require.True(r.ContainsHash(k), "key %d should be present", k)
	}
}

// Route all keys to a single shard: byte(k)==7.
func TestSharded_AdversarialSingleShard(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_adversarial.tmp")

	const keyCount = 100_000
	keys := make([]uint64, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = uint64(i)<<8 | 7
	}

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for _, k := range keys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()

	for _, k := range keys {
		require.True(r.ContainsHash(k))
	}

	// 255 shards are empty → any probe whose low byte differs must return false.
	for b := 0; b < 256; b++ {
		if b == 7 {
			continue
		}
		require.False(r.ContainsHash(uint64(b)<<8|uint64(b)), "empty shard %d unexpectedly matched", b)
	}
}

func TestSharded_UniformDistribution(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_uniform.tmp")

	rng := rand.New(rand.NewPCG(1, 2))
	const keyCount = 200_000
	keys := make(map[uint64]struct{}, keyCount)
	for len(keys) < keyCount {
		keys[rng.Uint64()] = struct{}{}
	}

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for k := range keys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()

	for k := range keys {
		require.True(r.ContainsHash(k))
	}

	// Probe with 200K fresh random keys that were not inserted. Expect ~0.3%
	// false-positive rate; allow generous slack for test stability.
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

// Exercise the per-shard partial-flush branch: a mix of shards with fill
// levels just below, at, and just above the 512-key page boundary.
func TestSharded_PageBoundary(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_pageboundary.tmp")

	// Plan per shard key count: 511 for shard 1, 512 for shard 2, 513 for shard 3,
	// 500 for shard 4, 1024 for shard 5, and a scattering for others.
	plan := map[byte]int{
		1: 511,
		2: 512,
		3: 513,
		4: 500,
		5: 1024,
		7: 100,
	}
	var allKeys []uint64
	for b, n := range plan {
		for i := 0; i < n; i++ {
			allKeys = append(allKeys, uint64(i)<<8|uint64(b))
		}
	}

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for _, k := range allKeys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()
	for _, k := range allKeys {
		require.True(r.ContainsHash(k))
	}
}

// Zero keys: all 256 shards empty; any probe returns false.
func TestSharded_EmptyAllShards(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_empty.tmp")

	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()

	for i := uint64(0); i < 10_000; i++ {
		require.False(r.ContainsHash(i))
	}
}

// Feed intentionally-duplicated hashes to one shard to exercise xorfilter's
// in-place pruneDuplicates pass against the RDWR mmap.
func TestSharded_MmapMutationOnDuplicates(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_dedup.tmp")

	const unique = 1000
	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for i := 0; i < unique; i++ {
		require.NoError(w.AddHash(uint64(i) << 8))
	}
	for i := 0; i < 100; i++ {
		require.NoError(w.AddHash(uint64(i) << 8)) // duplicates
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()
	for i := 0; i < unique; i++ {
		require.True(r.ContainsHash(uint64(i) << 8))
	}
}

// Force-in-mem detaches the reader from mmap; queries must still work.
func TestSharded_ForceInMem(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "sharded_forceinmem.tmp")

	rng := rand.New(rand.NewPCG(42, 43))
	const keyCount = 10_000
	keys := make([]uint64, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = rng.Uint64()
	}
	w, err := NewShardedWriter(filePath)
	require.NoError(err)
	for _, k := range keys {
		require.NoError(w.AddHash(k))
	}
	require.NoError(w.Build())
	w.Close()

	r, err := NewShardedReader(filePath)
	require.NoError(err)
	defer r.Close()
	r.ForceInMem()
	for _, k := range keys {
		require.True(r.ContainsHash(k))
	}
}
