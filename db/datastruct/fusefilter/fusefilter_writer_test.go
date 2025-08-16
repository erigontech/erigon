package fusefilter

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

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

func TestMultipleFilters(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()

	// Create multiple filters with different keys
	for i := 0; i < 3; i++ {
		filePath := filepath.Join(dir, fmt.Sprintf("filter_%d", i))
		baseKey := uint64(i * 1000)

		writer, err := NewWriter(filePath)
		require.NoError(err, "Failed to create writer %d", i)
		defer writer.Close()

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
		defer reader.Close()
		for j := 0; j < 100; j++ {
			key := baseKey + uint64(j)
			require.True(reader.ContainsHash(key), "Key %d not found in filter %d", key, i)
		}
		reader.Close()
	}
}
