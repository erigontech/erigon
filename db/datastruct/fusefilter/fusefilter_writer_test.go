package fusefilter

import (
	"bytes"
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

// TestHeaderRoundTrip encodes a filter to bytes and decodes it, then asserts
// every header field survives verbatim. This would have caught the bug where
// the reader decoded SegmentCount from offset 8 instead of offset 4, silently
// giving it the value of SegmentCountLength.
func TestHeaderRoundTrip(t *testing.T) {
	require := require.New(t)

	w, err := NewWriterOffHeap(filepath.Join(t.TempDir(), "hdr_rt"))
	require.NoError(err)
	defer w.Close()

	for i := uint64(0); i < 1000; i++ {
		require.NoError(w.AddHash(i))
	}

	original, err := w.build()
	require.NoError(err)

	var buf bytes.Buffer
	_, err = w.write(original, &buf)
	require.NoError(err)

	r, _, err := NewReaderOnBytes(buf.Bytes(), "test")
	require.NoError(err)

	got := r.inner
	require.Equal(original.SegmentCount, got.SegmentCount, "SegmentCount: reader used wrong header offset")
	require.Equal(original.SegmentCountLength, got.SegmentCountLength)
	require.Equal(original.Seed, got.Seed)
	require.Equal(original.SegmentLength, got.SegmentLength)
	require.Equal(original.SegmentLengthMask, got.SegmentLengthMask)
	require.Equal(len(original.Fingerprints), len(got.Fingerprints))
}

// TestDoubleSerializationRoundTrip re-serializes a filter loaded from bytes and
// decodes it a second time, checking that all fields are stable across both
// passes. A field decoded from the wrong offset would produce a different value
// on the second pass once the writer stores the wrong value back at a different
// offset.
func TestDoubleSerializationRoundTrip(t *testing.T) {
	require := require.New(t)

	w, err := NewWriterOffHeap(filepath.Join(t.TempDir(), "dbl_rt"))
	require.NoError(err)
	defer w.Close()

	for i := uint64(0); i < 500; i++ {
		require.NoError(w.AddHash(i))
	}

	var buf1 bytes.Buffer
	_, err = w.BuildTo(&buf1)
	require.NoError(err)

	r1, _, err := NewReaderOnBytes(buf1.Bytes(), "pass1")
	require.NoError(err)

	var buf2 bytes.Buffer
	_, err = w.write(r1.inner, &buf2)
	require.NoError(err)

	r2, _, err := NewReaderOnBytes(buf2.Bytes(), "pass2")
	require.NoError(err)

	require.Equal(r1.inner.SegmentCount, r2.inner.SegmentCount)
	require.Equal(r1.inner.SegmentCountLength, r2.inner.SegmentCountLength)
	require.Equal(r1.inner.Seed, r2.inner.Seed)
	require.Equal(r1.inner.SegmentLength, r2.inner.SegmentLength)
	require.Equal(r1.inner.SegmentLengthMask, r2.inner.SegmentLengthMask)
	require.Equal(len(r1.inner.Fingerprints), len(r2.inner.Fingerprints))
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
