package seg

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSparseDecompressorRealFile tests the SparseDecompressor against a real
// .seg snapshot file from disk. This validates that sparse reading works with
// production-format compressed data.
//
// Requires: SPARSE_TEST_SEG environment variable set to a .seg file path.
// Example: SPARSE_TEST_SEG=/erigon/erigon-test-nodeA/snapshots/v1.0-000000-000500-headers.seg go test -run TestSparseDecompressorRealFile ./db/seg/...
func TestSparseDecompressorRealFile(t *testing.T) {
	segPath := os.Getenv("SPARSE_TEST_SEG")
	if segPath == "" {
		t.Skip("SPARSE_TEST_SEG not set — skipping real file test")
	}

	// Read the file into memory to simulate a torrent reader
	fileData, err := os.ReadFile(segPath)
	require.NoError(t, err, "reading seg file")
	fileSize := int64(len(fileData))
	t.Logf("Loaded %s: %d bytes", segPath, fileSize)

	// Open with standard decompressor for comparison
	d, err := NewDecompressor(segPath)
	require.NoError(t, err, "opening standard decompressor")
	defer d.Close()

	t.Logf("Standard decompressor: count=%d, wordsSize=%d", d.Count(), d.Size())

	// Collect some records and their offsets via standard getter
	var records [][]byte
	var offsets []uint64
	g := d.MakeGetter()
	offset := uint64(0)
	maxRecords := 100 // only check first 100 records
	for g.HasNext() && len(records) < maxRecords {
		offsets = append(offsets, offset)
		word, nextOffset := g.Next(nil)
		offset = nextOffset
		records = append(records, word)
	}
	t.Logf("Collected %d records for comparison", len(records))

	// Now read the same records via SparseDecompressor
	reader := bytes.NewReader(fileData)
	sd := NewSparseDecompressor(reader, fileSize, "test-real-file")
	defer sd.Close()

	require.Equal(t, d.Count(), sd.Count(), "word count mismatch")

	// Verify sequential access
	for i, expected := range records {
		got, err := sd.SparseGet(offsets[i])
		require.NoError(t, err, "SparseGet failed for record %d at offset %d", i, offsets[i])
		require.True(t, bytes.Equal(expected, got),
			"record %d mismatch at offset %d: expected %d bytes, got %d bytes",
			i, offsets[i], len(expected), len(got))
	}
	t.Logf("All %d records match between standard and sparse decompressor", len(records))

	// Verify random access (reverse order)
	for i := len(records) - 1; i >= 0; i-- {
		got, err := sd.SparseGet(offsets[i])
		require.NoError(t, err, "SparseGet (reverse) failed for record %d", i)
		require.True(t, bytes.Equal(records[i], got),
			"record %d mismatch (reverse access)", i)
	}
	t.Log("Reverse access test passed")
}
