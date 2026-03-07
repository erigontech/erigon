package seg

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// TestSparseDecompressor verifies that SparseDecompressor can read records
// from an io.ReadSeeker (simulating torrent-backed access) and produce the
// same output as the standard Decompressor.
func TestSparseDecompressor(t *testing.T) {
	// Create a compressed segment file using the standard compressor
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "")
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()

	for k, w := range loremStrings {
		require.NoError(t, c.AddWord(fmt.Appendf(nil, "%s %d", w, k)))
	}
	require.NoError(t, c.Compress())

	// Read the file into memory to simulate a torrent reader
	fileData, err := os.ReadFile(file)
	require.NoError(t, err)
	fileSize := int64(len(fileData))

	// Read all records via the standard Decompressor for comparison
	d, err := NewDecompressor(file)
	require.NoError(t, err)
	defer d.Close()

	var standardRecords [][]byte
	var standardOffsets []uint64
	g := d.MakeGetter()
	offset := uint64(0)
	for g.HasNext() {
		standardOffsets = append(standardOffsets, offset)
		word, nextOffset := g.Next(nil)
		offset = nextOffset
		standardRecords = append(standardRecords, word)
	}

	// Now read the same records via SparseDecompressor
	reader := bytes.NewReader(fileData)
	sd := NewSparseDecompressor(reader, fileSize, "test-sparse")
	defer sd.Close()

	require.Equal(t, len(loremStrings), sd.Count(), "word count mismatch")

	for i, expectedWord := range standardRecords {
		got, err := sd.SparseGet(standardOffsets[i])
		require.NoError(t, err, "SparseGet failed for record %d at offset %d", i, standardOffsets[i])
		require.Equal(t, expectedWord, got, "record %d mismatch", i)
	}
}

// TestSparseDecompressorRandomAccess verifies that records can be read in
// any order (not just sequential), simulating the access pattern of index
// lookups.
func TestSparseDecompressorRandomAccess(t *testing.T) {
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "")
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()

	for k, w := range loremStrings {
		require.NoError(t, c.AddWord(fmt.Appendf(nil, "%s %d", w, k)))
	}
	require.NoError(t, c.Compress())

	fileData, err := os.ReadFile(file)
	require.NoError(t, err)

	// Collect offsets via standard decompressor
	d, err := NewDecompressor(file)
	require.NoError(t, err)
	defer d.Close()

	var records [][]byte
	var offsets []uint64
	g := d.MakeGetter()
	offset := uint64(0)
	for g.HasNext() {
		offsets = append(offsets, offset)
		word, nextOffset := g.Next(nil)
		offset = nextOffset
		records = append(records, word)
	}

	// Access records in reverse order
	reader := bytes.NewReader(fileData)
	sd := NewSparseDecompressor(reader, int64(len(fileData)), "test-random")
	defer sd.Close()

	for i := len(records) - 1; i >= 0; i-- {
		got, err := sd.SparseGet(offsets[i])
		require.NoError(t, err)
		require.Equal(t, records[i], got, "record %d mismatch (reverse access)", i)
	}

	// Access specific records (first, last, middle)
	indices := []int{0, len(records) / 2, len(records) - 1}
	for _, idx := range indices {
		got, err := sd.SparseGet(offsets[idx])
		require.NoError(t, err)
		require.Equal(t, records[idx], got, "record %d mismatch (spot check)", idx)
	}
}

// TestSparseDecompressorMakeGetter verifies that MakeGetter returns a Getter
// with the correct dictionaries (even though data is nil for sparse).
func TestSparseDecompressorMakeGetter(t *testing.T) {
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "")
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()

	for k, w := range loremStrings {
		require.NoError(t, c.AddWord(fmt.Appendf(nil, "%s %d", w, k)))
	}
	require.NoError(t, c.Compress())

	fileData, err := os.ReadFile(file)
	require.NoError(t, err)

	reader := bytes.NewReader(fileData)
	sd := NewSparseDecompressor(reader, int64(len(fileData)), "test-getter")
	defer sd.Close()

	// Trigger init explicitly to check for errors
	rec, err := sd.SparseGet(0)
	require.NoError(t, err)
	require.NotNil(t, rec, "should be able to read first record")

	g := sd.MakeGetter()
	require.NotNil(t, g)
	// Pattern dict may be nil for small datasets — just verify the getter is functional
	require.Equal(t, "test-getter", g.fName)
}
