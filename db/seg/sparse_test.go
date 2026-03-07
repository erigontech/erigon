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

// TestSparseDecompressor verifies that a sparse Decompressor (created via
// NewDecompressorFromReader) can read records from an io.ReadSeeker and
// produce the same output as the standard Decompressor.
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

	// Now read the same records via sparse Decompressor
	reader := bytes.NewReader(fileData)
	sd, err := NewDecompressorFromReader(reader, fileSize, "test-sparse")
	require.NoError(t, err)
	defer sd.Close()

	require.Equal(t, len(loremStrings), sd.Count(), "word count mismatch")
	require.True(t, sd.IsSparse(), "should be sparse")

	for i, expectedWord := range standardRecords {
		got, err := sd.GetRecord(standardOffsets[i])
		require.NoError(t, err, "GetRecord failed for record %d at offset %d", i, standardOffsets[i])
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

	// Access records in reverse order via sparse Decompressor
	reader := bytes.NewReader(fileData)
	sd, err := NewDecompressorFromReader(reader, int64(len(fileData)), "test-random")
	require.NoError(t, err)
	defer sd.Close()

	for i := len(records) - 1; i >= 0; i-- {
		got, err := sd.GetRecord(offsets[i])
		require.NoError(t, err)
		require.Equal(t, records[i], got, "record %d mismatch (reverse access)", i)
	}

	// Access specific records (first, last, middle)
	indices := []int{0, len(records) / 2, len(records) - 1}
	for _, idx := range indices {
		got, err := sd.GetRecord(offsets[idx])
		require.NoError(t, err)
		require.Equal(t, records[idx], got, "record %d mismatch (spot check)", idx)
	}
}

// TestSparseDecompressorMakeGetter verifies that MakeGetter + Reset + Next
// works transparently for sparse decompressors — the same path used by
// domain/history code.
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

	// Get ground truth from standard decompressor
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

	// Create sparse decompressor and test MakeGetter path
	reader := bytes.NewReader(fileData)
	sd, err := NewDecompressorFromReader(reader, int64(len(fileData)), "test-getter")
	require.NoError(t, err)
	defer sd.Close()

	require.True(t, sd.IsSparse())

	sg := sd.MakeGetter()
	require.NotNil(t, sg)
	require.Equal(t, "test-getter", sg.FileName())

	// Verify Reset + HasNext + Next works for each record
	for i, expected := range records {
		sg.Reset(offsets[i])
		require.True(t, sg.HasNext(), "HasNext should be true for record %d", i)
		got, _ := sg.Next(nil)
		require.Equal(t, expected, got, "record %d mismatch via MakeGetter path", i)
	}

	// Verify reverse access via MakeGetter
	for i := len(records) - 1; i >= 0; i-- {
		sg.Reset(offsets[i])
		require.True(t, sg.HasNext())
		got, _ := sg.Next(nil)
		require.Equal(t, records[i], got, "record %d mismatch (reverse via MakeGetter)", i)
	}
}
