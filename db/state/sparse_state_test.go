package state

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
)

// TestSparseStateKVAccess validates that domain .kv files can be read
// transparently via a sparse Decompressor (backed by io.ReadSeeker).
// This proves that the domain read path (Reset + Next for key/value pairs)
// works identically for both normal and sparse decompressors.
//
// Requires environment variables:
//   SPARSE_TEST_KV  - path to a .kv domain file (e.g., v1-accounts.0-64.kv)
//   SPARSE_TEST_KVI - path to its .kvi index file (e.g., v1-accounts.0-64.kvi)
//
// Example:
//   SPARSE_TEST_KV=/erigon/datadir/snapshots/domain/v1-accounts.0-64.kv \
//   SPARSE_TEST_KVI=/erigon/datadir/snapshots/domain/v1-accounts.0-64.kvi \
//   go test -run TestSparseStateKVAccess -v ./db/state/...
func TestSparseStateKVAccess(t *testing.T) {
	kvPath := os.Getenv("SPARSE_TEST_KV")
	kviPath := os.Getenv("SPARSE_TEST_KVI") // optional
	if kvPath == "" {
		t.Skip("SPARSE_TEST_KV not set — skipping")
	}

	// Open the .kvi index if provided (HashMap/RecSplit)
	var idx *recsplit.Index
	if kviPath != "" {
		var err error
		idx, err = recsplit.OpenIndex(kviPath)
		require.NoError(t, err, "opening .kvi index")
		defer idx.Close()
		t.Logf("KVI: %s (baseDataID=%d, keyCount=%d)", kviPath, idx.BaseDataID(), idx.KeyCount())
	}

	t.Logf("KV:  %s", kvPath)

	// Open normal decompressor
	d, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err, "opening normal decompressor")
	defer d.Close()

	t.Logf("Normal decompressor: count=%d, size=%d", d.Count(), d.Size())

	// Read all key-value pairs and their offsets via normal getter
	// Domain .kv files store alternating key, value records
	type kvRecord struct {
		key    []byte
		value  []byte
		offset uint64
	}
	var records []kvRecord
	g := d.MakeGetter()
	maxRecords := 200 // check first 200 key-value pairs
	offset := uint64(0)
	for g.HasNext() && len(records) < maxRecords {
		keyOffset := offset
		key, nextOff := g.Next(nil)
		offset = nextOff
		if !g.HasNext() {
			break // incomplete pair
		}
		value, nextOff := g.Next(nil)
		offset = nextOff
		records = append(records, kvRecord{key: key, value: value, offset: keyOffset})
	}
	t.Logf("Collected %d key-value pairs for comparison", len(records))
	require.Greater(t, len(records), 0, "should have at least one record")

	// Create sparse decompressor from in-memory reader
	fileData, err := os.ReadFile(kvPath)
	require.NoError(t, err, "reading kv file into memory")

	reader := bytes.NewReader(fileData)
	sd, err := seg.NewDecompressorFromReader(reader, int64(len(fileData)), filepath.Base(kvPath))
	require.NoError(t, err, "creating sparse decompressor")
	defer sd.Close()

	require.True(t, sd.IsSparse(), "should be sparse")
	require.Equal(t, d.Count(), sd.Count(), "word count mismatch")

	// Test 1: Direct GetRecord comparison at each offset
	for i, rec := range records {
		got, err := sd.GetRecord(rec.offset)
		require.NoError(t, err, "GetRecord failed for record %d at offset %d", i, rec.offset)
		require.True(t, bytes.Equal(rec.key, got),
			"record %d key mismatch at offset %d: expected %d bytes, got %d bytes",
			i, rec.offset, len(rec.key), len(got))
	}
	t.Logf("Test 1 PASS: %d keys match via GetRecord", len(records))

	// Test 2: MakeGetter + Reset + Next (the domain read path)
	// This simulates what domain.getLatestFromFile does:
	//   g.Reset(offset) -> g.Next(nil) for key -> g.Next(nil) for value
	sg := sd.MakeGetter()
	for i, rec := range records {
		sg.Reset(rec.offset)
		require.True(t, sg.HasNext(), "HasNext should be true for key at record %d", i)
		key, _ := sg.Next(nil)
		require.True(t, bytes.Equal(rec.key, key),
			"record %d key mismatch via MakeGetter path", i)

		require.True(t, sg.HasNext(), "HasNext should be true for value at record %d", i)
		value, _ := sg.Next(nil)
		require.True(t, bytes.Equal(rec.value, value),
			"record %d value mismatch via MakeGetter path: expected %d bytes, got %d bytes",
			i, len(rec.value), len(value))
	}
	t.Logf("Test 2 PASS: %d key-value pairs match via MakeGetter + Reset + Next", len(records))

	// Test 3: Index lookup → sparse read (full domain access path)
	// Look up records by ordinal position via the .kvi index
	if idx != nil && idx.KeyCount() > 0 {
		testPositions := []uint64{0}
		if idx.KeyCount() > 1 {
			testPositions = append(testPositions, 1)
		}
		if idx.KeyCount() > 10 {
			testPositions = append(testPositions, 10)
		}
		if idx.KeyCount() > 100 {
			testPositions = append(testPositions, 100)
		}
		mid := idx.KeyCount() / 2
		if mid > 0 {
			testPositions = append(testPositions, mid)
		}

		for _, pos := range testPositions {
			offset := idx.OrdinalLookup(pos)

			// Read via normal decompressor
			ng := d.MakeGetter()
			ng.Reset(offset)
			if !ng.HasNext() {
				continue
			}
			normalKey, _ := ng.Next(nil)
			var normalValue []byte
			if ng.HasNext() {
				normalValue, _ = ng.Next(nil)
			}

			// Read via sparse decompressor
			sg.Reset(offset)
			require.True(t, sg.HasNext(), "sparse HasNext at ordinal %d", pos)
			sparseKey, _ := sg.Next(nil)
			var sparseValue []byte
			if sg.HasNext() {
				sparseValue, _ = sg.Next(nil)
			}

			require.True(t, bytes.Equal(normalKey, sparseKey),
				"ordinal %d: key mismatch (normal %d bytes vs sparse %d bytes)",
				pos, len(normalKey), len(sparseKey))
			require.True(t, bytes.Equal(normalValue, sparseValue),
				"ordinal %d: value mismatch (normal %d bytes vs sparse %d bytes)",
				pos, len(normalValue), len(sparseValue))

			t.Logf("  Ordinal %d: key=%d bytes, value=%d bytes OK", pos, len(sparseKey), len(sparseValue))
		}
		t.Logf("Test 3 PASS: index lookup → sparse read matches normal path")
	}

	// Test 4: Reverse access (common for recent-first lookups)
	for i := len(records) - 1; i >= 0; i-- {
		sg.Reset(records[i].offset)
		require.True(t, sg.HasNext())
		key, _ := sg.Next(nil)
		require.True(t, bytes.Equal(records[i].key, key),
			"reverse record %d key mismatch", i)
	}
	t.Log("Test 4 PASS: reverse access pattern works")
}

// TestSparseHistoryVAccess validates that history .v files can be read
// transparently via a sparse Decompressor.
//
// Requires environment variables:
//   SPARSE_TEST_V  - path to a .v history file
//   SPARSE_TEST_VI - path to its .vi index file
//
// Example:
//   SPARSE_TEST_V=/erigon/datadir/snapshots/history/v1-accounts.0-64.v \
//   SPARSE_TEST_VI=/erigon/datadir/snapshots/accessor/v1-accounts.0-64.vi \
//   go test -run TestSparseHistoryVAccess -v ./db/state/...
func TestSparseHistoryVAccess(t *testing.T) {
	vPath := os.Getenv("SPARSE_TEST_V")
	viPath := os.Getenv("SPARSE_TEST_VI")
	if vPath == "" || viPath == "" {
		t.Skip("SPARSE_TEST_V and SPARSE_TEST_VI not set — skipping")
	}

	// Open the .vi index
	idx, err := recsplit.OpenIndex(viPath)
	require.NoError(t, err, "opening .vi index")
	defer idx.Close()

	t.Logf("V:  %s", vPath)
	t.Logf("VI: %s (baseDataID=%d, keyCount=%d)", viPath, idx.BaseDataID(), idx.KeyCount())

	// Open normal decompressor
	d, err := seg.NewDecompressor(vPath)
	require.NoError(t, err)
	defer d.Close()

	// Collect records via sequential scan
	var offsets []uint64
	var records [][]byte
	g := d.MakeGetter()
	maxRecords := 200
	offset := uint64(0)
	for g.HasNext() && len(records) < maxRecords {
		offsets = append(offsets, offset)
		word, nextOff := g.Next(nil)
		offset = nextOff
		records = append(records, word)
	}
	t.Logf("Collected %d records", len(records))
	require.Greater(t, len(records), 0)

	// Create sparse decompressor
	fileData, err := os.ReadFile(vPath)
	require.NoError(t, err)
	reader := bytes.NewReader(fileData)
	sd, err := seg.NewDecompressorFromReader(reader, int64(len(fileData)), filepath.Base(vPath))
	require.NoError(t, err)
	defer sd.Close()

	// Verify all records match
	sg := sd.MakeGetter()
	for i, expected := range records {
		sg.Reset(offsets[i])
		require.True(t, sg.HasNext())
		got, _ := sg.Next(nil)
		require.True(t, bytes.Equal(expected, got),
			"record %d mismatch at offset %d", i, offsets[i])
	}
	t.Logf("All %d history records match between normal and sparse", len(records))

	// Verify via index lookup (only if index supports ordinal/enum lookups)
	if idx.KeyCount() > 0 && idx.Enums() {
		for _, pos := range []uint64{0, idx.KeyCount() / 2, idx.KeyCount() - 1} {
			offset := idx.OrdinalLookup(pos)
			normalRec, err := d.GetRecord(offset)
			require.NoError(t, err)
			sparseRec, err := sd.GetRecord(offset)
			require.NoError(t, err)
			require.True(t, bytes.Equal(normalRec, sparseRec),
				"ordinal %d: mismatch", pos)
		}
		t.Log("Index lookup → sparse read matches for history")
	} else {
		t.Log("Skipping ordinal lookup test — index doesn't support it")
	}
}
