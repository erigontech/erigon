package freezeblocks

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
)

// TestSparseCaplinBeaconBlockAccess validates that beacon block .seg files can be
// read transparently via a sparse Decompressor (backed by io.ReadSeeker).
// This proves that the caplin read path (Index.OrdinalLookup → MakeGetter → Reset → Next)
// works identically for both normal and sparse decompressors.
//
// Requires environment variables:
//
//	SPARSE_TEST_BEACON_SEG - path to a beacon block .seg file (e.g., v1-000000-000500-beaconblocks.seg)
//	SPARSE_TEST_BEACON_IDX - path to its .idx index file (e.g., v1-000000-000500-beaconblocks.idx)
//
// Example:
//
//	SPARSE_TEST_BEACON_SEG=/erigon/erigon-test-nodeA/snapshots/caplin/beacon/v1-000000-000500-beaconblocks.seg \
//	SPARSE_TEST_BEACON_IDX=/erigon/erigon-test-nodeA/snapshots/caplin/beacon/v1-000000-000500-beaconblocks.idx \
//	go test -run TestSparseCaplinBeaconBlockAccess -v ./db/snapshotsync/freezeblocks/...
func TestSparseCaplinBeaconBlockAccess(t *testing.T) {
	segPath := os.Getenv("SPARSE_TEST_BEACON_SEG")
	idxPath := os.Getenv("SPARSE_TEST_BEACON_IDX")
	if segPath == "" || idxPath == "" {
		t.Skip("SPARSE_TEST_BEACON_SEG and SPARSE_TEST_BEACON_IDX not set — skipping")
	}

	// Open the index
	idx, err := recsplit.OpenIndex(idxPath)
	require.NoError(t, err, "opening beacon block index")
	defer idx.Close()

	t.Logf("SEG: %s", segPath)
	t.Logf("IDX: %s (baseDataID=%d, keyCount=%d)", idxPath, idx.BaseDataID(), idx.KeyCount())

	// Open standard decompressor for ground truth
	d, err := seg.NewDecompressor(segPath)
	require.NoError(t, err, "opening standard decompressor")
	defer d.Close()

	t.Logf("Standard decompressor: count=%d, size=%d", d.Count(), d.Size())

	// Collect records and offsets via standard getter (first 200 slots)
	type record struct {
		data   []byte
		offset uint64
	}
	var records []record
	g := d.MakeGetter()
	maxRecords := 200
	offset := uint64(0)
	for g.HasNext() && len(records) < maxRecords {
		recOffset := offset
		word, nextOff := g.Next(nil)
		offset = nextOff
		records = append(records, record{data: word, offset: recOffset})
	}
	t.Logf("Collected %d records for comparison", len(records))
	require.Greater(t, len(records), 0, "should have at least one record")

	// Create sparse decompressor from in-memory reader
	fileData, err := os.ReadFile(segPath)
	require.NoError(t, err, "reading seg file into memory")

	reader := bytes.NewReader(fileData)
	sd, err := seg.NewDecompressorFromReader(reader, int64(len(fileData)), filepath.Base(segPath))
	require.NoError(t, err, "creating sparse decompressor")
	defer sd.Close()

	require.True(t, sd.IsSparse(), "should be sparse")
	require.Equal(t, d.Count(), sd.Count(), "word count mismatch")

	// Test 1: GetRecord comparison at sequential offsets
	for i, rec := range records {
		got, err := sd.GetRecord(rec.offset)
		require.NoError(t, err, "GetRecord failed for record %d at offset %d", i, rec.offset)
		require.True(t, bytes.Equal(rec.data, got),
			"record %d mismatch at offset %d: expected %d bytes, got %d bytes",
			i, rec.offset, len(rec.data), len(got))
	}
	t.Logf("Test 1 PASS: %d records match via GetRecord", len(records))

	// Test 2: MakeGetter + Reset + Next (the caplin read path)
	sg := sd.MakeGetter()
	for i, rec := range records {
		sg.Reset(rec.offset)
		require.True(t, sg.HasNext(), "HasNext should be true for record %d", i)
		got, _ := sg.Next(nil)
		require.True(t, bytes.Equal(rec.data, got),
			"record %d mismatch via MakeGetter path: expected %d bytes, got %d bytes",
			i, len(rec.data), len(got))
	}
	t.Logf("Test 2 PASS: %d records match via MakeGetter + Reset + Next", len(records))

	// Test 3: Index ordinal lookup → sparse read (full caplin access path)
	if idx.KeyCount() > 0 {
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
		// Last slot
		testPositions = append(testPositions, idx.KeyCount()-1)

		for _, pos := range testPositions {
			wordOffset := idx.OrdinalLookup(pos)

			// Read via normal decompressor
			ng := d.MakeGetter()
			ng.Reset(wordOffset)
			if !ng.HasNext() {
				continue
			}
			normalRec, _ := ng.Next(nil)

			// Read via sparse decompressor
			sg.Reset(wordOffset)
			require.True(t, sg.HasNext(), "sparse HasNext at ordinal %d", pos)
			sparseRec, _ := sg.Next(nil)

			require.True(t, bytes.Equal(normalRec, sparseRec),
				"ordinal %d: mismatch (normal %d bytes vs sparse %d bytes)",
				pos, len(normalRec), len(sparseRec))

			t.Logf("  Slot ordinal %d: %d bytes OK (wordOffset=%d)", pos, len(sparseRec), wordOffset)
		}
		t.Logf("Test 3 PASS: index lookup → sparse read matches normal path")
	}

	// Test 4: Reverse access (common for recent-first lookups in caplin)
	for i := len(records) - 1; i >= 0; i-- {
		sg.Reset(records[i].offset)
		require.True(t, sg.HasNext())
		got, _ := sg.Next(nil)
		require.True(t, bytes.Equal(records[i].data, got),
			"reverse record %d mismatch", i)
	}
	t.Log("Test 4 PASS: reverse access pattern works")
}
