package btindex

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

func openScanFixture(tb testing.TB, keyCount int, compressed seg.FileCompression) (*seg.Decompressor, *BtIndex) {
	tb.Helper()
	tmp := tb.TempDir()
	logger := log.New()

	dataPath := generateKV(tb, tmp, 52, 120, keyCount, logger, compressed)
	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bt")
	buildBtreeIndex(tb, dataPath, indexPath, compressed, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, compressed, false)
	require.NoError(tb, err)
	tb.Cleanup(bt.Close)
	tb.Cleanup(kv.Close)
	return kv, bt
}

// TestCursor_ScanMatchesOrdinalLookup pins the forward-scan fast path: walking the
// index with Next must yield exactly what an EliasFano lookup of each pair yields.
func TestCursor_ScanMatchesOrdinalLookup(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		compressed seg.FileCompression
	}{
		{name: "uncompressed", compressed: seg.CompressNone},
		{name: "compressed_keys", compressed: seg.CompressKeys},
		{name: "compressed_kv", compressed: seg.CompressKeys | seg.CompressVals},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const keyCount = 1000
			kv, bt := openScanFixture(t, keyCount, tc.compressed)
			getter := seg.NewReader(kv.MakeGetter(), tc.compressed)

			cur, err := bt.Seek(getter, nil)
			require.NoError(t, err)
			defer cur.Close()

			refGetter := seg.NewReader(kv.MakeGetter(), tc.compressed)
			for di := range uint64(keyCount) {
				wantK, wantV, _, err := bt.dataLookup(di, refGetter)
				require.NoError(t, err)
				require.Equalf(t, wantK, cur.Key(), "key at %d", di)
				require.Equalf(t, wantV, cur.Value(), "value at %d", di)
				require.Equal(t, di+1 < keyCount, cur.Next(), "Next at %d", di)
			}
		})
	}
}

// TestCursor_ScanWithSharedGetter pins why the cursor re-anchors the getter on every
// read instead of assuming it is still where the previous read left it: one getter is
// shared per file (DomainRoTx.reusableReader), so a point lookup - or anything an
// IteratePrefix callback does - can reposition it between two Next calls.
func TestCursor_ScanWithSharedGetter(t *testing.T) {
	t.Parallel()

	const keyCount = 1000
	compressed := seg.CompressKeys
	kv, bt := openScanFixture(t, keyCount, compressed)
	getter := seg.NewReader(kv.MakeGetter(), compressed)
	refGetter := seg.NewReader(kv.MakeGetter(), compressed)

	keys := make([][]byte, keyCount)
	for di := range uint64(keyCount) {
		k, _, _, err := bt.dataLookup(di, refGetter)
		require.NoError(t, err)
		keys[di] = append([]byte(nil), k...)
	}

	cur, err := bt.Seek(getter, nil)
	require.NoError(t, err)
	defer cur.Close()
	for di := range keyCount {
		require.Equalf(t, keys[di], cur.Key(), "key at %d", di)

		_, _, _, found, err := bt.Get(keys[(di*7+3)%keyCount], getter) // moves the shared getter
		require.NoError(t, err)
		require.True(t, found)

		require.Equal(t, di+1 < keyCount, cur.Next(), "Next at %d", di)
	}
}

func BenchmarkCursorScan(b *testing.B) {
	const keyCount = 100_000
	kv, bt := openScanFixture(b, keyCount, seg.CompressKeys)
	getter := seg.NewReader(kv.MakeGetter(), seg.CompressKeys)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		cur, err := bt.Seek(getter, nil)
		require.NoError(b, err)
		n := 1
		for cur.Next() {
			n++
		}
		require.Equal(b, keyCount, n)
		cur.Close()
	}
}

// TestCursor_ScanAfterSeek pins the same for a scan that starts in the middle, and
// for a cursor taken from the pool after an earlier scan.
func TestCursor_ScanAfterSeek(t *testing.T) {
	t.Parallel()

	const keyCount = 1000
	compressed := seg.CompressKeys
	kv, bt := openScanFixture(t, keyCount, compressed)
	getter := seg.NewReader(kv.MakeGetter(), compressed)
	refGetter := seg.NewReader(kv.MakeGetter(), compressed)

	keys := make([][]byte, keyCount)
	for di := range uint64(keyCount) {
		k, _, _, err := bt.dataLookup(di, refGetter)
		require.NoError(t, err)
		keys[di] = append([]byte(nil), k...)
	}

	for _, from := range []int{0, 1, keyCount / 2, keyCount - 2} {
		cur, err := bt.Seek(getter, keys[from])
		require.NoError(t, err)
		for di := from; di < keyCount; di++ {
			require.Equalf(t, keys[di], cur.Key(), "from %d, key at %d", from, di)
			require.Equal(t, di+1 < keyCount, cur.Next())
		}
		cur.Close() // back to the pool: the next Seek must not trust stale offsets
	}
}
