package btindex

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// generateVarLenKV builds a .kv (+ .bt) of unique, variable-length keys (1..40 B)
// drawn from a small alphabet, so adjacent sorted keys share prefixes and some
// keys are prefixes of others — the distribution that stresses interpolation on
// variable-length keys (e.g. commitment), where u64At order-preservation and the
// interpMid clamp matter.
func generateVarLenKV(tb testing.TB, tmp string, keyCount int, logger log.Logger, compress seg.FileCompression) string {
	tb.Helper()
	rnd := newRnd(3)
	dataPath := filepath.Join(tmp, "varlen.kv")
	comp, err := seg.NewCompressor(tb.Context(), "cmp", dataPath, tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(tb, err)
	collector := etl.NewCollector(BtreeLogPrefix+" varlen", tb.TempDir(), etl.NewSortableBuffer(1*datasize.MB), logger)
	defer collector.Close()

	seen := make(map[string]struct{}, keyCount)
	val := make([]byte, 16)
	for len(seen) < keyCount {
		key := make([]byte, 1+rnd.IntN(40))
		for j := range key {
			key[j] = byte(rnd.IntN(6)) // small alphabet -> shared prefixes + prefix-of-another
		}
		if _, ok := seen[string(key)]; ok {
			continue
		}
		seen[string(key)] = struct{}{}
		n, err := rnd.Read(val[:rnd.IntN(16)+1])
		require.NoError(tb, err)
		require.NoError(tb, collector.Collect(key, val[:n]))
	}

	writer := seg.NewWriter(comp, compress)
	loader := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		if _, err := writer.Write(k); err != nil {
			return err
		}
		_, err := writer.Write(v)
		return err
	}
	require.NoError(tb, collector.Load(nil, "", loader, etl.TransformArgs{}))
	collector.Close()
	require.NoError(tb, comp.Compress())
	comp.Close()

	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()
	idx := strings.TrimSuffix(dataPath, ".kv") + ".bt"
	r := seg.NewReader(decomp.MakeGetter(), compress)
	require.NoError(tb, BuildBtreeIndexWithDecompressor(idx, strings.TrimSuffix(idx, ".bt")+".kvei", r,
		background.NewProgressSet(), tb.TempDir(), 777, logger, true, statecfg.AccessorBTree|statecfg.AccessorExistence))
	return decomp.FilePath()
}

// Interpolation must return exactly what binary returns for variable-length keys
// too (commitment-style). Covers hits + misses across budgets incl pure (1<<20)
// and budget 0 (immediate binary fallback).
func TestInterpEquivBinaryVarLen(t *testing.T) {
	saveInterp, saveBudget := BtInterp, BtInterpBudget
	defer func() { BtInterp, BtInterpBudget = saveInterp, saveBudget }()

	const keyCount = 20000
	compress := seg.CompressKeys
	kvPath := generateVarLenKV(t, t.TempDir(), keyCount, log.New(), compress)
	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, compress, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	g := seg.NewReader(kv.MakeGetter(), compress)
	get := func(interp bool, budget uint64, k []byte) ([]byte, bool, uint64) {
		BtInterp, BtInterpBudget = interp, budget
		v, ok, off, err := bt.bplus.Get(g, k)
		require.NoError(t, err)
		return v, ok, off
	}

	for _, budget := range []uint64{0, 1, 2, 8, 1 << 20} {
		for i, k := range keys {
			wv, wok, woff := get(false, 0, k)
			gv, gok, goff := get(true, budget, k)
			require.Equalf(t, wok, gok, "hit key %d budget %d", i, budget)
			require.Equalf(t, wv, gv, "hit value key %d budget %d", i, budget)
			require.Equalf(t, woff, goff, "hit offset key %d budget %d", i, budget)

			m := append([]byte(nil), k...)
			m[len(m)/2] ^= 0xff
			_, wokm, _ := get(false, 0, m)
			_, gokm, _ := get(true, budget, m)
			require.Equalf(t, wokm, gokm, "miss key %d budget %d", i, budget)
		}
	}
}
