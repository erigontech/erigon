package btindex

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

// Interpolation search must return exactly what binary search returns for every
// key: hits (value+offset) and misses. Checked across budgets, including pure
// interpolation (large budget) and budget=0 (immediate binary fallback).
func TestInterpEquivBinary(t *testing.T) {
	t.Parallel()
	saveInterp, saveBudget := BtInterp, BtInterpBudget
	defer func() { BtInterp, BtInterpBudget = saveInterp, saveBudget }()

	const keyCount = 50000
	compress := seg.CompressKeys
	kvPath := generateKV(t, t.TempDir(), 20, 10, keyCount, log.New(), compress)
	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
	buildBtreeIndex(t, kvPath, indexPath, compress, 1, log.New(), true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, compress, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	misses := make([][]byte, 0, len(keys))
	for _, k := range keys { // flip a middle byte -> key almost certainly absent
		m := append([]byte(nil), k...)
		m[len(m)/2] ^= 0xff
		misses = append(misses, m)
	}

	g := seg.NewReader(kv.MakeGetter(), compress)
	get := func(interp bool, budget uint64, k []byte) ([]byte, bool, uint64) {
		BtInterp, BtInterpBudget = interp, budget
		v, ok, off, err := bt.bplus.Get(g, k)
		require.NoError(t, err)
		return v, ok, off
	}

	for _, budget := range []uint64{0, 1, 2, 4, 8, 1 << 20} {
		for i, k := range keys {
			wv, wok, woff := get(false, 0, k)
			gv, gok, goff := get(true, budget, k)
			require.Equalf(t, wok, gok, "hit key %d budget %d", i, budget)
			require.Equalf(t, wv, gv, "hit value key %d budget %d", i, budget)
			require.Equalf(t, woff, goff, "hit offset key %d budget %d", i, budget)
		}
		for i, k := range misses {
			_, wok, _ := get(false, 0, k)
			_, gok, _ := get(true, budget, k)
			require.Equalf(t, wok, gok, "miss key %d budget %d", i, budget)
		}
	}
}
