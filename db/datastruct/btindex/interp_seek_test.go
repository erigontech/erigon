package btindex

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

type seekResult struct {
	found bool
	errS  string
	key   []byte
	value []byte
	di    uint64
	tail  [][]byte
}

func seekWith(t *testing.T, bt *BtIndex, g *seg.Reader, interp bool, budget uint64, k []byte) seekResult {
	t.Helper()
	BtInterp, BtInterpBudget = interp, budget
	cur, err := bt.bplus.Seek(g, k)
	if err != nil {
		return seekResult{errS: err.Error()}
	}
	if cur == nil {
		return seekResult{}
	}
	res := seekResult{
		found: true,
		key:   append([]byte(nil), cur.Key()...),
		value: append([]byte(nil), cur.Value()...),
		di:    cur.Di(),
	}
	for i := 0; i < 4 && cur.Next(); i++ {
		res.tail = append(res.tail, append([]byte(nil), cur.Key()...))
	}
	return res
}

func requireSeekEquiv(t *testing.T, bt *BtIndex, g *seg.Reader, budgets []uint64, probes [][]byte, what string) {
	t.Helper()
	for _, budget := range budgets {
		for i, k := range probes {
			want := seekWith(t, bt, g, false, 0, k)
			got := seekWith(t, bt, g, true, budget, k)
			require.Equalf(t, want.errS, got.errS, "%s %d budget %d: error", what, i, budget)
			require.Equalf(t, want.found, got.found, "%s %d budget %d: cursor presence", what, i, budget)
			require.Equalf(t, want.key, got.key, "%s %d budget %d: key", what, i, budget)
			require.Equalf(t, want.value, got.value, "%s %d budget %d: value", what, i, budget)
			require.Equalf(t, want.di, got.di, "%s %d budget %d: di", what, i, budget)
			require.Equalf(t, want.tail, got.tail, "%s %d budget %d: forward iteration", what, i, budget)
			if got.found {
				require.GreaterOrEqualf(t, bytes.Compare(got.key, k), 0, "%s %d budget %d: Seek landed before the probe", what, i, budget)
			}
		}
	}
}

func seekProbes(t *testing.T, keys [][]byte) (hits, misses, past [][]byte) {
	t.Helper()
	hits = keys
	for _, k := range keys {
		m := append([]byte(nil), k...)
		m[len(m)/2] ^= 0xff
		misses = append(misses, m)
	}
	last := append([]byte(nil), keys[len(keys)-1]...)
	for i := range last {
		last[i] = 0xff
	}
	past = [][]byte{last}
	return hits, misses, past
}

func TestSeekInterpEquivBinary(t *testing.T) {
	saveInterp, saveBudget := BtInterp, BtInterpBudget
	defer func() { BtInterp, BtInterpBudget = saveInterp, saveBudget }()

	budgets := []uint64{0, 1, 2, 4, 8, 64}

	for _, compress := range []seg.FileCompression{0, seg.CompressKeys, seg.CompressVals, seg.CompressKeys | seg.CompressVals} {
		t.Run(fmt.Sprintf("fixed-len-compress-%d", compress), func(t *testing.T) {
			kvPath := generateKV(t, t.TempDir(), 20, 10, 50000, log.New(), compress)
			indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
			buildBtreeIndex(t, kvPath, indexPath, compress, 1, log.New(), true)

			kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
			require.NoError(t, err)
			defer bt.Close()
			defer kv.Close()

			keys, err := pivotKeysFromKV(kvPath)
			require.NoError(t, err)
			require.NotEmpty(t, keys)

			g := seg.NewReader(kv.MakeGetter(), compress)
			hits, misses, past := seekProbes(t, keys)
			requireSeekEquiv(t, bt, g, budgets, hits, "hit")
			requireSeekEquiv(t, bt, g, budgets, misses, "miss")
			requireSeekEquiv(t, bt, g, budgets, past, "past-last")
			requireSeekEquiv(t, bt, g, budgets, [][]byte{{}, nil}, "empty")
		})
	}

	t.Run("var-len", func(t *testing.T) {
		compress := seg.CompressKeys
		kvPath := generateVarLenKV(t, t.TempDir(), 20000, log.New(), compress)
		indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
		buildBtreeIndex(t, kvPath, indexPath, compress, 1, log.New(), true)

		kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
		require.NoError(t, err)
		defer bt.Close()
		defer kv.Close()

		keys, err := pivotKeysFromKV(kvPath)
		require.NoError(t, err)
		require.NotEmpty(t, keys)

		g := seg.NewReader(kv.MakeGetter(), compress)
		hits, misses, past := seekProbes(t, keys)
		requireSeekEquiv(t, bt, g, budgets, hits, "hit")
		requireSeekEquiv(t, bt, g, budgets, misses, "miss")
		requireSeekEquiv(t, bt, g, budgets, past, "past-last")
	})
}
