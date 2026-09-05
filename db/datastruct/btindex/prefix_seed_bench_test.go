package btindex

import (
	"encoding/binary"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/seg"
)

func generateSkewedKV(tb testing.TB, tmp string, keyCount int, logger log.Logger, compress seg.FileCompression) string {
	tb.Helper()
	rnd := newRnd(7)
	dataPath := filepath.Join(tmp, "skewed.kv")
	comp, err := seg.NewCompressor(tb.Context(), "cmp", dataPath, tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(tb, err)
	collector := etl.NewCollector(BtreeLogPrefix+" skewed", tb.TempDir(), etl.NewSortableBuffer(32*datasize.MB), logger)
	defer collector.Close()

	val := make([]byte, 8)
	emitted := 0
	for contract := 0; emitted < keyCount; contract++ {
		slots := max(keyCount/(8*(contract+1)), 1)
		if emitted+slots > keyCount {
			slots = keyCount - emitted
		}
		addr := make([]byte, 20)
		binary.BigEndian.PutUint64(addr, uint64(contract)*0x9E3779B97F4A7C15)
		for s := 0; s < slots; s++ {
			key := make([]byte, 52)
			copy(key, addr)
			binary.BigEndian.PutUint64(key[44:], uint64(s))
			_, err := rnd.Read(val)
			require.NoError(tb, err)
			require.NoError(tb, collector.Collect(key, val))
			emitted++
		}
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
	return dataPath
}

func bucketOccupancy(b *BpsTree) (maxN, medianN, p99, empty int) {
	counts := make(map[uint32]int)
	n := b.numNodes()
	for i := range n {
		counts[nodePrefix(b.nodeKey(i))>>(16-b.prefixBits)]++
	}
	vals := make([]int, 0, len(counts))
	for _, c := range counts {
		vals = append(vals, c)
		if c > maxN {
			maxN = c
		}
	}
	sort.Ints(vals)
	if len(vals) > 0 {
		medianN = vals[len(vals)/2]
		p99 = vals[(len(vals)*99)/100]
	}
	return maxN, medianN, p99, len(b.prefixLo) - len(counts)
}

func TestSkewedFixtureShape(t *testing.T) {
	const keyCount = 200000
	compress := seg.CompressKeys
	for _, tc := range []struct {
		name  string
		build func() string
	}{
		{"uniform", func() string { return generateKV(t, t.TempDir(), 52, 8, keyCount, log.New(), compress) }},
		{"skewed", func() string { return generateSkewedKV(t, t.TempDir(), keyCount, log.New(), compress) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvPath := tc.build()
			indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
			buildBtreeIndex(t, kvPath, indexPath, compress, 1, log.New(), true)
			kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
			require.NoError(t, err)
			defer bt.Close()
			defer kv.Close()

			mx, med, p99, empty := bucketOccupancy(bt.bplus)
			t.Logf("%s: pivots=%d maxBucket=%d p99=%d median=%d emptyBuckets=%d prefixTable=%s nodeOfft=%s",
				tc.name, bt.bplus.numNodes(), mx, p99, med, empty,
				datasize.ByteSize(2*len(bt.bplus.prefixLo)*4).HR(),
				datasize.ByteSize(uint64(bt.bplus.numNodes())*4).HR())
			if tc.name == "skewed" {
				require.Greater(t, mx, med*4, "skewed fixture must be skewed")
			}
		})
	}
}

func sampleKeysAcrossRange(tb testing.TB, bt *BtIndex, kv *seg.Decompressor, compress seg.FileCompression, n int) [][]byte {
	tb.Helper()
	g := seg.NewReader(kv.MakeGetter(), compress)
	total := bt.KeyCount()
	require.Greater(tb, total, uint64(0))
	rnd := newRnd(11)
	out := make([][]byte, 0, n)
	for len(out) < n {
		c := bt.OrdinalLookup(g, uint64(rnd.IntN(int(total))))
		require.NotNil(tb, c)
		out = append(out, append([]byte(nil), c.Key()...))
		c.Close()
	}
	return out
}

func benchArms(b *testing.B, bt *BtIndex, g *seg.Reader, probes [][]byte) {
	t := bt.bplus
	saveLo, saveHi, saveOfft := t.prefixLo, t.prefixHi, t.nodeOfft
	defer func() { t.prefixLo, t.prefixHi, t.nodeOfft = saveLo, saveHi, saveOfft }()

	for _, arm := range []struct {
		name       string
		seed, offt bool
	}{{"base", false, false}, {"seed", true, false}, {"offt", false, true}, {"seed+offt", true, true}} {
		b.Run(arm.name, func(b *testing.B) {
			t.prefixLo, t.prefixHi = nil, nil
			t.nodeOfft = nil
			if arm.seed {
				t.prefixLo, t.prefixHi = saveLo, saveHi
			}
			if arm.offt {
				t.nodeOfft = saveOfft
			}
			b.Run("bs", func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					t.bs(probes[i%len(probes)])
					i++
				}
			})
			b.Run("seek", func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					c, err := t.Seek(g, probes[i%len(probes)])
					if err == nil && c != nil {
						c.Close()
					}
					i++
				}
			})
		})
	}
}

func BenchmarkBsArms(b *testing.B) {
	const keyCount = 1_000_000
	compress := seg.CompressKeys | seg.CompressVals
	for _, tc := range []struct {
		name  string
		build func() string
	}{
		{"uniform", func() string { return generateKV(b, b.TempDir(), 52, 8, keyCount, log.New(), compress) }},
		{"skewed", func() string { return generateSkewedKV(b, b.TempDir(), keyCount, log.New(), compress) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			kvPath := tc.build()
			indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
			buildBtreeIndex(b, kvPath, indexPath, compress, 1, log.New(), true)
			kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
			require.NoError(b, err)
			defer bt.Close()
			defer kv.Close()

			probes := sampleKeysAcrossRange(b, bt, kv, compress, 4096)
			g := seg.NewReader(kv.MakeGetter(), compress)
			b.Logf("%s: pivots=%d", tc.name, bt.bplus.numNodes())
			benchArms(b, bt, g, probes)
		})
	}
}
