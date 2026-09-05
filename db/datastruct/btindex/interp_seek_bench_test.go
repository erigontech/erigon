package btindex

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

func benchSeekArms(b *testing.B, probes [][]byte, bt *BtIndex, g *seg.Reader) {
	for _, arm := range []struct {
		name   string
		interp bool
	}{{"binary", false}, {"interp", true}} {
		b.Run(arm.name, func(b *testing.B) {
			save, saveB := BtInterp, BtInterpBudget
			defer func() { BtInterp, BtInterpBudget = save, saveB }()
			BtInterp, BtInterpBudget = arm.interp, 8
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				c, err := bt.bplus.Seek(g, probes[i%len(probes)])
				if err == nil && c != nil {
					c.Close()
				}
				i++
			}
		})
	}
}

func BenchmarkSeekInterp(b *testing.B) {
	const keyCount = 1_000_000
	compress := seg.CompressKeys | seg.CompressVals
	tmp := b.TempDir()
	logger := log.New()

	kvPath := generateKV(b, tmp, 52, 180, keyCount, logger, compress)
	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
	buildBtreeIndex(b, kvPath, indexPath, compress, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
	require.NoError(b, err)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(b, err)
	require.NotEmpty(b, keys)

	rnd := newRnd(42)
	hits := make([][]byte, 0, 4096)
	gaps := make([][]byte, 0, 4096)
	for i := 0; i < 4096; i++ {
		k := keys[rnd.IntN(len(keys))]
		hits = append(hits, k)
		gp := append([]byte(nil), k...)
		gp[len(gp)-1] ^= 0x01
		gaps = append(gaps, gp)
	}

	g := seg.NewReader(kv.MakeGetter(), compress)
	b.Run("hit", func(b *testing.B) { benchSeekArms(b, hits, bt, g) })
	b.Run("gap", func(b *testing.B) { benchSeekArms(b, gaps, bt, g) })
}
