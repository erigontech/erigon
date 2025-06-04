package state

import (
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon-lib/seg"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
)

func BenchmarkBpsTreeSeek(t *testing.B) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 12_000_000, 256
	t.Logf("N: %d, M: %d skip since shard <= %d", keyCount, M, DefaultBtreeStartSkip)

	compressCfg := seg.Cfg{WordLvl: seg.CompressKeys | seg.CompressVals, WordLvlCfg: seg.DefaultWordLvlCfg}
	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, compressCfg)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressCfg, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressCfg, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	var key []byte

	getter := seg.NewPagedReader(seg.NewReader(kv.MakeGetter(), compressCfg.WordLvl), compressCfg.PageLvl)
	getter.Reset(0)

	t.ResetTimer()
	t.ReportAllocs()
	//r := rand.New(rand.NewSource(0))
	for i := 0; i < t.N; i++ {
		if !getter.HasNext() {
			getter.Reset(0)
		}
		key, _, _, _, _ = getter.Next2(key[:0], nil)
		//_, err := bt.Seek(getter, keys[r.Intn(len(keys))])
		c, err := bt.Seek(getter, key)
		require.NoError(t, err)
		c.Close()
	}
	t.ReportAllocs()
}
