package state

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

func BenchmarkBpsTreeSeek(t *testing.B) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 12_000_000, 256
	t.Logf("N: %d, M: %d skip since shard <= %d", keyCount, M, DefaultBtreeStartSkip)
	compressFlags := seg.CompressKeys | seg.CompressVals

	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, 0)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	var key []byte

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)
	getter.Reset(0)

	t.ResetTimer()
	t.ReportAllocs()
	//r := rand.New(rand.NewSource(0))
	for i := 0; i < t.N; i++ {
		if !getter.HasNext() {
			getter.Reset(0)
		}
		key, _ = getter.Next(key[:0])
		getter.Skip()
		//_, err := bt.Seek(getter, keys[r.Intn(len(keys))])
		c, err := bt.Seek(getter, key)
		require.NoError(t, err)
		c.Close()
	}
	t.ReportAllocs()
}
