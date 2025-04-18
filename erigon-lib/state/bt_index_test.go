package state

import (
	"strings"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/stretchr/testify/require"
)

func TestNewBtIndex(t *testing.T) {
	t.Parallel()
	keyCount := 10000
	kvPath := generateKV(t, t.TempDir(), 20, 10, keyCount, log.New(), seg.CompressNone)

	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, seg.CompressNone, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()
	require.NotNil(t, kv)
	require.NotNil(t, bt)
	bplus := bt.BpsTree()
	require.GreaterOrEqual(t, len(bplus.mx), keyCount/int(DefaultBtreeM))

	for i := 1; i < len(bt.bplus.mx); i++ {
		require.NotZero(t, bt.bplus.mx[i].di)
		require.NotZero(t, bt.bplus.mx[i].off)
		require.NotEmpty(t, bt.bplus.mx[i].key)
	}
}
