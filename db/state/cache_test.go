package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestDomainCacheNilWithoutVisibleFiles(t *testing.T) {
	t.Parallel()

	dv := newDomainVisible(kv.AccountsDomain, visibleFiles{}, nil)
	require.Nil(t, dv.cache)
}

func TestDomainCacheAcrossVisibleFileGenerations(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Parallel()

	db, d, txs := filledDomain(t, log.New())
	err := db.UpdateNosync(t.Context(), func(tx kv.RwTx) error {
		collateAndMerge(t, tx, d, txs)
		return nil
	})
	require.NoError(t, err)

	dv, _, _ := d.calcVisibleFiles(d.dirtyFilesEndTxNumMinimax(), nil)
	require.NotEmpty(t, dv.files)
	require.NotNil(t, dv.cache)

	var keys uint32
	for _, f := range dv.files {
		keys += uint32(f.src.decompressor.Count() / 2)
	}
	require.LessOrEqual(t, dv.cache.limit, keys)

	t.Run("same visible end reuses cache", func(t *testing.T) {
		next := newDomainVisible(kv.AccountsDomain, dv.files, dv)
		require.Same(t, dv.cache, next.cache)
	})

	t.Run("different visible end rebuilds cache", func(t *testing.T) {
		require.Greater(t, len(dv.files), 1)
		next := newDomainVisible(kv.AccountsDomain, dv.files[:len(dv.files)-1], dv)
		require.NotSame(t, dv.cache, next.cache)
	})
}
