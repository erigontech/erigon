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

func TestDomainCacheCappedByVisibleFileKeys(t *testing.T) {
	if testing.Short() {
		t.Skip()
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

	var keys uint32
	for _, f := range dv.files {
		keys += uint32(f.src.decompressor.Count() / 2)
	}
	require.NotNil(t, dv.cache)
	require.LessOrEqual(t, dv.cache.limit, keys)
}
