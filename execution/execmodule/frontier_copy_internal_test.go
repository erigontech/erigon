package execmodule

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
)

func TestCopyingFromAGenerationLeavesItOnItsOwnTransaction(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	own, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer own.Rollback()
	src, err := membatchwithdb.NewMemoryBatch(own)
	require.NoError(t, err)
	require.NoError(t, src.Put(kv.HeaderCanonical, []byte{0, 0, 0, 0, 0, 0, 0, 1}, []byte{0xaa}))
	require.NoError(t, src.ResetSequence(kv.EthTx, 7))

	dstTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer dstTx.Rollback()
	dst, err := membatchwithdb.NewMemoryBatch(dstTx)
	require.NoError(t, err)

	copyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	require.NoError(t, copyFrontierChainTables(src, dst, copyTx))
	copyTx.Rollback()

	v, err := dst.GetOne(kv.HeaderCanonical, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	require.NoError(t, err)
	require.Equal(t, []byte{0xaa}, v)
	seq, err := dst.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(7), seq)
	require.Same(t, own, src.UnderlyingTx(), "the copy rebound the generation's overlay to its own transaction")
}
