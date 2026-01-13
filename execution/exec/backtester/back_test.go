package exec_backtester

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/node/ethconfig"
)

func dbCfg(label kv.Label, path string) mdbx.MdbxOpts {
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	return mdbx.New(label, log.New()).Path(path).
		RoTxsLimiter(limiterB).
		Accede(true) // integration tool: open db without creation and without blocking erigon
}

func TestReExecution(t *testing.T) {
	// https://github.com/erigontech/erigon/issues/18276
	dataDir := "/Users/andrew/Library/Erigon/chiado"
	blockNum := uint64(19366160)

	dirs, _, err := datadir.New(dataDir).MustFlock()
	require.NoError(t, err)
	// defer func() {
	// 	err := l.Unlock()
	// 	require.NoError(t, err)
	// }()
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	//defer chainDB.Close()
	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)
	ctx := context.TODO()
	snaps, _, err := app.OpenSnaps(ctx, cfg, dirs, chainDB, log.New())
	require.NoError(t, err)
	//	defer clean()
	blockReader, _ := snaps.BlockRetire.IO()
	db, err := temporal.New(chainDB, snaps.Aggregator)
	require.NoError(t, err)
	//	defer db.Close()

	tnr := blockReader.TxnumReader(ctx)
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	fromTxNum, err := tnr.Min(tx, blockNum)
	require.NoError(t, err)

	// Check changes at the beginning of the block
	changes := 0
	it, err := tx.HistoryRange(kv.AccountsDomain, int(fromTxNum), int(fromTxNum+1), order.Asc, -1)
	require.NoError(t, err)
	//defer it.Close()
	for it.HasNext() {
		_, _, err := it.Next()
		require.NoError(t, err)
		changes++
	}
	assert.Equal(t, 0, changes)

	changes = 0
	it, err = tx.HistoryRange(kv.StorageDomain, int(fromTxNum), int(fromTxNum+1), order.Asc, -1)
	require.NoError(t, err)
	//defer it.Close()
	for it.HasNext() {
		_, _, err := it.Next()
		require.NoError(t, err)
		changes++
	}
	assert.Equal(t, 0, changes)

	// Alternative: use HistoricalTraceWorker

	// TODO: re-execute and compare results
	// ?
	//	block, err := blockReader.BlockByNumber(ctx, tx, blockNum)
	//	require.NoError(t, err)

}
