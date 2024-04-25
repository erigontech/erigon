package state

import (
	"context"
	"fmt"
	"testing"

	"github.com/c2h5oh/datasize"
	datadir2 "github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/state"
)

func dbCfg(label kv.Label, path string) mdbx.MdbxOpts {
	const (
		ThreadsLimit = 9_000
		DBSizeLimit  = 3 * datasize.TB
		DBPageSize   = 8 * datasize.KB
		GrowthStep   = 2 * datasize.GB
	)
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	opts := mdbx.NewMDBX(log.New()).Path(path).Label(label).RoTxsLimiter(limiterB)
	if label == kv.ChainDB {
		opts = opts.MapSize(DBSizeLimit)
		opts = opts.PageSize(DBPageSize.Bytes())
		opts = opts.GrowthStep(GrowthStep)
	} else {
		opts = opts.GrowthStep(16 * datasize.MB)
	}

	// if db is not exists, we dont want to pass this flag since it will create db with maplimit of 1mb
	//if _, err := os.Stat(path); !os.IsNotExist(err) {
	//	// integration tool don't intent to create db, then easiest way to open db - it's pass mdbx.Accede flag, which allow
	//	// to read all options from DB, instead of overriding them
	//	opts = opts.Flags(func(f uint) uint { return f | mdbx.Accede })
	//}
	//
	return opts
}
func dbAggregatorOnDatadir(t *testing.T, ddir string) (kv.RwDB, *state.Aggregator) {
	t.Helper()
	logger := log.New()
	dirs := datadir2.New(ddir)
	db := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)

	agg, err := state.NewAggregator(context.Background(), dirs, config3.HistoryV3AggregationStep, db, logger)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	err = agg.OpenFolder(false)
	agg.DisableFsync()
	require.NoError(t, err)
	return db, agg
}

func TestRunnn(t *testing.T) {
	t.Skip()
	runAggregatorOnActualDatadir(t, "/Volumes/Untitled/chains/sepolia/")
}

func runAggregatorOnActualDatadir(t *testing.T, datadir string) {
	t.Helper()
	ctx := context.Background()
	db, agg := dbAggregatorOnDatadir(t, datadir)

	tdb, err := temporal.New(db, agg)
	require.NoError(t, err)

	tx, err := tdb.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	domCtx := agg.BeginFilesRo()
	defer domCtx.Close()

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	offt, err := domains.SeekCommitment(ctx, tx)
	require.NoError(t, err)
	txn := domains.TxNum()
	fmt.Printf("seek to block %d txn %d block beginning offset %d\n", domains.BlockNum(), txn, offt)

	hr := NewHistoryReaderV3()
	hr.SetTx(tx)
	for i := txn; i < txn+offt; i++ {
		hr.SetTxNum(i)

		acc, err := hr.ReadAccountData(common.HexToAddress("0xB5CAEc2ef7B24D644d1517c9286A17E73b5988F8"))
		require.NoError(t, err)
		fmt.Printf("history [%d] balance %s nonce %d\n", i, acc.Balance.String(), acc.Nonce)
		if acc.Nonce == 1 {
			break

		}
	}
	sr := NewStateReaderV3(domains)

	acc, err := sr.ReadAccountData(common.HexToAddress("0xB5CAEc2ef7B24D644d1517c9286A17E73b5988F8"))
	require.NoError(t, err)
	fmt.Printf("state balance %v nonce %d\n", acc.Balance.String(), acc.Nonce)
}
