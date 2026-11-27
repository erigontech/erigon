package temporal

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Close, also from two callers at once, must not release state files while a temporal tx that reads them is still open.
func TestTemporalDB_CloseKeepsFilesOfOpenTx(t *testing.T) {
	ctx := t.Context()
	mdbxDb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	agg := state.NewTest(datadir.New(t.TempDir())).StepSize(1).MustOpen(ctx)
	db, err := New(mdbxDb, agg, nil)
	require.NoError(t, err)

	acc := common.HexToAddress("0x1234567890123456789012345678901234567890")
	for txNum := uint64(1); txNum <= 3; txNum++ {
		require.NoError(t, db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
			sd, sdErr := execctx.NewSharedDomains(ctx, tx, log.Root())
			if sdErr != nil {
				return sdErr
			}
			defer sd.Close()
			if putErr := sd.DomainPut(kv.AccountsDomain, tx, acc[:], accountAt(txNum), txNum, nil); putErr != nil {
				return putErr
			}
			return sd.Flush(ctx, tx)
		}))
	}
	require.NoError(t, db.BuildFiles(3, unboundedFinalityCtx))

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	require.Positive(t, roTx.Debug().TxNumsInFiles(kv.AccountsDomain))
	v, _, err := roTx.GetAsOf(kv.AccountsDomain, acc[:], 2)
	require.NoError(t, err)
	require.Equal(t, accountAt(1), v)

	var closers sync.WaitGroup
	closers.Go(db.Close)
	// MDBX refuses new txs only once Close has reached it.
	require.Eventually(t, func() bool {
		tx, beginErr := mdbxDb.BeginRo(ctx) //nolint:gocritic
		if beginErr == nil {
			tx.Rollback()
		}
		return beginErr != nil
	}, 10*time.Second, time.Millisecond)
	secondClosed := make(chan struct{})
	closers.Go(func() {
		db.Close()
		close(secondClosed)
	})
	select {
	case <-secondClosed:
		t.Fatal("second Close returned while a tx is open")
	case <-time.After(100 * time.Millisecond):
	}

	require.Positive(t, roTx.Debug().TxNumsInFiles(kv.AccountsDomain), "open tx lost its files")
	v, _, err = roTx.GetAsOf(kv.AccountsDomain, acc[:], 2)
	require.NoError(t, err)
	require.Equal(t, accountAt(1), v)

	roTx.Rollback()
	closers.Wait()
}

func accountAt(nonce uint64) []byte {
	return accounts.SerialiseV3(&accounts.Account{Nonce: nonce})
}
