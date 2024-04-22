package integrity

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/temporal"
	"github.com/ledgerwatch/erigon-lib/state"
	"golang.org/x/sync/errgroup"
)

func E3EfFiles(ctx context.Context, chainDB kv.RwDB, agg *state.Aggregator) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	g := &errgroup.Group{}
	for _, idx := range []kv.InvertedIdx{kv.AccountsHistoryIdx, kv.StorageHistoryIdx, kv.CodeHistoryIdx, kv.CommitmentHistoryIdx, kv.LogTopicIdx, kv.LogAddrIdx, kv.TracesFromIdx, kv.TracesToIdx} {
		idx := idx
		g.Go(func() error {
			tx, err := db.BeginTemporalRo(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			err = tx.(state.HasAggCtx).AggCtx().(*state.AggregatorRoTx).DebugEFAllValuesAreInRange(ctx, idx)
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
