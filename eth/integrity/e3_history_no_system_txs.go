package integrity

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

// E3 History - usually don't have anything attributed to 1-st system txs (except genesis)
func E3HistoryNoSystemTxs(ctx context.Context, chainDB kv.RwDB, agg *state.Aggregator) error {
	count := atomic.Uint64{}
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	g := &errgroup.Group{}
	for j := 0; j < 256; j++ {
		j := j
		for jj := 0; jj < 255; jj++ {
			jj := jj
			g.Go(func() error {
				tx, err := db.BeginTemporalRo(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback()

				var minStep uint64 = math.MaxUint64
				keys, err := tx.(state.HasAggTx).AggTx().(*state.AggregatorRoTx).DomainRangeLatest(tx, kv.AccountsDomain, []byte{byte(j), byte(jj)}, []byte{byte(j), byte(jj + 1)}, -1)
				if err != nil {
					return err
				}
				defer keys.Close()

				for keys.HasNext() {
					key, _, err := keys.Next()
					if err != nil {
						return err
					}
					it, err := tx.IndexRange(kv.AccountsHistoryIdx, key, -1, 1_100_000_000, order.Desc, -1)
					if err != nil {
						return err
					}
					for it.HasNext() {
						txNum, err := it.Next()
						if err != nil {
							return err
						}
						ok, blockNum, err := rawdbv3.TxNums.FindBlockNum(tx, txNum)
						if err != nil {
							return err
						}
						if !ok {
							panic(fmt.Sprintf("blockNum not found for txNum=%d", txNum))
						}
						if blockNum == 0 {
							continue
						}
						_min, _ := rawdbv3.TxNums.Min(tx, blockNum)
						if txNum == _min {
							minStep = min(minStep, txNum/agg.StepSize())
							log.Warn(fmt.Sprintf("[dbg] minStep=%d, step=%d, txNum=%d, blockNum=%d, key=%x", minStep, txNum/agg.StepSize(), txNum, blockNum, key))
							break
						}

						select {
						case <-logEvery.C:
							log.Warn(fmt.Sprintf("[dbg] checked=%dK", count.Load()/1_000))
						default:
						}
					}
					it.Close()
					count.Add(1)
				}
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
