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
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

// E3 History - usually don't have anything attributed to 1-st system txs (except genesis)
func E3HistoryNoSystemTxs(ctx context.Context, chainDB kv.RoDB, agg *state.AggregatorV3) error {
	count := atomic.Uint64{}
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	g := &errgroup.Group{}
	for j := 0; j < 255; j++ {
		for jj := 0; jj < 255; jj++ {
			j, jj := j, jj
			g.Go(func() error {
				tx, err := chainDB.BeginRo(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback()

				var minStep uint64 = math.MaxUint64
				view := agg.MakeContext()
				defer view.Close()
				keys, err := view.DomainRangeLatest(tx, kv.AccountsDomain, []byte{byte(j), byte(jj)}, []byte{byte(j + 1), byte(jj + 1)}, -1)
				if err != nil {
					return err
				}
				for keys.HasNext() {
					key, _, err := keys.Next()
					if err != nil {
						return err
					}
					it, err := view.IndexRange(kv.AccountsHistoryIdx, key, -1, -1, order.Asc, -1, tx)
					if err != nil {
						return err
					}
					for it.HasNext() {
						txNum, _ := it.Next()
						ok, blockNum, err := rawdbv3.TxNums.FindBlockNum(tx, txNum)
						if err != nil {
							return err
						}
						if !ok {
							panic(txNum)
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
							log.Warn(fmt.Sprintf("[dbg] checked=%d", count.Load()))
						default:
						}
					}
					count.Add(1)
					if casted, ok := it.(kv.Closer); ok {
						casted.Close()
					}
				}
				log.Warn(fmt.Sprintf("[dbg] step=%d", minStep))

				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
