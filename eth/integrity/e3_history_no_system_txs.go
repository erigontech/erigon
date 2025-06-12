// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package integrity

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

// History - usually don't have anything attributed to 1-st system txs (except genesis)
func HistoryCheckNoSystemTxs(ctx context.Context, db kv.TemporalRwDB, blockReader services.FullBlockReader) error {
	defer log.Info("[integrity] HistoryCheckNoSystemTxs done")
	count := atomic.Uint64{}
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	g := &errgroup.Group{}
	g.SetLimit(estimate.AlmostAllCPUs())
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
				keys, err := state.AggTx(tx).DebugRangeLatest(tx, kv.AccountsDomain, []byte{byte(j), byte(jj)}, []byte{byte(j), byte(jj + 1)}, -1)
				if err != nil {
					return err
				}
				defer keys.Close()

				txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.TxBlockIndexFromBlockReader(ctx, blockReader))

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
						blockNum, ok, err := txNumsReader.FindBlockNum(tx, txNum)
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
							log.Info(fmt.Sprintf("[integrity] HistoryNoSystemTxs: minStep=%d, step=%d, txNum=%d, blockNum=%d, key=%x", minStep, txNum/agg.StepSize(), txNum, blockNum, key))
							break
						}
					}
					it.Close()
					count.Add(1)

					select {
					case <-logEvery.C:
						log.Info(fmt.Sprintf("[integrity] HistoryNoSystemTxs: checked=%.2fm keys", float64(count.Load())/1_000_000))
					default:
					}
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
