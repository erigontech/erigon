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

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

// E3 History - usually don't have anything attributed to 1-st system txs (except genesis)
func E3HistoryNoSystemTxs(ctx context.Context, chainDB kv.RwDB, blockReader services.FullBlockReader, agg *state.Aggregator) error {
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
						ok, blockNum, err := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, blockReader)).FindBlockNum(tx, txNum)
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
							log.Warn(fmt.Sprintf("[dbg] checked=%dK keys", count.Load()/1_000))
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
