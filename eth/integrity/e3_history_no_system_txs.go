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

	"github.com/erigontech/erigon-lib/estimate"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/turbo/services"
)

// History - usually don't have anything attributed to 1-st system txs (except genesis)
func HistoryCheckNoSystemTxs(ctx context.Context, db kv.TemporalRwDB, blockReader services.FullBlockReader) error {
	defer func(t time.Time) { log.Info("[integrity] HistoryNoSystemTxs done", "took", time.Since(t)) }(time.Now())
	count := atomic.Uint64{}
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	g := &errgroup.Group{}
	g.SetLimit(estimate.AlmostAllCPUs())

	skipForPerf := 11
	prefixesDone, prefixesTotal := atomic.Uint64{}, atomic.Uint64{}
	txNumsReader := blockReader.TxnumReader(ctx)

	for j := 0; j < 256; j++ {
		j := j
		for jj := 0; jj < 255; jj++ {
			jj := jj
			if (j+jj)%skipForPerf != 0 || (j+jj == 0) {
				continue
			}

			prefixesTotal.Add(1)
			g.Go(func() error {
				defer prefixesDone.Add(1)

				tx, err := db.BeginTemporalRo(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback()

				err = HistoryCheckNoSystemTxsRange(ctx, []byte{byte(j), byte(jj)}, []byte{byte(j), byte(jj + 1)}, tx, txNumsReader, logEvery, &count, &prefixesDone, &prefixesTotal)
				if err != nil {
					return err
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

func HistoryCheckNoSystemTxsRange(ctx context.Context, prefixFrom, prefixTo []byte, tx kv.TemporalTx,
	txNumsReader rawdbv3.TxNumsReader,
	logEvery *time.Ticker,
	keysCnt, prefixesDone, prefixesTotal *atomic.Uint64) error {
	agg := state.AggTx(tx)

	var minStep uint64 = math.MaxUint64
	keys, err := tx.Debug().RangeLatest(kv.AccountsDomain, prefixFrom, prefixTo, -1)
	if err != nil {
		return err
	}
	defer keys.Close()

	samplingKeys := 11
	samplingNums := 11
	keysI := 0
	numsI := 0

	for keys.HasNext() {
		key, _, err := keys.Next()
		if err != nil {
			return err
		}
		keysI++
		if keysI%samplingKeys != 0 {
			continue
		}

		it, err := tx.IndexRange(kv.AccountsHistoryIdx, key, -1, 1_100_000_000, order.Desc, -1)
		if err != nil {
			return err
		}

		blk, _min := int64(-1), int64(-1)

		for it.HasNext() {
			txNum, err := it.Next()
			if err != nil {
				return err
			}
			numsI++
			if numsI%samplingNums != 0 {
				continue
			}

			// Descending iteration: when we cross below current block's min, reset to find new block bounds
			if _min != -1 && int64(txNum) < _min {
				blk = -1
			}

			if blk == -1 {
				blockNum, ok, err := txNumsReader.FindBlockNum(tx, txNum)
				if err != nil {
					return err
				}
				if !ok {
					panic(fmt.Sprintf("blockNum not found for txNum=%d", txNum))
				}
				blk = int64(blockNum)
				if blockNum == 0 {
					continue
				}
				minT, err := rawdbv3.TxNums.Min(tx, blockNum)
				if err != nil {
					return err
				}
				_min = int64(minT)

			}

			if int64(txNum) == _min {
				minStep = min(minStep, txNum/agg.StepSize())
				log.Info(fmt.Sprintf("[integrity] HistoryNoSystemTxs: minStep=%d, step=%d, txNum=%d, blockNum=%d, key=%x", minStep, txNum/agg.StepSize(), txNum, blk, key))
				break
			}
		}
		it.Close()
		keysCnt.Add(1)

		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[integrity] HistoryNoSystemTxs: progress=%d/%d, keys=%.3fm", prefixesDone.Load(), prefixesTotal.Load(), float64(keysCnt.Load())/1_000_000))
		default:
		}
	}
	return nil
}
