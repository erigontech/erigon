// Copyright 2025 The Erigon Authors
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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/turbo/services"
)

// E3 History - usually don't have anything attributed to 1-st system txs (except genesis)
func CommitmentFilesSanity(ctx context.Context, db kv.TemporalRwDB, blockReader services.FullBlockReader, agg *state.Aggregator) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	start := time.Now()
	var count atomic.Uint64
	g := new(errgroup.Group)
	info := sync.Once{}

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

				aggTx := tx.(state.HasAggTx).AggTx().(*state.AggregatorRoTx)
				info.Do(func() {
					log.Info("Checking commitment files", "domain", kv.CommitmentDomain, "txn", aggTx.TxNumsInFiles(kv.CommitmentDomain))
				})
				keys, err := tx.Debug().RangeLatest(kv.CommitmentDomain, []byte{byte(j), byte(jj)}, []byte{byte(j), byte(jj + 1)}, -1)
				if err != nil {
					return err
				}
				defer keys.Close()

				for keys.HasNext() {
					prefix, branchData, err := keys.Next()
					if err != nil {
						return err
					}

					err = commitment.BranchData(branchData).Verify(prefix)
					if err != nil {
						return err
					}
					// if aggTx.DbgDomain(kv.CommitmentDomain).()
					// during fetching latest all branches are dereferenced so plain keys for nodes are available
					bdDereferenced, _, _, err := aggTx.GetLatest(kv.CommitmentDomain, prefix, tx)
					if err != nil {
						return err
					}
					if len(branchData) > len(bdDereferenced) {
						return fmt.Errorf("defererenced branch %x is shorter than referenced", prefix)
					}
					if err = commitment.BranchData(bdDereferenced).Verify(prefix); err != nil {
						return err
					}

					select {
					case <-logEvery.C:
						log.Warn(fmt.Sprintf("[dbg] checked=%s prefixes", common.PrettyCounter(count.Load())))
					default:
					}
					count.Add(1)
				}
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return err
	}
	log.Info("finished checking commitment sanity", "prefixes", common.PrettyCounter(count.Load()), "time spent", time.Since(start))
	return nil
}
