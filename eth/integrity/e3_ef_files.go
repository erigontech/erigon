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
	"time"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/state"
	"golang.org/x/sync/errgroup"
)

func E3EfFiles(ctx context.Context, chainDB kv.RwDB, agg *state.Aggregator, failFast bool, fromStep uint64) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	g := &errgroup.Group{}
	for _, idx := range []kv.InvertedIdx{kv.AccountsHistoryIdx, kv.StorageHistoryIdx, kv.CodeHistoryIdx, kv.CommitmentHistoryIdx, kv.ReceiptHistoryIdx, kv.LogTopicIdx, kv.LogAddrIdx, kv.TracesFromIdx, kv.TracesToIdx} {
		idx := idx
		g.Go(func() error {
			tx, err := db.BeginTemporalRo(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			err = tx.(state.HasAggTx).AggTx().(*state.AggregatorRoTx).DebugEFAllValuesAreInRange(ctx, idx, failFast, fromStep)
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
