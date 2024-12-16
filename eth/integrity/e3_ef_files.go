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
	"time"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
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
	//for _, idx := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain, kv.ReceiptDomain} {
	//	idx := idx
	//	g.Go(func() error {
	//		tx, err := db.BeginTemporalRo(ctx)
	//		if err != nil {
	//			return err
	//		}
	//		defer tx.Rollback()
	//
	//		err = tx.(state.HasAggTx).AggTx().(*state.AggregatorRoTx).DebugInvertedIndexOfDomainAllValuesAreInRange(ctx, idx, failFast, fromStep)
	//		if err != nil {
	//			return err
	//		}
	//		return nil
	//	})
	//}
	//for _, idx := range []kv.InvertedIdxPos{kv.LogTopicIdxPos, kv.LogAddrIdxPos, kv.TracesFromIdxPos, kv.TracesToIdxPos} {
	//	idx := idx
	//	g.Go(func() error {
	//		tx, err := db.BeginTemporalRo(ctx)
	//		if err != nil {
	//			return err
	//		}
	//		defer tx.Rollback()
	//
	//		err = tx.(state.HasAggTx).AggTx().(*state.AggregatorRoTx).DebugInvertedIndexAllValuesAreInRange(ctx, idx, failFast, fromStep)
	//		if err != nil {
	//			return err
	//		}
	//		return nil
	//	})
	//}

	g.Go(func() error {
		tx, err := db.BeginTemporalRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		aTx := tx.(state.HasAggTx).AggTx().(*state.AggregatorRoTx)
		_, bn, err := rawdbv3.TxNums.FindBlockNum(tx, (64-1)*aTx.StepSize())
		if err != nil {
			return err
		}
		systemTxsAmount := int(bn * 2)
		cnt, err := aTx.DebugInvertedIndexOfDomainCount(ctx, kv.ReceiptDomain, failFast, rawtemporaldb.CumulativeGasUsedInBlockKey, fromStep)
		if err != nil {
			return err
		}
		fmt.Printf("cnt: %d, systemTxsAmount=%d\n", cnt, systemTxsAmount)
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
