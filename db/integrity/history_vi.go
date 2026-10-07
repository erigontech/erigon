// Copyright 2026 The Erigon Authors
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

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
)

func HistoryValueIndexFiles(ctx context.Context, db kv.TemporalRwDB, failFast bool, fromStep uint64, logger log.Logger) error {
	start := time.Now()
	defer func() { logger.Info("[integrity] HistoryVi done", "dur", time.Since(start)) }()

	g, ctx := errgroup.WithContext(ctx)
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain, kv.ReceiptDomain, kv.RCacheDomain} {
		g.Go(func() error {
			tx, err := db.BeginTemporalRo(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			return state.AggTx(tx).IntegrityHistoryValueIndex(ctx, d, failFast, fromStep)
		})
	}
	return g.Wait()
}
