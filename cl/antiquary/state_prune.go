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

package antiquary

import (
	"context"
	"time"

	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

var (
	mxAntiquaryStatePruneBatchSeconds = metrics.GetOrCreateSummary(`caplin_antiquary_batch_seconds{phase="state_prune"}`)
	mxAntiquaryPrunedStateRows        = metrics.GetOrCreateCounter("caplin_antiquary_pruned_state_rows_total")
)

// pruneStateTables deletes state rows below each table's boundary, visiting
// tables once starting at startIdx and wrapping. Deletes are committed in
// batchLimit-sized transactions with the per-table marker persisted in the same
// transaction. ctx is checked only between transactions; on expiry the current
// table index is returned as nextStartIdx so the next call resumes there.
func pruneStateTables(ctx context.Context, db kv.RwDB, tables []string, boundaryFn func(table string) uint64, startIdx, batchLimit int, logger log.Logger) (nextStartIdx int, err error) {
	if len(tables) == 0 {
		return 0, nil
	}
	if startIdx < 0 || startIdx >= len(tables) {
		startIdx = 0
	}
	for i := range tables {
		idx := (startIdx + i) % len(tables)
		table := tables[idx]
		if ctx.Err() != nil {
			return idx, nil
		}
		boundary := boundaryFn(table)
		if boundary == 0 {
			continue
		}
		var marker uint64
		if err := db.View(ctx, func(tx kv.Tx) error {
			var err error
			marker, err = state_accessors.ReadStatePruneProgress(tx, table)
			return err
		}); err != nil {
			if ctx.Err() != nil {
				return idx, nil
			}
			return idx, err
		}
		if marker >= boundary {
			continue
		}
		pruneFrom := marker
		totalDeleted := 0
		for marker < boundary {
			deleted, newMarker, err := pruneStateBatch(ctx, db, table, marker, boundary, batchLimit, logger)
			if err != nil {
				if ctx.Err() != nil {
					return idx, nil
				}
				return idx, err
			}
			totalDeleted += deleted
			marker = newMarker
			if marker < boundary && ctx.Err() != nil {
				return idx, nil
			}
		}
		logger.Info("[Antiquary] Pruned state table to boundary", "table", table, "from", pruneFrom, "boundary", boundary, "deleted", totalDeleted)
	}
	return startIdx, nil
}

func pruneStateBatch(ctx context.Context, db kv.RwDB, table string, marker, boundary uint64, batchLimit int, logger log.Logger) (deleted int, newMarker uint64, err error) {
	start := time.Now()
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return 0, marker, err
	}
	defer tx.Rollback()

	c, err := tx.RwCursor(table)
	if err != nil {
		return 0, marker, err
	}
	defer c.Close()

	newMarker = marker
	k, _, err := c.Seek(base_encoding.Encode64ToBytes4(marker))
	for {
		if err != nil {
			return deleted, marker, err
		}
		if k == nil || base_encoding.Decode64FromBytes4(k) >= boundary {
			// no keys left below boundary: jump the marker to it, or tables
			// with sparse/rounded keys would be rescanned as backlog forever
			newMarker = boundary
			break
		}
		slot := base_encoding.Decode64FromBytes4(k)
		if err = c.DeleteCurrent(); err != nil {
			return deleted, marker, err
		}
		deleted++
		newMarker = slot + 1
		if deleted >= batchLimit {
			break
		}
		k, _, err = c.Next()
	}
	c.Close()
	if err = state_accessors.SetStatePruneProgress(tx, table, newMarker); err != nil {
		return deleted, marker, err
	}
	if err = tx.Commit(); err != nil {
		return deleted, marker, err
	}
	mxAntiquaryStatePruneBatchSeconds.ObserveDuration(start)
	if deleted > 0 {
		mxAntiquaryPrunedStateRows.AddInt(deleted)
	}
	logger.Debug("[Antiquary] Pruned state batch", "table", table, "from", marker, "to", newMarker, "deleted", deleted, "elapsed", time.Since(start))
	return deleted, newMarker, nil
}
