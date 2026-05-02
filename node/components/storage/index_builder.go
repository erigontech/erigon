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

package storage

import (
	"context"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// productionIndexBuilder satisfies lifecycle.IndexBuilder by triggering
// the existing global BuildMissedIndices and BuildMissedAccessors
// invocations. Per-file calls from the lifecycle driver are coalesced
// into a single in-flight build via a mutex guard — multiple files
// arriving at LifecycleDownloaded only fire one global build cycle.
//
// After the build, OnFilesChange callbacks (registered separately on
// ChainDB) propagate newly-built file names into the Inventory; the
// driver's next sweep observes the satisfied dependencies and advances
// each file from LifecycleDownloaded → LifecycleIndexed.
//
// This is the storage-owned home for what was previously
// stage_snapshots.go's buildOrDeferE2Indices /
// buildOrDeferE3Accessors. Activated only when
// Config.Snapshot.LifecycleDrivenByStorage is true.
//
// E2 path uses BlockRetire.BuildMissedIndicesIfNeed; E3 path uses
// Aggregator.BuildMissedAccessors. Both are nil-safe: a nil
// dependency skips that side of the build (useful for tests and
// tools that only construct one half of the storage stack).
type productionIndexBuilder struct {
	blockRetire  services.BlockRetire
	agg          *state.Aggregator
	notifier     services.DBEventNotifier
	logger       log.Logger
	indexWorkers int

	// inFlight guards against re-entry. The first per-file call fires
	// the global build; concurrent calls return immediately and let
	// the in-flight invocation cover their files too. The driver's
	// next sweep retries any file whose deps still aren't visible.
	inFlight sync.Mutex
}

// BuildMissedIndices implements lifecycle.IndexBuilder.
//
// Invokes BlockRetire.BuildMissedIndicesIfNeed for E2 (block) indices
// and Aggregator.BuildMissedAccessors for E3 (state) accessors. The
// primary parameter is logged for traceability — both builders scan
// globally for missing files; a per-file API would require refactoring
// those builders, which is out of scope for this branch.
//
// Logs at Info on entry and exit so operators can distinguish
// storage-lifecycle-driven invocations from stage/retire-driven ones
// (those use different prefixes).
//
// Returns the first error from either build. Caller (lifecycle.Driver)
// leaves the file at LifecycleDownloaded for retry on next sweep.
func (b *productionIndexBuilder) BuildMissedIndices(ctx context.Context, primary *snapshot.FileEntry) error {
	b.inFlight.Lock()
	defer b.inFlight.Unlock()

	logger := b.logger
	if logger == nil {
		logger = log.Root()
	}

	primaryName := ""
	if primary != nil {
		primaryName = primary.Name
	}
	logger.Info("[storage-lifecycle] BuildMissedIndices start",
		"trigger", primaryName,
		"e2", b.blockRetire != nil,
		"e3", b.agg != nil)
	start := time.Now()

	if b.blockRetire != nil {
		if err := b.blockRetire.BuildMissedIndicesIfNeed(ctx, "storage-lifecycle", b.notifier); err != nil {
			logger.Warn("[storage-lifecycle] E2 BuildMissedIndicesIfNeed failed",
				"trigger", primaryName, "elapsed", time.Since(start), "err", err)
			return err
		}
	}
	if b.agg != nil {
		if err := b.agg.BuildMissedAccessors(ctx, b.indexWorkers); err != nil {
			logger.Warn("[storage-lifecycle] E3 BuildMissedAccessors failed",
				"trigger", primaryName, "elapsed", time.Since(start), "err", err)
			return err
		}
	}

	logger.Info("[storage-lifecycle] BuildMissedIndices done",
		"trigger", primaryName, "elapsed", time.Since(start))
	return nil
}
