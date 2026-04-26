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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

var (
	// storage_bg_collate_ticks counts every collator wake-up (work or no-op).
	// Lets operators verify the bg loop is actually firing — at startup the
	// first tick is the cheap no-op confirmation; beyond that the value
	// climbs at roughly collateInterval^-1.
	mxBgCollateTicks = metrics.GetOrCreateCounter("storage_bg_collate_ticks")
	// storage_bg_collate_runs counts ticks where CollateAndPruneIfNeeded did
	// real work (returned without short-circuiting on stepsInDB threshold).
	// Inferred from non-zero duration; coarse but useful as a kick-rate signal.
	mxBgCollateRuns = metrics.GetOrCreateCounter("storage_bg_collate_runs")
	// storage_bg_collate_errors counts ticks that returned an error (excluding
	// context.Canceled on shutdown).
	mxBgCollateErrors = metrics.GetOrCreateCounter("storage_bg_collate_errors")
)

// PruneFn is the prune callback supplied by the execution layer. It is invoked
// inside an RW tx that the Aggregator opens via CollateAndPruneIfNeeded.
// Equivalent to calling sync.RunPrune(ctx, tx, false, 0).
type PruneFn func(ctx context.Context, tx kv.TemporalRwTx) error

// backgroundLoop owns the long-running goroutines that build snapshot files
// and prune MDBX. Stage A.1: only collation+prune is here (CollateAndPruneIfNeeded).
// Stage A.2 will add BlockRetire. Stage B will add foreground-priority yielding.
//
// Coordination with foreground work (FCU commit, etc.) is handled today by
// the existing Aggregator.commitGate (CollateAndPrune holds Lock around its
// prune+commit, foreground holds Lock around BeginRw). No additional gating
// is needed at this stage.
type backgroundLoop struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	db      kv.TemporalRwDB
	agg     *dbstate.Aggregator
	pruneFn PruneFn
	logger  log.Logger

	// collateInterval is the wall-clock interval between collation attempts.
	// CollateAndPruneIfNeeded is itself gated on stepsInDB > 1.5 so each tick
	// is cheap when there is no work to do. 5s is fast enough to keep up at
	// chain tip without monopolising the CPU during quiet periods.
	collateInterval time.Duration

	// firstTickLogged ensures we emit exactly one Info log when the loop's
	// first tick completes — operators want a visible confirmation that the
	// bg loop is alive without log spam from the per-tick path.
	firstTickLogged atomic.Bool
}

func newBackgroundLoop(parent context.Context, db kv.TemporalRwDB, agg *dbstate.Aggregator, pruneFn PruneFn, logger log.Logger) *backgroundLoop {
	ctx, cancel := context.WithCancel(parent)
	return &backgroundLoop{
		ctx:             ctx,
		cancel:          cancel,
		db:              db,
		agg:             agg,
		pruneFn:         pruneFn,
		logger:          logger,
		collateInterval: 5 * time.Second,
	}
}

// Start spawns the worker goroutines. Idempotent only in the sense that
// repeated calls would spawn extra goroutines — the caller (Provider) ensures
// it is invoked at most once per Storage lifecycle.
func (b *backgroundLoop) Start() {
	b.wg.Add(1)
	go b.runCollator()
}

// Stop cancels the background context and waits for all workers to exit.
// Safe to call from Provider.Close().
func (b *backgroundLoop) Stop() {
	b.cancel()
	b.wg.Wait()
}

// runCollator periodically invokes CollateAndPruneIfNeeded. The Aggregator
// internally checks stepsInDB > targetSteps before doing real work, so this
// tick is essentially a poll for "is there enough accumulated data to merit
// a collation+prune cycle". On work, it builds new snapshot files from MDBX
// domains and prunes the steps that are now in files.
func (b *backgroundLoop) runCollator() {
	defer b.wg.Done()
	t := time.NewTicker(b.collateInterval)
	defer t.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-t.C:
			mxBgCollateTicks.Inc()
			started := time.Now()
			err := b.agg.CollateAndPruneIfNeeded(b.ctx, b.db, func(tx kv.TemporalRwTx) error {
				return b.pruneFn(b.ctx, tx)
			}, b.logger)
			took := time.Since(started)
			// CollateAndPruneIfNeeded short-circuits cheaply (single Aggregator
			// gauge read) when stepsInDB <= target. Anything taking >50 ms is
			// real work — count it as a "run" for the kick-rate metric.
			if took > 50*time.Millisecond {
				mxBgCollateRuns.Inc()
			}
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				mxBgCollateErrors.Inc()
				b.logger.Warn("storage bg: CollateAndPruneIfNeeded", "err", err, "took", took)
			} else if b.firstTickLogged.CompareAndSwap(false, true) {
				b.logger.Info("storage bg: collator alive", "interval", b.collateInterval, "first_tick_took", took)
			}
		}
	}
}
