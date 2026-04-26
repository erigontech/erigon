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
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
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
	// storage_bg_collate_yields counts ticks skipped because a foreground
	// priority caller is active (Stage B's WithForegroundPriority).
	mxBgCollateYields = metrics.GetOrCreateCounter("storage_bg_collate_yields")

	// storage_bg_retire_ticks counts every retire-loop wake-up (work or no-op).
	mxBgRetireTicks = metrics.GetOrCreateCounter("storage_bg_retire_ticks")
	// storage_bg_retire_starts counts ticks that successfully kicked an async
	// RetireBlocksInBackground (i.e. there was new work and the prior retire
	// goroutine had finished). Compare to ticks for the kick-success rate.
	mxBgRetireStarts = metrics.GetOrCreateCounter("storage_bg_retire_starts")
	// storage_bg_retire_errors counts ticks that returned an error from
	// PruneAncientBlocks (excluding context.Canceled on shutdown).
	mxBgRetireErrors = metrics.GetOrCreateCounter("storage_bg_retire_errors")
	// storage_bg_retire_yields counts ticks skipped because a foreground
	// priority caller is active.
	mxBgRetireYields = metrics.GetOrCreateCounter("storage_bg_retire_yields")
	// storage_bg_retire_pruned counts blocks pruned across all PruneAncientBlocks
	// calls — useful to confirm the bg loop is actually shrinking MDBX over time.
	mxBgRetirePruned = metrics.GetOrCreateCounter("storage_bg_retire_pruned")
)

// RetireDeps wires the bg retire+prune worker. All fields are required when
// StartRetireLoop is called; pass NoopSeederClient for GetSeeder if no
// downloader is attached. Notifier callbacks may be no-ops if nothing
// consumes them.
type RetireDeps struct {
	BlockRetire       services.BlockRetire
	GetMaxBlock       func(ctx context.Context) (uint64, error) // ForwardProgress (head)
	GetSeeder         func() downloader.SeederClient
	OnRetirementStart func(started bool)
	OnRetirementDone  func()
	OnNewSnapshot     func()
	// Interval between retire+prune attempts. Retire is heavier than collation
	// (file build + index work) and only useful when block snapshots advance,
	// so a longer cadence than collation (5 s) is appropriate. Defaults to 30 s
	// when zero.
	Interval time.Duration
}

// PruneFn is the prune callback supplied by the execution layer. It is invoked
// inside an RW tx that the Aggregator opens via CollateAndPruneIfNeeded.
// Equivalent to calling sync.RunPrune(ctx, tx, false, 0).
type PruneFn func(ctx context.Context, tx kv.TemporalRwTx) error

// backgroundLoop owns the long-running goroutines that build snapshot files,
// retire block data, and prune MDBX. Each worker is a separate goroutine
// (collator, retirer) ticking on its own cadence — see runCollator and
// runRetirer.
//
// Coordination with foreground work (FCU commit, etc.) is handled today by
// the existing Aggregator.commitGate (CollateAndPrune and PruneAncientBlocks
// hold Lock around their prune+commit, foreground holds Lock around BeginRw).
// fgPriority lets foreground callers signal "yield" so bg workers skip a tick.
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

	// retire holds the deps for the retire+prune worker. nil means no retire
	// worker is started — back-compat for callers that don't want it.
	retire *RetireDeps

	// firstTickLogged ensures we emit exactly one Info log when the loop's
	// first tick completes — operators want a visible confirmation that the
	// bg loop is alive without log spam from the per-tick path.
	firstTickLogged       atomic.Bool
	firstRetireTickLogged atomic.Bool

	// fgPriority counts active foreground-priority callers (Stage B). When
	// non-zero, bg workers skip the next tick to yield CPU/IO/lock budget.
	// Counter (not bool) so multiple callers stack — the gate releases only
	// when the last one decrements back to zero. Atomic for lock-free reads
	// from worker goroutines.
	fgPriority atomic.Int32
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

// Start spawns the collator goroutine. Idempotent only in the sense that
// repeated calls would spawn extra goroutines — the caller (Provider) ensures
// it is invoked at most once per Storage lifecycle.
func (b *backgroundLoop) Start() {
	b.wg.Add(1)
	go b.runCollator()
}

// AddRetirer attaches a retire+prune worker to the running bg loop.
// Must be called after Start (which spawns the collator); typically wired
// from Provider.StartRetireLoop after the Sync object exists. Spawning
// here rather than in Start lets the retire worker be opt-in: callers
// (tests, integration tools) that don't need block retirement skip it.
func (b *backgroundLoop) AddRetirer(deps RetireDeps) {
	b.retire = &deps
	b.wg.Add(1)
	go b.runRetirer()
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
			// Yield to foreground priority callers — when WithForegroundPriority
			// is active the counter is >0 and we skip this tick. The next tick
			// (5 s later) re-checks. This is coarse-grained yielding (per-tick,
			// not per-key) but enough for FCU-cadence interruption since a tick
			// rarely runs longer than a few hundred ms.
			if b.fgPriority.Load() > 0 {
				mxBgCollateYields.Inc()
				continue
			}
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

// runRetirer periodically kicks BlockRetire and prunes ancient blocks.
// RetireBlocksInBackground is itself async (spawns its own goroutine and uses
// an atomic working flag for idempotency), so this loop just provides the
// trigger cadence. PruneAncientBlocks runs synchronously inside a brief RwTx
// — uses small limit/timeout so foreground writes are never blocked for long.
//
// Notifier callbacks fire OnRetirementStart when bg-retire is kicked,
// OnRetirementDone when it finishes, OnNewSnapshot when files are pruned.
func (b *backgroundLoop) runRetirer() {
	defer b.wg.Done()
	r := b.retire
	interval := r.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	// Steady-state worker count — matches the non-initial branch of the old
	// SnapshotsPrune stage. Initial-cycle bulk retire still goes via that
	// stage with estimate.CompressSnapshot.Workers() (much higher).
	r.BlockRetire.SetWorkers(1)
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-t.C:
			mxBgRetireTicks.Inc()
			if b.fgPriority.Load() > 0 {
				mxBgRetireYields.Inc()
				continue
			}
			maxBlock, err := r.GetMaxBlock(b.ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				mxBgRetireErrors.Inc()
				b.logger.Warn("storage bg: retire GetMaxBlock", "err", err)
				continue
			}
			started := r.BlockRetire.RetireBlocksInBackground(
				b.ctx,
				0, // minBlockNum: 0 means "from current FrozenBlocks boundary"
				maxBlock,
				log.LvlDebug,
				r.GetSeeder(),
				func() error {
					// onFinishRetire: notify so downloader can re-seed deleted files
					if r.OnNewSnapshot != nil {
						r.OnNewSnapshot()
					}
					return nil
				},
				func() {
					if r.OnRetirementDone != nil {
						r.OnRetirementDone()
					}
				},
			)
			if started {
				mxBgRetireStarts.Inc()
				if r.OnRetirementStart != nil {
					r.OnRetirementStart(true)
				}
			}

			// Prune ancient blocks in a brief RwTx. limit/timeout match the
			// steady-state values previously used in SnapshotsPrune
			// (initialCycle=false branch). Foreground writes are not blocked
			// for more than the timeout window.
			deleted := 0
			pruneErr := b.db.Update(b.ctx, func(tx kv.RwTx) error {
				d, err := r.BlockRetire.PruneAncientBlocks(tx, 10, 125*time.Millisecond)
				deleted = d
				return err
			})
			if pruneErr != nil {
				if errors.Is(pruneErr, context.Canceled) {
					return
				}
				mxBgRetireErrors.Inc()
				b.logger.Warn("storage bg: PruneAncientBlocks", "err", pruneErr)
			} else {
				if deleted > 0 {
					mxBgRetirePruned.Add(float64(deleted))
				}
				if b.firstRetireTickLogged.CompareAndSwap(false, true) {
					b.logger.Info("storage bg: retirer alive", "interval", interval, "first_tick_pruned", deleted)
				}
			}
		}
	}
}
