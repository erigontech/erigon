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

package execmodule

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/shards"
)

// PipelineExecutor centralises all staged sync pipeline invocations:
// ProcessFrozenBlocks (startup), RunLoop (FCU catchup), and ValidateBlock
// (fork validation). It is created once and stored on ExecModule.
type PipelineExecutor struct {
	sync                    *stagedsync.Sync
	db                      kv.TemporalRwDB
	blockReader             dbservices.FullBlockReader
	chainConfig             *chain.Config
	engine                  rules.Engine
	validationSync          *stagedsync.Sync
	validationNotifications *shards.Notifications
	dispatcher              *Dispatcher
	logger                  log.Logger

	// initialStateReady, when non-nil, is awaited in ProcessFrozenBlocks
	// BEFORE BeginTemporalRw. The storage component (in V2 mode,
	// --snap.lifecycle-driven-by-storage) owns post-download MDBX
	// seeding (OpenFolder, FillDBFromSnapshots, etc.) and runs those
	// in its own RW tx via the orchestrator's postIndexed callback.
	// MDBX is single-writer; if ProcessFrozenBlocks opens its tx
	// BEFORE the orchestrator's postIndexed completes, the two
	// writers deadlock. Set via SetInitialStateReady from production
	// wiring (backend.go) after the storage Provider's Initialize.
	// nil → no wait (legacy mode, non-V2 paths).
	initialStateReady <-chan struct{}

	// pendingSnapshotReconcile is set by the mode-B unwind path after
	// it has trimmed snapshot files. The next RunLoop reads + clears it
	// and passes FirstCycle=true to Sync.Run so OtterSync's
	// DownloadAndIndexSnapshotsIfNeed (gated on IsFirstCycle in
	// stage_snapshots.go) re-runs ReconcilePreverifiedAgainstDisk and
	// re-fetches the trimmed files. Without this signal a mid-life
	// mode-B unwind leaves a permanent snapshot gap — OtterSync's
	// reconciliation only runs on the very first cycle of the process.
	pendingSnapshotReconcile atomic.Bool
}

// SetInitialStateReady installs the orchestrator's "post-download work
// complete" signal. ProcessFrozenBlocks awaits this BEFORE opening its
// RW tx so the storage component's postIndexed callback (which needs
// its own RW tx to run FillDBFromSnapshots) can complete first. Must
// be called before any ProcessFrozenBlocks invocation; production
// wires this in backend.go after storage.Initialize.
func (pe *PipelineExecutor) SetInitialStateReady(ch <-chan struct{}) {
	pe.initialStateReady = ch
}

// SignalSnapshotReconcileNeeded marks the next RunLoop cycle as
// requiring a snapshot reconciliation pass. Called by the mode-B
// unwind path after it trims snapshot files. Idempotent.
func (pe *PipelineExecutor) SignalSnapshotReconcileNeeded() {
	pe.pendingSnapshotReconcile.Store(true)
}

// takeSnapshotReconcileSignal atomically reads and clears the pending
// flag. The caller passes the returned value as FirstCycle to the next
// Sync.Run so OtterSync's DownloadAndIndexSnapshotsIfNeed runs.
func (pe *PipelineExecutor) takeSnapshotReconcileSignal() bool {
	return pe.pendingSnapshotReconcile.Swap(false)
}

// NewPipelineExecutor creates a new executor. validationSync may be nil
// if fork validation is not needed (e.g. in tests that skip ValidateChain).
// validationNotifications must be the same object that was passed to
// NewInMemoryExecution when creating validationSync, so that state changes
// accumulated during fork validation are visible to the ForkValidator.
func NewPipelineExecutor(
	sync *stagedsync.Sync,
	db kv.TemporalRwDB,
	blockReader dbservices.FullBlockReader,
	chainConfig *chain.Config,
	engine rules.Engine,
	validationSync *stagedsync.Sync,
	validationNotifications *shards.Notifications,
	dispatcher *Dispatcher,
	logger log.Logger,
) *PipelineExecutor {
	return &PipelineExecutor{
		sync:                    sync,
		db:                      db,
		blockReader:             blockReader,
		chainConfig:             chainConfig,
		engine:                  engine,
		validationSync:          validationSync,
		validationNotifications: validationNotifications,
		dispatcher:              dispatcher,
		logger:                  logger,
	}
}

// ValidationNotifications returns the notifications object used by the
// validation pipeline. The ForkValidator uses this as extendingForkNotifications
// so that state changes accumulated during ValidateBlock are available for
// MergeExtendingFork to copy to the main accumulator.
func (pe *PipelineExecutor) ValidationNotifications() *shards.Notifications {
	return pe.validationNotifications
}

// Dispatcher returns the notification dispatcher for sending state-change
// notifications. Used by FCU commit paths and Hook.
func (pe *PipelineExecutor) Dispatcher() *Dispatcher {
	return pe.dispatcher
}

// Sync returns the main pipeline Sync object. Needed for PrevUnwindPoint().
func (pe *PipelineExecutor) Sync() *stagedsync.Sync {
	return pe.sync
}

// UnwindTo sets the unwind point on the main pipeline.
func (pe *PipelineExecutor) UnwindTo(unwindPoint uint64, reason stagedsync.UnwindReason, tx kv.Tx) error {
	return pe.sync.UnwindTo(unwindPoint, reason, tx)
}

// RunUnwind executes a pending unwind on the main pipeline.
func (pe *PipelineExecutor) RunUnwind(sd *execctx.SharedDomains, tx kv.TemporalRwTx) error {
	return pe.sync.RunUnwind(sd, tx)
}

// RunPrune executes pruning on the main pipeline.
func (pe *PipelineExecutor) RunPrune(ctx context.Context, tx kv.RwTx, initialCycle bool, timeout time.Duration) error {
	return pe.sync.RunPrune(ctx, tx, initialCycle, timeout)
}

// Commits sd and, if another iteration follows, returns a fresh tx+SD to run it on; (nil,nil,nil) leaves the loop unchanged.
type CommitCycleFn func(ctx context.Context, hasMore bool, sd *execctx.SharedDomains) (kv.TemporalRwTx, *execctx.SharedDomains, error)

// PruneFn replaces the in-loop pe.sync.RunPrune call. It is called after
// pe.sync.Run and before CommitCycle. Implementations typically close the
// read-side tx (if separate) and run pruning on a tx of their choosing.
type PruneFn func(ctx context.Context, initialCycle bool, rwtx kv.TemporalRwTx, sd *execctx.SharedDomains) error

// ShouldBreakFn is an optional callback to stop the loop early (return true).
type ShouldBreakFn func(tx kv.TemporalRwTx) (bool, error)

// RunLoopConfig configures a single RunLoop invocation.
type RunLoopConfig struct {
	InitialCycle bool
	FirstCycle   bool
	CommitCycle  CommitCycleFn
	PruneFn      PruneFn
	ShouldBreak  ShouldBreakFn // optional
}

// RunLoop runs sync.Run → PruneFn → ShouldBreak → CommitCycle in a hasMore loop.
// Exits when Run returns hasMore=false, ShouldBreak returns true, or on error.
// Returns the final tx and operational SD, owned by the caller (commit if CommitCycle didn't, then close). Intermediate SDs are closed here.
//
// Watchdog: if hasMore keeps saying "more work" but stages.Execution isn't
// advancing across runLoopStuckAbort iterations, aborts with an error. Prior
// history: post-mode-B recovery where a stage returned ErrLoopExhausted every
// iteration while exec applied zero blocks, silently starving concurrent
// callers of the exec semaphore for 5.4h before the soak's own timeout fired.
func (pe *PipelineExecutor) RunLoop(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, cfg RunLoopConfig) (kv.TemporalRwTx, *execctx.SharedDomains, error) {
	stop := false
	var stuckIters uint64
	lastExecProgress, _ := stages.GetStageProgress(tx, stages.Execution)
	for hasMore := true; hasMore && !stop; {
		var err error
		hasMore, err = pe.sync.Run(sd, tx, cfg.InitialCycle, cfg.FirstCycle)
		if err != nil {
			return tx, sd, err
		}

		if err := cfg.PruneFn(ctx, cfg.InitialCycle, tx, sd); err != nil {
			return tx, sd, err
		}

		if cfg.ShouldBreak != nil {
			stop, err = cfg.ShouldBreak(tx)
			if err != nil {
				return tx, sd, err
			}
		}

		newTx, newSD, err := cfg.CommitCycle(ctx, hasMore, sd)
		if err != nil {
			return tx, sd, err
		}
		if newTx != nil {
			tx = newTx
		}
		// CommitCycle committed sd and handed back a fresh one for the next
		// iteration: close the spent SD and continue on the fresh one. The exit
		// SD (newSD == nil) is returned for the caller to finalize, never closed
		// here.
		if newSD != nil {
			sd.Close()
			sd = newSD
		}

		if hasMore && !stop {
			curExecProgress, _ := stages.GetStageProgress(tx, stages.Execution)
			var action watchdogAction
			stuckIters, lastExecProgress, action = watchdogStep(stuckIters, lastExecProgress, curExecProgress)
			switch action {
			case watchdogWarn:
				pe.logger.Warn("[PipelineExecutor] RunLoop hasMore=true with no exec progress",
					"iterations", stuckIters, "stages.Execution", curExecProgress,
					"initialCycle", cfg.InitialCycle, "firstCycle", cfg.FirstCycle)
			case watchdogAbort:
				return tx, sd, fmt.Errorf("PipelineExecutor.RunLoop: watchdog: %d iterations with hasMore=true and stages.Execution stalled at %d", stuckIters, curExecProgress)
			}
		}
	}
	return tx, sd, nil
}

// runLoopStuckWarn / runLoopStuckAbort bound the RunLoop watchdog. Cheap
// stages take ~400µs, so 1000 iterations ≈ 400ms of stall (WARN) and 10000
// ≈ 4s (ABORT). Real work that legitimately spans many iterations advances
// stages.Execution and resets the counter.
const (
	runLoopStuckWarn  uint64 = 1000
	runLoopStuckAbort uint64 = 10000
)

type watchdogAction int

const (
	watchdogContinue watchdogAction = iota
	watchdogWarn
	watchdogAbort
)

// watchdogStep advances the RunLoop stall counter and reports the action the
// caller should take. Progress advance resets the counter; the WARN action
// fires exactly once at runLoopStuckWarn, ABORT fires at every iteration
// from runLoopStuckAbort on so the caller can't accidentally suppress it.
func watchdogStep(stuckIters, lastExecProgress, curExecProgress uint64) (uint64, uint64, watchdogAction) {
	if curExecProgress > lastExecProgress {
		return 0, curExecProgress, watchdogContinue
	}
	stuckIters++
	switch {
	case stuckIters >= runLoopStuckAbort:
		return stuckIters, lastExecProgress, watchdogAbort
	case stuckIters == runLoopStuckWarn:
		return stuckIters, lastExecProgress, watchdogWarn
	default:
		return stuckIters, lastExecProgress, watchdogContinue
	}
}

// ProcessFrozenBlocks runs the pipeline over snapshot blocks at startup.
// It downloads block files, then executes them in a hasMore loop until
// all frozen blocks are processed.
func (pe *PipelineExecutor) ProcessFrozenBlocks(ctx context.Context, hook *stageloop.Hook, onlySnapDownload bool) error {
	sawZeroBlocksTimes := 0

	// In V2 mode (storage owns post-download), wait for the storage
	// component's InitialStateReady before opening our RW tx. The
	// orchestrator's postIndexed callback runs FillDBFromSnapshots in
	// its own RW tx during this window; opening ours concurrently
	// would deadlock MDBX's single-writer slot (observed 2026-05-19:
	// goroutine 142 stuck in mdbx_txn_begin for 43min behind
	// stage_snapshots.go's open tx). By the time the signal fires,
	// storage has committed all its writes; we open a fresh tx and
	// run stages normally.
	if pe.initialStateReady != nil {
		select {
		case <-pe.initialStateReady:
			pe.logger.Info("[exec] storage signalled initialStateReady, opening framework tx")
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	tx, err := pe.db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	// Closure form: CommitCycle reassigns tx across iterations, so we need
	// to roll back the current value at function exit, not the original.
	defer func() { tx.Rollback() }()

	// Run snapshots stage — downloads block files.
	if err = pe.sync.RunSnapshots(nil, tx); err != nil {
		return err
	}
	if onlySnapDownload {
		return nil
	}

	// If domains are ahead of block files, nothing to execute.
	if execctx.IsDomainAheadOfBlocks(ctx, tx, pe.logger) {
		return tx.Commit()
	}

	doms, err := execctx.NewSharedDomains(ctx, tx, pe.logger)
	if err != nil {
		return err
	}
	defer func() { doms.Close() }() // RunLoop rotates doms; close whichever is current at exit
	doms.SetInMemHistoryReads(inMemHistoryReads)

	var finishStageBeforeSync uint64
	if hook != nil {
		finishStageBeforeSync, err = stages.GetStageProgress(tx, stages.Finish)
		if err != nil {
			return err
		}
		if err = hook.BeforeRun(tx, false); err != nil {
			return err
		}
	}

	tx, doms, err = pe.RunLoop(ctx, doms, tx, RunLoopConfig{
		InitialCycle: true,
		PruneFn: func(ctx context.Context, initialCycle bool, rwtx kv.TemporalRwTx, sd *execctx.SharedDomains) error {
			return pe.sync.RunPrune(ctx, rwtx, initialCycle, 0)
		},
		CommitCycle: func(ctx context.Context, hasMore bool, sd *execctx.SharedDomains) (kv.TemporalRwTx, *execctx.SharedDomains, error) {
			// The spent SD is closed by RunLoop; a fresh one opens for the next cycle.
			if err := sd.Commit(ctx, tx); err != nil {
				return nil, nil, fmt.Errorf("ProcessFrozenBlocks: flush+commit: %w", err)
			}
			// Prune runs via PruneFn (sync.RunPrune); kick file building so
			// snapshot files advance as PFB processes frozen blocks.
			if hasAgg, ok := pe.db.(dbstate.HasAgg); ok {
				if agg, ok := hasAgg.Agg().(*dbstate.Aggregator); ok && agg != nil {
					agg.BuildFilesInBackground(agg.EndTxNumMinimax() + agg.StepSize())
				}
			}
			// Last iter: skip BeginTemporalRw — no next iter will use it.
			if !hasMore {
				return nil, nil, nil
			}
			newTx, err := pe.db.BeginTemporalRw(ctx) //nolint:gocritic
			if err != nil {
				return nil, nil, err
			}
			tx = newTx
			newSD, err := execctx.NewSharedDomains(ctx, newTx, pe.logger)
			if err != nil {
				return nil, nil, err
			}
			newSD.SetInMemHistoryReads(inMemHistoryReads)
			hook.NotifySyncState(newTx)
			return newTx, newSD, nil
		},
		ShouldBreak: func(curTx kv.TemporalRwTx) (bool, error) {
			if pe.blockReader.FrozenBlocks() > 0 {
				p, err := stages.GetStageProgress(curTx, stages.Finish)
				if err != nil {
					return false, err
				}
				return p >= pe.blockReader.FrozenBlocks(), nil
			}
			sawZeroBlocksTimes++
			return sawZeroBlocksTimes > 2, nil
		},
	})
	if err != nil {
		return fmt.Errorf("ProcessFrozenBlocks: %w", err)
	}

	if hook != nil {
		if err := pe.db.View(ctx, func(tx kv.Tx) error {
			headersProgress, err := stages.GetStageProgress(tx, stages.Headers)
			if err != nil {
				return err
			}
			// Before UpdateHead, which publishes the sync state computed from it.
			hook.LastNewBlockSeen(headersProgress)
			if err = hook.SendNotifications(tx, finishStageBeforeSync); err != nil {
				return err
			}
			if err = hook.UpdateHead(tx, finishStageBeforeSync, false); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// ValidateBlock executes a fork validation by running the pipeline block-by-block
// over a side fork. All pipeline execution goes through PipelineExecutor.
func (pe *PipelineExecutor) ValidateBlock(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody) error {
	// Use a terse logger to suppress low-level noise during fork validation.
	// Defaults to LvlWarn (matching the original hard-coded level), but can
	// be overridden via dbg.ExecTerseLoggerLevel for debugging — Erigon's
	// logging has no per-subsystem level control, so this env-var-driven
	// override is the only way to selectively expose validation internals.
	terseLogger := log.New()
	terseLogger.SetHandler(log.LvlFilterHandler(log.Lvl(dbg.ExecTerseLoggerLevel), log.StderrHandler))

	chainReader := consensuschain.NewReader(pe.chainConfig, tx, pe.blockReader, terseLogger)

	// Reset the validation pipeline to stage 0 so it re-runs all stages.
	// The Sync object is reused across validations but RunNoInterrupt leaves
	// currentStage at the end after each run.
	if err := pe.validationSync.SetCurrentStage(stages.Headers); err != nil {
		return err
	}

	if err := stageloop.StateStep(ctx, chainReader, pe.engine, sd, tx, pe.validationSync, unwindPoint, headersChain, bodiesChain); err != nil {
		pe.logger.Warn("Could not validate block", "err", err)
		return err
	}

	progress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	lastNum := headersChain[len(headersChain)-1].Number.Uint64()
	if progress < lastNum {
		return fmt.Errorf("unsuccessful execution, progress %d < expected %d", progress, lastNum)
	}
	return nil
}
