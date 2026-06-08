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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
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
	blockReader             services.FullBlockReader
	chainConfig             *chain.Config
	engine                  rules.Engine
	validationSync          *stagedsync.Sync
	validationNotifications *shards.Notifications
	dispatcher              *Dispatcher
	logger                  log.Logger
	knownTipHint            atomic.Uint64
}

// NewPipelineExecutor creates a new executor. validationSync may be nil
// if fork validation is not needed (e.g. in tests that skip ValidateChain).
// validationNotifications must be the same object that was passed to
// NewInMemoryExecution when creating validationSync, so that state changes
// accumulated during fork validation are visible to the ForkValidator.
func NewPipelineExecutor(
	sync *stagedsync.Sync,
	db kv.TemporalRwDB,
	blockReader services.FullBlockReader,
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

// SetKnownTipHint records an external hint about where the chain head currently
// is. The single in-process caller is Caplin at startup, propagating either
// the latest beacon-state execution payload block number or a remote checkpoint
// sync head. The value is monotonic (max wins). Consumers (e.g. PruneInitialCycleFn)
// read it asynchronously each iteration — there is no blocking wait.
func (pe *PipelineExecutor) SetKnownTipHint(blockNum uint64) {
	if blockNum == 0 {
		return
	}
	for {
		cur := pe.knownTipHint.Load()
		if blockNum <= cur {
			return
		}
		if pe.knownTipHint.CompareAndSwap(cur, blockNum) {
			return
		}
	}
}

func (pe *PipelineExecutor) KnownTipHint() uint64 {
	return pe.knownTipHint.Load()
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

// CommitCycleFn is called after PruneFn to persist the iteration's writes
// and refresh the loop's tx. Implementations typically open a fresh RwTx,
// flush+clear the SharedDomains into it, commit, and re-open the read-side
// tx. hasMore tells the impl whether another iteration follows — useful to
// skip refreshing the tx on the final iter. May return (nil, nil) to leave
// the loop's tx unchanged.
type CommitCycleFn func(ctx context.Context, hasMore bool, sd *execctx.SharedDomains) (kv.TemporalRwTx, error)

// PruneFn replaces the in-loop pe.sync.RunPrune call. It is called after
// pe.sync.Run and before CommitCycle. Implementations typically close the
// read-side tx (if separate) and run pruning on a tx of their choosing.
type PruneFn func(ctx context.Context, initialCycle bool, rwtx kv.TemporalRwTx, sd *execctx.SharedDomains) error

// ShouldBreakFn is an optional callback to stop the loop early (return true).
type ShouldBreakFn func(tx kv.TemporalRwTx) (bool, error)

// PruneInitialCycleFn computes the initial-cycle flag passed to sync.RunPrune
// on each iteration. It is independent from RunLoopConfig.InitialCycle (which
// drives sync.Run / forward stages): forward keeps the original "true at startup,
// false after the first fast iteration" semantics, while prune uses the
// DB-backed lifecycle marker + --sync.initial-cycle-block-ttl. Returning true
// asks RunPrune to use the long-prune timeouts and unbounded limits.
type PruneInitialCycleFn func(tx kv.TemporalRwTx) (bool, error)

// AfterPruneFn is called after a successful RunPrune, before any commit.
// initialCycle is the value that was passed to RunPrune (the prune flag, not
// the forward flag).
type AfterPruneFn func(tx kv.TemporalRwTx, initialCycle bool) error

// BeforeIterationFn is called before each pipeline Run (e.g. to set state cache).
type BeforeIterationFn func(sd *execctx.SharedDomains)

// RunLoopConfig configures a single RunLoop invocation.
type RunLoopConfig struct {
	// InitialCycle drives forward stages (sync.Run): true at startup, set to
	// false by the caller after the first fast iteration. See StageLoop.
	InitialCycle bool
	FirstCycle   bool
	PruneTimeout time.Duration
	CommitCycle  CommitCycleFn // required when hasMore
	PruneFn      PruneFn       // required
	ShouldBreak  ShouldBreakFn // optional early exit
	// PruneInitialCycleFn picks the initial-cycle flag for sync.RunPrune. If
	// nil, PruneFn is called with InitialCycle (legacy behaviour).
	PruneInitialCycleFn PruneInitialCycleFn
	AfterPrune          AfterPruneFn      // optional post-prune hook
	BeforeIteration     BeforeIterationFn // optional per-iteration setup
}

// RunLoop runs sync.Run → PruneFn → ShouldBreak → CommitCycle in a hasMore loop.
// Exits when Run returns hasMore=false, ShouldBreak returns true, or on error.
func (pe *PipelineExecutor) RunLoop(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, cfg RunLoopConfig) (kv.TemporalRwTx, error) {
	stop := false
	for hasMore := true; hasMore && !stop; {
		var err error
		hasMore, err = pe.sync.Run(sd, tx, cfg.InitialCycle, cfg.FirstCycle)
		if err != nil {
			return tx, err
		}

		pruneInitialCycle := cfg.InitialCycle
		if cfg.PruneInitialCycleFn != nil {
			pruneInitialCycle, err = cfg.PruneInitialCycleFn(tx)
			if err != nil {
				return tx, err
			}
		}

		if err := cfg.PruneFn(ctx, pruneInitialCycle, tx, sd); err != nil {
			return tx, err
		}

		if cfg.AfterPrune != nil {
			if err := cfg.AfterPrune(tx, pruneInitialCycle); err != nil {
				return tx, err
			}
		}

		if cfg.ShouldBreak != nil {
			stop, err = cfg.ShouldBreak(tx)
			if err != nil {
				return tx, err
			}
		}

		newTx, err := cfg.CommitCycle(ctx, hasMore, sd)
		if err != nil {
			return tx, err
		}
		if newTx != nil {
			tx = newTx
		}
	}
	return tx, nil
}

// ProcessFrozenBlocks runs the pipeline over snapshot blocks at startup.
// It downloads block files, then executes them in a hasMore loop until
// all frozen blocks are processed.
func (pe *PipelineExecutor) ProcessFrozenBlocks(ctx context.Context, hook *stageloop.Hook, onlySnapDownload bool) error {
	sawZeroBlocksTimes := 0
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
		if err := stagedsync.UpdateTipReached(tx, pe.blockReader.FrozenBlocks(), pe.KnownTipHint()); err != nil {
			return err
		}
		return tx.Commit()
	}

	doms, err := execctx.NewSharedDomains(ctx, tx, pe.logger)
	if err != nil {
		return err
	}
	defer doms.Close()
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

	tx, err = pe.RunLoop(ctx, doms, tx, RunLoopConfig{
		// Forward stages: this is the snapshot-fill startup phase, always treat as initial.
		InitialCycle: true,
		FirstCycle:   false,
		PruneTimeout: 0,
		PruneFn: func(ctx context.Context, pruneInitialCycle bool, rwtx kv.TemporalRwTx, sd *execctx.SharedDomains) error {
			return pe.sync.RunPrune(ctx, rwtx, pruneInitialCycle, 0)
		},
		PruneInitialCycleFn: func(curTx kv.TemporalRwTx) (bool, error) {
			return stagedsync.IsInitialCycle(curTx, pe.sync.Cfg(), pe.blockReader.FrozenBlocks(), pe.KnownTipHint())
		},
		AfterPrune: func(curTx kv.TemporalRwTx, _ bool) error {
			return stagedsync.UpdateTipReached(curTx, pe.blockReader.FrozenBlocks(), pe.KnownTipHint())
		},
		CommitCycle: func(ctx context.Context, hasMore bool, sd *execctx.SharedDomains) (kv.TemporalRwTx, error) {
			if err := sd.Flush(ctx, tx); err != nil {
				return nil, fmt.Errorf("ProcessFrozenBlocks: flush: %w", err)
			}
			sd.ClearRam(true)
			if err := tx.Commit(); err != nil {
				return nil, err
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
				return nil, nil
			}
			newTx, err := pe.db.BeginTemporalRw(ctx) //nolint:gocritic
			if err != nil {
				return nil, err
			}
			tx = newTx
			return newTx, nil
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
			if err = hook.SendNotifications(tx, finishStageBeforeSync); err != nil {
				return err
			}
			if err = hook.UpdateHead(tx, finishStageBeforeSync, false); err != nil {
				return err
			}
			hook.LastNewBlockSeen(headersProgress)
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
