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
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
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

// CommitCycleFn is called between hasMore iterations to persist accumulated
// state and obtain a fresh tx for the next iteration. Implementations must
// flush the SharedDomains, commit, and return a fresh kv.TemporalRwTx.
type CommitCycleFn func(ctx context.Context, sd *execctx.SharedDomains) (kv.TemporalRwTx, error)

// ShouldBreakFn is called after each iteration (after prune, before commit)
// to check whether the loop should stop early. Return true to break.
type ShouldBreakFn func(tx kv.TemporalRwTx) (bool, error)

// BeforeIterationFn is called before each pipeline Run (e.g. to set state cache).
type BeforeIterationFn func(sd *execctx.SharedDomains)

// RunLoopConfig configures a single RunLoop invocation.
type RunLoopConfig struct {
	InitialCycle    bool
	FirstCycle      bool
	PruneTimeout    time.Duration
	CommitCycle     CommitCycleFn     // required when hasMore
	ShouldBreak     ShouldBreakFn     // optional early exit
	BeforeIteration BeforeIterationFn // optional per-iteration setup
}

// RunLoop executes the pipeline in a hasMore loop:
//
//	for hasMore {
//	    [BeforeIteration] → sync.Run → RunPrune → [ShouldBreak] → if hasMore { CommitCycle }
//	}
//
// RunPrune runs unconditionally on every iteration (including the last) to
// match the original stageloop.ProcessFrozenBlocks behaviour. The loop
// continues until Run returns hasMore=false, ShouldBreak returns true, or
// an error occurs. The returned tx may differ from the input tx if
// CommitCycle was called (which returns a fresh tx).
func (pe *PipelineExecutor) RunLoop(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, cfg RunLoopConfig) (kv.TemporalRwTx, error) {
	for hasMore := true; hasMore; {
		if cfg.BeforeIteration != nil {
			cfg.BeforeIteration(sd)
		}

		var err error
		hasMore, err = pe.sync.Run(sd, tx, cfg.InitialCycle, cfg.FirstCycle)
		if err != nil {
			return tx, err
		}

		if err := pe.sync.RunPrune(ctx, tx, cfg.InitialCycle, cfg.PruneTimeout); err != nil {
			return tx, err
		}

		if cfg.ShouldBreak != nil {
			stop, err := cfg.ShouldBreak(tx)
			if err != nil {
				return tx, err
			}
			if stop {
				break
			}
		}

		if hasMore {
			newTx, err := cfg.CommitCycle(ctx, sd)
			if err != nil {
				return tx, err
			}
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
	defer tx.Rollback()

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
		InitialCycle: true,
		FirstCycle:   false,
		PruneTimeout: 0,
		CommitCycle: func(ctx context.Context, sd *execctx.SharedDomains) (kv.TemporalRwTx, error) {
			if err := sd.Flush(ctx, tx); err != nil {
				return nil, fmt.Errorf("ProcessFrozenBlocks: flush: %w", err)
			}
			sd.ClearRam(true)
			if err := tx.Commit(); err != nil {
				return nil, err
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

	if err := doms.Flush(ctx, tx); err != nil {
		return fmt.Errorf("ProcessFrozenBlocks: final flush: %w", err)
	}
	doms.ClearRam(true)
	if err := tx.Commit(); err != nil {
		return err
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
