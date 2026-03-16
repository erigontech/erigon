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
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync"
)

// PipelineExecutor runs the staged sync pipeline in a hasMore loop,
// abstracting the common pattern shared by ProcessFrozenBlocks (startup
// catchup) and updateForkChoice (FCU catchup). The executor is stateless
// between calls — callers provide their own tx/SD lifecycle and commit
// strategy via callbacks.
type PipelineExecutor struct {
	sync   *stagedsync.Sync
	logger log.Logger
}

// NewPipelineExecutor creates a new executor wrapping the given pipeline.
func NewPipelineExecutor(sync *stagedsync.Sync, logger log.Logger) *PipelineExecutor {
	return &PipelineExecutor{sync: sync, logger: logger}
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
	SD              *execctx.SharedDomains
	Tx              kv.TemporalRwTx
	InitialCycle    bool
	FirstCycle      bool
	PruneTimeout    time.Duration
	CommitCycle     CommitCycleFn     // required when hasMore
	ShouldBreak     ShouldBreakFn     // optional early exit
	BeforeIteration BeforeIterationFn // optional per-iteration setup
}

// RunLoopResult contains the state after RunLoop completes.
type RunLoopResult struct {
	// FinalTx is the tx after the last iteration. May differ from the
	// input Tx if CommitCycle was called (which returns a fresh tx).
	FinalTx kv.TemporalRwTx
}

// RunLoop executes the pipeline in a hasMore loop:
//
//	for hasMore {
//	    [BeforeIteration] → sync.Run → if hasMore { RunPrune → [ShouldBreak] → CommitCycle }
//	}
//
// The loop continues until Run returns hasMore=false, ShouldBreak returns
// true, or an error occurs. On error, FinalTx is the tx from the last
// iteration (may be used by the caller for error recovery reads).
func (pe *PipelineExecutor) RunLoop(ctx context.Context, cfg RunLoopConfig) (RunLoopResult, error) {
	tx := cfg.Tx
	for hasMore := true; hasMore; {
		if cfg.BeforeIteration != nil {
			cfg.BeforeIteration(cfg.SD)
		}

		var err error
		hasMore, err = pe.sync.Run(cfg.SD, tx, cfg.InitialCycle, cfg.FirstCycle)
		if err != nil {
			return RunLoopResult{FinalTx: tx}, err
		}

		if hasMore {
			if err := pe.sync.RunPrune(ctx, tx, cfg.InitialCycle, cfg.PruneTimeout); err != nil {
				return RunLoopResult{FinalTx: tx}, err
			}

			if cfg.ShouldBreak != nil {
				stop, err := cfg.ShouldBreak(tx)
				if err != nil {
					return RunLoopResult{FinalTx: tx}, err
				}
				if stop {
					break
				}
			}

			newTx, err := cfg.CommitCycle(ctx, cfg.SD)
			if err != nil {
				return RunLoopResult{FinalTx: tx}, err
			}
			tx = newTx
		}
	}
	return RunLoopResult{FinalTx: tx}, nil
}
