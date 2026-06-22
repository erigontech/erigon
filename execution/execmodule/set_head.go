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

package execmodule

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

func getLatestBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceHeadHash := rawdb.ReadForkchoiceHead(tx)
	if forkchoiceHeadHash != (common.Hash{}) {
		forkchoiceHeadNum := rawdb.ReadHeaderNumber(tx, forkchoiceHeadHash)
		if forkchoiceHeadNum != nil {
			return *forkchoiceHeadNum, nil
		}
	}

	blockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, fmt.Errorf("getting latest block number: %w", err)
	}

	return blockNum, nil
}

// SetHead rewinds the local chain to the specified block number by unwinding
// all staged sync stages. This is the core implementation used by debug_setHead.
func (e *ExecModule) SetHead(ctx context.Context, targetBlock uint64) error {
	// Wait for any in-flight execution to drain BEFORE acquiring the
	// semaphore. Holding the semaphore during the wait would block
	// UpdateForkChoice, which is the only path that clears
	// e.currentContext — a classic deadlock: SetHead waits for
	// currentContext to clear; FCU can't clear it because SetHead
	// holds the semaphore it needs. With the wait-then-acquire order,
	// an in-flight newPayload+FCU cycle completes naturally and we
	// acquire cleanly.
	if err := e.waitForQuiescence(ctx); err != nil {
		return err
	}

	// Acquire the semaphore. A new newPayload may slip in during the
	// gap between quiescence detection and acquire — re-check under
	// the semaphore. With it held, no new payloads/FCUs can start; we
	// only need to wait for any one in flight to drain.
	for {
		acquireCtx, acquireCancel := context.WithTimeout(ctx, 5*time.Second)
		err := e.semaphore.Acquire(acquireCtx, 1)
		acquireCancel()
		if err != nil {
			return fmt.Errorf("execution module is busy: %w", err)
		}
		e.lock.RLock()
		quiescent := e.currentContext == nil
		e.lock.RUnlock()
		if quiescent {
			break
		}
		// Lost the race — release so FCU can clear, then wait again.
		e.semaphore.Release(1)
		if err := e.waitForQuiescence(ctx); err != nil {
			return err
		}
	}
	defer e.semaphore.Release(1)

	tx, err := e.db.BeginTemporalRw(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin rw transaction: %w", err)
	}
	defer tx.Rollback()

	// Get the current head block number
	currentHead, err := getLatestBlockNumber(tx)
	if err != nil {
		return fmt.Errorf("failed to get current head: %w", err)
	}

	if targetBlock > currentHead {
		return fmt.Errorf("cannot set head to a future block: target %d, current head %d", targetBlock, currentHead)
	}

	if targetBlock == currentHead {
		return nil // already at the target
	}

	// Hard constraint: cannot setHead beyond the consensus
	// weak-subjectivity window. Below WS, peers aren't required to
	// serve the data and Caplin's restart-based reanchor cannot
	// converge.
	if e.setHeadMaxDepthBlocks > 0 && (currentHead-targetBlock) > e.setHeadMaxDepthBlocks {
		return fmt.Errorf("setHead target %d is %d blocks below head %d; exceeds the consensus weak-subjectivity window of %d blocks — Caplin cannot reanchor that deep",
			targetBlock, currentHead-targetBlock, currentHead, e.setHeadMaxDepthBlocks)
	}

	// Iteration-1 (experimental) stopgap: refuse mode-B unwinds whose
	// post-unwind gap to the next forward-pushed block would exceed
	// Caplin's bridgeable window. After mode-B unwind to target T past
	// the snapshot tip, Caplin restarts and checkpoint-syncs from
	// /finalized; its BlockCollector cache anchors at the new
	// finalised slot's block (well above T). The gap-prune Case C
	// FCU-nudge path assumes the gap blocks are in snapshot files
	// (see persistent_block_collector.go:480-487), but for T past the
	// snapshot tip they aren't — they were wiped by mode-B and aren't
	// frozen. The walk back from Caplin's cached high block then
	// runs into missing headers / missing bodies and wedges with
	// "append with gap". Refuse loudly with the actionable diagnostic
	// rather than silently wedging post-FCU. See
	// docs/plans/20260614-deep-mode-b-gap-bridging.md for the full
	// finding and the Option B medium-term fix that lifts this cap.
	frozenBlocks := e.blockReader.FrozenBlocks()
	if e.setHeadMaxModeBGapBlocks > 0 && targetBlock > frozenBlocks && (currentHead-targetBlock) > e.setHeadMaxModeBGapBlocks {
		return fmt.Errorf("setHead target %d would create a %d-block gap past snapshot tip %d that exceeds Caplin's bridgeable window of %d blocks — shallower targets or fresh-sync required (iter-1 stopgap; see docs/plans/20260614-deep-mode-b-gap-bridging.md)",
			targetBlock, currentHead-targetBlock, frozenBlocks, e.setHeadMaxModeBGapBlocks)
	}

	// Check if we can unwind that far back. minUnwindableBlock is the
	// boundary of the diffset window. Targets inside it ride the
	// existing incremental path (mode A); targets past it engage the
	// admin-unwind path (mode B) provided an Unwinder is wired in.
	// Mode B handles both aligned and non-aligned chains: aligned cuts
	// trim entire files at step boundaries that coincide with block
	// boundaries; non-aligned cuts keep the file containing toBlock
	// and use the writable shadow's boundary-step diff-replay (see
	// state.WipeWritableShadowPast). See
	// docs/plans/20260525-admin-sethead-unwind-design.md.
	minUnwindableBlock, err := rawtemporaldb.CanUnwindToBlockNum(tx)
	if err != nil {
		return fmt.Errorf("failed to check minimum unwindable block: %w", err)
	}
	if targetBlock < minUnwindableBlock {
		if e.unwinder == nil {
			return fmt.Errorf("cannot unwind to block %d: minimum unwindable block is %d (no admin Unwinder wired)", targetBlock, minUnwindableBlock)
		}
		return e.setHeadModeB(ctx, tx, targetBlock, currentHead)
	}

	// Verify the target block exists in the canonical chain
	targetHash, ok, err := e.blockReader.CanonicalHash(ctx, tx, targetBlock)
	if err != nil {
		return fmt.Errorf("failed to get canonical hash for block %d: %w", targetBlock, err)
	}
	if !ok {
		return fmt.Errorf("block %d not found in canonical chain", targetBlock)
	}

	// Create SharedDomains context for the unwind
	sd, err := execctx.NewSharedDomains(ctx, tx, e.logger)
	if err != nil {
		return fmt.Errorf("failed to create shared domains: %w", err)
	}
	defer sd.Close()

	// Set the unwind point and run the unwind
	if err := e.pipelineExecutor.UnwindTo(targetBlock, stagedsync.StagedUnwind, tx); err != nil {
		return fmt.Errorf("failed to set unwind point: %w", err)
	}

	if err := e.hook.BeforeRun(tx, true); err != nil {
		return fmt.Errorf("hook BeforeRun failed: %w", err)
	}

	if err := e.pipelineExecutor.RunUnwind(sd, tx); err != nil {
		return fmt.Errorf("failed to run unwind: %w", err)
	}

	// Truncate TxNums above the target block
	if err := rawdbv3.TxNums.Truncate(tx, targetBlock+1); err != nil {
		return fmt.Errorf("failed to truncate tx nums: %w", err)
	}

	// Remove stale canonical hashes above the target block
	if err := rawdb.TruncateCanonicalHash(tx, targetBlock+1, false); err != nil {
		return fmt.Errorf("failed to truncate canonical hashes: %w", err)
	}

	// Update the head block hash, head header hash, and forkchoice head
	rawdb.WriteHeadBlockHash(tx, targetHash)
	if err := rawdb.WriteHeadHeaderHash(tx, targetHash); err != nil {
		return fmt.Errorf("failed to write head header hash: %w", err)
	}
	rawdb.WriteForkchoiceHead(tx, targetHash)

	// Update stage progress for headers and bodies
	if err := stages.SaveStageProgress(tx, stages.Headers, targetBlock); err != nil {
		return fmt.Errorf("failed to save headers stage progress: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, targetBlock); err != nil {
		return fmt.Errorf("failed to save bodies stage progress: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.BlockHashes, targetBlock); err != nil {
		return fmt.Errorf("failed to save block hashes stage progress: %w", err)
	}

	if err := sd.Commit(ctx, tx); err != nil {
		return fmt.Errorf("failed to commit shared domains: %w", err)
	}

	e.logger.Info("SetHead: successfully rewound chain", "targetBlock", targetBlock, "previousHead", currentHead)
	e.publishUnwindCompleted(targetBlock, currentHead)
	return nil
}

// publishUnwindCompleted broadcasts flow.UnwindCompleted to the storage
// bus so the CL component can rewind Caplin's in-memory anchors. The
// publish is best-effort: nil bus (harness/test paths) and any bus
// implementation failures are silent — the EL unwind already committed
// and the operator can restart the node to recover if the CL doesn't
// catch up. See docs/plans/20260609-mode-b-cl-rewind-gap.md.
func (e *ExecModule) publishUnwindCompleted(toBlock, tipBlock uint64) {
	if e.eventBus == nil {
		return
	}
	e.eventBus.Publish(flow.UnwindCompleted{ToBlock: toBlock, TipBlock: tipBlock})
}
