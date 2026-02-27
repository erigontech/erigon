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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
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
	if !e.semaphore.TryAcquire(1) {
		return fmt.Errorf("execution module is busy")
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

	// Check if we can unwind that far back
	minUnwindableBlock, err := rawtemporaldb.CanUnwindToBlockNum(tx)
	if err != nil {
		return fmt.Errorf("failed to check minimum unwindable block: %w", err)
	}
	if targetBlock < minUnwindableBlock {
		return fmt.Errorf("cannot unwind to block %d: minimum unwindable block is %d", targetBlock, minUnwindableBlock)
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
	if err := e.executionPipeline.UnwindTo(targetBlock, stagedsync.StagedUnwind, tx); err != nil {
		return fmt.Errorf("failed to set unwind point: %w", err)
	}

	if err := e.hook.BeforeRun(tx, true); err != nil {
		return fmt.Errorf("hook BeforeRun failed: %w", err)
	}

	if err := e.executionPipeline.RunUnwind(sd, tx); err != nil {
		return fmt.Errorf("failed to run unwind: %w", err)
	}

	// Truncate TxNums above the target block
	if err := rawdbv3.TxNums.Truncate(tx, targetBlock+1); err != nil {
		return fmt.Errorf("failed to truncate tx nums: %w", err)
	}

	// Update the head block hash
	rawdb.WriteHeadBlockHash(tx, targetHash)

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

	// Flush and commit
	if err := sd.Flush(ctx, tx); err != nil {
		return fmt.Errorf("failed to flush shared domains: %w", err)
	}
	sd.Close()

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	e.logger.Info("SetHead: successfully rewound chain", "targetBlock", targetBlock, "previousHead", currentHead)
	return nil
}
