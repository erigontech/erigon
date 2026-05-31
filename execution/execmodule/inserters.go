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
	"errors"
	"fmt"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/types"
)

// flushBlockOverlayToDB flushes the block overlay to DB, bounding memory
// during bulk inserts. Not called for single-block chain-tip inserts.
func (e *ExecModule) flushBlockOverlayToDB(ctx context.Context, sd *execctx.SharedDomains) error {
	overlay := sd.BlockOverlay()
	if overlay == nil {
		return nil
	}
	rwTx, err := e.db.BeginTemporalRw(ctx)
	if err != nil {
		return fmt.Errorf("ethereumExecutionModule.InsertBlocks: begin rw for overlay flush: %w", err)
	}
	defer rwTx.Rollback()
	if err := overlay.Flush(ctx, rwTx); err != nil {
		return fmt.Errorf("ethereumExecutionModule.InsertBlocks: flush overlay: %w", err)
	}
	if err := rwTx.Commit(); err != nil {
		return fmt.Errorf("ethereumExecutionModule.InsertBlocks: commit overlay: %w", err)
	}
	sd.CloseBlockOverlay()
	return nil
}

func (e *ExecModule) InsertBlocks(ctx context.Context, blocks []*types.RawBlock) (ExecutionStatus, error) {
	defer insertBlocksDuration.ObserveDuration(time.Now())
	// Block until the executor's serialization semaphore frees up. The
	// previous TryAcquire-or-return-Busy pattern relied on the caller's
	// 100 ms retry loop in InsertBlocksAndWaitWithAccessLists, which added
	// up to 100 ms of latency every time the executor was briefly busy on
	// the engine_newPayload hot path. Acquire(ctx, 1) is equivalent to
	// "channel-notify when the previous insert completes" — the semaphore's
	// internal wait queue does the wakeup without any sleep / poll.
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: semaphore acquire: %w", err)
	}
	defer e.semaphore.Release(1)
	e.forkValidator.ClearWithUnwind()
	frozenBlocks := e.blockReader.FrozenBlocks()

	// Open a read-only tx for the base data; writes accumulate in the
	// SharedDomains block overlay and are flushed via a brief RwTx.
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not begin transaction: %s", err)
	}
	defer roTx.Rollback()

	// Ensure currentContext has a block overlay for accumulating writes.
	sd := e.currentContext
	if sd == nil {
		sd, err = execctx.NewSharedDomains(ctx, roTx, e.logger)
		// ErrBehindCommitment is tolerated: sd is usable, catch-up drives txNums forward.
		if err != nil {
			if !errors.Is(err, commitmentdb.ErrBehindCommitment) {
				return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not create shared domains: %s", err)
			}
			e.logger.Info("ethereumExecutionModule.InsertBlocks: state ahead of blocks, proceeding with catch-up", "err", err)
		}
		e.lock.Lock()
		e.currentContext = sd
		e.lock.Unlock()
	}
	if sd.BlockOverlay() == nil {
		if err := sd.InitBlockOverlay(roTx, roTx.Debug().Dirs().Tmp); err != nil {
			return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: %w", err)
		}
	} else {
		sd.BlockOverlay().UpdateTxn(roTx)
	}
	blockOverlay := sd.BlockOverlay()

	for _, block := range blocks {
		header := block.Header
		body := block.Body

		// Skip frozen blocks.
		if header.Number.Uint64() < frozenBlocks {
			continue
		}

		rawBlock := types.RawBlock{Header: header, Body: body}
		if err := rawBlock.ValidateMaxRlpSize(e.config); err != nil {
			return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: max rlp size validation: %w", err)
		}

		var parentTd *uint256.Int
		height := header.Number.Uint64()
		if height > 0 {
			// Parent's total difficulty — reads from overlay first, then base RO tx.
			parentTd, err = rawdb.ReadTd(blockOverlay, header.ParentHash, height-1)
			if err != nil || parentTd == nil {
				return 0, fmt.Errorf("parent's total difficulty not found with hash %x and height %d: %v", header.ParentHash, height-1, err)
			}
		} else {
			parentTd = new(uint256.Int)
		}

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height, e.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height, e.logger)

		// Sum TDs.
		var td uint256.Int
		if _, overflow := td.AddOverflow(parentTd, &header.Difficulty); overflow {
			return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: TD overflows uint256 at height %d hash %x", height, header.Hash())
		}
		if err := rawdb.WriteHeader(blockOverlay, header); err != nil {
			return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeHeader: %s", err)
		}
		if err := rawdb.WriteTd(blockOverlay, header.Hash(), height, td); err != nil {
			return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeTd: %s", err)
		}
		if _, err := rawdb.WriteRawBodyIfNotExists(blockOverlay, header.Hash(), height, body); err != nil {
			return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBody: %s", err)
		}
		if len(block.BlockAccessList) > 0 {
			if header.BlockAccessListHash == nil {
				return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: block access list provided without hash for block %d", height)
			}
			if err := rawdb.WriteBlockAccessListBytes(blockOverlay, header.Hash(), height, block.BlockAccessList); err != nil {
				return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBlockAccessList, block %d: %s", height, err)
			}
		}
		e.logger.Trace("Inserted block", "hash", header.Hash(), "number", header.Number)
	}

	// On ChainTip - store blocks in Overlay
	// On Non-ChainTip - flush to db because batches are big
	if len(blocks) > 16 {
		if err := e.flushBlockOverlayToDB(ctx, sd); err != nil {
			return 0, err
		}
	}
	return ExecutionStatusSuccess, nil
}
