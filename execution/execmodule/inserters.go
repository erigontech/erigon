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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/types"
)

// flushBlockOverlayToDB commits the in-memory block overlay to the DB so
// per-batch memory stays bounded. After the commit the overlay is closed;
// the next InsertBlocks call opens a fresh RoTx that sees the committed TDs.
func (e *ExecModule) flushBlockOverlayToDB(ctx context.Context, sd *execctx.SharedDomains, overlay interface {
	Flush(context.Context, kv.RwTx) error
}) (ExecutionStatus, error) {
	rwTx, err := e.db.BeginTemporalRw(ctx)
	if err != nil {
		return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: begin rw for overlay flush: %w", err)
	}
	defer rwTx.Rollback()
	if err := overlay.Flush(ctx, rwTx); err != nil {
		return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: flush overlay: %w", err)
	}
	if err := rwTx.Commit(); err != nil {
		return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: commit overlay: %w", err)
	}
	sd.CloseBlockOverlay()
	return ExecutionStatusSuccess, nil
}

func (e *ExecModule) InsertBlocks(ctx context.Context, blocks []*types.RawBlock) (ExecutionStatus, error) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.InsertBlocks: ExecutionStatus_Busy")
		return ExecutionStatusBusy, nil
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

	// Flush block overlay to DB so in-memory usage stays bounded to one batch.
	// After the commit, the next InsertBlocks call opens a fresh RoTx that sees
	// the committed TDs, so parent TD lookups in subsequent batches still work.
	// UpdateForkChoice detects hasOverlay=false and reads block data from DB.
	if overlay := sd.BlockOverlay(); overlay != nil {
		if status, err2 := e.flushBlockOverlayToDB(ctx, sd, overlay); err2 != nil {
			return status, err2
		}
	}
	return ExecutionStatusSuccess, nil
}
