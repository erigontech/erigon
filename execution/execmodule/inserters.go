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
	"math/big"

	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/types"
)

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
		if err != nil {
			return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not create shared domains: %s", err)
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

		var parentTd *big.Int
		height := header.Number.Uint64()
		if height > 0 {
			// Parent's total difficulty — reads from overlay first, then base RO tx.
			parentTd, err = rawdb.ReadTd(blockOverlay, header.ParentHash, height-1)
			if err != nil || parentTd == nil {
				return 0, fmt.Errorf("parent's total difficulty not found with hash %x and height %d: %v", header.ParentHash, height-1, err)
			}
		} else {
			parentTd = big.NewInt(0)
		}

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height, e.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height, e.logger)

		// Sum TDs.
		td := parentTd.Add(parentTd, header.Difficulty.ToBig())
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
			balBytes := block.BlockAccessList
			if len(balBytes) == 0 {
				balBytes, err = types.EncodeBlockAccessListBytes(nil)
				if err != nil {
					return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: encode empty block access list, block %d: %s", height, err)
				}
			}
			if err := rawdb.WriteBlockAccessListBytes(blockOverlay, header.Hash(), height, balBytes); err != nil {
				return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBlockAccessList, block %d: %s", height, err)
			}
		}
		e.logger.Trace("Inserted block", "hash", header.Hash(), "number", header.Number)
	}

	// Writes stay in the block overlay on currentContext — no flush or commit here.
	// ValidateChain reads from the overlay; UpdateForkChoice flushes everything
	// in a single commit at the end.
	return ExecutionStatusSuccess, nil
}
