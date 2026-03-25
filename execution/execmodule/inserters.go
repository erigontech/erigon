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
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/types"
)

func (e *ExecModule) InsertBlocks(ctx context.Context, blocks []*types.RawBlock) (ExecutionStatus, error) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.InsertBlocks: ExecutionStatus_Busy")
		return ExecutionStatusBusy, nil
	}
	defer e.semaphore.Release(1)
	e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)
	frozenBlocks := e.blockReader.FrozenBlocks()

	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return ExecutionStatusSuccess, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not begin transaction: %s", err)
	}
	defer tx.Rollback()

	for _, block := range blocks {
		header := block.Header
		body := block.Body

		// Skip frozen blocks.
		if header.Number.Uint64() < frozenBlocks {
			continue
		}

		rawBlock := types.RawBlock{Header: header, Body: body}
		if err := rawBlock.ValidateMaxRlpSize(e.config); err != nil {
			return ExecutionStatusSuccess, fmt.Errorf("ethereumExecutionModule.InsertBlocks: max rlp size validation: %w", err)
		}

		var parentTd *big.Int
		height := header.Number.Uint64()
		if height > 0 {
			parentTd, err = rawdb.ReadTd(tx, header.ParentHash, height-1)
			if err != nil || parentTd == nil {
				return ExecutionStatusSuccess, fmt.Errorf("parent's total difficulty not found with hash %x and height %d: %v", header.ParentHash, height-1, err)
			}
		} else {
			parentTd = big.NewInt(0)
		}

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height, e.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height, e.logger)

		// Sum TDs.
		td := parentTd.Add(parentTd, header.Difficulty.ToBig())
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return ExecutionStatusSuccess, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeHeader: %s", err)
		}
		if err := rawdb.WriteTd(tx, header.Hash(), height, td); err != nil {
			return ExecutionStatusSuccess, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeTd: %s", err)
		}
		if _, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), height, body); err != nil {
			return ExecutionStatusSuccess, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBody: %s", err)
		}
		if len(block.BlockAccessList) > 0 {
			if header.BlockAccessListHash == nil {
				return ExecutionStatusSuccess, fmt.Errorf("ethereumExecutionModule.InsertBlocks: block access list provided without hash for block %d", height)
			}
			if err := rawdb.WriteBlockAccessListBytes(tx, header.Hash(), height, block.BlockAccessList); err != nil {
				return ExecutionStatusSuccess, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBlockAccessList, block %d: %s", height, err)
			}
		}
		e.logger.Trace("Inserted block", "hash", header.Hash(), "number", header.Number)
	}
	if err := tx.Commit(); err != nil {
		return ExecutionStatusSuccess, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not commit: %s", err)
	}

	return ExecutionStatusSuccess, nil
}
