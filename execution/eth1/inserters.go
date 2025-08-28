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

package eth1

import (
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common/metrics"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/eth1/eth1_utils"
	"github.com/erigontech/erigon/execution/types"
)

func (e *EthereumExecutionModule) InsertBlocks(ctx context.Context, req *execution.InsertBlocksRequest) (*execution.InsertionResult, error) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.InsertBlocks: ExecutionStatus_Busy")
		return &execution.InsertionResult{
			Result: execution.ExecutionStatus_Busy,
		}, nil
	}
	defer e.semaphore.Release(1)
	e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)
	frozenBlocks := e.blockReader.FrozenBlocks()

	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not begin transaction: %s", err)
	}
	defer tx.Rollback()

	for _, block := range req.Blocks {
		// Skip frozen blocks.
		if block.Header.BlockNumber < frozenBlocks {
			continue
		}
		header, err := eth1_utils.HeaderRpcToHeader(block.Header)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: cannot convert headers: %s", err)
		}
		body, err := eth1_utils.ConvertRawBlockBodyFromRpc(block.Body)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: cannot convert body: %s", err)
		}
		rawBlock := types.RawBlock{Header: header, Body: body}
		err = rawBlock.ValidateMaxRlpSize(e.config)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: max rlp size validation: %w", err)
		}
		var parentTd *big.Int
		height := header.Number.Uint64()
		if height > 0 {
			// Parent's total difficulty
			parentTd, err = rawdb.ReadTd(tx, header.ParentHash, height-1)
			if err != nil || parentTd == nil {
				return nil, fmt.Errorf("parent's total difficulty not found with hash %x and height %d: %v", header.ParentHash, height-1, err)
			}
		} else {
			parentTd = big.NewInt(0)
		}

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height, e.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height, e.logger)

		// Sum TDs.
		td := parentTd.Add(parentTd, header.Difficulty)
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeHeader: %s", err)
		}
		if err := rawdb.WriteTd(tx, header.Hash(), height, td); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeTd: %s", err)
		}
		if _, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), height, body); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBody: %s", err)
		}
		e.logger.Trace("Inserted block", "hash", header.Hash(), "number", header.Number)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not commit: %s", err)
	}

	return &execution.InsertionResult{
		Result: execution.ExecutionStatus_Success,
	}, nil
}
