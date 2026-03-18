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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/execmodule/moduleutil"
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
)

func (e *ExecModule) InsertBlocks(ctx context.Context, req *executionproto.InsertBlocksRequest) (*executionproto.InsertionResult, error) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.InsertBlocks: ExecutionStatus_Busy")
		return &executionproto.InsertionResult{
			Result: executionproto.ExecutionStatus_Busy,
		}, nil
	}
	defer e.semaphore.Release(1)
	e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)
	frozenBlocks := e.blockReader.FrozenBlocks()

	// Open a read-only tx for the base data; writes accumulate in the
	// SharedDomains block overlay and are flushed via a brief RwTx.
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not begin transaction: %s", err)
	}
	defer roTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, roTx, e.logger)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not create shared domains: %s", err)
	}
	defer sd.Close()

	if err := sd.InitBlockOverlay(roTx, roTx.Debug().Dirs().Tmp); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: %w", err)
	}
	blockOverlay := sd.BlockOverlay()

	type balKey struct {
		hash   common.Hash
		number uint64
	}
	balEntries := make(map[balKey]*executionproto.BlockAccessListEntry, len(req.BlockAccessLists))
	for _, entry := range req.BlockAccessLists {
		if entry == nil || entry.BlockHash == nil {
			continue
		}
		hash := gointerfaces.ConvertH256ToHash(entry.BlockHash)
		balEntries[balKey{hash: hash, number: entry.BlockNumber}] = entry
	}

	e.logger.Info("[InsertBlocks] batch start",
		"blocks", len(req.Blocks), "frozenBlocks", frozenBlocks)
	var prevHash common.Hash
	var prevHeight uint64
	skipped := 0
	for _, block := range req.Blocks {
		// Skip frozen blocks.
		if block.Header.BlockNumber < frozenBlocks {
			skipped++
			continue
		}
		header, err := moduleutil.HeaderRpcToHeader(block.Header)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: cannot convert headers: %s", err)
		}
		body, err := moduleutil.ConvertRawBlockBodyFromRpc(block.Body)
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

		// Detect parentHash discontinuity
		if prevHeight > 0 && height == prevHeight+1 && header.ParentHash != prevHash {
			e.logger.Error("[InsertBlocks] PARENT HASH MISMATCH",
				"blockNum", height, "blockHash", header.Hash(),
				"parentHash", header.ParentHash,
				"prevBlockNum", prevHeight, "prevBlockHash", prevHash)
		}

		// Detect block number gap (non-sequential heights)
		if prevHeight > 0 && height != prevHeight+1 {
			e.logger.Error("[InsertBlocks] BLOCK NUMBER GAP",
				"blockNum", height, "prevBlockNum", prevHeight,
				"gap", int64(height)-int64(prevHeight),
				"blockHash", header.Hash(), "parentHash", header.ParentHash)
		}

		// Detect duplicate block numbers
		if prevHeight > 0 && height == prevHeight {
			e.logger.Error("[InsertBlocks] DUPLICATE BLOCK NUMBER",
				"blockNum", height, "blockHash", header.Hash(),
				"prevBlockHash", prevHash)
		}

		if height > 0 {
			// For the first non-skipped block in this batch, check if parent TD exists in base DB
			if prevHeight == 0 {
				baseTd, baseErr := rawdb.ReadTd(roTx, header.ParentHash, height-1)
				e.logger.Info("[InsertBlocks] first block parent TD check",
					"blockNum", height, "parentHash", header.ParentHash,
					"parentHeight", height-1,
					"parentTdInBaseDB", baseTd, "baseErr", baseErr)
			}

			// Parent's total difficulty — reads from overlay first, then base RO tx.
			parentTd, err = rawdb.ReadTd(blockOverlay, header.ParentHash, height-1)
			if err != nil || parentTd == nil {
				// Try reading from base DB directly to distinguish overlay vs base issue
				baseTd, baseErr := rawdb.ReadTd(roTx, header.ParentHash, height-1)
				e.logger.Error("[InsertBlocks] parent TD not found",
					"blockNum", height, "blockHash", header.Hash(),
					"parentHash", header.ParentHash, "parentHeight", height-1,
					"prevWrittenHash", prevHash, "prevWrittenHeight", prevHeight,
					"skippedByFrozen", skipped,
					"parentTdInBaseDB", baseTd, "baseErr", baseErr,
					"err", err)
				return nil, fmt.Errorf("parent's total difficulty not found with hash %x and height %d: %v", header.ParentHash, height-1, err)
			}
		} else {
			parentTd = big.NewInt(0)
		}

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height, e.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height, e.logger)

		// Sum TDs.
		td := parentTd.Add(parentTd, header.Difficulty.ToBig())
		prevHash = header.Hash()
		prevHeight = height
		if err := rawdb.WriteHeader(blockOverlay, header); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeHeader: %s", err)
		}
		if err := rawdb.WriteTd(blockOverlay, header.Hash(), height, td); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeTd: %s", err)
		}
		// Verify WriteTd read-back from overlay
		verifyTd, verifyErr := rawdb.ReadTd(blockOverlay, header.Hash(), height)
		if verifyErr != nil || verifyTd == nil {
			e.logger.Error("[InsertBlocks] WRITE-TD READBACK FAILED",
				"blockNum", height, "blockHash", header.Hash(),
				"writtenTd", td, "readBackTd", verifyTd, "readBackErr", verifyErr)
		}
		if _, err := rawdb.WriteRawBodyIfNotExists(blockOverlay, header.Hash(), height, body); err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBody: %s", err)
		}
		key := balKey{hash: header.Hash(), number: height}
		if entry, ok := balEntries[key]; ok && entry != nil {
			if header.BlockAccessListHash == nil {
				return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: block access list provided without hash for block %d", height)
			}
			balBytes := entry.BlockAccessList
			if len(balBytes) == 0 {
				balBytes, err = types.EncodeBlockAccessListBytes(nil)
				if err != nil {
					return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: encode empty block access list, block %d: %s", height, err)
				}
			}
			if err := rawdb.WriteBlockAccessListBytes(blockOverlay, header.Hash(), height, balBytes); err != nil {
				return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBlockAccessList, block %d: %s", height, err)
			}
		}
		// Log every block in the 3220-3250 range for debugging
		if height >= 3220 && height <= 3250 {
			e.logger.Info("[InsertBlocks] block detail",
				"blockNum", height, "blockHash", header.Hash(),
				"parentHash", header.ParentHash, "td", td)
		}
		e.logger.Trace("Inserted block", "hash", header.Hash(), "number", header.Number)
	}

	e.logger.Info("[InsertBlocks] batch end",
		"lastBlockNum", prevHeight, "lastBlockHash", prevHash,
		"skippedByFrozen", skipped,
		"processedBlocks", len(req.Blocks)-skipped)

	// Brief RwTx only for flushing accumulated writes to disk.
	rwTx, err := e.db.BeginRw(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not begin write transaction: %s", err)
	}
	defer rwTx.Rollback()
	if err := sd.Flush(ctx, rwTx); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not flush: %s", err)
	}
	if err := rwTx.Commit(); err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not commit: %s", err)
	}

	return &executionproto.InsertionResult{
		Result: executionproto.ExecutionStatus_Success,
	}, nil
}
