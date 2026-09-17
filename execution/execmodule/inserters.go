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
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
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

// moduleContextLocked returns the module context with its block overlay ready on roTx — where block data is staged
// for the canonical path (InsertBlocks, the fork choice) — creating either if it does not exist: a fork choice tears
// the context down. Caller holds e.semaphore.
func (e *ExecModule) moduleContextLocked(ctx context.Context, roTx kv.TemporalTx) (*execctx.SharedDomains, error) {
	sd := e.currentContext
	if sd == nil {
		var err error
		sd, err = execctx.NewSharedDomains(ctx, roTx, e.logger)
		// ErrBehindCommitment is tolerated: sd is usable, catch-up drives txNums forward.
		if err != nil {
			if !errors.Is(err, commitmentdb.ErrBehindCommitment) {
				return nil, fmt.Errorf("could not create shared domains: %s", err)
			}
			e.logger.Info("ethereumExecutionModule.InsertBlocks: state ahead of blocks, proceeding with catch-up", "err", err)
		}
		e.lock.Lock()
		e.currentContext = sd
		e.lock.Unlock()
	}
	if sd.BlockOverlay() == nil {
		if err := sd.InitBlockOverlay(roTx, roTx.Debug().Dirs().Tmp); err != nil {
			return nil, err
		}
	} else {
		sd.BlockOverlay().UpdateTxn(roTx)
	}
	return sd, nil
}

// writePreExecBlock stores a pre-exec round's block — header, total difficulty, body — in the round's own overlay, where
// its execution, the close and the seal read it. The parent is read from the pre-exec generation holding it when there
// is one (before newPayload a sealed parent exists nowhere else), otherwise from canonical data beneath the overlay.
// It never touches the module context: that is the canonical flow's. Caller holds e.semaphore.
func (e *ExecModule) writePreExecBlock(tx kv.RwTx, roTx kv.TemporalTx, block *types.RawBlock) error {
	header := block.Header
	number := header.Number.Uint64()
	if gen := e.generationOverlay(header.ParentHash, number-1); gen != nil && kv.RwTx(gen) != tx {
		// The block's execution verifies it against its parent in its own overlay, and raw tables do not read through
		// to the parent generation, so the parent's header and TD are carried in.
		gen.UpdateTxn(roTx)
		parent := rawdb.ReadHeader(gen, header.ParentHash, number-1)
		if parent == nil {
			return fmt.Errorf("parent header %d %x not in its generation", number-1, header.ParentHash)
		}
		genTd, err := rawdb.ReadTd(gen, header.ParentHash, number-1)
		if err != nil {
			return fmt.Errorf("read parent TD: %w", err)
		}
		if genTd == nil {
			return fmt.Errorf("parent TD %d %x not in its generation", number-1, header.ParentHash)
		}
		if err := rawdb.WriteHeader(tx, parent); err != nil {
			return fmt.Errorf("carry parent header: %w", err)
		}
		if err := rawdb.WriteTd(tx, header.ParentHash, number-1, *genTd); err != nil {
			return fmt.Errorf("carry parent TD: %w", err)
		}
	}
	parentTd, err := rawdb.ReadTd(tx, header.ParentHash, number-1)
	if err != nil {
		return fmt.Errorf("read parent TD: %w", err)
	}
	if parentTd == nil {
		return fmt.Errorf("parent's total difficulty not found with hash %x and height %d", header.ParentHash, number-1)
	}
	var td uint256.Int
	if _, overflow := td.AddOverflow(parentTd, &header.Difficulty); overflow {
		return fmt.Errorf("TD overflows uint256 at height %d hash %x", number, header.Hash())
	}
	if err := rawdb.WriteHeader(tx, header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if err := rawdb.WriteTd(tx, header.Hash(), number, td); err != nil {
		return fmt.Errorf("write TD: %w", err)
	}
	seqBefore, _ := tx.ReadSequence(kv.EthTx)
	allocated, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), number, block.Body)
	if err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	traceBodyIds(e.logger, "preexec-round", tx, header.Hash(), number, seqBefore, allocated)
	return nil
}

// traceBodyIds records the txn-id range a body owns, at a site that creates a block's DB representation,
// and whether either system slot holds a key. A system slot never should: its id is reserved by
// IncrementSequence and nothing is inserted there. Warn when one does, so the write that strands a row names
// itself instead of being reconstructed from segments. Every such site is traced — pre-exec rounds, the seal,
// the seal's allocating fallback, newPayload's fork validation and InsertBlocks — because they write into
// different overlays, each allocating from its own view of the sequence.
func traceBodyIds(logger log.Logger, when string, tx kv.Getter, hash common.Hash, number, seqBefore uint64, allocated bool) {
	bfs, err := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(number, hash))
	if err != nil || bfs == nil {
		logger.Warn("[TXID-TRACE] body record unreadable", "when", when, "block", number, "hash", hash, "err", err)
		return
	}
	first, last := bfs.BaseTxnID.U64(), bfs.BaseTxnID.LastSystemTx(bfs.TxCount)
	var id [8]byte
	taken := func(txnID uint64) bool {
		binary.BigEndian.PutUint64(id[:], txnID)
		v, gerr := tx.GetOne(kv.EthTx, id[:])
		return gerr == nil && v != nil
	}
	startTaken, endTaken := taken(first), taken(last)
	record := logger.Debug
	if startTaken || endTaken {
		record = logger.Warn
	}
	record("[TXID-TRACE] body ids", "when", when, "block", number, "hash", hash,
		"base", first, "last", last, "txCount", bfs.TxCount, "allocated", allocated, "seqBefore", seqBefore,
		"startSlotTaken", startTaken, "endSlotTaken", endTaken)
}

// clearSystemSlotRows makes the SEALED body's txnum→txhash mapping correct by emptying its two
// system-transaction slots.
//
// Block-start and block-end are executed by the executor, not stored: WriteRawTransactions only ever writes
// At(i) = BaseTxnID+1+i, so the ids at each end of the range must hold nothing, and the transactions index
// then keys them off the txnum (pad32) rather than a transaction hash. A one-pass chain produces exactly
// that — dev-L1: 2000 system slots over 1000 blocks, every one empty, zero duplicate keys.
//
// Multi-round construction does not, because a successor takes its id range while its parent is still
// accumulating. The parent grows over that range, the successor is re-allocated higher, and its earlier rows
// are left behind — one landing on the successor's own BaseTxnID. The block-start txnum then maps to a user
// transaction's hash: the wrong mapping, and a duplicate key that no salt can index (measured: 3 blocks per
// 1000 on trading, 0 on dev-L1).
//
// The seal is the right moment. During the rounds the block overlay is SHARED (BorrowBlockOverlay), so a
// deletion by a round that is later dropped still persists and can strand the block with no body at all.
// Here the body is final, and no live transaction can occupy these two ids.
func clearSystemSlotRows(tx kv.RwTx, bfs *types.BodyForStorage) error {
	var id [8]byte
	for _, txnID := range [...]uint64{bfs.BaseTxnID.U64(), bfs.BaseTxnID.LastSystemTx(bfs.TxCount)} {
		binary.BigEndian.PutUint64(id[:], txnID)
		if err := tx.Delete(kv.EthTx, id[:]); err != nil {
			return fmt.Errorf("clear system slot %d: %w", txnID, err)
		}
	}
	return nil
}

func (e *ExecModule) InsertBlocks(ctx context.Context, blocks []*types.RawBlock) (ExecutionStatus, error) {
	// Serialize behind any in-flight exec-module op, blocking rather than failing fast with Busy.
	start := time.Now()
	// Timed across the whole call so the metric includes semaphore wait.
	defer insertBlocksDuration.ObserveDuration(start)
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: semaphore acquire: %w", err)
	}
	defer e.semaphore.Release(1)
	e.logger.Debug("ethereumExecutionModule.InsertBlocks: semaphore acquired", "wait", time.Since(start))
	return e.insertBlocksLocked(ctx, blocks)
}

// insertBlocksLocked is InsertBlocks' body with the caller ALREADY holding e.semaphore. It is the reusable core
// so the atomic assemble (SealBlock sealing N then opening N+1) can insert the successor block under its
// single semaphore hold — re-acquiring here would deadlock. InsertBlocks acquires the semaphore and calls this.
func (e *ExecModule) insertBlocksLocked(ctx context.Context, blocks []*types.RawBlock) (ExecutionStatus, error) {
	// Clearing the validation candidate here is safe again: the producer's in-progress and run-ahead blocks
	// live in the pre-exec frontier, which this cannot reach. (This used to need two guards — a flashblock
	// UPDATE and a frontier EXTENSION — because both kinds of insert would otherwise have closed the
	// SharedDomains the producer was still accumulating into or chaining onto.)
	e.forkValidator.ClearWithUnwind()
	frozenBlocks := e.blockReader.FrozenBlocks()

	// Open a read-only tx for the base data; writes accumulate in the
	// SharedDomains block overlay and are flushed via a brief RwTx.
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: could not begin transaction: %s", err)
	}
	defer roTx.Rollback()

	sd, err := e.moduleContextLocked(ctx, roTx)
	if err != nil {
		return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: %w", err)
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
		seqBefore, _ := blockOverlay.ReadSequence(kv.EthTx)
		allocated, err := rawdb.WriteRawBodyIfNotExists(blockOverlay, header.Hash(), height, body)
		if err != nil {
			return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBody: %s", err)
		}
		traceBodyIds(e.logger, "insert-blocks", blockOverlay, header.Hash(), height, seqBefore, allocated)
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
