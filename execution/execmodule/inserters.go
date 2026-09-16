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
	if _, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), number, block.Body); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	if err := clearSystemSlotRows(tx, header.Hash(), number); err != nil {
		return fmt.Errorf("clear system slots: %w", err)
	}
	return nil
}

// clearSystemSlotRows removes any transaction row sitting in this block's two SYSTEM-transaction slots.
//
// A body owns the txn-id range [BaseTxnID, LastSystemTx], and WriteRawTransactions only ever writes real
// transactions at At(i) = BaseTxnID+1+i, so both ends of that range must hold nothing. Canonical reads honour
// that — ReadBodyWithTransactions walks from First() — so a row left in a system slot is invisible to
// eth_getBlockByNumber, to receipts and to re-execution. The chain looks perfect with one there.
//
// Exactly one consumer reads those slots: the snapshot dumper, whose DumpTxs calls addSystemTx(body.BaseTxnID)
// and emits whatever it finds. The transactions index then keys every emitted record on txn.Hash(), so a
// leftover real transaction is emitted TWICE — once from the slot, once from its own position — and recsplit
// cannot build an index over a duplicated key. It retries with another salt, forever.
//
// The rows get there because a block's body is written more than once. WriteRawBodyIfNotExists keys its
// existence check on (number, hash), so every accumulation round that re-hashes the header, and every
// withdrawals re-stamp, misses and takes a FRESH id range; rawdb.DeleteBody then frees kv.BlockBody and
// kv.BlockAccessList but never the kv.EthTx rows. A superseded range that overlaps the surviving one leaves
// its first transaction sitting exactly on the survivor's BaseTxnID.
//
// Measured across four segments on two machines, the violation is always the FIRST system slot and never the
// last: 20 blocks per 1000 before the duplicate-transaction fix and 3 per 1000 after it — and a single one
// wedges its whole 1000-block segment permanently.
//
// Deleting these two ids is safe by construction: no live transaction can occupy them.
func clearSystemSlotRows(tx kv.RwTx, hash common.Hash, number uint64) error {
	bfs, err := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(number, hash))
	if err != nil || bfs == nil {
		return err
	}
	var id [8]byte
	for _, txnID := range [...]uint64{bfs.BaseTxnID.U64(), bfs.BaseTxnID.LastSystemTx(bfs.TxCount)} {
		binary.BigEndian.PutUint64(id[:], txnID)
		if derr := tx.Delete(kv.EthTx, id[:]); derr != nil {
			return fmt.Errorf("delete system-slot row %d: %w", txnID, derr)
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
