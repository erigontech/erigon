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
	// The block's ids start at the sequence as it stands when the block is open: nothing advances it until the
	// seal, so every round writes the same range.
	base, err := tx.ReadSequence(kv.EthTx)
	if err != nil {
		return fmt.Errorf("read txn id sequence: %w", err)
	}
	if err := writePreExecBody(tx, header.Hash(), number, block.Body, base); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	traceBodyIds(e.logger, "preexec-round", tx, header.Hash(), number, base, false)
	return nil
}

// writePreExecBody stores an in-progress block's body at a FIXED base id without advancing the kv.EthTx sequence.
//
// A block's ids are its start system slot at base, its transactions at base+1.., and its end system slot after
// them — nil entries that only consume a txnum. WriteRawBody allocates that range with IncrementSequence, which is
// right for a block written once. A pre-exec block is written every round under a new header hash, so allocating
// each time gave it a fresh range per round and left the earlier ones behind, still holding transactions, where
// they could land on a later block's system slot. Writing every round at the same base, in place, and advancing the
// sequence once at the seal gives exactly the ids InsertBlocks would.
func writePreExecBody(tx kv.RwTx, hash common.Hash, number uint64, body *types.RawBody, base uint64) error {
	bfs := types.BodyForStorage{
		BaseTxnID:   types.BaseTxnID(base),
		TxCount:     types.TxCountToTxAmount(len(body.Transactions)),
		Uncles:      body.Uncles,
		Withdrawals: body.Withdrawals,
	}
	if err := rawdb.WriteBodyForStorage(tx, hash, number, &bfs); err != nil {
		return err
	}
	for i, txn := range body.Transactions {
		id := make([]byte, 8)
		binary.BigEndian.PutUint64(id, bfs.BaseTxnID.At(i))
		// Put, not Append: a later round rewrites the ids an earlier one wrote.
		if err := tx.Put(kv.EthTx, id, txn); err != nil {
			return fmt.Errorf("txn %d at id %d: %w", i, bfs.BaseTxnID.At(i), err)
		}
	}
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
	// Only a SETTLED body must have both slots empty. During rounds a body that shrank still overlaps rows a longer
	// one wrote; the seal removes them (sealPreExecBodyIds), so a taken slot there is expected and not a defect.
	inProgress := when == "preexec-round" || when == "execute-into"
	record := logger.Debug
	if (startTaken || endTaken) && !inProgress {
		record = logger.Warn
	}
	record("[TXID-TRACE] body ids", "when", when, "block", number, "hash", hash,
		"base", first, "last", last, "txCount", bfs.TxCount, "allocated", allocated, "seqBefore", seqBefore,
		"startSlotTaken", startTaken, "endSlotTaken", endTaken)
}

// sealPreExecBodyIds settles a sealed pre-exec block's ids: it advances the kv.EthTx sequence ONCE, by the final
// TxCount, and removes any row at or above the block's end system slot.
//
// Rounds write at a fixed base without advancing the sequence (writePreExecBody), so the sequence still stands at
// this block's base and the advance must return exactly that; anything else means some other write moved it and
// the successor would be handed overlapping ids. Rows above the final transactions come from a round that wrote a
// longer body and was then dropped: the block overlay is shared across rounds, so its rows persist although the
// body was rolled back. Ids above the base are unallocated until this advance, so every such row is stale — and the
// lowest of them is the end system slot, a nil entry that must hold nothing.
func sealPreExecBodyIds(tx kv.RwTx, bfs *types.BodyForStorage) error {
	endSlot := bfs.BaseTxnID.LastSystemTx(bfs.TxCount)
	from := make([]byte, 8)
	binary.BigEndian.PutUint64(from, endSlot)
	c, err := tx.Cursor(kv.EthTx)
	if err != nil {
		return err
	}
	var stale [][]byte
	for k, _, cerr := c.Seek(from); k != nil; k, _, cerr = c.Next() {
		if cerr != nil {
			c.Close()
			return cerr
		}
		stale = append(stale, common.Copy(k))
	}
	c.Close()
	for _, k := range stale {
		if err := tx.Delete(kv.EthTx, k); err != nil {
			return fmt.Errorf("delete stale txn id %d: %w", binary.BigEndian.Uint64(k), err)
		}
	}
	base, err := tx.IncrementSequence(kv.EthTx, uint64(bfs.TxCount))
	if err != nil {
		return err
	}
	if base != bfs.BaseTxnID.U64() {
		return fmt.Errorf("txn id sequence at %d, expected the block's base %d: something advanced it before the seal",
			base, bfs.BaseTxnID.U64())
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
		// A block THIS node sealed already has its transaction ids, settled at the seal in its pre-exec generation.
		// newPayload runs this insert before ValidateChain, so writing the body here would allocate it a second
		// range from the module context's sequence — a snapshot taken when that overlay was created, which only
		// matches the seal's base while the two happen to agree. Hand the sealed block over instead, exactly as
		// ValidateChain does.
		hash := header.Hash()
		e.pendingBlockMu.Lock()
		sealedHere := e.sealedByHash[hash] != nil
		e.pendingBlockMu.Unlock()
		if sealedHere && e.generationOverlay(hash, height) != nil {
			if err := e.stageSealedForCanonicalLocked(ctx, hash, height); err != nil {
				return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: %w", err)
			}
			// Staging reads through its own transaction and rolls it back; the rest of this insert reads through ours.
			blockOverlay.UpdateTxn(roTx)
			traceBodyIds(e.logger, "insert-blocks", blockOverlay, hash, height, 0, false)
		} else {
			if err := rawdb.WriteHeader(blockOverlay, header); err != nil {
				return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeHeader: %s", err)
			}
			if err := rawdb.WriteTd(blockOverlay, hash, height, td); err != nil {
				return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeTd: %s", err)
			}
			seqBefore, _ := blockOverlay.ReadSequence(kv.EthTx)
			allocated, err := rawdb.WriteRawBodyIfNotExists(blockOverlay, hash, height, body)
			if err != nil {
				return 0, fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBody: %s", err)
			}
			traceBodyIds(e.logger, "insert-blocks", blockOverlay, hash, height, seqBefore, allocated)
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
