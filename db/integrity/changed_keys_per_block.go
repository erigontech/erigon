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

package integrity

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/services"
)

// ChangedKeysPerBlock is an in-memory index of changed keys grouped by block number.
// Built from a single pass over HistoryKeyTxNumRange output for a txNum window.
//
// Memory layout: unique keys are stored once in a flat slice; each block maps to
// a []uint32 of offsets (indices) into that slice.  Keys that change in multiple
// txNums within the same block are deduplicated, so each key appears at most once
// per block entry.
//
// Typical usage: build once for a 10K-block window, look up O(1) per block instead
// of driving a merge-heap per block.
type ChangedKeysPerBlock struct {
	keys   []string            // flat, deduplicated key storage
	blocks map[uint64][]uint32 // blockNum -> offsets into keys
}

// TxNumToBlock maps txNums to block numbers for a contiguous block window using
// a pre-built per-block maxTxNum array.
// HistoryKeyTxNumRange sorts by key; within a key txNums are ascending, so
// blockNums are non-decreasing per key.  BlockOf uses a forward-scan cursor
// that is reset to 0 on each new key (via ResetCursor), giving O(1) amortized
// cost instead of O(log N) binary search per txNum.
type TxNumToBlock struct {
	maxTxNums    []uint64
	fromBlockNum uint64
	toBlockNum   uint64
	cursor       int
}

// NewTxNumToBlock builds a TxNumToBlock for [fromBlockNum, toBlockNum) by reading
// MaxTxNum for each block from the provided reader.
func NewTxNumToBlock(ctx context.Context, tx kv.Tx, br services.FullBlockReader, fromBlockNum, toBlockNum uint64) (*TxNumToBlock, error) {
	r := br.TxnumReader()
	windowLen := toBlockNum - fromBlockNum
	m := &TxNumToBlock{
		maxTxNums:    make([]uint64, windowLen),
		fromBlockNum: fromBlockNum,
		toBlockNum:   toBlockNum,
	}
	var err error
	for i := uint64(0); i < windowLen; i++ {
		m.maxTxNums[i], err = r.Max(ctx, tx, fromBlockNum+i)
		if err != nil {
			return nil, fmt.Errorf("NewTxNumToBlock: Max(block=%d): %w", fromBlockNum+i, err)
		}
	}
	return m, nil
}

// ToTxNum returns the exclusive upper txNum bound for the window (maxTxNum of last block + 1).
func (m *TxNumToBlock) ToTxNum() uint64 { return m.maxTxNums[len(m.maxTxNums)-1] + 1 }

// ResetCursor resets the forward-scan position to the start of the window.
// Must be called whenever the iterator moves to a new key.
func (m *TxNumToBlock) ResetCursor() { m.cursor = 0 }

// BlockOf returns the block number that contains txNum by scanning forward from
// the current cursor position.  Callers must invoke ResetCursor on each new key.
func (m *TxNumToBlock) BlockOf(txNum uint64) (uint64, error) {
	for m.cursor < len(m.maxTxNums) && m.maxTxNums[m.cursor] < txNum {
		m.cursor++
	}
	if m.cursor < len(m.maxTxNums) {
		return m.fromBlockNum + uint64(m.cursor), nil
	}
	return 0, fmt.Errorf("TxNumToBlock: txNum %d beyond window [%d,%d)", txNum, m.fromBlockNum, m.toBlockNum)
}

// NewChangedKeysPerBlock calls HistoryKeyTxNumRange for domain over [fromTxNum, toTxNum)
// and builds a blockNum→keys index from the result.
func NewChangedKeysPerBlock(tx kv.TemporalDebugTx, domain kv.Domain, fromTxNum, toTxNum int, txNums *TxNumToBlock) (*ChangedKeysPerBlock, error) {
	it, err := tx.HistoryKeyTxNumRange(domain, fromTxNum, toTxNum, order.Asc, -1)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	return changedKeysPerBlock(it, txNums.BlockOf, txNums.ResetCursor)
}

// changedKeysPerBlock scans it once and builds the index.  The caller is
// responsible for closing it.  onNewKey is called whenever the key changes so
// that a stateful txNum2Block can reset its cursor; pass nil if not needed.
func changedKeysPerBlock(it stream.KU64, txNum2Block func(txNum uint64) (uint64, error), onNewKey func()) (*ChangedKeysPerBlock, error) {
	idx := &ChangedKeysPerBlock{
		blocks: make(map[uint64][]uint32),
	}
	// keyIdx maps key string -> offset in idx.keys (used only during build).
	keyIdx := make(map[string]uint32)

	// prevKey / prevBlockNum track the previous (key, blockNum) pair so that
	// multiple txNums for the same key in the same block are deduplicated.
	// The iterator emits keys in ascending order; txNums per key are ascending,
	// so blockNums per key are non-decreasing — a duplicate (key, block) pair is
	// always the immediately preceding entry.
	var prevKey string
	prevBlockNum := ^uint64(0) // sentinel: no previous entry

	for it.HasNext() {
		k, txNum, err := it.Next()
		if err != nil {
			return nil, err
		}

		ks := string(k)
		if ks != prevKey && onNewKey != nil {
			onNewKey() // txNums restart from a lower value on each new key, so cursor must reset
		}

		blockNum, err := txNum2Block(txNum)
		if err != nil {
			return nil, err
		}
		if blockNum == prevBlockNum && ks == prevKey {
			continue
		}

		ki, ok := keyIdx[ks]
		if !ok {
			ki = uint32(len(idx.keys))
			keyIdx[ks] = ki
			idx.keys = append(idx.keys, ks)
		}

		idx.blocks[blockNum] = append(idx.blocks[blockNum], ki)
		prevKey = ks
		prevBlockNum = blockNum
	}
	return idx, nil
}

// Offsets returns the key offsets (indices into Key()) for the given blockNum.
// Returns nil if no keys changed in that block within the indexed window.
func (idx *ChangedKeysPerBlock) Offsets(blockNum uint64) []uint32 {
	return idx.blocks[blockNum]
}

// Key returns the key at the given offset (as returned by Offsets).
func (idx *ChangedKeysPerBlock) Key(offset uint32) string {
	return idx.keys[offset]
}

// Has reports whether any keys changed in blockNum within the indexed window.
func (idx *ChangedKeysPerBlock) Has(blockNum uint64) bool {
	return len(idx.blocks[blockNum]) > 0
}

// NumKeys returns the total number of unique keys in the index.
func (idx *ChangedKeysPerBlock) NumKeys() int { return len(idx.keys) }

// NumBlocks returns the number of blocks that have at least one changed key.
func (idx *ChangedKeysPerBlock) NumBlocks() int { return len(idx.blocks) }

// RamBytes returns an estimate of heap bytes used by the index.
func (idx *ChangedKeysPerBlock) RamBytes() uint64 {
	var n uint64
	for _, k := range idx.keys {
		n += uint64(len(k)) + 16 // string header (ptr+len) + backing bytes
	}
	for _, offsets := range idx.blocks {
		n += uint64(len(offsets))*4 + 24 // uint32 entries + slice header + map overhead
	}
	return n
}

// ChangedKeysPerBlockIdx holds pre-built per-domain key change indices for a block window.
// Populated by NewChangedKeysPerBlockIdx for the requested domains.
type ChangedKeysPerBlockIdx [kv.DomainLen]*ChangedKeysPerBlock

// ChangedKeysPerBlockIdx scans HistoryKeyTxNumRange once per domain for the txNum
// range covering [fromBlockNum, toBlockNum) blocks and returns the resulting index.
// The index is fully in-memory; the tx used for scanning is closed on return.
// domains selects which domains to index; typically kv.StateDomains[:kv.CommitmentDomain].
func NewChangedKeysPerBlockIdx(ctx context.Context, db kv.TemporalRoDB, br services.FullBlockReader, fromBlockNum, toBlockNum uint64, domains []kv.Domain, logger log.Logger) (*ChangedKeysPerBlockIdx, error) {
	start := time.Now()
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	fromTxNum, err := br.TxnumReader().Min(ctx, tx, fromBlockNum)
	if err != nil {
		return nil, err
	}
	// NewTxNumToBlock reads Max for every block in the window; derive toTxNum from
	// its last entry instead of making a separate redundant Max(toBlockNum-1) call.
	txNums, err := NewTxNumToBlock(ctx, tx, br, fromBlockNum, toBlockNum)
	if err != nil {
		return nil, err
	}
	toTxNum := txNums.ToTxNum()

	var idx ChangedKeysPerBlockIdx
	var ramBytes uint64
	logArgs := []any{"fromBlockNum", fromBlockNum, "toBlockNum", toBlockNum}
	for _, d := range domains {
		if idx[d], err = NewChangedKeysPerBlock(tx.Debug(), d, int(fromTxNum), int(toTxNum), txNums); err != nil {
			return nil, fmt.Errorf("ChangedKeysPerBlockIdx domain=%s blocks=[%d,%d): %w", d, fromBlockNum, toBlockNum, err)
		}
		ramBytes += idx[d].RamBytes()
		logArgs = append(logArgs, d.String(), idx[d].NumKeys())
	}
	logger.Info("[integrity] built block domain index",
		append(logArgs, "ram", common.ByteCount(ramBytes), "took", time.Since(start))...,
	)
	return &idx, nil
}
