// Copyright 2026 The Erigon Authors
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
	"bytes"
	"fmt"
	"maps"
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
)

type pendingBlock struct {
	number uint64
	parent common.Hash
}

func (e *ExecModule) addPendingBlock(hash common.Hash, number uint64, parent common.Hash) {
	if e.pendingBlocks == nil {
		e.pendingBlocks = make(map[common.Hash]pendingBlock)
	}
	e.pendingBlocks[hash] = pendingBlock{number: number, parent: parent}
}

func (e *ExecModule) copyPendingChain(tx kv.Tx, dst kv.RwTx, head common.Hash) ([]common.Hash, error) {
	src := e.pendingBlocksView(tx)
	if src == nil {
		return nil, nil
	}
	var chain []common.Hash
	hash := head
	for {
		block, ok := e.pendingBlocks[hash]
		if !ok {
			break
		}
		if err := copyBlockRows(src, dst, hash, block.number); err != nil {
			return nil, err
		}
		chain = append(chain, hash)
		hash = block.parent
	}
	if len(chain) == 0 {
		return nil, nil
	}
	return chain, raiseTxSequence(src, dst)
}

func (e *ExecModule) prepareSideBlocks(tx kv.TemporalTx, commit kv.RwTx, committed []common.Hash, finalized, head uint64) (map[common.Hash]pendingBlock, *membatchwithdb.MemoryMutation, error) {
	side := sideBlocksToRetain(e.pendingBlocks, committed, finalized)
	src := e.pendingBlocksView(tx)
	if src != nil && commit != nil {
		if err := persistDeepSideBlocks(src, commit, side, head, e.syncCfg.MaxReorgDepth); err != nil {
			return nil, nil, err
		}
	}
	if len(side) == 0 || src == nil {
		return nil, nil, nil
	}
	retained, err := membatchwithdb.NewMemoryBatch(tx)
	if err != nil {
		return nil, nil, err
	}
	for hash, block := range side {
		if err := copyBlockRows(src, retained, hash, block.number); err != nil {
			retained.Close()
			return nil, nil, err
		}
	}
	if err := raiseTxSequence(src, retained); err != nil {
		retained.Close()
		return nil, nil, err
	}
	retained.DetachDB()
	return side, retained, nil
}

func (e *ExecModule) swapSideBlocks(side map[common.Hash]pendingBlock, retained *membatchwithdb.MemoryMutation) {
	if len(side) == 0 {
		e.dropPendingBlocks()
		return
	}
	e.pendingBlocks = side
	e.swapRetainedBlocks(retained)
}

type sideBlocksState struct {
	pending  map[common.Hash]pendingBlock
	retained *membatchwithdb.MemoryMutation
	context  *execctx.SharedDomains
}

func (e *ExecModule) swapSideBlocksForForkChoice(side map[common.Hash]pendingBlock, retained *membatchwithdb.MemoryMutation) sideBlocksState {
	e.lock.Lock()
	defer e.lock.Unlock()
	old := sideBlocksState{
		pending:  e.pendingBlocks,
		retained: e.retainedBlocks,
		context:  e.currentContext,
	}
	e.pendingBlocks = side
	e.retainedBlocks = retained
	e.currentContext = nil
	return old
}

func (e *ExecModule) restoreSideBlocksForForkChoice(old sideBlocksState) {
	e.lock.Lock()
	e.pendingBlocks = old.pending
	e.retainedBlocks = old.retained
	e.currentContext = old.context
	e.lock.Unlock()
}

func (s sideBlocksState) close() {
	if s.retained != nil {
		s.retained.Close()
	}
	if s.context != nil {
		s.context.Close()
	}
}

func sideBlocksToRetain(pending map[common.Hash]pendingBlock, committed []common.Hash, finalized uint64) map[common.Hash]pendingBlock {
	side := maps.Clone(pending)
	for _, hash := range committed {
		delete(side, hash)
	}
	maps.DeleteFunc(side, func(_ common.Hash, b pendingBlock) bool { return b.number <= finalized })
	return side
}

func persistDeepSideBlocks(src kv.Tx, dst kv.RwTx, side map[common.Hash]pendingBlock, head, maxReorgDepth uint64) error {
	persisted := false
	for hash, block := range side {
		if block.number+maxReorgDepth >= head {
			continue
		}
		if err := copyBlockRows(src, dst, hash, block.number); err != nil {
			return err
		}
		delete(side, hash)
		persisted = true
	}
	if uint64(len(side)) > maxReorgDepth {
		blocks := make([]struct {
			hash  common.Hash
			block pendingBlock
		}, 0, len(side))
		for hash, block := range side {
			blocks = append(blocks, struct {
				hash  common.Hash
				block pendingBlock
			}{hash: hash, block: block})
		}
		sort.Slice(blocks, func(i, j int) bool {
			if blocks[i].block.number != blocks[j].block.number {
				return blocks[i].block.number < blocks[j].block.number
			}
			return bytes.Compare(blocks[i].hash[:], blocks[j].hash[:]) < 0
		})
		for _, item := range blocks[:len(blocks)-int(maxReorgDepth)] {
			if err := copyBlockRows(src, dst, item.hash, item.block.number); err != nil {
				return err
			}
			delete(side, item.hash)
			persisted = true
		}
	}
	if !persisted {
		return nil
	}
	return raiseTxSequence(src, dst)
}

func (e *ExecModule) initSeededOverlay(tx kv.TemporalTx, sd *execctx.SharedDomains) error {
	if err := sd.InitBlockOverlay(tx); err != nil {
		return err
	}
	if err := e.seedRetainedBlocks(tx, sd.BlockOverlay()); err != nil {
		sd.CloseBlockOverlay()
		return fmt.Errorf("seed retained blocks: %w", err)
	}
	return nil
}

func (e *ExecModule) seedRetainedBlocks(tx kv.Tx, overlay kv.RwTx) error {
	if e.retainedBlocks == nil {
		return nil
	}
	src := e.retainedBlocks.NewReadView(tx)
	for hash, block := range e.pendingBlocks {
		if err := copyBlockRows(src, overlay, hash, block.number); err != nil {
			return err
		}
	}
	return raiseTxSequence(src, overlay)
}

func (e *ExecModule) publishSeededContext(sd *execctx.SharedDomains) {
	e.lock.Lock()
	e.currentContext = sd
	retained := e.retainedBlocks
	e.retainedBlocks = nil
	e.lock.Unlock()
	if retained != nil {
		retained.Close()
	}
}

func (e *ExecModule) dropBadChain(badHead, latestValidHash common.Hash) common.Hash {
	if latestValidHash == (common.Hash{}) {
		delete(e.pendingBlocks, badHead)
		return common.Hash{}
	}
	for hash := badHead; hash != latestValidHash; {
		block, ok := e.pendingBlocks[hash]
		if !ok {
			return hash
		}
		delete(e.pendingBlocks, hash)
		hash = block.parent
	}
	return common.Hash{}
}

func (e *ExecModule) dropPendingBlocks() {
	e.pendingBlocks = nil
	e.swapRetainedBlocks(nil)
}

func (e *ExecModule) swapRetainedBlocks(retained *membatchwithdb.MemoryMutation) {
	e.lock.Lock()
	old := e.retainedBlocks
	e.retainedBlocks = retained
	e.lock.Unlock()
	if old != nil {
		old.Close()
	}
}

func (e *ExecModule) pendingBlocksView(tx kv.Tx) kv.Tx {
	if e.currentContext != nil {
		if overlay := e.currentContext.BlockOverlay(); overlay != nil {
			return overlay.NewReadView(tx)
		}
	}
	if e.retainedBlocks != nil {
		return e.retainedBlocks.NewReadView(tx)
	}
	return nil
}

func copyBlockRows(src kv.Tx, dst kv.RwTx, hash common.Hash, number uint64) error {
	headerKey := dbutils.HeaderKey(number, hash)
	bodyKey := dbutils.BlockBodyKey(number, hash)
	for _, row := range [...]struct {
		table string
		key   []byte
	}{
		{kv.Headers, headerKey},
		{kv.HeaderNumber, hash[:]},
		{kv.HeaderTD, headerKey},
		{kv.BlockBody, bodyKey},
		{kv.BlockAccessList, bodyKey},
	} {
		if err := copyRow(src, dst, row.table, row.key); err != nil {
			return err
		}
	}
	body, err := rawdb.ReadBodyForStorageByKey(src, bodyKey)
	if err != nil || body == nil {
		return err
	}
	first := uint64(body.BaseTxnID)
	txns, err := src.Range(kv.EthTx, hexutil.EncodeTs(first), hexutil.EncodeTs(first+uint64(body.TxCount)), order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	defer txns.Close()
	for txns.HasNext() {
		id, txn, err := txns.Next()
		if err != nil {
			return err
		}
		if err := dst.Put(kv.EthTx, id, txn); err != nil {
			return err
		}
	}
	return nil
}

func copyRow(src kv.Tx, dst kv.RwTx, table string, key []byte) error {
	v, err := src.GetOne(table, key)
	if err != nil || v == nil {
		return err
	}
	return dst.Put(table, key, v)
}

func raiseTxSequence(src kv.Tx, dst kv.RwTx) error {
	srcSeq, err := src.ReadSequence(kv.EthTx)
	if err != nil {
		return err
	}
	dstSeq, err := dst.ReadSequence(kv.EthTx)
	if err != nil {
		return err
	}
	if srcSeq <= dstSeq {
		return nil
	}
	return dst.ResetSequence(kv.EthTx, srcSeq)
}
