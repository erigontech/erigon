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
	"cmp"
	"encoding/binary"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
)

const retainedBlockLimit = 16

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

func (e *ExecModule) retainSideBlocks(tx kv.TemporalTx, committed []common.Hash) error {
	for _, hash := range committed {
		delete(e.pendingBlocks, hash)
	}
	side := make([]common.Hash, 0, len(e.pendingBlocks))
	for hash := range e.pendingBlocks {
		side = append(side, hash)
	}
	slices.SortFunc(side, func(a, b common.Hash) int {
		return cmp.Compare(e.pendingBlocks[b].number, e.pendingBlocks[a].number)
	})
	for _, hash := range side[min(len(side), retainedBlockLimit):] {
		delete(e.pendingBlocks, hash)
	}
	src := e.pendingBlocksView(tx)
	if len(e.pendingBlocks) == 0 || src == nil {
		e.dropPendingBlocks()
		return nil
	}
	retained, err := membatchwithdb.NewMemoryBatch(tx, tx.Debug().Dirs().Tmp, e.logger)
	if err != nil {
		return err
	}
	for hash, block := range e.pendingBlocks {
		if err := copyBlockRows(src, retained, hash, block.number); err != nil {
			retained.Close()
			return err
		}
	}
	if err := raiseTxSequence(src, retained); err != nil {
		retained.Close()
		return err
	}
	retained.DetachDB()
	e.swapRetainedBlocks(retained)
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
	if err := raiseTxSequence(src, overlay); err != nil {
		return err
	}
	e.swapRetainedBlocks(nil)
	return nil
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
	for id := first; id < first+uint64(body.TxCount); id++ {
		if err := copyRow(src, dst, kv.EthTx, binary.BigEndian.AppendUint64(nil, id)); err != nil {
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
	return dst.Put(table, bytes.Clone(key), bytes.Clone(v))
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
