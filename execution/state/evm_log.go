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

package state

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// EvmLog is the EVM-internal log entry stored in IntraBlockState's log buffer.
// Topics live inline as a fixed array (LOG emits at most 4), so a log emitted
// during execution needs no per-log topics allocation. It is materialized into
// a types.Log only at the receipt/tracing boundary (see materializeLogs).
//
// The derived fields (BlockNumber, TxIndex, Index) are not stored: the receipt
// layer recomputes them from block/tx context and the cumulative log index
// (see CreateReceipt / Receipt.DeriveFields), so materialization fills them from
// context instead.
type EvmLog struct {
	Data      hexutil.Bytes
	Topics    [4]common.Hash
	Address   common.Address
	NumTopics uint8
	Removed   bool
}

// setTopics copies a topics slice into the inline array. Topics beyond the 4th
// are dropped — consensus caps LOG at 4 topics, and no callers exceed it.
func (l *EvmLog) setTopics(topics []common.Hash) {
	l.NumTopics = uint8(min(len(topics), len(l.Topics)))
	copy(l.Topics[:], topics)
}

// toTypesLog materializes a single owned types.Log with the given derived fields,
// allocating its Topics slice.
func (l *EvmLog) toTypesLog(blockNum uint64, txIndex uint) types.Log {
	return types.Log{
		Address:     l.Address,
		Topics:      append([]common.Hash(nil), l.Topics[:l.NumTopics]...),
		Data:        l.Data,
		BlockNumber: hexutil.Uint64(blockNum),
		TxIndex:     hexutil.Uint(txIndex),
		Removed:     l.Removed,
	}
}

// MaterializeLogs converts an EVM-internal log slice into owned types.Log
func MaterializeLogs(src []EvmLog, blockNum uint64, txIndex uint) (types.Logs, []types.Log) {
	if len(src) == 0 {
		return nil, nil
	}
	var totalTopics int
	for i := range src {
		totalTopics += int(src[i].NumTopics)
	}
	topicsBuf := make([]common.Hash, totalTopics)
	backing := make([]types.Log, len(src))
	logs := make(types.Logs, len(src))
	off := 0
	for i := range src {
		s := &src[i]
		n := int(s.NumTopics)
		t := topicsBuf[off : off+n : off+n]
		copy(t, s.Topics[:n])
		off += n
		backing[i] = types.Log{
			Address:     s.Address,
			Topics:      t,
			Data:        s.Data,
			BlockNumber: hexutil.Uint64(blockNum),
			TxIndex:     hexutil.Uint(txIndex),
			Removed:     s.Removed,
		}
		logs[i] = &backing[i]
	}
	return logs, backing
}
