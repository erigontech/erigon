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

// Package logquery reads the logs of a block range that match a filter, from
// the log indexes and the receipts.
package logquery

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// ErrTooManyLogs reports a query that matches more logs than its limit.
var ErrTooManyLogs = errors.New("query returns too many logs")

// Query reads logs through a node's block readers and receipts.
type Query struct {
	TxNums   rawdbv3.TxNumsReader
	Headers  dbservices.HeaderReader
	Txns     dbservices.TxnReader
	Receipts *receipts.Generator
}

// Logs returns the logs of blocks [begin, end] that match crit, in order.
// A positive maxResults bounds them; more fail with ErrTooManyLogs.
func (q *Query) Logs(ctx context.Context, tx kv.TemporalTx, cfg *chain.Config, begin, end uint64, crit filters.FilterCriteria, maxResults int) (types.Logs, error) {
	logs := types.Logs{}
	filter := types.NewLogFilter(crit.Addresses, crit.Topics)

	txNumbers, err := MatchingTxNums(q.TxNums, tx, begin, end, crit, order.Asc)
	if err != nil {
		return nil, err
	}
	it := rawdbv3.TxNums2BlockNums(ctx, tx, q.TxNums, txNumbers, order.Asc)
	defer it.Close()

	var header *types.Header
	for it.HasNext() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		txNum, blockNum, txIndex, _, isSystemTxn, blockNumChanged, err := it.Next()
		if err != nil {
			return nil, err
		}
		if blockNumChanged {
			if header, err = q.Headers.HeaderByNumber(ctx, tx, blockNum); err != nil {
				return nil, err
			}
			if header == nil {
				return nil, fmt.Errorf("header not found: %d", blockNum)
			}
		}
		if isSystemTxn {
			continue
		}

		if r, ok := q.Receipts.TryGetCachedReceipt(header.Hash(), txNum, txIndex); ok {
			if logs, err = AppendLogs(logs, r.Logs, filter, header.Time, maxResults); err != nil {
				return nil, err
			}
			continue
		}
		r, ok, err := q.Receipts.PersistedReceipt(ctx, tx, header, txIndex, txNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			var txn types.Transaction
			if txn, ok, err = q.Txns.TxnByIdxInBlock(ctx, tx, blockNum, txIndex); err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("transaction not found at block %d, index %d", blockNum, txIndex)
			}
			if r, err = q.Receipts.GetReceipt(ctx, cfg, tx, header, txn, txIndex, txNum, nil); err != nil {
				return nil, err
			}
		}
		if r == nil {
			return nil, err
		}
		if logs, err = AppendLogs(logs, r.Logs, filter, header.Time, maxResults); err != nil {
			return nil, err
		}
	}
	return logs, nil
}

// AppendLogs appends the receiptLogs that match filter to logs; a positive
// maxResults bounds the result, more fail with ErrTooManyLogs.
func AppendLogs(logs types.Logs, receiptLogs types.Logs, filter *types.LogFilter, blockTime uint64, maxResults int) (types.Logs, error) {
	// One entry past the cap is enough to detect the overflow without converting the rest.
	// At math.MaxInt there is no such entry, so the append stays unlimited.
	var limit int
	if maxResults != 0 && maxResults != math.MaxInt {
		limit = maxResults + 1
	}
	logs = receiptLogs.AppendFilteredLogs(logs, filter, blockTime, limit)
	if maxResults != 0 && len(logs) > maxResults {
		return nil, ErrTooManyLogs
	}
	return logs, nil
}

// MatchingTxNums streams the txNums of blocks [begin, end] the log indexes
// match against crit's addresses and topics, every one when crit sets none.
func MatchingTxNums(txNumsReader rawdbv3.TxNumsReader, tx kv.TemporalTx, begin, end uint64, crit filters.FilterCriteria, asc order.By) (out stream.U64, err error) {
	//[from,to)
	var fromTxNum, toTxNum uint64
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return out, err
	}
	defer c.Close()
	if begin > 0 {
		fromTxNum, err = txNumsReader.MinWithCursor(context.Background(), tx, c, begin)
		if err != nil {
			return out, err
		}
	}
	toTxNum, err = txNumsReader.MaxWithCursor(context.Background(), tx, c, end)
	if err != nil {
		return out, err
	}

	if asc {
		toTxNum++
	} else {
		toTxNum--
	}

	var topicsBitmap stream.U64

	if asc {
		topicsBitmap, err = topicsBitmapV3(tx, crit.Topics, fromTxNum, toTxNum, asc)
	} else {
		topicsBitmap, err = topicsBitmapV3(tx, crit.Topics, toTxNum, fromTxNum, asc)
	}
	if err != nil {
		return out, err
	}
	if topicsBitmap != nil {
		out = topicsBitmap
	}

	var addrBitmap stream.U64

	if asc {
		addrBitmap, err = addrsBitmapV3(tx, crit.Addresses, fromTxNum, toTxNum, asc)
	} else {
		addrBitmap, err = addrsBitmapV3(tx, crit.Addresses, toTxNum, fromTxNum, asc)
	}
	if err != nil {
		return out, err
	}
	if addrBitmap != nil {
		if out == nil {
			out = addrBitmap
		} else {
			out = stream.Intersect[uint64](out, addrBitmap, asc, kv.Unlim)
		}
	}
	if out == nil {
		if asc == order.Asc {
			out = stream.Range[uint64](fromTxNum, toTxNum)
		} else {
			out = stream.ReverseRange[uint64](toTxNum, fromTxNum)
		}
	}
	return out, nil
}

// The Topic list restricts matches to particular event topics. Each event has a list
// of topics. Topics matches a prefix of that list. An empty element slice matches any
// topic. Non-empty elements represent an alternative that matches any of the
// contained topics.
//
// Examples:
// {} or nil          matches any topic list
// {{A}}              matches topic A in first position
// {{}, {B}}          matches any topic in first position AND B in second position
// {{A}, {B}}         matches topic A in first position AND B in second position
// {{A, B}, {C, D}}   matches topic (A OR B) in first position AND (C OR D) in second position
func topicsBitmapV3(tx kv.TemporalTx, topics [][]common.Hash, from, to uint64, asc order.By) (res stream.U64, err error) {
	for _, sub := range topics {
		if len(sub) == 0 {
			continue
		}

		var topicsUnion stream.U64
		for _, topic := range sub {
			it, err := tx.IndexRange(kv.LogTopicIdx, topic[:], int(from), int(to), asc, kv.Unlim)
			if err != nil {
				return nil, err
			}
			topicsUnion = stream.Union[uint64](topicsUnion, it, asc, kv.Unlim)
		}

		if res == nil {
			res = topicsUnion
			continue
		}
		res = stream.Intersect[uint64](res, topicsUnion, asc, kv.Unlim)
	}
	return res, nil
}

func addrsBitmapV3(tx kv.TemporalTx, addrs []common.Address, from, to uint64, asc order.By) (res stream.U64, err error) {
	for _, addr := range addrs {
		it, err := tx.IndexRange(kv.LogAddrIdx, addr[:], int(from), int(to), asc, kv.Unlim)
		if err != nil {
			return nil, err
		}
		res = stream.Union[uint64](res, it, asc, kv.Unlim)
	}
	return res, nil
}
