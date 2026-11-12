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

// MissingError reports the header of a block with matched logs, or a matched
// transaction, that the node does not hold.
type MissingError struct {
	Block uint64
	// TxIndex is the matched transaction's index in the block, -1 for the
	// header.
	TxIndex int
}

func (e *MissingError) Error() string {
	if e.TxIndex < 0 {
		return fmt.Sprintf("header not found: %d", e.Block)
	}
	return fmt.Sprintf("transaction not found at block %d, index %d", e.Block, e.TxIndex)
}

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
	it := rawdbv3.TxNums2BlockNumsFrom(ctx, tx, q.TxNums, txNumbers, order.Asc, begin)
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
				return nil, &MissingError{Block: blockNum, TxIndex: -1}
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
				return nil, &MissingError{Block: blockNum, TxIndex: txIndex}
			}
			if r, err = q.Receipts.GetReceipt(ctx, cfg, tx, header, txn, txIndex, txNum, nil); err != nil {
				return nil, err
			}
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

	clauses := IndexClauses(crit)
	if len(clauses) == 0 {
		if asc == order.Asc {
			return stream.Range[uint64](fromTxNum, toTxNum), nil
		}
		return stream.ReverseRange[uint64](toTxNum, fromTxNum), nil
	}
	q := kv.IndexQuery{Clauses: clauses, FromTs: int(fromTxNum), ToTs: int(toTxNum), Asc: asc}
	if !asc {
		q.FromTs, q.ToTs = int(toTxNum), int(fromTxNum)
	}
	return tx.IndexJoin(q)
}

// IndexClauses returns the index query clauses of a log filter: one clause per topic position that
// is not empty, then one for the addresses. An empty topic position matches any topic, so it has
// no clause.
func IndexClauses(crit filters.FilterCriteria) [][]kv.IndexKey {
	var clauses [][]kv.IndexKey
	for _, position := range crit.Topics {
		if len(position) == 0 {
			continue
		}
		clause := make([]kv.IndexKey, len(position))
		for i := range position {
			clause[i] = kv.IndexKey{Idx: kv.LogTopicIdx, Key: position[i][:]}
		}
		clauses = append(clauses, clause)
	}
	if len(crit.Addresses) > 0 {
		clause := make([]kv.IndexKey, len(crit.Addresses))
		for i := range crit.Addresses {
			clause[i] = kv.IndexKey{Idx: kv.LogAddrIdx, Key: crit.Addresses[i][:]}
		}
		clauses = append(clauses, clause)
	}
	return clauses
}
