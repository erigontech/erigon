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

package l1

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonrpc/logquery"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// Native is a ChainReader over an Erigon L1 node's database, local or remote. Its
// head is the latest executed block, the last one with logs.
type Native struct {
	db     kv.TemporalRoDB
	blocks dbservices.FullBlockReader
	query  logquery.Query
	config *chain.Config
}

var _ ChainReader = (*Native)(nil)

// NewNative returns the reader of the L1 chain config whose blocks blocks
// reads from db, with receipts from receipts.
func NewNative(db kv.TemporalRoDB, blocks dbservices.FullBlockReader, receipts *receipts.Generator, config *chain.Config) *Native {
	return &Native{
		db:     db,
		blocks: blocks,
		query:  logquery.Query{TxNums: blocks.TxnumReader(), Headers: blocks, Txns: blocks, Receipts: receipts, Strict: true},
		config: config,
	}
}

func (r *Native) ChainID(context.Context) (uint64, error) { return r.config.ChainID.Uint64(), nil }

func toHeader(h *types.Header) *Header {
	return &Header{Number: h.Number.Uint64(), Hash: h.Hash(), ParentHash: h.ParentHash, Time: h.Time}
}

func (r *Native) HeaderByNumber(ctx context.Context, number uint64) (h *Header, err error) {
	err = r.db.View(ctx, func(tx kv.Tx) error {
		executed, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		if number > executed {
			return fmt.Errorf("%w: block %d above the executed head %d", ErrNotFound, number, executed)
		}
		hdr, err := r.blocks.HeaderByNumber(ctx, tx, number)
		if err != nil {
			return err
		}
		if hdr == nil {
			return fmt.Errorf("%w: block %d", ErrNotFound, number)
		}
		h = toHeader(hdr)
		return nil
	})
	return h, err
}

func (r *Native) HeaderByHash(ctx context.Context, hash common.Hash) (h *Header, err error) {
	err = r.db.View(ctx, func(tx kv.Tx) error {
		hdr, err := r.blocks.HeaderByHash(ctx, tx, hash)
		if err != nil {
			return err
		}
		if hdr == nil {
			return fmt.Errorf("%w: block %x", ErrNotFound, hash)
		}
		h = toHeader(hdr)
		return nil
	})
	return h, err
}

func (r *Native) Head(ctx context.Context) (h *Header, err error) {
	var executed uint64
	if err := r.db.View(ctx, func(tx kv.Tx) error {
		executed, err = stages.GetStageProgress(tx, stages.Execution)
		return err
	}); err != nil {
		return nil, err
	}
	return r.HeaderByNumber(ctx, executed)
}

func (r *Native) Finalized(ctx context.Context) (*Header, error) {
	var hash common.Hash
	if err := r.db.View(ctx, func(tx kv.Tx) error {
		hash = rawdb.ReadForkchoiceFinalized(tx)
		return nil
	}); err != nil {
		return nil, err
	}
	if hash == (common.Hash{}) {
		return nil, fmt.Errorf("%w: no finalized block", ErrNotFound)
	}
	return r.HeaderByHash(ctx, hash)
}

func (r *Native) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) (logs []*types.Log, err error) {
	crit := filters.FilterCriteria{Addresses: []common.Address{address}, Topics: topics}
	err = r.db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		executed, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		if to > executed {
			return fmt.Errorf("%w: logs of [%d, %d] above the executed head %d", ErrNotFound, from, to, executed)
		}
		logs, err = r.query.Logs(ctx, tx, r.config, from, to, crit, 0)
		return err
	})
	return logs, err
}

func (r *Native) TransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index uint64) (txn types.Transaction, err error) {
	err = r.db.View(ctx, func(tx kv.Tx) error {
		number, err := r.blocks.HeaderNumber(ctx, tx, blockHash)
		if err != nil {
			return err
		}
		if number == nil {
			return fmt.Errorf("%w: block %x", ErrNotFound, blockHash)
		}
		body, err := r.blocks.BodyWithTransactions(ctx, tx, blockHash, *number)
		if err != nil {
			return err
		}
		if body == nil || index >= uint64(len(body.Transactions)) {
			return fmt.Errorf("%w: transaction %d of block %x", ErrNotFound, index, blockHash)
		}
		txn = body.Transactions[index]
		return nil
	})
	return txn, err
}
