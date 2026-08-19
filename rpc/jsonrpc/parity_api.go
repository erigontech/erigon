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

package jsonrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var latestTag = common.BytesToHash([]byte("latest"))

var ErrWrongTag = fmt.Errorf("listStorageKeys wrong block tag or number: must be '%s' ('latest')", latestTag)

// ParityAPI the interface for the parity_ RPC commands
type ParityAPI interface {
	ListStorageKeys(ctx context.Context, account common.Address, quantity int, offset *hexutil.Bytes, blockNumber rpc.BlockNumberOrHash) ([]hexutil.Bytes, error)
}

// ParityAPIImpl data structure to store things needed for parity_ commands
type ParityAPIImpl struct {
	*BaseAPI
	db kv.TemporalRoDB
}

// NewParityAPIImpl returns ParityAPIImpl instance
func NewParityAPIImpl(base *BaseAPI, db kv.TemporalRoDB) *ParityAPIImpl {
	return &ParityAPIImpl{
		BaseAPI: base,
		db:      db,
	}
}

// ListStorageKeys implements parity_listStorageKeys. Returns all storage keys of the given address
func (api *ParityAPIImpl) ListStorageKeys(ctx context.Context, account common.Address, quantity int, offset *hexutil.Bytes, blockNumberOrTag rpc.BlockNumberOrHash) ([]hexutil.Bytes, error) {
	if err := api.checkBlockNumber(blockNumberOrTag); err != nil {
		return nil, err
	}
	keys := make([]hexutil.Bytes, 0)

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("listStorageKeys cannot open tx: %w", err)
	}
	defer tx.Rollback()
	a, err := rpchelper.NewLatestStateReader(execctx.NewTemporalTxStateGetter(tx)).ReadAccountData(accounts.InternAddress(account))
	if err != nil {
		return nil, err
	} else if a == nil {
		return nil, errors.New("acc not found")
	}

	// Committed view: bn must match the state version the RangeAsOf scan
	// below can see (the overlay exposes no domain range reads).
	bn := rawdb.ReadCurrentBlockNumber(tx)
	if bn == nil {
		return nil, errors.New("current block number not found")
	}
	// Min(bn+1) is the first txNum past bn — the state the latest-state account
	// read above sees. Min(bn) would scan storage as of the end of bn-1.
	minTxNum, err := api._txNumReader.Min(ctx, tx, *bn+1)
	if err != nil {
		return nil, err
	}

	from := account[:]
	if offset != nil {
		from = append(from, *offset...)
	}
	to, _ := kv.NextSubtree(account[:])
	r, err := tx.RangeAsOf(kv.StorageDomain, from, to, minTxNum, order.Asc, quantity)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	for r.HasNext() {
		k, _, err := r.Next()
		if err != nil {
			return nil, err
		}
		keys = append(keys, bytes.Clone(k[20:]))
	}
	return keys, nil
}

func (api *ParityAPIImpl) checkBlockNumber(blockNumber rpc.BlockNumberOrHash) error {
	num, isNum := blockNumber.Number()
	if isNum && rpc.LatestBlockNumber == num {
		return nil
	}
	return ErrWrongTag
}
