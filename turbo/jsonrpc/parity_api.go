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
	"context"
	"errors"
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/turbo/rpchelper"

	"github.com/erigontech/erigon/rpc"
)

var latestTag = libcommon.BytesToHash([]byte("latest"))

var ErrWrongTag = fmt.Errorf("listStorageKeys wrong block tag or number: must be '%s' ('latest')", latestTag)

// ParityAPI the interface for the parity_ RPC commands
type ParityAPI interface {
	ListStorageKeys(ctx context.Context, account libcommon.Address, quantity int, offset *hexutility.Bytes, blockNumber rpc.BlockNumberOrHash) ([]hexutility.Bytes, error)
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
func (api *ParityAPIImpl) ListStorageKeys(ctx context.Context, account libcommon.Address, quantity int, offset *hexutility.Bytes, blockNumberOrTag rpc.BlockNumberOrHash) ([]hexutility.Bytes, error) {
	if err := api.checkBlockNumber(blockNumberOrTag); err != nil {
		return nil, err
	}
	keys := make([]hexutility.Bytes, 0)

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("listStorageKeys cannot open tx: %w", err)
	}
	defer tx.Rollback()
	a, err := rpchelper.NewLatestStateReader(tx).ReadAccountData(account)
	if err != nil {
		return nil, err
	} else if a == nil {
		return nil, errors.New("acc not found")
	}

	bn := rawdb.ReadCurrentBlockNumber(tx)
	minTxNum, err := api._txNumReader.Min(tx, *bn)
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
		keys = append(keys, libcommon.CopyBytes(k[20:]))
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
