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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// RPC response types for eth_getBlockAccessList (EIP-7928).

type rpcStorageChange struct {
	Index hexutil.Uint64 `json:"index"`
	Value common.Hash    `json:"value"`
}

type rpcSlotChanges struct {
	Key     common.Hash         `json:"key"`
	Changes []*rpcStorageChange `json:"changes"`
}

type rpcBalanceChange struct {
	Index hexutil.Uint64 `json:"index"`
	Value *hexutil.Big   `json:"value"`
}

type rpcNonceChange struct {
	Index hexutil.Uint64 `json:"index"`
	Value hexutil.Uint64 `json:"value"`
}

type rpcCodeChange struct {
	Index hexutil.Uint64 `json:"index"`
	Code  hexutil.Bytes  `json:"code"`
}

type rpcAccountAccess struct {
	Address        common.Address      `json:"address"`
	StorageChanges []*rpcSlotChanges   `json:"storageChanges"`
	StorageReads   []common.Hash       `json:"storageReads"`
	BalanceChanges []*rpcBalanceChange `json:"balanceChanges"`
	NonceChanges   []*rpcNonceChange   `json:"nonceChanges"`
	CodeChanges    []*rpcCodeChange    `json:"codeChanges"`
}

// marshalBlockAccessList converts a types.BlockAccessList into the JSON-RPC response format.
func marshalBlockAccessList(bal types.BlockAccessList) []*rpcAccountAccess {
	result := make([]*rpcAccountAccess, len(bal))
	for i, ac := range bal {
		entry := &rpcAccountAccess{
			Address:        ac.Address.Value(),
			StorageChanges: make([]*rpcSlotChanges, len(ac.StorageChanges)),
			StorageReads:   make([]common.Hash, len(ac.StorageReads)),
			BalanceChanges: make([]*rpcBalanceChange, len(ac.BalanceChanges)),
			NonceChanges:   make([]*rpcNonceChange, len(ac.NonceChanges)),
			CodeChanges:    make([]*rpcCodeChange, len(ac.CodeChanges)),
		}
		for j, sc := range ac.StorageChanges {
			slot := &rpcSlotChanges{
				Key:     sc.Slot.Value(),
				Changes: make([]*rpcStorageChange, len(sc.Changes)),
			}
			for k, ch := range sc.Changes {
				slot.Changes[k] = &rpcStorageChange{
					Index: hexutil.Uint64(ch.Index),
					Value: ch.Value.Bytes32(),
				}
			}
			entry.StorageChanges[j] = slot
		}
		for j, sr := range ac.StorageReads {
			entry.StorageReads[j] = sr.Value()
		}
		for j, bc := range ac.BalanceChanges {
			v := bc.Value.ToBig()
			entry.BalanceChanges[j] = &rpcBalanceChange{
				Index: hexutil.Uint64(bc.Index),
				Value: (*hexutil.Big)(v),
			}
		}
		for j, nc := range ac.NonceChanges {
			entry.NonceChanges[j] = &rpcNonceChange{
				Index: hexutil.Uint64(nc.Index),
				Value: hexutil.Uint64(nc.Value),
			}
		}
		for j, cc := range ac.CodeChanges {
			entry.CodeChanges[j] = &rpcCodeChange{
				Index: hexutil.Uint64(cc.Index),
				Code:  cc.Bytecode,
			}
		}
		result[i] = entry
	}
	return result
}

// GetBlockAccessList returns the block access list for a given block (EIP-7928).
func (api *APIImpl) GetBlockAccessList(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) ([]*rpcAccountAccess, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if numberOrHash.BlockNumber != nil && *numberOrHash.BlockNumber == rpc.PendingBlockNumber {
		return nil, errors.New("pending block access list is not available")
	}

	blockNum, blockHash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, numberOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil
		}
		return nil, err
	}

	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !chainConfig.IsAmsterdam(header.Time) {
		return nil, &rpc.CustomError{
			Code:    4445,
			Message: "block access list not available for pre-Amsterdam blocks",
		}
	}

	data, err := rawdb.ReadBlockAccessListBytes(tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	bal, err := types.DecodeBlockAccessListBytes(data)
	if err != nil {
		return nil, err
	}

	return marshalBlockAccessList(bal), nil
}
