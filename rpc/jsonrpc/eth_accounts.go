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
	"fmt"
	"math/big"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// maxGetStorageSlots caps the total number of storage slots allowed across
// all addresses in a single eth_getStorageValues request.
const maxGetStorageSlots = 1024

// stateReaderAt opens a temporal read transaction, resolves the canonical block number,
// checks prune history and block execution, and creates a state reader.
// The caller must defer tx.Rollback() on the returned tx.
func (api *APIImpl) stateReaderAt(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (kv.TemporalTx, state.StateReader, error) {
	tx, err := api.db.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, nil, err
	}

	blockNumber, _, latest, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		tx.Rollback()
		return nil, nil, err
	}

	if err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNumber); err != nil {
		tx.Rollback()
		return nil, nil, err
	}

	stateTx := api.filters.WithTemporalOverlay(tx)
	if err = rpchelper.CheckBlockExecuted(stateTx, blockNumber); err != nil {
		tx.Rollback()
		return nil, nil, err
	}

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, stateTx, blockNumber, latest, 0, api.stateCache, api._txNumReader)
	if err != nil {
		tx.Rollback()
		return nil, nil, err
	}
	return tx, reader, nil
}

// GetBalance implements eth_getBalance. Returns the balance of an account for a given address.
func (api *APIImpl) GetBalance(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error) {
	tx, reader, err := api.stateReaderAt(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	acc, err := reader.ReadAccountData(accounts.InternAddress(address))
	if err != nil {
		return nil, fmt.Errorf("cant get a balance for account %x: %w", address.String(), err)
	}
	if acc == nil {
		// Special case - non-existent account is assumed to have zero balance
		return (*hexutil.Big)(big.NewInt(0)), nil
	}

	return (*hexutil.Big)(acc.Balance.ToBig()), nil
}

// GetTransactionCount implements eth_getTransactionCount. Returns the number of transactions sent from an address (the nonce).
func (api *APIImpl) GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error) {
	if blockNrOrHash.BlockNumber != nil && *blockNrOrHash.BlockNumber == rpc.PendingBlockNumber {
		reply, err := api.txPool.Nonce(ctx, &txpoolproto.NonceRequest{
			Address: gointerfaces.ConvertAddressToH160(address),
		}, &grpc.EmptyCallOption{})
		if err != nil {
			return nil, err
		}
		if reply.Found {
			reply.Nonce++
			return (*hexutil.Uint64)(&reply.Nonce), nil
		}
		// txpool doesn't know about this address yet: fall through to DB lookup
	}
	tx, reader, err := api.stateReaderAt(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	nonce := hexutil.Uint64(0)
	acc, err := reader.ReadAccountData(accounts.InternAddress(address))
	if acc == nil || err != nil {
		return &nonce, err
	}
	return (*hexutil.Uint64)(&acc.Nonce), err
}

// GetCode implements eth_getCode. Returns the byte code at a given address (if it's a smart contract).
func (api *APIImpl) GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	tx, reader, err := api.stateReaderAt(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	addr := accounts.InternAddress(address)
	acc, err := reader.ReadAccountData(addr)
	if acc == nil || err != nil || acc.IsEmptyCodeHash() {
		return hexutil.Bytes(""), nil
	}
	res, _ := reader.ReadAccountCode(addr)
	if res == nil {
		return hexutil.Bytes(""), nil
	}
	return res, nil
}

// GetStorageValues implements eth_getStorageValues. Returns the values of multiple
// storage slots for multiple accounts in a single request.
func (api *APIImpl) GetStorageValues(ctx context.Context, requests map[common.Address][]common.Hash, blockNrOrHash rpc.BlockNumberOrHash) (map[common.Address][]hexutil.Bytes, error) {
	var totalSlots int

	for _, keys := range requests {
		totalSlots += len(keys)
		if totalSlots > maxGetStorageSlots {
			return nil, clientLimitExceededError(fmt.Sprintf("too many slots (max %d)", maxGetStorageSlots))
		}
	}
	if totalSlots == 0 {
		return nil, &rpc.InvalidParamsError{Message: "empty request"}
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNrOrHash.RequireCanonical = true
	blockNumber, _, latest, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}

	stateTx := api.filters.WithTemporalOverlay(tx)
	err = rpchelper.CheckBlockExecuted(stateTx, blockNumber)
	if err != nil {
		return nil, err
	}

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, stateTx, blockNumber, latest, 0, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, err
	}

	result := make(map[common.Address][]hexutil.Bytes, len(requests))
	for addr, keys := range requests {
		vals := make([]hexutil.Bytes, len(keys))
		address := accounts.InternAddress(addr)
		for i, key := range keys {
			location := accounts.InternKey(key)
			res, _, err := reader.ReadAccountStorage(address, location)
			if err != nil {
				return nil, err
			}

			vals[i] = res.PaddedBytes(32)
		}
		result[addr] = vals
	}

	return result, nil
}

// GetStorageAt implements eth_getStorageAt. Returns the value from a storage position at a given address.
func (api *APIImpl) GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error) {
	var empty []byte
	// Validation for index i.e. storage slot is non-standard: it can be interpreted as QUANTITY (stricter) or as DATA (like Hive tests do).
	// Waiting for a spec, we choose the latter because it's more general, but we check that the length is not greater than 64 hex-digits.
	indexBytes, err := hexutil.FromHexWithValidation(index)
	if err != nil {
		return "", &rpc.InvalidParamsError{Message: "unable to decode storage key: " + hexutil.ErrHexStringInvalid.Error()}
	}
	if len(indexBytes) > 32 {
		return "", &rpc.InvalidParamsError{Message: hexutil.ErrTooBigHexString.Error()}
	}
	tx, reader, err := api.stateReaderAt(ctx, blockNrOrHash)
	if err != nil {
		return hexutil.Encode(common.LeftPadBytes(empty, 32)), err
	}
	defer tx.Rollback()

	addr := accounts.InternAddress(address)
	acc, err := reader.ReadAccountData(addr)
	if acc == nil || err != nil {
		return hexutil.Encode(common.LeftPadBytes(empty, 32)), err
	}

	location := accounts.InternKey(common.HexToHash(index))
	res, _, err := reader.ReadAccountStorage(addr, location)
	if err != nil {
		return hexutil.Encode(common.LeftPadBytes(empty, 32)), err
	}
	return hexutil.Encode(res.PaddedBytes(32)), err
}

// Exist returns whether an account for a given address exists in the database.
func (api *APIImpl) Exist(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (bool, error) {
	tx, reader, err := api.stateReaderAt(ctx, blockNrOrHash)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	acc, err := reader.ReadAccountData(accounts.InternAddress(address))
	if err != nil {
		return false, err
	}
	if acc == nil {
		return false, nil
	}

	return true, nil
}
