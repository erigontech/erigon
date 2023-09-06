package jsonrpc

import (
	"context"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon/turbo/rpchelper"

	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rpc"
)

// GetBalance implements eth_getBalance. Returns the balance of an account for a given address.
func (api *APIImpl) GetBalance(ctx context.Context, address libcommon.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error) {
	tx, err1 := api.db.BeginRo(ctx)
	if err1 != nil {
		return nil, fmt.Errorf("getBalance cannot open tx: %w", err1)
	}
	defer tx.Rollback()
	reader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(tx), "")
	if err != nil {
		return nil, err
	}

	acc, err := reader.ReadAccountData(address)
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
func (api *APIImpl) GetTransactionCount(ctx context.Context, address libcommon.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error) {
	if blockNrOrHash.BlockNumber != nil && *blockNrOrHash.BlockNumber == rpc.PendingBlockNumber {
		reply, err := api.txPool.Nonce(ctx, &txpool_proto.NonceRequest{
			Address: gointerfaces.ConvertAddressToH160(address),
		}, &grpc.EmptyCallOption{})
		if err != nil {
			return nil, err
		}
		if reply.Found {
			reply.Nonce++
			return (*hexutil.Uint64)(&reply.Nonce), nil
		}
	}
	tx, err1 := api.db.BeginRo(ctx)
	if err1 != nil {
		return nil, fmt.Errorf("getTransactionCount cannot open tx: %w", err1)
	}
	defer tx.Rollback()
	reader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(tx), "")
	if err != nil {
		return nil, err
	}
	nonce := hexutil.Uint64(0)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return &nonce, err
	}
	return (*hexutil.Uint64)(&acc.Nonce), err
}

// GetCode implements eth_getCode. Returns the byte code at a given address (if it's a smart contract).
func (api *APIImpl) GetCode(ctx context.Context, address libcommon.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutility.Bytes, error) {
	tx, err1 := api.db.BeginRo(ctx)
	if err1 != nil {
		return nil, fmt.Errorf("getCode cannot open tx: %w", err1)
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, fmt.Errorf("read chain config: %v", err)
	}
	reader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(tx), chainConfig.ChainName)
	if err != nil {
		return nil, err
	}

	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return hexutility.Bytes(""), nil
	}
	res, _ := reader.ReadAccountCode(address, acc.Incarnation, acc.CodeHash)
	if res == nil {
		return hexutility.Bytes(""), nil
	}
	return res, nil
}

// GetStorageAt implements eth_getStorageAt. Returns the value from a storage position at a given address.
func (api *APIImpl) GetStorageAt(ctx context.Context, address libcommon.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error) {
	var empty []byte

	tx, err1 := api.db.BeginRo(ctx)
	if err1 != nil {
		return hexutility.Encode(common.LeftPadBytes(empty, 32)), err1
	}
	defer tx.Rollback()

	reader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(tx), "")
	if err != nil {
		return hexutility.Encode(common.LeftPadBytes(empty, 32)), err
	}
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return hexutility.Encode(common.LeftPadBytes(empty, 32)), err
	}

	location := libcommon.HexToHash(index)
	res, err := reader.ReadAccountStorage(address, acc.Incarnation, &location)
	if err != nil {
		res = empty
	}
	return hexutility.Encode(common.LeftPadBytes(res, 32)), err
}

// Exist returns whether an account for a given address exists in the database.
func (api *APIImpl) Exist(ctx context.Context, address libcommon.Address, blockNrOrHash rpc.BlockNumberOrHash) (bool, error) {
	tx, err1 := api.db.BeginRo(ctx)
	if err1 != nil {
		return false, err1
	}
	defer tx.Rollback()

	reader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(tx), "")
	if err != nil {
		return false, err
	}
	acc, err := reader.ReadAccountData(address)
	if err != nil {
		return false, err
	}
	if acc == nil {
		return false, nil
	}

	return true, nil
}
