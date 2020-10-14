package commands

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// GetBalance returns the balance of the address at the given block
func (api *APIImpl) GetBalance(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error) {
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return nil, err
	}

	tx, err1 := api.db.Begin(ctx, nil, false)
	if err1 != nil {
		return nil, fmt.Errorf("getBalance cannot open tx: %v", err1)
	}
	defer tx.Rollback()
	acc, err := rpchelper.GetAccount(tx, blockNumber, address)
	if err != nil {
		return nil, fmt.Errorf("cant get a balance for account %q for block %v", address.String(), blockNumber)
	}
	if acc == nil {
		// Special case - non-existent account is assumed to have zero balance
		return (*hexutil.Big)(big.NewInt(0)), nil
	}

	return (*hexutil.Big)(acc.Balance.ToBig()), nil
}

// GetTransactionCount returns the number of transactions the given address has sent for the given block number
func (api *APIImpl) GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error) {
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return nil, err
	}
	nonce := hexutil.Uint64(0)
	tx, err1 := api.db.Begin(ctx, nil, false)
	if err1 != nil {
		return nil, fmt.Errorf("getTransactionCount cannot open tx: %v", err1)
	}
	defer tx.Rollback()
	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return &nonce, err
	}
	return (*hexutil.Uint64)(&acc.Nonce), err
}

// GetCode returns the code stored at the given address in the state for the given block number.
func (api *APIImpl) GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return nil, err
	}

	tx, err1 := api.db.Begin(ctx, nil, false)
	if err1 != nil {
		return nil, fmt.Errorf("getCode cannot open tx: %v", err1)
	}
	defer tx.Rollback()
	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return hexutil.Bytes(""), nil
	}
	res, _ := reader.ReadAccountCode(address, acc.CodeHash)
	if res == nil {
		return hexutil.Bytes(""), nil
	}
	return res, nil
}

// GetStorageAt returns a 32-byte long, zero-left-padded value at storage location 'index' of address 'address'. Returns '0x' if no value
func (api *APIImpl) GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error) {
	var empty []byte

	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return hexutil.Encode(common.LeftPadBytes(empty[:], 32)), err
	}

	tx, err1 := api.db.Begin(ctx, nil, false)
	if err1 != nil {
		return "", fmt.Errorf("getStorageAt cannot open tx: %v", err1)
	}
	defer tx.Rollback()
	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return hexutil.Encode(common.LeftPadBytes(empty[:], 32)), err
	}

	location := common.HexToHash(index)
	res, err := reader.ReadAccountStorage(address, acc.Incarnation, &location)
	if err != nil {
		res = empty
	}
	return hexutil.Encode(common.LeftPadBytes(res[:], 32)), err
}
