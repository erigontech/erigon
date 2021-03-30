package commands

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// GetBalance implements eth_getBalance. Returns the balance of an account for a given address.
func (api *APIImpl) GetBalance(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error) {
	tx, err1 := api.db.Begin(ctx)
	if err1 != nil {
		return nil, fmt.Errorf("getBalance cannot open tx: %v", err1)
	}
	defer tx.Rollback()
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, ethdb.NewRoTxDb(tx), api.pending)
	if err != nil {
		return nil, err
	}

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

// GetTransactionCount implements eth_getTransactionCount. Returns the number of transactions sent from an address (the nonce).
func (api *APIImpl) GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error) {
	tx, err1 := api.db.Begin(ctx)
	if err1 != nil {
		return nil, fmt.Errorf("getTransactionCount cannot open tx: %v", err1)
	}
	defer tx.Rollback()
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, ethdb.NewRoTxDb(tx), api.pending)
	if err != nil {
		return nil, err
	}
	nonce := hexutil.Uint64(0)
	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return &nonce, err
	}
	return (*hexutil.Uint64)(&acc.Nonce), err
}

// GetCode implements eth_getCode. Returns the byte code at a given address (if it's a smart contract).
func (api *APIImpl) GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	tx, err1 := api.db.Begin(ctx)
	if err1 != nil {
		return nil, fmt.Errorf("getCode cannot open tx: %v", err1)
	}
	defer tx.Rollback()
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, ethdb.NewRoTxDb(tx), api.pending)
	if err != nil {
		return nil, err
	}

	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return hexutil.Bytes(""), nil
	}
	res, _ := reader.ReadAccountCode(address, acc.Incarnation, acc.CodeHash)
	if res == nil {
		return hexutil.Bytes(""), nil
	}
	return res, nil
}

// GetStorageAt implements eth_getStorageAt. Returns the value from a storage position at a given address.
func (api *APIImpl) GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error) {
	var empty []byte

	tx, err1 := api.db.Begin(ctx)
	if err1 != nil {
		return hexutil.Encode(common.LeftPadBytes(empty[:], 32)), err1
	}
	defer tx.Rollback()

	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, ethdb.NewRoTxDb(tx), api.pending)
	if err != nil {
		return hexutil.Encode(common.LeftPadBytes(empty[:], 32)), err
	}
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
