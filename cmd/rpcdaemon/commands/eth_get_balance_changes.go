package commands

import (
	"bytes"
	"context"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

type oldNewBalance struct {
	oldBalance *hexutil.Big
	newBalance *hexutil.Big
}

func (api *APIImpl) GetBalanceChangesInBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (map[common.Address]*hexutil.Big, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNumber, _, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}

	c, err := tx.Cursor(kv.AccountChangeSet)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	startkey := dbutils.EncodeBlockNumber(blockNumber)

	decodeFn := changeset.Mapper[kv.AccountChangeSet].Decode

	balancesMapping := make(map[common.Address]*hexutil.Big)

	newReader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, api.filters, api.stateCache)
	if err != nil {
		return nil, err
	}

	for dbKey, dbValue, _ := c.Seek(startkey); bytes.Equal(dbKey, startkey) && dbKey != nil; dbKey, dbValue, _ = c.Next() {
		_, addressBytes, v, err := decodeFn(dbKey, dbValue)
		if err != nil {
			return nil, err
		}

		var oldAcc accounts.Account
		if err = oldAcc.DecodeForStorage(v); err != nil {
			return nil, err
		}
		oldBalance := oldAcc.Balance

		address := common.BytesToAddress(addressBytes)

		newAcc, err := newReader.ReadAccountData(address)
		if err != nil {
			return nil, err
		}

		newBalance := uint256.NewInt(0)
		if newAcc != nil {
			newBalance = &newAcc.Balance
		}

		if !oldBalance.Eq(newBalance) {
			newBalanceDesc := (*hexutil.Big)(newBalance.ToBig())
			balancesMapping[address] = newBalanceDesc
		}
	}

	return balancesMapping, nil
}

func PrintChangedBalances(mapping map[common.Address]oldNewBalance) error {

	for address, balances := range mapping {
		fmt.Println("address: ", address)
		fmt.Println("old balance: ", balances.oldBalance)
		fmt.Println("new balance: ", balances.newBalance)
	}

	return nil
}
