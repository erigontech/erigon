package commands

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

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
