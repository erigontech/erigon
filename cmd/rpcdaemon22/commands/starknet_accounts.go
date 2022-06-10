package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

// GetCode implements starknet_getCode. Returns the byte code at a given address (if it's a smart contract).
func (api *StarknetImpl) GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	tx, err1 := api.db.BeginRo(ctx)
	if err1 != nil {
		return nil, fmt.Errorf("getCode cannot open tx: %w", err1)
	}
	defer tx.Rollback()
	blockNumber, _, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}

	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return hexutil.Bytes(""), nil
	}
	res, err := reader.ReadAccountCode(address, acc.Incarnation, acc.CodeHash)
	if res == nil || err != nil {
		return hexutil.Bytes(""), nil
	}
	if res == nil {
		return hexutil.Bytes(""), nil
	}
	return res, nil
}
