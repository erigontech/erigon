package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

func (api *APIImpl) GetBalance(_ context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error) {
	var blockNumber uint64

	hash, ok := blockNrOrHash.Hash()
	if !ok {
		blockNumber = uint64(blockNrOrHash.BlockNumber.Int64())
	} else {
		block := rawdb.ReadBlockByHash(api.dbReader, hash)
		if block == nil {
			return nil, fmt.Errorf("block %x not found", hash)
		}
		blockNumber = block.NumberU64()

		if blockNrOrHash.RequireCanonical && rawdb.ReadCanonicalHash(api.dbReader, blockNumber) != hash {
			return nil, fmt.Errorf("hash %q is not currently canonical", hash.String())
		}
	}

	acc, err := GetAccount(api.db, blockNumber, address)
	if err != nil {
		return nil, fmt.Errorf("cant get a balance for account %q for block %v", address.String(), blockNumber)
	}

	return (*hexutil.Big)(acc.Balance.ToBig()), nil
}

func GetAccount(chainKV ethdb.KV, blockNumber uint64, address common.Address) (*accounts.Account, error) {
	reader := NewStateReader(chainKV, blockNumber)
	return reader.ReadAccountData(address)
}
