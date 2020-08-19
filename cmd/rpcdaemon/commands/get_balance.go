package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

func (api *APIImpl) GetBalance(_ context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error) {
	blockNumber, _, err := GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return nil, err
	}

	acc, err := GetAccount(api.db, blockNumber, address)
	if err != nil {
		return nil, fmt.Errorf("cant get a balance for account %q for block %v", address.String(), blockNumber)
	}

	return (*hexutil.Big)(acc.Balance.ToBig()), nil
}

func GetBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, dbReader rawdb.DatabaseReader) (uint64, common.Hash, error) {
	var blockNumber uint64
	var err error

	hash, ok := blockNrOrHash.Hash()
	if !ok {
		blockNumber = uint64(blockNrOrHash.BlockNumber.Int64())
		hash, err = GetHashByNumber(blockNumber, blockNrOrHash.RequireCanonical, dbReader)
		if err != nil {
			return 0, common.Hash{}, err
		}
	} else {
		block := rawdb.ReadBlockByHash(dbReader, hash)
		if block == nil {
			return 0, common.Hash{}, fmt.Errorf("block %x not found", hash)
		}
		blockNumber = block.NumberU64()

		if blockNrOrHash.RequireCanonical && rawdb.ReadCanonicalHash(dbReader, blockNumber) != hash {
			return 0, common.Hash{}, fmt.Errorf("hash %q is not currently canonical", hash.String())
		}
	}
	return blockNumber, hash, nil
}

func GetAccount(chainKV ethdb.KV, blockNumber uint64, address common.Address) (*accounts.Account, error) {
	reader := adapter.NewStateReader(chainKV, blockNumber)
	return reader.ReadAccountData(address)
}

func GetHashByNumber(blockNumber uint64, requireCanonical bool, dbReader rawdb.DatabaseReader) (common.Hash, error) {
	if requireCanonical {
		return rawdb.ReadCanonicalHash(dbReader, blockNumber), nil
	}

	block := rawdb.ReadBlockByNumber(dbReader, blockNumber)
	if block == nil {
		return common.Hash{}, fmt.Errorf("block %d not found", blockNumber)
	}
	return block.Hash(), nil
}
