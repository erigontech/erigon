package rpchelper

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
)

func GetBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, dbReader ethdb.Database) (uint64, common.Hash, error) {
	var blockNumber uint64
	var err error
	hash, ok := blockNrOrHash.Hash()
	if !ok {
		number := *blockNrOrHash.BlockNumber
		if number == rpc.LatestBlockNumber {
			blockNumber, _, err = stages.GetStageProgress(dbReader, stages.Execution)
			if err != nil {
				return 0, common.Hash{}, fmt.Errorf("getting latest block number: %v", err)
			}

		} else if number == rpc.EarliestBlockNumber {
			blockNumber = 0

		} else if number == rpc.PendingBlockNumber {
			return 0, common.Hash{}, fmt.Errorf("pending blocks are not supported")

		} else {
			blockNumber = uint64(number.Int64())
		}
		hash, err = GetHashByNumber(blockNumber, blockNrOrHash.RequireCanonical, dbReader)
		if err != nil {
			return 0, common.Hash{}, err
		}
	} else {
		block, err := rawdb.ReadBlockByHash(dbReader, hash)
		if err != nil {
			return 0, common.Hash{}, err
		}
		if block == nil {
			return 0, common.Hash{}, fmt.Errorf("block %x not found", hash)
		}
		blockNumber = block.NumberU64()

		ch, err := rawdb.ReadCanonicalHash(dbReader, blockNumber)
		if err != nil {
			return 0, common.Hash{}, err
		}
		if blockNrOrHash.RequireCanonical && ch != hash {
			return 0, common.Hash{}, fmt.Errorf("hash %q is not currently canonical", hash.String())
		}
	}
	return blockNumber, hash, nil
}

func GetAccount(tx ethdb.Tx, blockNumber uint64, address common.Address) (*accounts.Account, error) {
	reader := adapter.NewStateReader(tx, blockNumber)
	return reader.ReadAccountData(address)
}

func GetHashByNumber(blockNumber uint64, requireCanonical bool, dbReader ethdb.Database) (common.Hash, error) {
	if requireCanonical {
		return rawdb.ReadCanonicalHash(dbReader, blockNumber)
	}

	block, err := rawdb.ReadBlockByNumber(dbReader, blockNumber)
	if err != nil {
		return common.Hash{}, fmt.Errorf("block read fail: %w", err)
	}
	if block == nil {
		return common.Hash{}, fmt.Errorf("block %d not found", blockNumber)
	}
	return block.Hash(), nil
}
