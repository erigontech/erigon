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

func GetBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, dbReader ethdb.Database, pending *Pending) (uint64, common.Hash, error) {
	var blockNumber uint64
	var err error
	hash, ok := blockNrOrHash.Hash()
	if !ok {
		number := *blockNrOrHash.BlockNumber
		if number == rpc.LatestBlockNumber {
			blockNumber, err = stages.GetStageProgress(dbReader, stages.Execution)
			if err != nil {
				return 0, common.Hash{}, fmt.Errorf("getting latest block number: %w", err)
			}
		} else if number == rpc.EarliestBlockNumber {
			blockNumber = 0
		} else if number == rpc.PendingBlockNumber {
			pendingBlock := pending.Block()
			if pendingBlock == nil {
				blockNumber, err = stages.GetStageProgress(dbReader, stages.Execution)
				if err != nil {
					return 0, common.Hash{}, fmt.Errorf("getting latest block number: %w", err)
				}
			} else {
				return pendingBlock.NumberU64(), pendingBlock.Hash(), nil
			}
		} else {
			blockNumber = uint64(number.Int64())
		}
		hash, err = rawdb.ReadCanonicalHash(dbReader, blockNumber)
		if err != nil {
			return 0, common.Hash{}, err
		}
	} else {
		number := rawdb.ReadHeaderNumber(dbReader, hash)
		if number == nil {
			return 0, common.Hash{}, fmt.Errorf("block %x not found", hash)
		}
		blockNumber = *number

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

func GetAccount(tx ethdb.Database, blockNumber uint64, address common.Address) (*accounts.Account, error) {
	reader := adapter.NewStateReader(tx.(ethdb.HasTx).Tx(), blockNumber)
	return reader.ReadAccountData(address)
}
