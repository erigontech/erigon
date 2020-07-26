package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

func (api *PrivateDebugAPIImpl) GetModifiedAccountsByNumber(_ context.Context, startNum rpc.BlockNumber, endNum *rpc.BlockNumber) ([]common.Address, error) {
	if endNum != nil && startNum.Int64() >= endNum.Int64() {
		return nil, fmt.Errorf("start block height (%d) must be less than end block height (%d)", startNum.Int64(), endNum.Int64())
	}

	execution, _, err := stages.GetStageProgress(api.dbReader, stages.Execution)
	if err != nil {
		return nil, err
	}

	lastBlockNumber := execution

	if startNum.Int64() < 1 || uint64(startNum.Int64()) > lastBlockNumber {
		return nil, fmt.Errorf("start block %x not found", uint64(startNum.Int64()))
	}

	if endNum == nil {
		*endNum = startNum
	} else {
		if endNum.Int64() < 1 || uint64(endNum.Int64()) > lastBlockNumber {
			return nil, fmt.Errorf("end block %x not found", uint64(endNum.Int64()))
		}
	}

	return api.getModifiedAccounts(uint64(startNum.Int64()), uint64(endNum.Int64()))
}

func (api *PrivateDebugAPIImpl) GetModifiedAccountsByHash(_ context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error) {
	startBlock := rawdb.ReadBlockByHash(api.dbReader, startHash)
	if startBlock == nil {
		return nil, fmt.Errorf("start block %x not found", startHash)
	}

	var endBlock *types.Block
	if endHash == nil {
		endBlock = startBlock
	} else {
		endBlock = rawdb.ReadBlockByHash(api.dbReader, *endHash)
		if endBlock == nil {
			return nil, fmt.Errorf("end block %x not found", *endHash)
		}
	}
	return api.getModifiedAccounts(startBlock.NumberU64(), endBlock.NumberU64())
}

func (api *PrivateDebugAPIImpl) getModifiedAccounts(startNum, endNum uint64) ([]common.Address, error) {
	if startNum >= endNum {
		return nil, fmt.Errorf("start block height (%d) must be less than end block height (%d)", startNum, endNum)
	}

	return ethdb.GetModifiedAccounts(api.dbReader, startNum, endNum)
}
