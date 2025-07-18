// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/services"
)

// GetHeaderByNumber implements erigon_getHeaderByNumber. Returns a block's header given a block number ignoring the block's transaction and uncle list (may be faster).
func (api *ErigonImpl) GetHeaderByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (*types.Header, error) {
	// Pending block is only known by the miner
	if blockNumber == rpc.PendingBlockNumber {
		block := api.pendingBlock()
		if block == nil {
			return nil, nil
		}
		return block.Header(), nil
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header, err := api.headerByRPCNumber(ctx, blockNumber, tx)
	if err != nil {
		return nil, err
	}

	if header == nil {
		return nil, fmt.Errorf("block header not found: %d", blockNumber)
	}

	return header, nil
}

// GetHeaderByHash implements erigon_getHeaderByHash. Returns a block's header given a block's hash.
func (api *ErigonImpl) GetHeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header, err := api.headerByHash(ctx, hash, tx)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("block header not found: %s", hash.String())
	}

	return header, nil
}

func (api *ErigonImpl) GetBlockByTimestamp(ctx context.Context, timeStamp rpc.Timestamp, fullTx bool) (map[string]interface{}, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	uintTimestamp := timeStamp.TurnIntoUint64()

	currentHeader := rawdb.ReadCurrentHeader(tx)
	if currentHeader == nil {
		return nil, errors.New("current header not found")
	}
	currentHeaderTime := currentHeader.Time
	highestNumber := currentHeader.Number.Uint64()

	firstHeader, err := api.headerByRPCNumber(ctx, 0, tx)
	if err != nil {
		return nil, err
	}

	if firstHeader == nil {
		return nil, errors.New("no genesis header found")
	}

	firstHeaderTime := firstHeader.Time
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if currentHeaderTime <= uintTimestamp {
		blockResponse, err := buildBlockResponse(ctx, api._blockReader, tx, highestNumber, fullTx, chainConfig)
		if err != nil {
			return nil, err
		}

		return blockResponse, nil
	}

	if firstHeaderTime >= uintTimestamp {
		blockResponse, err := buildBlockResponse(ctx, api._blockReader, tx, 0, fullTx, chainConfig)
		if err != nil {
			return nil, err
		}

		return blockResponse, nil
	}

	blockNum := sort.Search(int(currentHeader.Number.Uint64()), func(blockNum int) bool {
		currentHeader, err := api._blockReader.HeaderByNumber(ctx, tx, uint64(blockNum))
		if err != nil {
			return false
		}

		if currentHeader == nil {
			return false
		}

		return currentHeader.Time >= uintTimestamp
	})

	resultingHeader, err := api.headerByRPCNumber(ctx, rpc.BlockNumber(blockNum), tx)
	if err != nil {
		return nil, err
	}

	if resultingHeader == nil {
		return nil, fmt.Errorf("no header found with header number: %d", blockNum)
	}

	for resultingHeader.Time > uintTimestamp {
		beforeHeader, err := api.headerByRPCNumber(ctx, rpc.BlockNumber(blockNum)-1, tx)
		if err != nil {
			return nil, err
		}

		if beforeHeader == nil || beforeHeader.Time < uintTimestamp {
			break
		}

		blockNum--
		resultingHeader = beforeHeader
	}

	response, err := buildBlockResponse(ctx, api._blockReader, tx, uint64(blockNum), fullTx, chainConfig)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func buildBlockResponse(ctx context.Context, br services.FullBlockReader, db kv.Tx, blockNum uint64, fullTx bool, chainConfig *chain.Config) (map[string]interface{}, error) {
	header, err := br.HeaderByNumber(ctx, db, blockNum)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}

	block, _, err := br.BlockWithSenders(ctx, db, header.Hash(), blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	additionalFields := make(map[string]interface{})

	response, err := ethapi.RPCMarshalBlockEx(block, true, fullTx, nil, common.Hash{}, additionalFields, chainConfig.IsArbitrumNitro(block.Number()))

	if err == nil && rpc.BlockNumber(block.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

func (api *ErigonImpl) GetBalanceChangesInBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (map[common.Address]*hexutil.Big, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	balancesMapping := make(map[common.Address]*hexutil.Big)
	latestState, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, blockNrOrHash, 0, api.filters, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, err
	}

	blockNumber, _, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	minTxNum, err := api._txNumReader.Min(tx, blockNumber)
	if err != nil {
		return nil, err
	}
	maxTxNum, err := api._txNumReader.Max(tx, blockNumber)
	if err != nil {
		return nil, err
	}
	it, err := tx.HistoryRange(kv.AccountsDomain, int(minTxNum), int(maxTxNum+1), order.Asc, -1)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.HasNext() {
		addressBytes, v, err := it.Next()
		if err != nil {
			return nil, err
		}

		var oldAcc accounts.Account
		if len(v) > 0 {
			if err = accounts.DeserialiseV3(&oldAcc, v); err != nil {
				return nil, err
			}
		}
		oldBalance := oldAcc.Balance

		address := common.BytesToAddress(addressBytes)
		newAcc, err := latestState.ReadAccountData(address)
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
