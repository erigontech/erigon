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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
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

	header, err := api.headerByNumber(ctx, blockNumber, tx)
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

func (api *ErigonImpl) GetBlockByTimestamp(ctx context.Context, timeStamp rpc.Timestamp, fullTx bool) (map[string]any, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Use one overlay view for the timestamp search and block assembly so the
	// head, bounds, and lookups cannot switch generations.
	overlayTx := api.filters.WithOverlay(tx)

	blockNum, err := api.blockNumByTimestamp(ctx, overlayTx, timeStamp.TurnIntoUint64())
	if err != nil {
		return nil, err
	}

	err = api.BaseAPI.checkPruneBlocks(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	return buildBlockResponse(ctx, api._blockReader, overlayTx, blockNum, fullTx)
}

func (api *ErigonImpl) blockNumByTimestamp(ctx context.Context, tx kv.Tx, uintTimestamp uint64) (uint64, error) {
	currentHeader := rawdb.ReadCurrentHeader(tx)
	if currentHeader == nil {
		return 0, errors.New("current header not found")
	}
	highestNumber := currentHeader.Number.Uint64()

	if currentHeader.Time <= uintTimestamp {
		return highestNumber, nil
	}

	firstHeader, err := api.headerByNumber(ctx, 0, tx)
	if err != nil {
		return 0, err
	}

	if firstHeader == nil {
		return 0, errors.New("no genesis header found")
	}

	if firstHeader.Time >= uintTimestamp {
		return 0, nil
	}

	blockNum := sort.Search(int(highestNumber), func(blockNum int) bool {
		header, err := api._blockReader.HeaderByNumber(ctx, tx, uint64(blockNum))
		if err != nil {
			return false
		}

		if header == nil {
			return false
		}

		return header.Time >= uintTimestamp
	})

	resultingHeader, err := api.headerByNumber(ctx, rpc.BlockNumber(blockNum), tx)
	if err != nil {
		return 0, err
	}

	if resultingHeader == nil {
		return 0, fmt.Errorf("no header found with header number: %d", blockNum)
	}

	for resultingHeader.Time > uintTimestamp {
		beforeHeader, err := api.headerByNumber(ctx, rpc.BlockNumber(blockNum)-1, tx)
		if err != nil {
			return 0, err
		}

		if beforeHeader == nil || beforeHeader.Time < uintTimestamp {
			break
		}

		blockNum--
		resultingHeader = beforeHeader
	}

	return uint64(blockNum), nil
}

func buildBlockResponse(ctx context.Context, br dbservices.FullBlockReader, db kv.Tx, blockNum uint64, fullTx bool) (map[string]any, error) {
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

	additionalFields := make(map[string]any)

	response, err := ethapi.RPCMarshalBlockEx(block, true, fullTx, additionalFields)

	if err == nil && rpc.BlockNumber(block.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

func (api *ErigonImpl) GetBalanceChangesInBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (map[common.Address]*hexutil.U256, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	balancesMapping := make(map[common.Address]*hexutil.U256)

	blockNumber, _, latest, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}

	err = rpchelper.CheckBlockExecuted(api.filters.WithOverlay(tx), blockNumber)
	if err != nil {
		return nil, err
	}

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, tx, blockNumber, latest, 0, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, err
	}

	minTxNum, err := api._txNumReader.Min(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}
	maxTxNum, err := api._txNumReader.Max(ctx, tx, blockNumber)
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
			if err := accounts.DeserialiseV3(&oldAcc, v); err != nil {
				return nil, err
			}
		}
		oldBalance := oldAcc.Balance

		address := accounts.InternAddress(common.BytesToAddress(addressBytes))
		newAcc, err := reader.ReadAccountData(address)
		if err != nil {
			return nil, err
		}

		newBalance := uint256.NewInt(0)
		if newAcc != nil {
			newBalance = &newAcc.Balance
		}

		if !oldBalance.Eq(newBalance) {
			balancesMapping[address.Value()] = (*hexutil.U256)(newBalance)
		}
	}

	return balancesMapping, nil
}
