package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
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

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(blockNumber), tx, api.filters)
	if err != nil {
		return nil, err
	}

	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	if header == nil {
		return nil, fmt.Errorf("block header not found: %d", blockNum)
	}

	return header, nil
}

// GetHeaderByHash implements erigon_getHeaderByHash. Returns a block's header given a block's hash.
func (api *ErigonImpl) GetHeaderByHash(ctx context.Context, hash libcommon.Hash) (*types.Header, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header, err := api._blockReader.HeaderByHash(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("block header not found: %s", hash.String())
	}

	return header, nil
}

func (api *ErigonImpl) GetBlockByTimestamp(ctx context.Context, timeStamp rpc.Timestamp, fullTx bool) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	uintTimestamp := timeStamp.TurnIntoUint64()

	currentHeader := rawdb.ReadCurrentHeader(tx)
	currenttHeaderTime := currentHeader.Time
	highestNumber := currentHeader.Number.Uint64()

	firstHeader, err := api._blockReader.HeaderByNumber(ctx, tx, 0)
	if err != nil {
		return nil, err
	}

	if firstHeader == nil {
		return nil, errors.New("no genesis header found")
	}

	firstHeaderTime := firstHeader.Time

	if currenttHeaderTime <= uintTimestamp {
		blockResponse, err := buildBlockResponse(tx, highestNumber, fullTx)
		if err != nil {
			return nil, err
		}

		return blockResponse, nil
	}

	if firstHeaderTime >= uintTimestamp {
		blockResponse, err := buildBlockResponse(tx, 0, fullTx)
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

	resultingHeader, err := api._blockReader.HeaderByNumber(ctx, tx, uint64(blockNum))
	if err != nil {
		return nil, err
	}

	if resultingHeader == nil {
		return nil, fmt.Errorf("no header found with header number: %d", blockNum)
	}

	if resultingHeader.Time > uintTimestamp {
		response, err := buildBlockResponse(tx, uint64(blockNum)-1, fullTx)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	response, err := buildBlockResponse(tx, uint64(blockNum), fullTx)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func buildBlockResponse(db kv.Tx, blockNum uint64, fullTx bool) (map[string]interface{}, error) {
	block, err := rawdb.ReadBlockByNumber(db, blockNum)
	if err != nil {
		return nil, err
	}

	if block == nil {
		return nil, nil
	}

	additionalFields := make(map[string]interface{})
	td, err := rawdb.ReadTd(db, block.Hash(), block.NumberU64())
	if err != nil {
		return nil, err
	}
	if td != nil {
		additionalFields["totalDifficulty"] = (*hexutil.Big)(td)
	}

	response, err := ethapi.RPCMarshalBlockEx(block, true, fullTx, nil, libcommon.Hash{}, additionalFields)

	if err == nil && rpc.BlockNumber(block.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

func (api *ErigonImpl) GetBalanceChangesInBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (map[libcommon.Address]*hexutil.Big, error) {
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

	startkey := hexutility.EncodeTs(blockNumber)

	decodeFn := historyv2.Mapper[kv.AccountChangeSet].Decode

	balancesMapping := make(map[libcommon.Address]*hexutil.Big)

	newReader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(tx), api._agg, "")
	if err != nil {
		return nil, err
	}

	for dbKey, dbValue, err := c.Seek(startkey); bytes.Equal(dbKey, startkey) && dbKey != nil; dbKey, dbValue, err = c.Next() {
		if err != nil {
			return nil, err
		}
		_, addressBytes, v, err := decodeFn(dbKey, dbValue)
		if err != nil {
			return nil, err
		}

		var oldAcc accounts.Account
		if err = oldAcc.DecodeForStorage(v); err != nil {
			return nil, err
		}
		oldBalance := oldAcc.Balance

		address := libcommon.BytesToAddress(addressBytes)

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
