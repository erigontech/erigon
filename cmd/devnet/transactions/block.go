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

package transactions

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/erigon-lib/common/hexutil"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/log/v3"

	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/devnetutils"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/cmd/devnet/services"
	"github.com/erigontech/erigon/rpc"
)

// max number of blocks to look for a transaction in
const defaultMaxNumberOfEmptyBlockChecks = 25

func AwaitTransactions(ctx context.Context, hashes ...libcommon.Hash) (map[libcommon.Hash]uint64, error) {
	devnet.Logger(ctx).Info("Awaiting transactions in confirmed blocks...")

	hashmap := map[libcommon.Hash]bool{}

	for _, hash := range hashes {
		hashmap[hash] = true
	}

	maxNumberOfEmptyBlockChecks := defaultMaxNumberOfEmptyBlockChecks
	network := devnet.CurrentNetwork(ctx)
	if (network != nil) && (network.MaxNumberOfEmptyBlockChecks > 0) {
		maxNumberOfEmptyBlockChecks = network.MaxNumberOfEmptyBlockChecks
	}

	m, err := searchBlockForHashes(ctx, hashmap, maxNumberOfEmptyBlockChecks)
	if err != nil {
		return nil, fmt.Errorf("failed to search reserves for hashes: %v", err)
	}

	return m, nil
}

func searchBlockForHashes(
	ctx context.Context,
	hashmap map[libcommon.Hash]bool,
	maxNumberOfEmptyBlockChecks int,
) (map[libcommon.Hash]uint64, error) {
	logger := devnet.Logger(ctx)

	if len(hashmap) == 0 {
		return nil, errors.New("no hashes to search for")
	}

	txToBlock := make(map[libcommon.Hash]uint64, len(hashmap))

	headsSub := services.GetSubscription(devnet.CurrentChainName(ctx), requests.Methods.ETHNewHeads)

	// get a block from the new heads channel
	if headsSub == nil {
		return nil, errors.New("no block heads subscription")
	}

	var blockCount int
	for {
		block := <-headsSub.SubChan
		blockNum := block.(map[string]interface{})["number"].(string)

		_, numFound, foundErr := txHashInBlock(headsSub.Client, hashmap, blockNum, txToBlock, logger)

		if foundErr != nil {
			return nil, fmt.Errorf("failed to find hash in block with number %q: %v", foundErr, blockNum)
		}

		if len(hashmap) == 0 { // this means we have found all the txs we're looking for
			logger.Info("All the transactions created have been included in blocks")
			return txToBlock, nil
		}

		if numFound == 0 {
			blockCount++ // increment the number of blocks seen to check against the max number of blocks to iterate over
		}

		if blockCount == maxNumberOfEmptyBlockChecks {
			for h := range hashmap {
				logger.Error("Missing Tx", "txHash", h)
			}

			return nil, errors.New("timeout when searching for tx")
		}
	}
}

// Block represents a simple block for queries
type Block struct {
	Number       *hexutil.Big
	Transactions []libcommon.Hash
	BlockHash    libcommon.Hash
}

// txHashInBlock checks if the block with block number has the transaction hash in its list of transactions
func txHashInBlock(client *rpc.Client, hashmap map[libcommon.Hash]bool, blockNumber string, txToBlockMap map[libcommon.Hash]uint64, logger log.Logger) (uint64, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel() // releases the resources held by the context

	var (
		currBlock Block
		numFound  int
	)
	err := client.CallContext(ctx, &currBlock, string(requests.Methods.ETHGetBlockByNumber), blockNumber, false)
	if err != nil {
		return uint64(0), 0, fmt.Errorf("failed to get block by number: %v", err)
	}

	for _, txnHash := range currBlock.Transactions {
		// check if txn is in the hash set and remove it from the set if it is present
		if _, ok := hashmap[txnHash]; ok {
			numFound++
			logger.Info("SUCCESS => Txn included into block", "txHash", txnHash, "blockNum", blockNumber)
			// add the block number as an entry to the map
			txToBlockMap[txnHash] = devnetutils.HexToInt(blockNumber)
			delete(hashmap, txnHash)
			if len(hashmap) == 0 {
				return devnetutils.HexToInt(blockNumber), numFound, nil
			}
		}
	}

	return uint64(0), 0, nil
}
