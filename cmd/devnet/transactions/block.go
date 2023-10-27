package transactions

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/rpc"
)

// MaxNumberOfBlockChecks is the max number of blocks to look for a transaction in
var MaxNumberOfEmptyBlockChecks = 25

func AwaitTransactions(ctx context.Context, hashes ...libcommon.Hash) (map[libcommon.Hash]uint64, error) {
	devnet.Logger(ctx).Info("Awaiting transactions in confirmed blocks...")

	hashmap := map[libcommon.Hash]bool{}

	for _, hash := range hashes {
		hashmap[hash] = true
	}

	m, err := searchBlockForHashes(ctx, hashmap)
	if err != nil {
		return nil, fmt.Errorf("failed to search reserves for hashes: %v", err)
	}

	return m, nil
}

func searchBlockForHashes(ctx context.Context, hashmap map[libcommon.Hash]bool) (map[libcommon.Hash]uint64, error) {
	logger := devnet.Logger(ctx)

	if len(hashmap) == 0 {
		return nil, fmt.Errorf("no hashes to search for")
	}

	txToBlock := make(map[libcommon.Hash]uint64, len(hashmap))

	headsSub := services.GetSubscription(devnet.CurrentChainName(ctx), requests.Methods.ETHNewHeads)

	// get a block from the new heads channel
	if headsSub == nil {
		return nil, fmt.Errorf("no block heads subscription")
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

		if blockCount == MaxNumberOfEmptyBlockChecks {
			for h := range hashmap {
				logger.Error("Missing Tx", "txHash", h)
			}

			return nil, fmt.Errorf("timeout when searching for tx")
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
		// check if tx is in the hash set and remove it from the set if it is present
		if _, ok := hashmap[txnHash]; ok {
			numFound++
			logger.Info("SUCCESS => Tx included into block", "txHash", txnHash, "blockNum", blockNumber)
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
