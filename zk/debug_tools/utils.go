package debug_tools

import (
	"context"
	"fmt"
	"os"
	"reflect"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/log/v3"

	"gopkg.in/yaml.v2"
)

func CompareBlocks(ctx context.Context, blockRemote, blockLocal *types.Block, rpcClientLocal, rpcClientRemote *ethclient.Client) bool {
	blocksMatch := true
	// check all fields
	if blockRemote.ParentHash() != blockLocal.ParentHash() {
		log.Warn("ParentHash", "Rpc", blockRemote.ParentHash().Hex(), "Rpc", blockLocal.ParentHash().Hex())
		blocksMatch = false
	}
	if blockRemote.UncleHash() != blockLocal.UncleHash() {
		log.Warn("UnclesHash", "Rpc", blockRemote.UncleHash().Hex(), "Local", blockLocal.UncleHash().Hex())
		blocksMatch = false
	}
	if blockRemote.Root() != blockLocal.Root() {
		log.Warn("Root", "Rpc", blockRemote.Root().Hex(), "Local", blockLocal.Root().Hex())
		blocksMatch = false
	}
	if blockRemote.TxHash() != blockLocal.TxHash() {
		log.Warn("TxHash", "Rpc", blockRemote.TxHash().Hex(), "Local", blockLocal.TxHash().Hex())
		blocksMatch = false
	}

	remoteTxHashes := make([]common.Hash, len(blockRemote.Transactions()))
	for i, tx := range blockRemote.Transactions() {
		remoteTxHashes[i] = tx.Hash()
	}
	localTxHashes := make([]common.Hash, len(blockLocal.Transactions()))
	for i, tx := range blockLocal.Transactions() {
		localTxHashes[i] = tx.Hash()
	}

	if len(remoteTxHashes) != len(localTxHashes) {
		log.Warn("Transactions amount mismatch", "Rpc", len(remoteTxHashes), "Local", len(localTxHashes))

		log.Warn("RPc transactions", "txs", remoteTxHashes)
		log.Warn("Local transactions", "txs", localTxHashes)
		blocksMatch = false
	} else {
		for i, txRemote := range localTxHashes {
			txLocal := localTxHashes[i]
			if txRemote != txLocal {
				log.Warn("TxHash", txRemote.Hex(), txLocal.Hex())
				blocksMatch = false
			}
		}
	}

	if blockRemote.ReceiptHash() != blockLocal.ReceiptHash() {
		log.Warn("ReceiptHash mismatch. Checking receipts", "Rpc receipt hash", blockRemote.ReceiptHash().Hex(), "Local receipt hash", blockLocal.ReceiptHash().Hex())
		for y, tx := range remoteTxHashes {
			receiptLocal, receiptRpc, err := getReceipt(ctx, rpcClientLocal, rpcClientRemote, tx)
			if err != nil {
				log.Error(fmt.Sprintf("getReceipt: %s", err))
				return false
			}
			log.Warn("-------------------------------------------------")
			log.Warn("Checking Receipts for tx.", "TxHash", tx.Hex())

			if receiptLocal.Status != receiptRpc.Status {
				log.Warn("ReceiptStatus", "Rpc", receiptRpc.Status, "Local", receiptLocal.Status)
				blocksMatch = false
			}
			if receiptLocal.CumulativeGasUsed != receiptRpc.CumulativeGasUsed {
				log.Warn("CumulativeGasUsed", "Rpc", receiptRpc.CumulativeGasUsed, "Local", receiptLocal.CumulativeGasUsed)
				blocksMatch = false
			}
			if !reflect.DeepEqual(receiptLocal.PostState, receiptRpc.PostState) {
				log.Warn("PostState", "Rpc", common.BytesToHash(receiptRpc.PostState), "Local", common.BytesToHash(receiptLocal.PostState))
				blocksMatch = false
			}
			if receiptLocal.ContractAddress != receiptRpc.ContractAddress {
				log.Warn("ContractAddress", "Rpc", receiptRpc.ContractAddress, "Local", receiptLocal.ContractAddress)
				blocksMatch = false
			}
			if receiptLocal.GasUsed != receiptRpc.GasUsed {
				log.Warn("GasUsed", "Rpc", receiptRpc.GasUsed, "Local", receiptLocal.GasUsed)
				blocksMatch = false
			}
			if receiptLocal.Bloom != receiptRpc.Bloom {
				log.Warn("LogsBloom", "Rpc", receiptRpc.Bloom, "Local", receiptLocal.Bloom)
				blocksMatch = false
			}

			if len(receiptRpc.Logs) != len(receiptLocal.Logs) {
				log.Warn("Receipt log amount mismatch", "receipt index", y, "Rpc log amount", len(receiptRpc.Logs), "Local log amount", len(receiptLocal.Logs))
				blocksMatch = false

				rpcLogIndexes := make([]uint, len(receiptRpc.Logs))
				for i, log := range receiptRpc.Logs {
					rpcLogIndexes[i] = log.Index
				}
				localLogIndexes := make([]uint, len(receiptLocal.Logs))
				for i, log := range receiptLocal.Logs {
					localLogIndexes[i] = log.Index
				}

				log.Warn("RPc log indexes", "Remote log indexes", rpcLogIndexes)
				log.Warn("Local log indexes", "Local log indexes", localLogIndexes)
			}

			// still check the available logs
			// there should be a mismatch on the first index they differ
			smallerLogLength := len(receiptLocal.Logs)
			if len(receiptRpc.Logs) < len(receiptLocal.Logs) {
				smallerLogLength = len(receiptRpc.Logs)
			}
			for i := 0; i < smallerLogLength; i++ {

				log.Warn("-------------------------------------------------")
				log.Warn("	Checking Logs for receipt.", "index", i)
				logLocal := receiptLocal.Logs[i]
				logRemote := receiptRpc.Logs[i]

				if logRemote.Address != logLocal.Address {
					log.Warn("Log Address", "index", i, "Rpc", logRemote.Address, "Local", logLocal.Address)
					blocksMatch = false
				}
				if !reflect.DeepEqual(logRemote.Data, logLocal.Data) {
					log.Warn("Log Data", "index", i, "Rpc", logRemote.Data, "Local", logLocal.Data)
					blocksMatch = false
				}

				if logRemote.Index != logLocal.Index {
					log.Warn("Log Index", "index", i, "Rpc", logRemote.Index, "Local", logLocal.Index)
					blocksMatch = false
				}

				if logRemote.BlockNumber != logLocal.BlockNumber {
					log.Warn("Log BlockNumber", "index", i, "Rpc", logRemote.BlockNumber, "Local", logLocal.BlockNumber)
					blocksMatch = false
				}

				if logRemote.TxHash != logLocal.TxHash {
					log.Warn("Log TxHash", "index", i, "Rpc", logRemote.TxHash, "Local", logLocal.TxHash)
					blocksMatch = false
				}

				if logRemote.TxIndex != logLocal.TxIndex {
					log.Warn("Log TxIndex", "index", i, "Rpc", logRemote.TxIndex, "Local", logLocal.TxIndex)
					blocksMatch = false
				}

				// don't check blockhash at this point it is most certainly mismatching and this only spams
				// if logRemote.BlockHash != logLocal.BlockHash {
				// 	log.Warn("Log BlockHash", "index", i, "Rpc", logRemote.BlockHash, "Local", logLocal.BlockHash)
				// }

				if len(logRemote.Topics) != len(logLocal.Topics) {
					log.Warn("Log Topics amount mismatch", "Log index", i, "Rpc", len(logRemote.Topics), "Local", len(logLocal.Topics))
					log.Warn("Rpc Topics", "Topics", logRemote.Topics)
					log.Warn("Local Topics", "Topics", logLocal.Topics)
					blocksMatch = false
				} else {
					for j, topicRemote := range logRemote.Topics {
						topicLocal := logLocal.Topics[j]
						if topicRemote != topicLocal {
							log.Warn("Log Topic", "Log index", i, "Topic index", j, "Rpc", topicRemote, "Local", topicLocal)
							blocksMatch = false
						}
					}
				}
				log.Warn("-------------------------------------------------")
			}
			log.Warn("Finished tx check")
			log.Warn("-------------------------------------------------")
		}
	}
	if blockRemote.Bloom() != blockLocal.Bloom() {
		log.Warn("Bloom", "Rpc", blockRemote.Bloom(), "Local", blockLocal.Bloom())
		blocksMatch = false
	}
	if blockRemote.Difficulty().Cmp(blockLocal.Difficulty()) != 0 {
		log.Warn("Difficulty", "Rpc", blockRemote.Difficulty().Uint64(), "Local", blockLocal.Difficulty().Uint64())
		blocksMatch = false
	}
	if blockRemote.NumberU64() != blockLocal.NumberU64() {
		log.Warn("NumberU64", "Rpc", blockRemote.NumberU64(), "Local", blockLocal.NumberU64())
		blocksMatch = false
	}
	if blockRemote.GasLimit() != blockLocal.GasLimit() {
		log.Warn("GasLimit", "Rpc", blockRemote.GasLimit(), "Local", blockLocal.GasLimit())
		blocksMatch = false
	}
	if blockRemote.GasUsed() != blockLocal.GasUsed() {
		log.Warn("GasUsed", "Rpc", blockRemote.GasUsed(), "Local", blockLocal.GasUsed())
		blocksMatch = false
	}
	if blockRemote.Time() != blockLocal.Time() {
		log.Warn("Time", "Rpc", blockRemote.Time(), "Local", blockLocal.Time())
		blocksMatch = false
	}
	if blockRemote.MixDigest() != blockLocal.MixDigest() {
		log.Warn("MixDigest", "Rpc", blockRemote.MixDigest().Hex(), "Local", blockLocal.MixDigest().Hex())
		blocksMatch = false
	}
	if blockRemote.Nonce() != blockLocal.Nonce() {
		log.Warn("Nonce", "Rpc", blockRemote.Nonce(), "Local", blockLocal.Nonce())
		blocksMatch = false
	}
	if blockRemote.BaseFee() != blockLocal.BaseFee() {
		log.Warn("BaseFee", "Rpc", blockRemote.BaseFee(), "Local", blockLocal.BaseFee())
		blocksMatch = false
	}

	return blocksMatch
}

func getReceipt(ctx context.Context, clientLocal, clientRemote *ethclient.Client, txHash common.Hash) (*types.Receipt, *types.Receipt, error) {
	receiptsLocal, err := clientLocal.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, nil, fmt.Errorf("clientLocal.TransactionReceipts: %s", err)
	}
	receiptsRemote, err := clientRemote.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, nil, fmt.Errorf("clientRemote.TransactionReceipts: %s", err)
	}
	return receiptsLocal, receiptsRemote, nil
}

func GetConf() (RpcConfig, error) {
	yamlFile, err := os.ReadFile("debugToolsConfig.yaml")
	if err != nil {
		return RpcConfig{}, err
	}

	c := RpcConfig{}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		return RpcConfig{}, err
	}

	return c, nil
}

type RpcConfig struct {
	Url          string `yaml:"url"`
	DumpFileName string `yaml:"dumpFileName"`
	Block        int64  `yaml:"block"`
}
