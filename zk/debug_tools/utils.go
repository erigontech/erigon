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

func CompareBlocks(ctx context.Context, silent bool, blockRemote, blockLocal *types.Block, rpcClientLocal, rpcClientRemote *ethclient.Client) bool {
	blocksMatch := true
	// check all fields
	if blockRemote.ParentHash() != blockLocal.ParentHash() {
		if !silent {
			log.Warn("ParentHash", "Rpc", blockRemote.ParentHash().Hex(), "Rpc", blockLocal.ParentHash().Hex())
		}
		blocksMatch = false
	}
	if blockRemote.UncleHash() != blockLocal.UncleHash() {
		if !silent {
			log.Warn("UnclesHash", "Rpc", blockRemote.UncleHash().Hex(), "Local", blockLocal.UncleHash().Hex())
		}
		blocksMatch = false
	}
	if blockRemote.Root() != blockLocal.Root() {
		if !silent {
			log.Warn("Root", "Rpc", blockRemote.Root().Hex(), "Local", blockLocal.Root().Hex())
		}
		blocksMatch = false
	}
	if blockRemote.TxHash() != blockLocal.TxHash() {
		if !silent {
			log.Warn("TxHash", "Rpc", blockRemote.TxHash().Hex(), "Local", blockLocal.TxHash().Hex())
		}
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
		if !silent {
			log.Warn("Transactions amount mismatch", "Rpc", len(remoteTxHashes), "Local", len(localTxHashes))

			log.Warn("RPc transactions", "txs", remoteTxHashes)
			log.Warn("Local transactions", "txs", localTxHashes)
		}
		blocksMatch = false
	} else {
		for i, txRemote := range localTxHashes {
			txLocal := localTxHashes[i]
			if txRemote != txLocal {
				if !silent {
					log.Warn("TxHash", txRemote.Hex(), txLocal.Hex())
				}
				blocksMatch = false
			}
		}
	}

	if blockRemote.ReceiptHash() != blockLocal.ReceiptHash() {
		if !silent {
			log.Warn("ReceiptHash mismatch. Checking receipts", "Rpc receipt hash", blockRemote.ReceiptHash().Hex(), "Local receipt hash", blockLocal.ReceiptHash().Hex())
		}
		for y, tx := range remoteTxHashes {
			receiptLocal, receiptRpc, err := getReceipt(ctx, rpcClientLocal, rpcClientRemote, tx)
			if err != nil {
				if !silent {
					log.Error(fmt.Sprintf("getReceipt: %s", err))
				}
				return false
			}
			if !silent {
				log.Warn("-------------------------------------------------")
				log.Warn("Checking Receipts for tx.", "TxHash", tx.Hex())
			}
			if receiptLocal.Status != receiptRpc.Status {
				if !silent {
					log.Warn("ReceiptStatus", "Rpc", receiptRpc.Status, "Local", receiptLocal.Status)
				}
				blocksMatch = false
			}
			if receiptLocal.CumulativeGasUsed != receiptRpc.CumulativeGasUsed {
				if !silent {
					log.Warn("CumulativeGasUsed", "Rpc", receiptRpc.CumulativeGasUsed, "Local", receiptLocal.CumulativeGasUsed)
				}
				blocksMatch = false
			}
			if !reflect.DeepEqual(receiptLocal.PostState, receiptRpc.PostState) {
				if !silent {
					log.Warn("PostState", "Rpc", common.BytesToHash(receiptRpc.PostState), "Local", common.BytesToHash(receiptLocal.PostState))
				}
				blocksMatch = false
			}
			if receiptLocal.ContractAddress != receiptRpc.ContractAddress {
				if !silent {
					log.Warn("ContractAddress", "Rpc", receiptRpc.ContractAddress, "Local", receiptLocal.ContractAddress)
				}
				blocksMatch = false
			}
			if receiptLocal.GasUsed != receiptRpc.GasUsed {
				if !silent {
					log.Warn("GasUsed", "Rpc", receiptRpc.GasUsed, "Local", receiptLocal.GasUsed)
				}
				blocksMatch = false
			}
			if receiptLocal.Bloom != receiptRpc.Bloom {
				if !silent {
					log.Warn("LogsBloom", "Rpc", receiptRpc.Bloom, "Local", receiptLocal.Bloom)
				}
				blocksMatch = false
			}

			if len(receiptRpc.Logs) != len(receiptLocal.Logs) {
				if !silent {
					log.Warn("Receipt log amount mismatch", "receipt index", y, "Rpc log amount", len(receiptRpc.Logs), "Local log amount", len(receiptLocal.Logs))
				}
				blocksMatch = false

				rpcLogIndexes := make([]uint, len(receiptRpc.Logs))
				for i, log := range receiptRpc.Logs {
					rpcLogIndexes[i] = log.Index
				}
				localLogIndexes := make([]uint, len(receiptLocal.Logs))
				for i, log := range receiptLocal.Logs {
					localLogIndexes[i] = log.Index
				}

				if !silent {
					log.Warn("RPc log indexes", "Remote log indexes", rpcLogIndexes)
				}
				if !silent {
					log.Warn("Local log indexes", "Local log indexes", localLogIndexes)
				}
			}

			// still check the available logs
			// there should be a mismatch on the first index they differ
			smallerLogLength := len(receiptLocal.Logs)
			if len(receiptRpc.Logs) < len(receiptLocal.Logs) {
				smallerLogLength = len(receiptRpc.Logs)
			}
			for i := 0; i < smallerLogLength; i++ {

				if !silent {
					log.Warn("-------------------------------------------------")
				}
				if !silent {
					log.Warn("	Checking Logs for receipt.", "index", i)
				}
				logLocal := receiptLocal.Logs[i]
				logRemote := receiptRpc.Logs[i]

				if logRemote.Address != logLocal.Address {
					if !silent {
						log.Warn("Log Address", "index", i, "Rpc", logRemote.Address, "Local", logLocal.Address)
					}
					blocksMatch = false
				}
				if !reflect.DeepEqual(logRemote.Data, logLocal.Data) {
					if !silent {
						log.Warn("Log Data", "index", i, "Rpc", logRemote.Data, "Local", logLocal.Data)
					}
					blocksMatch = false
				}

				if logRemote.Index != logLocal.Index {
					if !silent {
						log.Warn("Log Index", "index", i, "Rpc", logRemote.Index, "Local", logLocal.Index)
					}
					blocksMatch = false
				}

				if logRemote.BlockNumber != logLocal.BlockNumber {
					if !silent {
						log.Warn("Log BlockNumber", "index", i, "Rpc", logRemote.BlockNumber, "Local", logLocal.BlockNumber)
					}
					blocksMatch = false
				}

				if logRemote.TxHash != logLocal.TxHash {
					if !silent {
						log.Warn("Log TxHash", "index", i, "Rpc", logRemote.TxHash, "Local", logLocal.TxHash)
					}
					blocksMatch = false
				}

				if logRemote.TxIndex != logLocal.TxIndex {
					if !silent {
						log.Warn("Log TxIndex", "index", i, "Rpc", logRemote.TxIndex, "Local", logLocal.TxIndex)
					}
					blocksMatch = false
				}

				// don't check blockhash at this point it is most certainly mismatching and this only spams
				// if logRemote.BlockHash != logLocal.BlockHash {
				// 	if !silent {
				// 	log.Warn("Log BlockHash", "index", i, "Rpc", logRemote.BlockHash, "Local", logLocal.BlockHash)
				// }
				// }

				if len(logRemote.Topics) != len(logLocal.Topics) {
					if !silent {
						log.Warn("Log Topics amount mismatch", "Log index", i, "Rpc", len(logRemote.Topics), "Local", len(logLocal.Topics))
					}
					if !silent {
						log.Warn("Rpc Topics", "Topics", logRemote.Topics)
					}
					if !silent {
						log.Warn("Local Topics", "Topics", logLocal.Topics)
					}
					blocksMatch = false
				} else {
					for j, topicRemote := range logRemote.Topics {
						topicLocal := logLocal.Topics[j]
						if topicRemote != topicLocal {
							if !silent {
								log.Warn("Log Topic", "Log index", i, "Topic index", j, "Rpc", topicRemote, "Local", topicLocal)
							}
							blocksMatch = false
						}
					}
				}
				if !silent {
					log.Warn("-------------------------------------------------")
				}
			}
			if !silent {
				log.Warn("Finished tx check")
			}
			if !silent {
				log.Warn("-------------------------------------------------")
			}
		}
	}
	if blockRemote.Bloom() != blockLocal.Bloom() {
		if !silent {
			log.Warn("Bloom", "Rpc", blockRemote.Bloom(), "Local", blockLocal.Bloom())
		}
		blocksMatch = false
	}
	if blockRemote.Difficulty().Cmp(blockLocal.Difficulty()) != 0 {
		if !silent {
			log.Warn("Difficulty", "Rpc", blockRemote.Difficulty().Uint64(), "Local", blockLocal.Difficulty().Uint64())
		}
		blocksMatch = false
	}
	if blockRemote.NumberU64() != blockLocal.NumberU64() {
		if !silent {
			log.Warn("NumberU64", "Rpc", blockRemote.NumberU64(), "Local", blockLocal.NumberU64())
		}
		blocksMatch = false
	}
	if blockRemote.GasLimit() != blockLocal.GasLimit() {
		if !silent {
			log.Warn("GasLimit", "Rpc", blockRemote.GasLimit(), "Local", blockLocal.GasLimit())
		}
		blocksMatch = false
	}
	if blockRemote.GasUsed() != blockLocal.GasUsed() {
		if !silent {
			log.Warn("GasUsed", "Rpc", blockRemote.GasUsed(), "Local", blockLocal.GasUsed())
		}
		blocksMatch = false
	}
	if blockRemote.Time() != blockLocal.Time() {
		if !silent {
			log.Warn("Time", "Rpc", blockRemote.Time(), "Local", blockLocal.Time())
		}
		blocksMatch = false
	}
	if blockRemote.MixDigest() != blockLocal.MixDigest() {
		if !silent {
			log.Warn("MixDigest", "Rpc", blockRemote.MixDigest().Hex(), "Local", blockLocal.MixDigest().Hex())
		}
		blocksMatch = false
	}
	if blockRemote.Nonce() != blockLocal.Nonce() {
		if !silent {
			log.Warn("Nonce", "Rpc", blockRemote.Nonce(), "Local", blockLocal.Nonce())
		}
		blocksMatch = false
	}
	if blockRemote.BaseFee() != blockLocal.BaseFee() {
		if !silent {
			log.Warn("BaseFee", "Rpc", blockRemote.BaseFee(), "Local", blockLocal.BaseFee())
		}
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
	Url              string `yaml:"url"`
	LocalUrl         string `yaml:"localUrl"`
	Datastream       string `yaml:"datastream"`
	DumpFileName     string `yaml:"dumpFileName"`
	Block            int64  `yaml:"block"`
	AddressRollup    string `yaml:"addressRollup"`
	L1Url            string `yaml:"l1Url"`
	L1ChainId        uint64 `yaml:"l1ChainId"`
	L1SyncStartBlock uint64 `yaml:"l1SyncStartBlock"`
	RollupId         uint64 `yaml:"rollupId"`
	ElderberryBachNo uint64 `yaml:"elderberryBachNo"`
}
