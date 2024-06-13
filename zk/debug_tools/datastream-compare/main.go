package main

import (
	"context"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/datastream/client"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/debug_tools"
	"github.com/ledgerwatch/log/v3"
)

const localDatastream = "localhost:6900"
const fromBlock = 18809
const amountToRead = 10

// This code downloads headers and blocks from a datastream server.
func main() {
	ctx := context.Background()
	cfg, err := debug_tools.GetConf()
	if err != nil {
		panic(fmt.Sprintf("RPGCOnfig: %s", err))
	}

	// Create client
	localClient := client.NewClient(ctx, localDatastream, 3, 500, 0)
	remoteClient := client.NewClient(ctx, cfg.Datastream, 3, 500, 0)

	// Start client (connect to the server)
	defer localClient.Stop()
	if err := localClient.Start(); err != nil {
		panic(err)
	}

	defer remoteClient.Stop()
	if err := remoteClient.Start(); err != nil {
		panic(err)
	}

	// create bookmark
	bookmark := types.NewBookmarkProto(fromBlock, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)

	// Read all entries from server
	blocksReadLocal, gerUpdatesLocal, _, _, _, err := localClient.ReadEntries(bookmark, amountToRead)
	if err != nil {
		panic(err)
	}
	// Read all entries from server
	blocksReadRemote, gerUpdatesRemote, _, _, _, err := remoteClient.ReadEntries(bookmark, amountToRead)
	if err != nil {
		panic(err)
	}

	for i, block := range *blocksReadLocal {
		fmt.Println(i)
		fmt.Println(block.L2BlockNumber)
	}

	blockCountMatches := len(*blocksReadLocal) == len(*blocksReadRemote)

	if !blockCountMatches {
		log.Error("Block amounts don't match", "localBlocks", len(*blocksReadLocal), "remoteBlocks", len(*blocksReadRemote))
	} else {
		blockMismatch := false
		for i, localBlock := range *blocksReadLocal {
			remoteBlock := (*blocksReadRemote)[i]

			if localBlock.BatchNumber != remoteBlock.BatchNumber {
				log.Error("Block batch numbers don't match", "blockNum", localBlock.L2BlockNumber, "localBatchNumber", localBlock.BatchNumber, "remoteBatchNumber", remoteBlock.BatchNumber)
				blockMismatch = true
			}

			if localBlock.L2BlockNumber != remoteBlock.L2BlockNumber {
				log.Error("Block numbers don't match", "localBlockNumber", localBlock.L2BlockNumber, "remoteBlockNumber", remoteBlock.L2BlockNumber)
				blockMismatch = true
			}

			if localBlock.Timestamp != remoteBlock.Timestamp {
				log.Error("Block timestamps don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockTimestamp", localBlock.Timestamp, "remoteBlockTimestamp", remoteBlock.Timestamp)
				blockMismatch = true
			}

			if localBlock.DeltaTimestamp != remoteBlock.DeltaTimestamp && localBlock.L2BlockNumber != 1 {
				log.Error("Block delta timestamps don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockDeltaTimestamp", localBlock.DeltaTimestamp, "remoteBlockDeltaTimestamp", remoteBlock.DeltaTimestamp)
				blockMismatch = true
			}

			if localBlock.L1InfoTreeIndex != remoteBlock.L1InfoTreeIndex {
				log.Error("Block L1InfoTreeIndex don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockL1InfoTreeIndex", localBlock.L1InfoTreeIndex, "remoteBlockL1InfoTreeIndex", remoteBlock.L1InfoTreeIndex)
				blockMismatch = true
			}

			if localBlock.GlobalExitRoot != remoteBlock.GlobalExitRoot && localBlock.GlobalExitRoot != *new(common.Hash) {
				log.Error("Block global exit roots don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockGlobalExitRoot", localBlock.GlobalExitRoot, "remoteBlockGlobalExitRoot", remoteBlock.GlobalExitRoot)
				blockMismatch = true
			}

			if localBlock.Coinbase != remoteBlock.Coinbase {
				log.Error("Block coinbases don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockCoinbase", localBlock.Coinbase, "remoteBlockCoinbase", remoteBlock.Coinbase)
				blockMismatch = true
			}

			if localBlock.ForkId != remoteBlock.ForkId {
				log.Error("Block forkids don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockForkId", localBlock.ForkId, "remoteBlockForkId", remoteBlock.ForkId)
				blockMismatch = true
			}

			if localBlock.ChainId != remoteBlock.ChainId {
				log.Error("Block chainids don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockChainId", localBlock.ChainId, "remoteBlockChainId", remoteBlock.ChainId)
				blockMismatch = true
			}

			if localBlock.L1BlockHash != remoteBlock.L1BlockHash {
				log.Error("Block L1BlockHash don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockL1BlockHash", localBlock.L1BlockHash, "remoteBlockL1BlockHash", remoteBlock.L1BlockHash)
				blockMismatch = true
			}

			//don't check blockhash, because of pre forkid8 bugs it will mismatch for sure
			// if localBlock.L2Blockhash != remoteBlock.L2Blockhash {
			// 	log.Error("Block hashes don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockHash", localBlock.L2Blockhash, "remoteBlockHash", remoteBlock.L2Blockhash)
			// }

			if localBlock.StateRoot != remoteBlock.StateRoot {
				log.Error("Block state roots don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockStateRoot", localBlock.StateRoot, "remoteBlockStateRoot", remoteBlock.StateRoot)
				blockMismatch = true
			}

			if len(localBlock.L2Txs) != len(remoteBlock.L2Txs) {
				log.Error("Block transactions don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockTxs", localBlock.L2Txs, "remoteBlock")
				blockMismatch = true
			}

			for i, localTx := range localBlock.L2Txs {
				remoteTx := remoteBlock.L2Txs[i]

				if localTx.EffectiveGasPricePercentage != remoteTx.EffectiveGasPricePercentage {
					log.Error("Block txs EffectiveGasPricePercentage don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockTx", localTx.EffectiveGasPricePercentage, "remoteBlockTx", remoteTx.EffectiveGasPricePercentage)
					blockMismatch = true
				}

				if localTx.IsValid != remoteTx.IsValid {
					log.Error("Block txs IsValid don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockTx", localTx.IsValid, "remoteBlockTx", remoteTx.IsValid)
					blockMismatch = true
				}

				if localTx.IntermediateStateRoot != remoteTx.IntermediateStateRoot {
					log.Error("Block txs StateRoot don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockTx", localTx.IntermediateStateRoot, "remoteBlockTx", remoteTx.IntermediateStateRoot)
					blockMismatch = true
				}

				for i, b := range localTx.Encoded {
					if b != remoteTx.Encoded[i] {
						log.Error("Block txs Encoded don't match", "localBlockNumber", localBlock.L2BlockNumber, "localBlockTx", localTx.Encoded, "remoteBlockTx", remoteTx.Encoded)
						blockMismatch = true
					}

					if blockMismatch {
						break
					}
				}

				if blockMismatch {
					break
				}

			}

			if blockMismatch {
				break
			}
		}
	}

	gerCountMatches := len(*gerUpdatesLocal) == len(*gerUpdatesRemote)
	if !gerCountMatches {
		log.Error("GerUpdate amounts don't match", "localGerUpdates", len(*gerUpdatesLocal), "remoteGerUpdates", len(*gerUpdatesRemote))
	} else {
		gerMismatch := false
		for i, localGerUpdate := range *gerUpdatesLocal {
			remoteGerUpate := (*gerUpdatesRemote)[i]

			if localGerUpdate.BatchNumber != remoteGerUpate.BatchNumber {
				log.Error("GerUpdate batch numbers don't match", "localGerUpdate", localGerUpdate, "remoteGerUpdate", remoteGerUpate)
				gerMismatch = true
			}

			// their gerupdate chainId is wrong for some reason
			// if localGerUpdate.ChainId != remoteGerUpate.ChainId {
			// 	log.Error("GerUpdate ChainId don't match", "localGerUpdate", localGerUpdate.ChainId, "remoteGerUpdate", remoteGerUpate.ChainId)
			// 	gerMismatch = true
			// }

			if localGerUpdate.Coinbase != remoteGerUpate.Coinbase {
				log.Error("GerUpdate.Coinbase don't match", "localGerUpdate", localGerUpdate.Coinbase, "remoteGerUpdate", remoteGerUpate.Coinbase)
				gerMismatch = true
			}

			if localGerUpdate.ForkId != remoteGerUpate.ForkId {
				log.Error("GerUpdate.ForkId don't match", "localGerUpdate", localGerUpdate.ForkId, "remoteGerUpdate", remoteGerUpate.ForkId)
				gerMismatch = true
			}

			if localGerUpdate.GlobalExitRoot != remoteGerUpate.GlobalExitRoot {
				log.Error("GerUpdate.GlobalExitRoot don't match", "localGerUpdate", localGerUpdate.GlobalExitRoot, "remoteGerUpdate", remoteGerUpate.GlobalExitRoot)
				gerMismatch = true
			}

			if localGerUpdate.StateRoot != remoteGerUpate.StateRoot {
				log.Error("GerUpdate.StateRoot don't match", "localGerUpdate", localGerUpdate.StateRoot, "remoteGerUpdate", remoteGerUpate.StateRoot)
				gerMismatch = true
			}

			if localGerUpdate.Timestamp != remoteGerUpate.Timestamp {
				log.Error("GerUpdate.Timestamp don't match", "localGerUpdate", localGerUpdate.Timestamp, "remoteGerUpdate", remoteGerUpate.Timestamp)
				gerMismatch = true
			}

			if gerMismatch {
				break
			}
		}
	}

	fmt.Println("Check finished")
}
