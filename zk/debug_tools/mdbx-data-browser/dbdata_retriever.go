package main

import (
	"errors"
	"fmt"
	"sort"

	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	coreTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	rpcTypes "github.com/ledgerwatch/erigon/zk/rpcdaemon"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/utils"
)

type DbDataRetriever struct {
	tx       kv.Tx
	dbReader state.ReadOnlyHermezDb
}

// NewDbDataRetriever instantiates DbDataRetriever instance
func NewDbDataRetriever(tx kv.Tx) *DbDataRetriever {
	return &DbDataRetriever{
		tx:       tx,
		dbReader: hermez_db.NewHermezDbReader(tx),
	}
}

// GetBatchByNumber reads batch by number from the database
func (d *DbDataRetriever) GetBatchByNumber(batchNum uint64, verboseOutput bool) (*rpcTypes.Batch, error) {
	highestBlock, err := rawdb.ReadLastBlockSynced(d.tx)
	if err != nil {
		return nil, err
	}

	highestBatchNo, err := d.dbReader.GetBatchNoByL2Block(highestBlock.NumberU64())
	if err != nil {
		return nil, err
	}

	// Short circuit in case the given batch number is not present in the db
	if batchNum > highestBatchNo {
		return nil, fmt.Errorf("batch %d does not exist (the highest persisted batch is %d)", batchNum, highestBatchNo)
	}

	// Get highest block in batch
	latestBlockInBatch, err := d.getHighestBlockInBatch(batchNum)
	if err != nil {
		return nil, err
	}

	if latestBlockInBatch == nil {
		return nil, errors.New("failed to retrieve the latest block in batch")
	}

	// Get global exit root of the batch
	batchGER, _, err := d.dbReader.GetLastBlockGlobalExitRoot(latestBlockInBatch.NumberU64())
	if err != nil {
		return nil, err
	}

	// Initialize batch
	batch := &rpcTypes.Batch{
		Number:         rpcTypes.ArgUint64(batchNum),
		Coinbase:       latestBlockInBatch.Coinbase(),
		StateRoot:      latestBlockInBatch.Root(),
		Timestamp:      rpcTypes.ArgUint64(latestBlockInBatch.Time()),
		GlobalExitRoot: batchGER,
	}

	// Get sequence
	seq, err := d.dbReader.GetSequenceByBatchNoOrHighest(batchNum)
	if err != nil {
		return nil, err
	}
	if seq != nil {
		batch.SendSequencesTxHash = &seq.L1TxHash
	}

	// sequenced, genesis or injected batch:
	// - batches 0 and 1 will always be closed and
	// - if next batch has blocks, the given batch is closed
	_, lowestBlockInNextBatchExists, err := d.dbReader.GetLowestBlockInBatch(batchNum + 1)
	if err != nil {
		return nil, err
	}
	batch.Closed = (seq != nil || batchNum <= 1 || lowestBlockInNextBatchExists)

	// Get verification
	verification, err := d.dbReader.GetVerificationByBatchNoOrHighest(batchNum)
	if err != nil {
		return nil, err
	}
	if verification != nil {
		batch.VerifyBatchTxHash = &verification.L1TxHash
	}

	// Set L1 info tree (Mainnet Exit Root and Rollup Exit Root) if Global Exit Root exists
	if batchGER != rpcTypes.ZeroHash {
		l1InfoTreeUpdate, err := d.dbReader.GetL1InfoTreeUpdateByGer(batchGER)
		if err != nil {
			return nil, err
		}
		if l1InfoTreeUpdate != nil {
			batch.MainnetExitRoot = l1InfoTreeUpdate.MainnetExitRoot
			batch.RollupExitRoot = l1InfoTreeUpdate.RollupExitRoot
		}
	}

	// Get Local Exit Root
	localExitRoot, err := utils.GetBatchLocalExitRootFromSCStorageForLatestBlock(batchNum, d.dbReader, d.tx)
	if err != nil {
		return nil, err
	}
	batch.LocalExitRoot = localExitRoot

	// Generate batch l2 data on fly
	forkId, err := d.dbReader.GetForkId(batchNum)
	if err != nil {
		return nil, err
	}

	// Collect blocks in batch
	batchBlocks, err := d.getBatchBlocks(batchNum)
	if err != nil {
		return nil, err
	}

	batchL2Data, err := utils.GenerateBatchDataFromDb(d.tx, d.dbReader, batchBlocks, forkId)
	if err != nil {
		return nil, err
	}
	batch.BatchL2Data = batchL2Data

	// Populate blocks and transactions to the batch
	if err := d.populateBlocksAndTransactions(batch, batchBlocks, verboseOutput); err != nil {
		return nil, err
	}

	return batch, nil
}

// getHighestBlockInBatch reads the block with the highest block number from the batch
func (d *DbDataRetriever) getHighestBlockInBatch(batchNum uint64) (*coreTypes.Block, error) {
	_, found, err := d.dbReader.GetLowestBlockInBatch(batchNum)
	if err != nil {
		return nil, err
	}

	if !found && batchNum != 0 {
		return nil, nil
	}

	blockNum, found, err := d.dbReader.GetHighestBlockInBatch(batchNum)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("block not found in batch %d", batchNum)
	}
	blockHash, err := rawdb.ReadCanonicalHash(d.tx, blockNum)
	if err != nil {
		return nil, err
	}

	latestBlockInBatch := rawdb.ReadBlock(d.tx, blockHash, blockNum)
	if latestBlockInBatch == nil {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}
	return latestBlockInBatch, nil
}

// populateBlocksAndTransactions populates blocks and transactions to the given batch.
// In case verboseOutput is set to true, entire blocks and transactions are populated. Otherwise only hashes.
func (d *DbDataRetriever) populateBlocksAndTransactions(batch *rpcTypes.Batch, blocks []*coreTypes.Block, verboseOutput bool) error {
	batch.Blocks = make([]interface{}, 0, len(blocks))
	if !verboseOutput {
		for _, block := range blocks {
			batch.Blocks = append(batch.Blocks, block.Hash())
			for _, tx := range block.Transactions() {
				batch.Transactions = append(batch.Transactions, tx.Hash())
			}
		}
	} else {
		for _, block := range blocks {
			blockInfoRoot, err := d.dbReader.GetBlockInfoRoot(block.NumberU64())
			if err != nil {
				return err
			}

			blockGER, err := d.dbReader.GetBlockGlobalExitRoot(block.NumberU64())
			if err != nil {
				return err
			}

			rpcBlock, err := d.convertToRPCBlock(block, verboseOutput, verboseOutput)
			if err != nil {
				return err
			}

			batchBlockExtra := &rpcTypes.BlockWithInfoRootAndGer{
				Block:          rpcBlock,
				BlockInfoRoot:  blockInfoRoot,
				GlobalExitRoot: blockGER,
			}

			batch.Blocks = append(batch.Blocks, batchBlockExtra)

			for _, tx := range block.Transactions() {
				receipt, _, _, _, err := rawdb.ReadReceipt(d.tx, tx.Hash())
				if err != nil {
					return err
				}

				rpcTx, err := rpcTypes.NewTransaction(tx, receipt, verboseOutput)
				if err != nil {
					return err
				}

				l2TxHash, err := zktx.ComputeL2TxHash(
					tx.GetChainID().ToBig(),
					tx.GetValue(),
					tx.GetPrice(),
					tx.GetNonce(),
					tx.GetGas(),
					tx.GetTo(),
					&rpcTx.From,
					tx.GetData(),
				)
				if err != nil {
					return err
				}

				if rpcTx.Receipt != nil {
					rpcTx.Receipt.TransactionL2Hash = l2TxHash
				}
				rpcTx.L2Hash = l2TxHash

				batch.Transactions = append(batch.Transactions, rpcTx)
			}
		}
	}
	return nil
}

// getBatchBlocks retrieve blocks from the provided batch number
func (d *DbDataRetriever) getBatchBlocks(batchNum uint64) ([]*coreTypes.Block, error) {
	// Get block numbers in the batch
	blockNums, err := d.dbReader.GetL2BlockNosByBatch(batchNum)
	if err != nil {
		return nil, err
	}

	blocks := make([]*coreTypes.Block, 0, len(blockNums))

	// Handle genesis block separately
	if batchNum == 0 {
		block, err := rawdb.ReadBlockByNumber(d.tx, 0)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, block)
	}

	for _, blockNum := range blockNums {
		block, err := rawdb.ReadBlockByNumber(d.tx, blockNum)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

// GetBlockByNumber reads block based on its block number from the database
func (d *DbDataRetriever) GetBlockByNumber(blockNum uint64, includeTxs, includeReceipts bool) (*rpcTypes.Block, error) {
	blockHash, err := rawdb.ReadCanonicalHash(d.tx, blockNum)
	if err != nil {
		return nil, err
	}

	block := rawdb.ReadBlock(d.tx, blockHash, blockNum)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}

	return d.convertToRPCBlock(block, verboseOutput, verboseOutput)
}

// GetBatchAffiliation retrieves the batch affiliation for the provided block numbers
func (d *DbDataRetriever) GetBatchAffiliation(blocks []uint64) ([]*BatchAffiliationInfo, error) {
	batchInfoMap := make(map[uint64]*BatchAffiliationInfo)
	for _, blockNum := range blocks {
		batchNum, err := d.dbReader.GetBatchNoByL2Block(blockNum)
		if errors.Is(err, hermez_db.ErrorNotStored) && !(blockNum == 0 && batchNum == 0) {
			return nil, fmt.Errorf("batch is not found for block num %d", blockNum)
		} else if err != nil {
			return nil, err
		}

		if blockNum > 0 && batchNum == 0 {
			return nil, fmt.Errorf("batch is not found for block num %d", blockNum)
		}

		batchInfo, exists := batchInfoMap[batchNum]
		if !exists {
			batchInfo = &BatchAffiliationInfo{Number: batchNum}
			batchInfoMap[batchNum] = batchInfo
		}
		batchInfo.Blocks = append(batchInfo.Blocks, blockNum)
	}

	res := make([]*BatchAffiliationInfo, 0, len(batchInfoMap))
	for _, bi := range batchInfoMap {
		res = append(res, bi)
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Number < res[j].Number
	})

	return res, nil
}

// convertToRPCBlock converts the coreTypes.Block into rpcTypes.Block
func (d *DbDataRetriever) convertToRPCBlock(block *coreTypes.Block, includeTxs, includeReceipts bool) (*rpcTypes.Block, error) {
	receipts := rawdb.ReadReceipts(d.tx, block, block.Body().SendersFromTxs())
	return rpcTypes.NewBlock(block, receipts.ToSlice(), includeTxs, includeReceipts)
}

type BatchAffiliationInfo struct {
	Number uint64   `json:"batch"`
	Blocks []uint64 `json:"blocks"`
}
