package main

import (
	"errors"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	coreTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	rpcTypes "github.com/ledgerwatch/erigon/zk/rpcdaemon"
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
	// Get highest block in batch
	latestBlockInBatch, err := d.getHighestBlockInBatch(batchNum)
	if err != nil {
		return nil, err
	}

	if latestBlockInBatch == nil {
		return nil, errors.New("failed to retrieve the latest block in batch")
	}

	// Initialize batch
	batch := &rpcTypes.Batch{
		Number:    rpcTypes.ArgUint64(batchNum),
		Coinbase:  latestBlockInBatch.Coinbase(),
		StateRoot: latestBlockInBatch.Root(),
		Timestamp: rpcTypes.ArgUint64(latestBlockInBatch.Time()),
	}

	// Collect blocks in batch
	if err := d.collectBlocksInBatch(batch, batchNum, verboseOutput); err != nil {
		return nil, err
	}

	// Get global exit root
	ger, err := d.dbReader.GetBatchGlobalExitRoot(batchNum)
	if err != nil {
		return nil, err
	}
	if ger != nil {
		batch.GlobalExitRoot = ger.GlobalExitRoot
	}

	// Get sequence
	seq, err := d.dbReader.GetSequenceByBatchNo(batchNum)
	if err != nil {
		return nil, err
	}
	if seq != nil {
		batch.SendSequencesTxHash = &seq.L1TxHash
	}
	batch.Closed = (seq != nil || batchNum <= 1)

	// Get verification
	ver, err := d.dbReader.GetVerificationByBatchNo(batchNum)
	if err != nil {
		return nil, err
	}
	if ver != nil {
		batch.VerifyBatchTxHash = &ver.L1TxHash
	}

	// Get batch L2 data
	batchL2Data, err := d.dbReader.GetL1BatchData(batchNum)
	if err != nil {
		return nil, err
	}
	batch.BatchL2Data = batchL2Data

	// Set L1 info tree if needed
	if batch.GlobalExitRoot != rpcTypes.ZeroHash {
		l1InfoTree, err := d.dbReader.GetL1InfoTreeUpdateByGer(batch.GlobalExitRoot)
		if err != nil {
			return nil, err
		}
		if l1InfoTree != nil {
			batch.MainnetExitRoot = l1InfoTree.MainnetExitRoot
			batch.RollupExitRoot = l1InfoTree.RollupExitRoot
		}
	}

	return batch, nil
}

// getHighestBlockInBatch reads the block with the highest block number from the batch
func (d *DbDataRetriever) getHighestBlockInBatch(batchNum uint64) (*coreTypes.Block, error) {
	_, found, err := d.dbReader.GetLowestBlockInBatch(batchNum)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	blockNum, err := d.dbReader.GetHighestBlockInBatch(batchNum)
	if err != nil {
		return nil, err
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

// collectBlocksInBatch retrieve blocks from the batch
func (d *DbDataRetriever) collectBlocksInBatch(batch *rpcTypes.Batch, batchNum uint64, verboseOutput bool) error {
	// Get block numbers in the batch
	blocksInBatch, err := d.dbReader.GetL2BlockNosByBatch(batchNum)
	if err != nil {
		return err
	}

	// Handle genesis block separately
	if batchNum == 0 {
		if err := d.addBlockToBatch(batch, 0, verboseOutput); err != nil {
			return err
		}
	}

	// Collect blocks and their transactions
	for _, blockNum := range blocksInBatch {
		if err := d.addBlockToBatch(batch, blockNum, verboseOutput); err != nil {
			return err
		}
	}

	return nil
}

// addBlockToBatch adds a block and its transactions to the batch
func (d *DbDataRetriever) addBlockToBatch(batch *rpcTypes.Batch, blockNum uint64, verboseOutput bool) error {
	block, err := rawdb.ReadBlockByNumber(d.tx, blockNum)
	if err != nil {
		return err
	}

	if verboseOutput {
		batch.Blocks = append(batch.Blocks, block)
	} else {
		batch.Blocks = append(batch.Blocks, block.Hash())
	}

	for _, tx := range block.Transactions() {
		if verboseOutput {
			batch.Transactions = append(batch.Transactions, tx)
		} else {
			batch.Transactions = append(batch.Transactions, tx.Hash())
		}
	}

	return nil
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

	receipts := rawdb.ReadReceipts(d.tx, block, block.Body().SendersFromTxs())
	rpcBlock, err := rpcTypes.NewBlock(block, receipts.ToSlice(), includeTxs, includeReceipts)
	if err != nil {
		return nil, err
	}

	return rpcBlock, nil
}
