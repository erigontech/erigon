package server

import (
	"fmt"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/log/v3"
)

const GenesisForkId = 0 // genesis fork is always 0 in the datastream

func (srv *DataStreamServer) WriteBlocksToStream(
	tx kv.Tx,
	reader DbReader,
	from, to uint64,
	logPrefix string,
) error {
	t := utils.StartTimer("write-stream", "writeblockstostream")
	defer t.LogTimer()

	var err error

	logTicker := time.NewTicker(10 * time.Second)
	var lastBlock *eritypes.Block
	if err = srv.stream.StartAtomicOp(); err != nil {
		return err
	}
	totalToWrite := to - (from - 1)
	insertEntryCount := 100_000
	entries := make([]DataStreamEntryProto, insertEntryCount)
	index := 0
	copyFrom := from
	var latestbatchNum uint64
	for currentBlockNumber := from; currentBlockNumber <= to; currentBlockNumber++ {
		select {
		case <-logTicker.C:
			log.Info(fmt.Sprintf("[%s]: progress", logPrefix),
				"block", currentBlockNumber,
				"target", to, "%", float64(currentBlockNumber-copyFrom)/float64(totalToWrite)*100)
		default:
		}

		if lastBlock == nil {
			lastBlock, err = rawdb.ReadBlockByNumber(tx, currentBlockNumber-1)
			if err != nil {
				return err
			}
		}

		block, blockEntries, batchNum, err := srv.createBlockStreamEntriesWithBatchCheck(logPrefix, tx, reader, lastBlock, currentBlockNumber)
		if err != nil {
			return err
		}
		latestbatchNum = batchNum

		for _, entry := range blockEntries {
			entries[index] = entry
			index++
		}

		// basically commit once 80% of the entries array is filled
		if index+1 >= insertEntryCount*4/5 {
			log.Info(fmt.Sprintf("[%s] Commit count reached, committing entries", logPrefix), "block", currentBlockNumber)
			if err = srv.CommitEntriesToStreamProto(entries[:index], &currentBlockNumber, &batchNum); err != nil {
				return err
			}
			entries = make([]DataStreamEntryProto, insertEntryCount)
			index = 0
		}

		lastBlock = block
	}

	if err = srv.CommitEntriesToStreamProto(entries[:index], &to, &latestbatchNum); err != nil {
		return err
	}

	if err = srv.stream.CommitAtomicOp(); err != nil {
		return err
	}

	return nil
}

func (srv *DataStreamServer) WriteBlockToStream(
	logPrefix string,
	tx kv.Tx,
	reader DbReader,
	batchNum, prevBatchNum,
	blockNum uint64,
) error {
	t := utils.StartTimer("write-stream", "writeblockstostream")
	defer t.LogTimer()

	var err error

	if err = srv.UnwindIfNecessary(logPrefix, reader, blockNum, prevBatchNum, batchNum); err != nil {
		return err
	}

	if err = srv.stream.StartAtomicOp(); err != nil {
		return err
	}

	lastBlock, err := rawdb.ReadBlockByNumber(tx, blockNum-1)
	if err != nil {
		return err
	}
	block, err := rawdb.ReadBlockByNumber(tx, blockNum)
	if err != nil {
		return err
	}

	entries, err := createBlockWithBatchCheckStreamEntriesProto(srv.chainId, reader, tx, block, lastBlock, batchNum, prevBatchNum, make(map[uint64]uint64), false, nil)
	if err != nil {
		return err
	}

	if err = srv.CommitEntriesToStreamProto(entries, &blockNum, &batchNum); err != nil {
		return err
	}

	if err = srv.stream.CommitAtomicOp(); err != nil {
		return err
	}

	return nil
}

func (srv *DataStreamServer) UnwindIfNecessary(logPrefix string, reader DbReader, blockNum, prevBatchNum, batchNum uint64) error {
	// if from is higher than the last datastream block number - unwind the stream
	highestDatastreamBlock, err := srv.GetHighestBlockNumber()
	if err != nil {
		return err
	}

	//if this is a new batch case, we must unwind to previous batch's batch end
	// otherwise it would corrupt the datastream with batch bookmark after a batch start or something similar
	if highestDatastreamBlock >= blockNum {
		if prevBatchNum != batchNum {
			log.Warn(fmt.Sprintf("[%s] Datastream must unwind to batch", logPrefix), "prevBatchNum", prevBatchNum, "batchNum", batchNum)

			//get latest block in prev batch
			lastBlockInPrevbatch, err := reader.GetHighestBlockInBatch(prevBatchNum)
			if err != nil {
				return err
			}

			// this represents a case where the block we must unwind to is part of a previous batch
			// this should never happen since previous batch in this use must be already completed
			if lastBlockInPrevbatch != blockNum-1 {
				return fmt.Errorf("datastream must unwind to prev batch, but it would corrupt the datastream: prevBatchNum: %d, abtchNum: %d, blockNum: %d", prevBatchNum, batchNum, blockNum)
			}

			if err := srv.UnwindToBatchStart(batchNum); err != nil {
				return err
			}
		} else {
			if err := srv.UnwindToBlock(blockNum); err != nil {
				return err
			}
		}
	}

	return nil
}

func (srv *DataStreamServer) WriteBatchEnd(
	logPrefix string,
	tx kv.Tx,
	reader DbReader,
	batchNumber,
	lastBatchNumber uint64,
	stateRoot *common.Hash,
	localExitRoot *common.Hash,
) (err error) {
	gers, err := reader.GetBatchGlobalExitRootsProto(lastBatchNumber, batchNumber)
	if err != nil {
		return err
	}

	if err = srv.stream.StartAtomicOp(); err != nil {
		return err
	}

	batchEndEntries, err := addBatchEndEntriesProto(tx, batchNumber, lastBatchNumber, stateRoot, gers, localExitRoot)
	if err != nil {
		return err
	}

	if err = srv.CommitEntriesToStreamProto(batchEndEntries, nil, nil); err != nil {
		return err
	}

	if err = srv.stream.CommitAtomicOp(); err != nil {
		return err
	}

	return nil
}

func (srv *DataStreamServer) createBlockStreamEntriesWithBatchCheck(
	logPrefix string,
	tx kv.Tx,
	reader DbReader,
	lastBlock *eritypes.Block,
	blockNumber uint64,
) (*eritypes.Block, []DataStreamEntryProto, uint64, error) {
	block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
	if err != nil {
		return nil, nil, 0, err
	}

	batchNum, err := reader.GetBatchNoByL2Block(blockNumber)
	if err != nil {
		return nil, nil, 0, err
	}

	prevBatchNum, err := reader.GetBatchNoByL2Block(blockNumber - 1)
	if err != nil {
		return nil, nil, 0, err
	}

	if err = srv.UnwindIfNecessary(logPrefix, reader, blockNumber, prevBatchNum, batchNum); err != nil {
		return nil, nil, 0, err
	}

	nextBatchNum, nextBatchExists, err := reader.CheckBatchNoByL2Block(blockNumber + 1)
	if err != nil {
		return nil, nil, 0, err
	}

	// a 0 next batch num here would mean we don't know about the next batch so must be at the end of the batch
	isBatchEnd := !nextBatchExists || nextBatchNum > batchNum

	entries, err := createBlockWithBatchCheckStreamEntriesProto(srv.chainId, reader, tx, block, lastBlock, batchNum, prevBatchNum, make(map[uint64]uint64), isBatchEnd, nil)
	if err != nil {
		return nil, nil, 0, err
	}

	return block, entries, batchNum, nil
}

func (srv *DataStreamServer) WriteGenesisToStream(
	genesis *eritypes.Block,
	reader *hermez_db.HermezDbReader,
	tx kv.Tx,
) error {
	batchNo, err := reader.GetBatchNoByL2Block(0)
	if err != nil {
		return err
	}

	ger, err := reader.GetBlockGlobalExitRoot(genesis.NumberU64())
	if err != nil {
		return err
	}

	err = srv.stream.StartAtomicOp()
	if err != nil {
		return err
	}

	batchBookmark := newBatchBookmarkEntryProto(genesis.NumberU64())
	l2BlockBookmark := newL2BlockBookmarkEntryProto(genesis.NumberU64())

	l2Block := newL2BlockProto(genesis, genesis.Hash().Bytes(), batchNo, ger, 0, 0, common.Hash{}, 0, common.Hash{})
	batchStart := newBatchStartProto(batchNo, srv.chainId, GenesisForkId, datastream.BatchType_BATCH_TYPE_REGULAR)

	ler, err := utils.GetBatchLocalExitRoot(0, reader, tx)
	if err != nil {
		return err
	}
	batchEnd := newBatchEndProto(ler, genesis.Root(), 0)

	blockNum := uint64(0)
	if err = srv.CommitEntriesToStreamProto([]DataStreamEntryProto{batchBookmark, batchStart, l2BlockBookmark, l2Block, batchEnd}, &blockNum, &batchNo); err != nil {
		return err
	}

	err = srv.stream.CommitAtomicOp()
	if err != nil {
		return err
	}

	return nil
}
