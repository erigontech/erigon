package server

import (
	"context"
	"errors"
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
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

const (
	GenesisForkId         = 0 // genesis fork is always 0 in the datastream
	insertEntryCount      = 100_000
	commitEntryCountLimit = 80_000
)

// gets the blocks for the said batch from the reader
// writes a bookmarks, batch start, blocks and batch end
// basically writes a whole standalone batch
// plus the GER updates if the batch gap is > 1
// starts atomicOp and commits it internally
func (srv *DataStreamServer) WriteWholeBatchToStream(
	logPrefix string,
	tx kv.Tx,
	reader DbReader,
	prevBatchNum,
	batchNum uint64,
) error {
	var err error
	blocksForBatch, err := reader.GetL2BlockNosByBatch(batchNum)
	if err != nil {
		return err
	}

	var fromBlockNum, toBlockNum uint64
	for _, blockNum := range blocksForBatch {
		if fromBlockNum == 0 || blockNum < fromBlockNum {
			fromBlockNum = blockNum
		}
		if blockNum > toBlockNum {
			toBlockNum = blockNum
		}
	}

	if err = srv.UnwindIfNecessary(logPrefix, reader, fromBlockNum, prevBatchNum, batchNum); err != nil {
		return err
	}

	if err = srv.stream.StartAtomicOp(); err != nil {
		return err
	}
	defer srv.stream.RollbackAtomicOp()

	blocks := make([]eritypes.Block, 0)
	txsPerBlock := make(map[uint64][]eritypes.Transaction)
	for blockNumber := fromBlockNum; blockNumber <= toBlockNum; blockNumber++ {
		block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
		if err != nil {
			return err
		}

		blocks = append(blocks, *block)
		txsPerBlock[blockNumber] = block.Transactions()
	}

	entries, err := BuildWholeBatchStreamEntriesProto(tx, reader, srv.GetChainId(), batchNum, batchNum, blocks, txsPerBlock, make(map[uint64]uint64))
	if err != nil {
		return err
	}

	if err = srv.commitEntriesToStreamProto(entries.Entries()); err != nil {
		return err
	}

	if err = srv.commitAtomicOp(&toBlockNum, &batchNum, &batchNum); err != nil {
		return err
	}

	return nil
}

// writes consecutively blocks from-to
// checks for all batch related stuff in the meantime - batch start, batche end, etc
// starts atomicOp and commits it internally
func (srv *DataStreamServer) WriteBlocksToStreamConsecutively(
	ctx context.Context,
	logPrefix string,
	tx kv.Tx,
	reader DbReader,
	from, to uint64,
) error {
	var err error

	// logger stuff
	t := utils.StartTimer("write-stream", "writeblockstostream")
	defer t.LogTimer()
	logTicker := time.NewTicker(10 * time.Second)
	totalToWrite := to - (from - 1)
	copyFrom := from
	//////////

	latestbatchNum, err := reader.GetBatchNoByL2Block(from - 1)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return err
	}

	batchNum, err := reader.GetBatchNoByL2Block(from)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return err
	}

	if err = srv.UnwindIfNecessary(logPrefix, reader, from, latestbatchNum, batchNum); err != nil {
		return err
	}

	if err = srv.stream.StartAtomicOp(); err != nil {
		return err
	}
	defer srv.stream.RollbackAtomicOp()

	// check if a new batch starts and the old needs closing before that
	// if it is already closed with a batch end, do not add a new batch end
	// this is needed because we have to write a batch end when writing a new block from the next batch
	// because at the current block we might not know if it is the last one in the batch
	// but we know for certain if it is a 1st block from a new batch
	islastEntrybatchEnd, err := srv.IsLastEntryBatchEnd()
	if err != nil {
		return err
	}

	lastBlock, err := rawdb.ReadBlockByNumber(tx, from-1)
	if err != nil {
		return err
	}

	entries := make([]DataStreamEntryProto, 0, insertEntryCount)
	var forkId uint64

	batchesProgress, err := stages.GetStageProgress(tx, stages.Batches)
	if err != nil {
		return err
	}
LOOP:
	for currentBlockNumber := from; currentBlockNumber <= to; currentBlockNumber++ {
		select {
		case <-logTicker.C:
			log.Info(fmt.Sprintf("[%s]: progress", logPrefix),
				"block", currentBlockNumber,
				"target", to, "%", float64(currentBlockNumber-copyFrom)/float64(totalToWrite)*100)
		case <-ctx.Done():
			break LOOP
		default:
		}

		block, err := rawdb.ReadBlockByNumber(tx, currentBlockNumber)
		if err != nil {
			return err
		}

		batchNum, err := reader.GetBatchNoByL2Block(currentBlockNumber)
		if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
			return err
		}

		// fork id changes only per batch so query it only once per batch
		if batchNum != latestbatchNum {
			forkId, err = reader.GetForkId(batchNum)
			if err != nil {
				return err
			}
		}

		checkBatchEnd := currentBlockNumber == batchesProgress

		blockEntries, err := createBlockWithBatchCheckStreamEntriesProto(reader, tx, block, lastBlock, batchNum, latestbatchNum, srv.chainId, forkId, islastEntrybatchEnd, checkBatchEnd)
		if err != nil {
			return err
		}
		entries = append(entries, blockEntries.Entries()...)

		latestbatchNum = batchNum
		lastBlock = block

		// the check is needed only before the first block
		// after that - write batch end before each batch start
		islastEntrybatchEnd = false

		// basically commit once 80% of the entries array is filled
		if len(entries) >= commitEntryCountLimit {
			log.Info(fmt.Sprintf("[%s] Commit count reached, committing entries", logPrefix), "block", currentBlockNumber)
			if err = srv.commitEntriesToStreamProto(entries); err != nil {
				return err
			}
			entries = make([]DataStreamEntryProto, 0, insertEntryCount)
			if err = srv.stream.CommitAtomicOp(); err != nil {
				return err
			}
			if err = srv.stream.StartAtomicOp(); err != nil {
				return err
			}
		}
	}

	if err = srv.commitEntriesToStreamProto(entries); err != nil {
		return err
	}

	if err = srv.commitAtomicOp(&to, &batchNum, &latestbatchNum); err != nil {
		return err
	}

	return nil
}

// gets other needed data from the reader
// writes a batchBookmark and batch start (if needed), block bookmark, block and txs in it
// basically a full standalone block
func (srv *DataStreamServer) WriteBlockWithBatchStartToStream(
	logPrefix string,
	tx kv.Tx,
	reader DbReader,
	forkId,
	batchNum, prevBlockBatchNum uint64,
	prevBlock, block eritypes.Block,
) (err error) {
	t := utils.StartTimer("write-stream", "writeblockstostream")
	defer t.LogTimer()

	blockNum := block.NumberU64()

	if err = srv.UnwindIfNecessary(logPrefix, reader, blockNum, prevBlockBatchNum, batchNum); err != nil {
		return err
	}

	if err = srv.stream.StartAtomicOp(); err != nil {
		return err
	}
	defer srv.stream.RollbackAtomicOp()

	// if start of new batch add batch start entries
	var batchStartEntries *DataStreamEntries
	if prevBlockBatchNum != batchNum {
		gers, err := reader.GetBatchGlobalExitRootsProto(prevBlockBatchNum, batchNum)
		if err != nil {
			return err
		}

		if batchStartEntries, err = createBatchStartEntriesProto(reader, tx, batchNum, prevBlockBatchNum, batchNum-prevBlockBatchNum, srv.GetChainId(), block.Root(), gers); err != nil {
			return err
		}
	}

	blockEntries, err := createFullBlockStreamEntriesProto(reader, tx, &block, &prevBlock, block.Transactions(), forkId, batchNum, make(map[uint64]uint64))
	if err != nil {
		return err
	}

	if batchStartEntries != nil {
		if err = srv.commitEntriesToStreamProto(batchStartEntries.Entries()); err != nil {
			return err
		}
	}

	if err = srv.commitEntriesToStreamProto(blockEntries.Entries()); err != nil {
		return err
	}

	if err = srv.commitAtomicOp(&blockNum, &batchNum, nil); err != nil {
		return err
	}

	return nil
}

// checks if the stream has blocks above the current one
// if there is something, try to unwind it
// in the unwind chek if the block is at batch start
// if it is - unwind to previous batch's end, so it deletes batch stat of current batch as well
func (srv *DataStreamServer) UnwindIfNecessary(logPrefix string, reader DbReader, blockNum, prevBlockBatchNum, batchNum uint64) error {
	// if from is higher than the last datastream block number - unwind the stream
	highestDatastreamBlock, err := srv.GetHighestBlockNumber()
	if err != nil {
		return err
	}

	// if this is a new batch case, we must unwind to previous batch's batch end
	// otherwise it would corrupt the datastream with batch bookmark after a batch start or something similar
	if highestDatastreamBlock >= blockNum {
		if prevBlockBatchNum != batchNum {
			log.Warn(fmt.Sprintf("[%s] Datastream must unwind to batch", logPrefix), "prevBlockBatchNum", prevBlockBatchNum, "batchNum", batchNum)

			//get latest block in prev batch
			lastBlockInPrevbatch, _, err := reader.GetHighestBlockInBatch(prevBlockBatchNum)
			if err != nil {
				return err
			}

			// this represents a case where the block we must unwind to is part of a previous batch
			// this should never happen since previous batch in this use must be already completed
			if lastBlockInPrevbatch != blockNum-1 {
				return fmt.Errorf("datastream must unwind to prev batch, but it would corrupt the datastream: prevBlockBatchNum: %d, batchNum: %d, blockNum: %d", prevBlockBatchNum, batchNum, blockNum)
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
	reader DbReader,
	batchNumber uint64,
	stateRoot *common.Hash,
	localExitRoot *common.Hash,
) (err error) {
	lastBatchNumber, err := srv.GetHighestClosedBatch()
	if err != nil {
		return err
	}

	gers, err := reader.GetBatchGlobalExitRootsProto(lastBatchNumber, batchNumber)
	if err != nil {
		return err
	}

	if err = srv.stream.StartAtomicOp(); err != nil {
		return err
	}
	defer srv.stream.RollbackAtomicOp()

	batchEndEntries, err := addBatchEndEntriesProto(batchNumber, stateRoot, gers, localExitRoot)
	if err != nil {
		return err
	}

	if err = srv.commitEntriesToStreamProto(batchEndEntries); err != nil {
		return err
	}

	// we write only batch end, so dont't update latest block and batch
	if err = srv.commitAtomicOp(nil, nil, &batchNumber); err != nil {
		return err
	}

	return nil
}

func (srv *DataStreamServer) WriteGenesisToStream(
	genesis *eritypes.Block,
	reader *hermez_db.HermezDbReader,
	tx kv.Tx,
) error {
	batchNo, err := reader.GetBatchNoByL2Block(0)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
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
	defer srv.stream.RollbackAtomicOp()

	batchBookmark := newBatchBookmarkEntryProto(genesis.NumberU64())
	l2BlockBookmark := newL2BlockBookmarkEntryProto(genesis.NumberU64())

	l2Block := newL2BlockProto(genesis, genesis.Hash().Bytes(), batchNo, ger, 0, 0, common.Hash{}, 0, common.Hash{})
	l2BlockEnd := newL2BlockEndProto(0)
	batchStart := newBatchStartProto(batchNo, srv.chainId, GenesisForkId, datastream.BatchType_BATCH_TYPE_REGULAR)

	ler, err := utils.GetBatchLocalExitRootFromSCStorageForLatestBlock(0, reader, tx)
	if err != nil {
		return err
	}
	batchEnd := newBatchEndProto(ler, genesis.Root(), 0)

	if err = srv.commitEntriesToStreamProto([]DataStreamEntryProto{batchBookmark, batchStart, l2BlockBookmark, l2Block, l2BlockEnd, batchEnd}); err != nil {
		return err
	}

	// should be okay to write just zeroes here, but it is a single time in a node start, so no use to risk
	err = srv.commitAtomicOp(nil, nil, nil)
	if err != nil {
		return err
	}

	return nil
}
