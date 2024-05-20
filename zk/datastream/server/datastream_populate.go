package server

import (
	"fmt"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

func getLatestBlockNumberWritten(stream *datastreamer.StreamServer, header *datastreamer.HeaderEntry) (uint64, error) {
	if header.TotalEntries == 0 {
		return 0, nil
	}

	entry, err := stream.GetEntry(header.TotalEntries - 1)
	if err != nil {
		return 0, err
	}

	if entry.Type != datastreamer.EntryType(3) {
		return 0, fmt.Errorf("expected endL2BlockEntry, got %d", entry.Type)
	}

	l2EndBlock, err := types.DecodeEndL2BlockBigEndian(entry.Data)
	if err != nil {
		return 0, err
	}

	return l2EndBlock.L2BlockNumber, nil
}

func ConsecutiveWriteBlocksToStream(
	tx kv.Tx,
	reader *hermez_db.HermezDbReader,
	srv *DataStreamServer,
	stream *datastreamer.StreamServer,
	to uint64,
	logPrefix string,
) error {
	lastWrittenBlockNum, dserr := srv.GetHighestBlockNumber()
	if dserr != nil {
		return dserr
	}

	from := lastWrittenBlockNum + 1

	return WriteBlocksToStream(tx, reader, srv, stream, from, to, logPrefix)
}

func WriteBlocksToStream(
	tx kv.Tx,
	reader *hermez_db.HermezDbReader,
	srv *DataStreamServer,
	stream *datastreamer.StreamServer,
	from, to uint64,
	logPrefix string,
) error {
	var err error

	// if from is higher than the last datastream block number - unwind the stream
	highestDatastreamBlock, err := srv.GetHighestBlockNumber()
	if err != nil {
		return err
	}

	if highestDatastreamBlock > from {
		if err := srv.UnwindToBlock(from); err != nil {
			return err
		}
	}

	foo := stream.GetHeader()
	_ = foo

	logTicker := time.NewTicker(10 * time.Second)
	var lastBlock *eritypes.Block
	if err = stream.StartAtomicOp(); err != nil {
		return err
	}
	totalToWrite := to - (from - 1)
	insertEntryCount := 1000000
	entries := make([]DataStreamEntry, insertEntryCount)
	index := 0
	copyFrom := from
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

		block, err := rawdb.ReadBlockByNumber(tx, currentBlockNumber)
		if err != nil {
			return err
		}

		batchNum, err := reader.GetBatchNoByL2Block(currentBlockNumber)
		if err != nil {
			return err
		}

		prevBatchNum, err := reader.GetBatchNoByL2Block(currentBlockNumber - 1)
		if err != nil {
			return err
		}

		gersInBetween, err := reader.GetBatchGlobalExitRoots(prevBatchNum, batchNum)
		if err != nil {
			return err
		}

		l1InfoMinTimestamps := make(map[uint64]uint64)
		blockEntries, err := srv.CreateStreamEntries(block, reader, lastBlock, batchNum, prevBatchNum, gersInBetween, l1InfoMinTimestamps)
		if err != nil {
			return err
		}

		for _, entry := range *blockEntries {
			entries[index] = entry
			index++
		}

		// basically commit onece 80% of the entries array is filled
		if index+1 >= insertEntryCount*4/5 {
			log.Info(fmt.Sprintf("[%s] Commit count reached, committing entries", logPrefix), "block", currentBlockNumber)
			if err = srv.CommitEntriesToStream(entries[:index], true); err != nil {
				return err
			}
			entries = make([]DataStreamEntry, insertEntryCount)
			index = 0
		}

		lastBlock = block
	}

	if err = srv.CommitEntriesToStream(entries[:index], true); err != nil {
		return err
	}

	if err = stream.CommitAtomicOp(); err != nil {
		return err
	}

	return nil
}

func WriteGenesisToStream(
	genesis *eritypes.Block,
	reader *hermez_db.HermezDbReader,
	stream *datastreamer.StreamServer,
	srv *DataStreamServer,
) error {

	batch, err := reader.GetBatchNoByL2Block(0)
	if err != nil {
		return err
	}

	ger, err := reader.GetBlockGlobalExitRoot(genesis.NumberU64())
	if err != nil {
		return err
	}

	fork, err := reader.GetForkId(batch)
	if err != nil {
		return err
	}

	err = stream.StartAtomicOp()
	if err != nil {
		return err
	}

	batchBookmark := srv.CreateBookmarkEntry(BatchBookmarkType, genesis.NumberU64())
	bookmark := srv.CreateBookmarkEntry(BlockBookmarkType, genesis.NumberU64())
	blockStart := srv.CreateBlockStartEntry(genesis, batch, uint16(fork), ger, 0, 0, common.Hash{})
	blockEnd := srv.CreateBlockEndEntry(genesis.NumberU64(), genesis.Hash(), genesis.Root())

	if err = srv.CommitEntriesToStream([]DataStreamEntry{batchBookmark, bookmark, blockStart, blockEnd}, true); err != nil {
		return err
	}

	err = stream.CommitAtomicOp()
	if err != nil {
		return err
	}

	return nil
}
