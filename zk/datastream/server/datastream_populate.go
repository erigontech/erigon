package server

import (
	"fmt"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

const GenesisForkId = 0 // genesis fork is always 0 in the datastream

func getLatestBlockNumberWritten(stream *datastreamer.StreamServer, header *datastreamer.HeaderEntry) (uint64, error) {
	total := header.TotalEntries
	if total == 0 {
		return 0, nil
	}

	var blockNumber uint64
	var entry datastreamer.FileEntry

	for ; total > 0 && entry.Type != datastreamer.EntryType(1); total-- {
		entry, err := stream.GetEntry(total)
		if err != nil {
			return 0, err
		}
		if entry.Type == datastreamer.EntryType(2) {
			l2Block, err := types.UnmarshalL2Block(entry.Data)
			if err != nil {
				return 0, err
			}
			blockNumber = l2Block.L2BlockNumber
			break
		}
	}

	return blockNumber, nil
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

	logTicker := time.NewTicker(10 * time.Second)
	var lastBlock *eritypes.Block
	if err = stream.StartAtomicOp(); err != nil {
		return err
	}
	totalToWrite := to - (from - 1)
	insertEntryCount := 100_000
	entries := make([]DataStreamEntryProto, insertEntryCount)
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

		nextBatchNum, nextBatchExists, err := reader.CheckBatchNoByL2Block(currentBlockNumber + 1)
		if err != nil {
			return err
		}

		// a 0 next batch num here would mean we don't know about the next batch so must be at the end of the batch
		isBatchEnd := !nextBatchExists || nextBatchNum > batchNum

		gersInBetween, err := reader.GetBatchGlobalExitRootsProto(prevBatchNum, batchNum)
		if err != nil {
			return err
		}

		blockEntries, err := srv.CreateStreamEntriesProto(block, reader, tx, lastBlock, batchNum, prevBatchNum, gersInBetween, make(map[uint64]uint64), isBatchEnd, nil)
		if err != nil {
			return err
		}

		for _, entry := range blockEntries {
			entries[index] = entry
			index++
		}

		// basically commit onece 80% of the entries array is filled
		if index+1 >= insertEntryCount*4/5 {
			log.Info(fmt.Sprintf("[%s] Commit count reached, committing entries", logPrefix), "block", currentBlockNumber)
			if err = srv.CommitEntriesToStreamProto(entries[:index]); err != nil {
				return err
			}
			entries = make([]DataStreamEntryProto, insertEntryCount)
			index = 0
		}

		lastBlock = block
	}

	if err = srv.CommitEntriesToStreamProto(entries[:index]); err != nil {
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
	tx kv.Tx,
	stream *datastreamer.StreamServer,
	srv *DataStreamServer,
	chainId uint64,
) error {

	batchNo, err := reader.GetBatchNoByL2Block(0)
	if err != nil {
		return err
	}

	ger, err := reader.GetBlockGlobalExitRoot(genesis.NumberU64())
	if err != nil {
		return err
	}

	err = stream.StartAtomicOp()
	if err != nil {
		return err
	}

	batchBookmark := srv.CreateBatchBookmarkEntryProto(genesis.NumberU64())
	l2BlockBookmark := srv.CreateL2BlockBookmarkEntryProto(genesis.NumberU64())

	l2Block := srv.CreateL2BlockProto(genesis, genesis.Hash().Bytes(), batchNo, ger, 0, 0, common.Hash{}, 0, common.Hash{})
	batchStart := srv.CreateBatchStartProto(batchNo, chainId, GenesisForkId, datastream.BatchType_BATCH_TYPE_REGULAR)

	ler, err := srv.getLocalExitRoot(0, reader, tx)
	if err != nil {
		return err
	}
	batchEnd := srv.CreateBatchEndProto(ler, genesis.Root(), 0)

	if err = srv.CommitEntriesToStreamProto([]DataStreamEntryProto{batchBookmark, batchStart, l2BlockBookmark, l2Block, batchEnd}); err != nil {
		return err
	}

	err = stream.CommitAtomicOp()
	if err != nil {
		return err
	}

	return nil
}
