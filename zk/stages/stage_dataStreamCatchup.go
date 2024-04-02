package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

var (
	NodeTypeSequencer    = byte(0)
	NodeTypeSynchronizer = byte(1)
)

type DataStreamCatchupCfg struct {
	db       kv.RwDB
	stream   *datastreamer.StreamServer
	chainId  uint64
	nodeType byte
}

func StageDataStreamCatchupCfg(stream *datastreamer.StreamServer, db kv.RwDB, chainId uint64, nodeType byte) DataStreamCatchupCfg {
	return DataStreamCatchupCfg{
		stream:   stream,
		db:       db,
		chainId:  chainId,
		nodeType: nodeType,
	}
}

func SpawnStageDataStreamCatchup(
	s *stagedsync.StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg DataStreamCatchupCfg,
) error {

	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting...", logPrefix))
	stream := cfg.stream

	if stream == nil {
		// skip the stage if there is no streamer provided
		log.Info(fmt.Sprintf("[%s] no streamer provided, skipping stage", logPrefix))
		return nil
	}

	createdTx := false
	if tx == nil {
		log.Debug(fmt.Sprintf("[%s] data stream: no tx provided, creating a new one", logPrefix))
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("failed to open tx, %w", err)
		}
		defer tx.Rollback()
		createdTx = true
	}

	srv := server.NewDataStreamServer(stream, cfg.chainId, server.StandardOperationMode)
	reader := hermez_db.NewHermezDbReader(tx)

	var finalBlockNumber, highestVerifiedBatch uint64

	switch cfg.nodeType {
	case NodeTypeSequencer:
		// read the highest batch number from the verified stage.  We cannot add data to the stream that
		// has not been verified by the executor because we cannot unwind this later
		executorVerifyProgress, err := stages.GetStageProgress(tx, stages.SequenceExecutorVerify)
		if err != nil {
			return err
		}
		highestVerifiedBatch = executorVerifyProgress
	case NodeTypeSynchronizer:
		// synchronizer gets the highest verified batch number in l1 syncer stage
		highestVerifiedBatchSyncer, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
		if err != nil {
			return err
		}
		highestVerifiedBatch = highestVerifiedBatchSyncer
	default:
		return fmt.Errorf("unknown node type: %d", cfg.nodeType)
	}

	highestVerifiedBlock, err := reader.GetHighestBlockInBatch(highestVerifiedBatch)
	if err != nil {
		return err
	}

	// we might have not executed to that batch yet, so we need to check the highest executed block
	// and get it's batch
	highestExecutedBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}

	finalBlockNumber = highestVerifiedBlock
	if highestExecutedBlock < finalBlockNumber {
		finalBlockNumber = highestExecutedBlock
	}

	previousProgress, err := stages.GetStageProgress(tx, stages.DataStream)
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Getting progress", logPrefix),
		"highestVerifiedBlock", highestVerifiedBlock,
		"highestExecutedBlock", highestExecutedBlock,
		"adding up to blockNum", finalBlockNumber,
		"previousProgress", previousProgress,
	)

	var lastBlock *eritypes.Block

	// skip genesis if we have no data in the stream yet
	if previousProgress == 0 {
		genesis, err := rawdb.ReadBlockByNumber(tx, 0)
		if err != nil {
			return err
		}
		lastBlock = genesis
		if err = writeGenesisToStream(genesis, reader, stream, srv); err != nil {
			return err
		}
	}

	logTicker := time.NewTicker(10 * time.Second)

	if err = stream.StartAtomicOp(); err != nil {
		return err
	}
	totalToWrite := finalBlockNumber - previousProgress

	insertEntryCount := 1000000
	entries := make([]server.DataStreamEntry, insertEntryCount)
	index := 0
	for currentBlockNumber := previousProgress + 1; currentBlockNumber <= finalBlockNumber; currentBlockNumber++ {
		select {
		case <-logTicker.C:
			log.Info(fmt.Sprintf("[%s]: progress", logPrefix),
				"block", currentBlockNumber,
				"target", finalBlockNumber, "%", float64(currentBlockNumber-previousProgress)/float64(totalToWrite)*100)
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

		blockEntries, err := srv.CreateStreamEntries(block, reader, lastBlock, batchNum, prevBatchNum, gersInBetween)
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
			if err = stages.SaveStageProgress(tx, stages.DataStream, currentBlockNumber); err != nil {
				return err
			}
			entries = make([]server.DataStreamEntry, insertEntryCount)
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

	if err = stages.SaveStageProgress(tx, stages.DataStream, finalBlockNumber); err != nil {
		return err
	}

	if createdTx {
		err = tx.Commit()
		if err != nil {
			log.Error(fmt.Sprintf("[%s] error: %s", logPrefix, err))
		}
	}

	log.Info(fmt.Sprintf("[%s] stage complete", logPrefix), "block", finalBlockNumber)

	return err
}

func writeGenesisToStream(
	genesis *eritypes.Block,
	reader *hermez_db.HermezDbReader,
	stream *datastreamer.StreamServer,
	srv *server.DataStreamServer,
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

	bookmark := srv.CreateBookmarkEntry(server.BlockBookmarkType, genesis.NumberU64())
	blockStart := srv.CreateBlockStartEntry(genesis, batch, uint16(fork), ger, 0, 0, common.Hash{})
	blockEnd := srv.CreateBlockEndEntry(genesis.NumberU64(), genesis.Hash(), genesis.Root())

	if err = srv.CommitEntriesToStream([]server.DataStreamEntry{bookmark, blockStart, blockEnd}, true); err != nil {
		return err
	}

	err = stream.CommitAtomicOp()
	if err != nil {
		return err
	}

	return nil
}
