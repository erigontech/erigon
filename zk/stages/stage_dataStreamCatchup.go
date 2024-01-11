package stages

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

type DataStreamCatchupCfg struct {
	db     kv.RwDB
	stream *datastreamer.StreamServer
}

func StageDataStreamCatchupCfg(stream *datastreamer.StreamServer, db kv.RwDB) DataStreamCatchupCfg {
	return DataStreamCatchupCfg{
		stream: stream,
		db:     db,
	}
}

func SpawnStageDataStreamCatchup(
	s *stagedsync.StageState,
	ctx context.Context,
	tx kv.Tx,
	cfg DataStreamCatchupCfg,
) error {

	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s]: Starting...", logPrefix))
	stream := cfg.stream

	if stream == nil {
		// skip the stage if there is no streamer provided
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
		createdTx = true
		defer tx.Rollback()
	}

	srv := server.NewDataStreamServer(stream)
	reader := hermez_db.NewHermezDbReader(tx)

	/* find out where we are at in the stream, compare with the DB/stage progress and catchup the entries */
	header := stream.GetHeader()

	// no data at all so we need to start from genesis
	if header.TotalEntries == 0 {
		genesis, err := rawdb.ReadBlockByNumber(tx, 0)
		if err != nil {
			return err
		}

		batch, err := reader.GetBatchNoByL2Block(0)
		if err != nil {
			return err
		}

		ger, err := reader.GetBlockGlobalExitRoot(genesis.NumberU64())
		if err != nil {
			return err
		}

		fork, err := reader.GetForkId(batch)

		err = stream.StartAtomicOp()
		if err != nil {
			return err
		}

		err = srv.AddBookmark(server.BlockBookmarkType, genesis.NumberU64())
		if err != nil {
			return err
		}

		err = srv.AddBlockStart(genesis, batch, uint16(fork), ger)
		if err != nil {
			return err
		}

		err = srv.AddBlockEnd(genesis.NumberU64(), genesis.Hash(), genesis.Root())
		if err != nil {
			return err
		}

		err = stream.CommitAtomicOp()
		if err != nil {
			return err
		}
	}

	// re-fetch the header as we might have inserted genesis at this point
	header = stream.GetHeader()

	// now check if we have any more data to add

	var currentBatchNumber uint64 = 0
	var currentL2Block uint64 = 0

	latest, err := stream.GetEntry(header.TotalEntries - 1)
	if err != nil {
		return err
	}

	// get the latest block so when to terminate the loop.  This is because not all batches contain blocks
	// so we cannot use this reliably to break the loop.  Block number is more reliable
	highestSeenBatchNumber, err := stages.GetStageProgress(tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	switch latest.Type {
	case server.EntryTypeUpdateGer:
		currentBatchNumber = binary.LittleEndian.Uint64(latest.Data[0:8])
	case server.EntryTypeL2BlockEnd:
		currentL2Block = binary.LittleEndian.Uint64(latest.Data[0:8])
		bookmark := types.Bookmark{
			Type: types.BookmarkTypeStart,
			From: currentL2Block,
		}
		firstEntry, err := stream.GetFirstEventAfterBookmark(bookmark.Encode())
		if err != nil {
			return err
		}
		currentBatchNumber = binary.LittleEndian.Uint64(firstEntry.Data[0:8])
	}

	var entry = header.TotalEntries

	if entry > 0 {
		entry--
	}

	// hold the mapping of block batches to block numbers - this is an expensive call so just
	// do it once
	// todo: can we not use memory here, could be a problem with a larger chain?
	batchToBlocks := make(map[uint64][]uint64)
	c, err := tx.Cursor(hermez_db.BLOCKBATCHES)
	if err != nil {
		return err
	}
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		block := hermez_db.BytesToUint64(k)
		batch := hermez_db.BytesToUint64(v)
		_, ok := batchToBlocks[batch]
		if !ok {
			batchToBlocks[batch] = []uint64{block}
		} else {
			batchToBlocks[batch] = append(batchToBlocks[batch], block)
		}
	}

	// Start on the current batch number + 1
	currentBatchNumber++
	target := highestSeenBatchNumber - currentBatchNumber

	var currentGER = common.Hash{}
	logTicker := time.NewTicker(10 * time.Second)

	var currentBlock uint64 = 0
	var total uint64 = 0
	count := 0
	skipped := false

LOOP:
	for err == nil {
		log.Debug("Current entry number: %d", entry)
		log.Debug("Current batch number: %d", currentBatchNumber)

		select {
		case <-logTicker.C:
			log.Info(fmt.Sprintf("[%s]: progress", logPrefix),
				"batch", currentBatchNumber,
				"target", highestSeenBatchNumber, "%", math.Round(float64(total)/float64(target)*100),
				"currentBlock", currentBlock)
		default:
		}

		if count == 0 && !skipped {
			if err = stream.StartAtomicOp(); err != nil {
				return err
			}
		}

		// get the blocks for this batch
		blockNumbers, ok := batchToBlocks[currentBatchNumber]

		// no block numbers means an empty batch so just skip it
		if !ok || len(blockNumbers) == 0 {
			// check for a ger update on the batch as it could have one
			ger, err := reader.GetBatchGlobalExitRoot(currentBatchNumber)
			if err != nil {
				return err
			}

			if ger != nil && ger.GlobalExitRoot != currentGER && ger.GlobalExitRoot != (common.Hash{}) {
				entry, err = srv.AddGerUpdateFromDb(ger)
				if err != nil {
					return err
				}
			}

			skipped = true
			currentBatchNumber++
			log.Debug(fmt.Sprintf("[%s]: found batch with no blocks - skipping", logPrefix), "number", currentBatchNumber)
			continue
		}

		skipped = false

		for _, blockNumber := range blockNumbers {
			currentBlock = blockNumber
			fork, err := reader.GetForkId(currentBatchNumber)
			if err != nil {
				return err
			}

			ger, err := reader.GetBlockGlobalExitRoot(blockNumber)
			if err != nil {
				return err
			}

			block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
			if err != nil {
				return err
			}

			// now write the block and txs
			err = srv.AddBookmark(server.BlockBookmarkType, block.NumberU64())
			if err != nil {
				return err
			}

			err = srv.AddBlockStart(block, currentBatchNumber, uint16(fork), ger)
			if err != nil {
				return err
			}

			for _, tx := range block.Transactions() {
				effectiveGasPricePercentage, err := reader.GetEffectiveGasPricePercentage(tx.Hash())
				if err != nil {
					return err
				}
				stateRoot, err := reader.GetStateRoot(block.NumberU64())
				if err != nil {
					return err
				}
				entry, err = srv.AddTransaction(effectiveGasPricePercentage, stateRoot, uint16(fork), tx)
				if err != nil {
					return err
				}
			}

			err = srv.AddBlockEnd(block.NumberU64(), block.Hash(), block.Root())
			if err != nil {
				return err
			}
		}

		currentBatchNumber++
		total++
		count++

		if count >= 1000 {
			if err := commitBatch(stream); err != nil {
				return err
			}
			count = 0
		} else if currentBatchNumber > highestSeenBatchNumber {
			if err := commitBatch(stream); err != nil {
				return err
			}
			break LOOP
		}
	}

	if createdTx {
		err = tx.Commit()
		if err != nil {
			log.Error(fmt.Sprintf("[%s] error: %s", logPrefix, err))
		}
	}

	log.Info(fmt.Sprintf("[%s]: stage complete", logPrefix),
		"batch", currentBatchNumber,
		"target", highestSeenBatchNumber, "%", math.Round(float64(total)/float64(target)*100),
		"currentBlock", currentBlock)

	return err
}

func commitBatch(stream *datastreamer.StreamServer) error {
	err := stream.CommitAtomicOp()
	if err != nil {
		return err
	}
	return nil
}
