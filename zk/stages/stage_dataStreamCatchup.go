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
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

type DataStreamCatchupCfg struct {
	db      kv.RwDB
	stream  *datastreamer.StreamServer
	chainId uint64
}

func StageDataStreamCatchupCfg(stream *datastreamer.StreamServer, db kv.RwDB, chainId uint64) DataStreamCatchupCfg {
	return DataStreamCatchupCfg{
		stream:  stream,
		db:      db,
		chainId: chainId,
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
		log.Info(fmt.Sprintf("[%s]: no streamer provided, skipping stage", logPrefix))
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

	srv := server.NewDataStreamServer(stream, cfg.chainId)
	reader := hermez_db.NewHermezDbReader(tx)

	var lastBlock *eritypes.Block

	/* find out where we are at in the stream, compare with the DB/stage progress and catchup the entries */
	header := stream.GetHeader()

	// no data at all so we need to start from genesis
	if header.TotalEntries == 0 {
		genesis, err := rawdb.ReadBlockByNumber(tx, 0)
		if err != nil {
			return err
		}
		lastBlock = genesis

		batch, err := reader.GetBatchNoByL2Block(0)
		if err != nil {
			return err
		}

		ger, _, err := reader.GetBlockGlobalExitRoot(genesis.NumberU64())
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

		err = srv.AddBookmark(server.BlockBookmarkType, genesis.NumberU64())
		if err != nil {
			return err
		}

		err = srv.AddBlockStart(genesis, batch, uint16(fork), ger, 0, 0, common.Hash{})
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

	var currentBatchNumber uint64
	var currentL2Block uint64

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
		currentBatchNumber = binary.BigEndian.Uint64(latest.Data[0:8])
	case server.EntryTypeL2BlockEnd:
		currentL2Block = binary.BigEndian.Uint64(latest.Data[0:8])
		bookmark := types.Bookmark{
			Type: types.BookmarkTypeStart,
			From: currentL2Block,
		}
		firstEntry, err := stream.GetFirstEventAfterBookmark(bookmark.EncodeBigEndian())
		if err != nil {
			return err
		}
		currentBatchNumber = binary.BigEndian.Uint64(firstEntry.Data[0:8])
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
				"target", highestSeenBatchNumber, "%", math.Round(float64(currentBatchNumber)/float64(highestSeenBatchNumber)*100),
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

		// if there are no blocks to process just continue - previously this would check for a GER update in
		// the pre-etrog world but this isn't possible now because of the l1 info tree indexes, so we just
		// skip on
		if !ok || len(blockNumbers) == 0 {
			log.Info(fmt.Sprintf("[%s] found a batch with no blocks during data stream catchup", logPrefix), "batch", currentBatchNumber)
			skipped = true
			currentBatchNumber++
			continue
		}

		skipped = false

		for _, blockNumber := range blockNumbers {
			// ensure we have the last block if we haven't set it already
			if lastBlock == nil {
				lastBlock, err = rawdb.ReadBlockByNumber(tx, blockNumber-1)
				if err != nil {
					return err
				}
			}

			currentBlock = blockNumber
			fork, err := reader.GetForkId(currentBatchNumber)
			if err != nil {
				return err
			}

			block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
			if err != nil {
				return err
			}
			deltaTimestamp := block.Time() - lastBlock.Time()
			lastBlock = block

			var ger common.Hash
			var l1BlockHash common.Hash

			l1Index, err := reader.GetBlockL1InfoTreeIndex(blockNumber)
			if err != nil {
				return err
			}

			if blockNumber == 1 {
				// injected batch at the start of the network
				injected, err := reader.GetL1InjectedBatch(0)
				if err != nil {
					return err
				}
				ger = injected.LastGlobalExitRoot
				l1BlockHash = injected.L1ParentHash

				// block 1 in the stream has a delta timestamp of the block time itself
				deltaTimestamp = block.Time()
			} else {
				// standard behaviour for non-injected or forced batches
				if l1Index != 0 {
					// read the index info itself
					l1Info, err := reader.GetL1InfoTreeUpdate(l1Index)
					if err != nil {
						return err
					}
					if l1Info != nil {
						ger = l1Info.GER
						l1BlockHash = l1Info.ParentHash
					}
				}
			}

			// now write the block and txs
			err = srv.AddBookmark(server.BlockBookmarkType, block.NumberU64())
			if err != nil {
				return err
			}

			err = srv.AddBlockStart(block, currentBatchNumber, uint16(fork), ger, uint32(deltaTimestamp), uint32(l1Index), l1BlockHash)
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

			err = srv.AddBlockEnd(block.NumberU64(), block.Root(), block.Root())
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
		"batch", currentBatchNumber-1,
		"target", highestSeenBatchNumber, "%", math.Round(float64(currentBatchNumber-1)/float64(highestSeenBatchNumber)*100),
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
