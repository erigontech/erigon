package stages

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/ledgerwatch/erigon/zk/datastream/client"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/log/v3"
)

const (
	STAGE_PROGRESS_SAVE    = 100_000
	NEW_BLOCKS_ON_DS_LIMIT = 10000
)

var (
	// ErrFailedToFindCommonAncestor denotes error suggesting that the common ancestor is not found in the database
	ErrFailedToFindCommonAncestor = errors.New("failed to find common ancestor block in the db")
)

type ErigonDb interface {
	WriteHeader(batchNo *big.Int, blockHash common.Hash, stateRoot, txHash, parentHash common.Hash, coinbase common.Address, ts, gasLimit uint64, chainConfig *chain.Config) (*ethTypes.Header, error)
	WriteBody(batchNo *big.Int, headerHash common.Hash, txs []ethTypes.Transaction) error
}

type HermezDb interface {
	DeleteForkIds(fromBatchNum, toBatchNum uint64) error
	DeleteBlockBatches(fromBlockNum, toBlockNum uint64) error

	DeleteBlockGlobalExitRoots(fromBlockNum, toBlockNum uint64) error
	DeleteGlobalExitRoots(l1BlockHashes *[]common.Hash) error

	DeleteReusedL1InfoTreeIndexes(fromBlockNum, toBlockNum uint64) error
	DeleteBlockL1BlockHashes(fromBlockNum, toBlockNum uint64) error
	WriteBlockL1InfoTreeIndex(blockNumber uint64, l1Index uint64) error
	WriteBlockL1InfoTreeIndexProgress(blockNumber uint64, l1Index uint64) error
}

type DatastreamClient interface {
	RenewEntryChannel()
	RenewMaxEntryChannel()
	ReadAllEntriesToChannel() error
	StopReadingToChannel()
	GetEntryChan() *chan interface{}
	GetL2BlockByNumber(blockNum uint64) (*types.FullL2Block, error)
	GetLatestL2Block() (*types.FullL2Block, error)
	GetProgressAtomic() *atomic.Uint64
	Start() error
	Stop() error
	PrepUnwind()
	HandleStart() error
}

type DatastreamReadRunner interface {
	StartRead()
	StopRead()
}

type dsClientCreatorHandler func(context.Context, *ethconfig.Zk, uint64) (DatastreamClient, error)

type BatchesCfg struct {
	db                   kv.RwDB
	blockRoutineStarted  bool
	dsClient             DatastreamClient
	dsQueryClientCreator dsClientCreatorHandler
	zkCfg                *ethconfig.Zk
	chainConfig          *chain.Config
	miningConfig         *params.MiningConfig
}

func StageBatchesCfg(db kv.RwDB, dsClient DatastreamClient, zkCfg *ethconfig.Zk, chainConfig *chain.Config, miningConfig *params.MiningConfig, options ...Option) BatchesCfg {
	cfg := BatchesCfg{
		db:                  db,
		blockRoutineStarted: false,
		dsClient:            dsClient,
		zkCfg:               zkCfg,
		chainConfig:         chainConfig,
		miningConfig:        miningConfig,
	}

	for _, opt := range options {
		opt(&cfg)
	}

	return cfg
}

type Option func(*BatchesCfg)

// WithDSClientCreator is a functional option to set the datastream client creator callback.
func WithDSClientCreator(handler dsClientCreatorHandler) Option {
	return func(c *BatchesCfg) {
		c.dsQueryClientCreator = handler
	}
}

var emptyHash = common.Hash{0}

func SpawnStageBatches(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg BatchesCfg,
) error {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting batches stage", logPrefix))
	if sequencer.IsSequencer() {
		log.Info(fmt.Sprintf("[%s] skipping -- sequencer", logPrefix))
		return nil
	}
	defer log.Info(fmt.Sprintf("[%s] Finished Batches stage", logPrefix))

	freshTx := false
	if tx == nil {
		freshTx = true
		log.Debug(fmt.Sprintf("[%s] batches: no tx provided, creating a new one", logPrefix))
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("cfg.db.BeginRw, %w", err)
		}
		defer tx.Rollback()
	}

	eriDb := erigon_db.NewErigonDb(tx)
	hermezDb := hermez_db.NewHermezDb(tx)

	stageProgressBlockNo, err := stages.GetStageProgress(tx, stages.Batches)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	//// BISECT ////
	if cfg.zkCfg.DebugLimit > 0 {
		finishProg, err := stages.GetStageProgress(tx, stages.Finish)
		if err != nil {
		}
		if finishProg >= cfg.zkCfg.DebugLimit {
			log.Info(fmt.Sprintf("[%s] Debug limit reached", logPrefix), "finishProg", finishProg, "debugLimit", cfg.zkCfg.DebugLimit)
			syscall.Kill(os.Getpid(), syscall.SIGINT)
		}

		if stageProgressBlockNo >= cfg.zkCfg.DebugLimit {
			log.Info(fmt.Sprintf("[%s] Debug limit reached", logPrefix), "stageProgressBlockNo", stageProgressBlockNo, "debugLimit", cfg.zkCfg.DebugLimit)
			return nil
		}
	}

	// this limit is blocknumber not included, so up to limit-1
	if cfg.zkCfg.SyncLimit > 0 && stageProgressBlockNo+1 >= cfg.zkCfg.SyncLimit {
		log.Info(fmt.Sprintf("[%s] Sync limit reached", logPrefix), "stageProgressBlockNo", stageProgressBlockNo, "syncLimit", cfg.zkCfg.SyncLimit)
		time.Sleep(2 * time.Second)
		return nil
	}

	reader := hermez_db.NewHermezDbReader(tx)
	highestVerifiedBatch, err := reader.GetLatestVerification()
	if err != nil {
		return fmt.Errorf("GetLatestVerification: %w", err)
	}

	// get batch for batches progress
	stageProgressBatchNo, err := hermezDb.GetBatchNoByL2Block(stageProgressBlockNo)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return fmt.Errorf("GetBatchNoByL2Block: %w", err)
	}

	if cfg.zkCfg.SyncLimitVerifiedEnabled && stageProgressBatchNo >= highestVerifiedBatch.BatchNo+cfg.zkCfg.SyncLimitUnverifiedCount {
		log.Info(fmt.Sprintf("[%s] Verified batch sync limit reached", logPrefix), "highestVerifiedBatch", highestVerifiedBatch, "stageProgressBatchNo", stageProgressBatchNo)
		time.Sleep(2 * time.Second)
		return nil
	}

	startSyncTime := time.Now()

	latestForkId, err := stages.GetStageProgress(tx, stages.ForkId)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	dsQueryClient, stopDsClient, err := newStreamClient(ctx, cfg, latestForkId)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s] %s", logPrefix, err))
		return fmt.Errorf("newStreamClient: %w", err)
	}
	defer stopDsClient()

	if err := dsQueryClient.HandleStart(); err != nil {
		return err
	}

	var highestDSL2Block uint64
	newBlockCheckStartTIme := time.Now()
	newBlockCheckCounter := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		highestDSL2Block, err = getHighestDSL2Block(ctx, cfg, uint16(latestForkId))
		if err != nil {
			// if we return error, stage will replay and block all other stages
			log.Warn(fmt.Sprintf("[%s] Failed to get latest l2 block from datastream: %v", logPrefix, err))
			// because this is likely something network related lets put a pause here for just a couple of
			// seconds to save the node going into a crazy loop
			time.Sleep(2 * time.Second)
			return nil
		}

		// a lower block should also break the loop because that means the datastream was unwound
		// thus we should unwind as well and continue from there
		if highestDSL2Block != stageProgressBlockNo {
			log.Info(fmt.Sprintf("[%s] Highest block in datastream", logPrefix), "datastreamBlock", highestDSL2Block, "stageProgressBlockNo", stageProgressBlockNo)
			break
		}
		if time.Since(newBlockCheckStartTIme) > 10*time.Second {
			log.Info(fmt.Sprintf("[%s] Waiting for at least one new block in datastream", logPrefix), "datastreamBlock", highestDSL2Block, "last processed block", stageProgressBlockNo)
			newBlockCheckStartTIme = time.Now()
			newBlockCheckCounter++
			if newBlockCheckCounter > 3 {
				log.Info(fmt.Sprintf("[%s] No new blocks in datastream, entering stage loop", logPrefix))
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	log.Debug(fmt.Sprintf("[%s] Highest block in db and datastream", logPrefix), "datastreamBlock", highestDSL2Block, "dbBlock", stageProgressBlockNo)
	unwindFn := func(unwindBlock uint64) (uint64, error) {
		return rollback(ctx, cfg, logPrefix, eriDb, hermezDb, unwindBlock, uint16(latestForkId), tx, u)
	}
	if highestDSL2Block < stageProgressBlockNo {
		log.Info(fmt.Sprintf("[%s] Datastream behind, unwinding", logPrefix))
		if _, err := unwindFn(highestDSL2Block); err != nil {
			return err
		}
		return nil
	}

	dsClientProgress := dsQueryClient.GetProgressAtomic()
	dsClientProgress.Swap(stageProgressBlockNo)

	// start a routine to print blocks written progress
	progressChan, stopProgressPrinter := zk.ProgressPrinterWithoutTotal(fmt.Sprintf("[%s] Downloaded blocks from datastream progress", logPrefix))
	defer stopProgressPrinter()

	_, highestL1InfoTreeIndex, err := hermezDb.GetLatestBlockL1InfoTreeIndexProgress()
	if err != nil {
		return fmt.Errorf("GetLatestBlockL1InfoTreeIndexProgress: %w", err)
	}

	stageExecProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	// just exit the stage early if there is more execution work to do
	if stageExecProgress < stageProgressBlockNo {
		log.Info(fmt.Sprintf("[%s] Execution behind, skipping stage", logPrefix))
		return nil
	}

	log.Info(fmt.Sprintf("[%s] Reading blocks from the datastream.", logPrefix))

	lastProcessedBlockHash, err := eriDb.ReadCanonicalHash(stageProgressBlockNo)
	if err != nil {
		return fmt.Errorf("ReadCanonicalHash %d: %w", stageProgressBlockNo, err)
	}

	batchProcessor, err := NewBatchesProcessor(ctx, logPrefix, tx, hermezDb, eriDb, cfg.zkCfg.SyncLimit, cfg.zkCfg.DebugLimit, cfg.zkCfg.DebugStepAfter, cfg.zkCfg.DebugStep, stageProgressBlockNo, stageProgressBatchNo, lastProcessedBlockHash, dsQueryClient, progressChan, cfg.chainConfig, cfg.miningConfig, unwindFn)
	if err != nil {
		return fmt.Errorf("NewBatchesProcessor: %w", err)
	}

	// start routine to download blocks and push them in a channel
	errorChan := make(chan struct{})
	dsClientRunner := NewDatastreamClientRunner(dsQueryClient, logPrefix)
	err = dsClientRunner.StartRead(errorChan, highestDSL2Block-stageProgressBlockNo)
	if err != nil {
		return fmt.Errorf("StartRead: %w", err)
	}
	defer dsClientRunner.StopRead()

	entryChan := dsQueryClient.GetEntryChan()

	prevAmountBlocksWritten := uint64(0)
	endLoop := false

	for {
		// get batch start and use to update forkid
		// get block
		// if no blocks available should block
		// if download routine finished, should continue to read from channel until it's empty
		// if both download routine stopped and channel empty - stop loop
		select {
		case <-errorChan:
			log.Warn("Error in datastream client, stopping consumption")
			endLoop = true
		case entry := <-*entryChan:
			// DEBUG LIMIT - don't write more than we need to
			if cfg.zkCfg.DebugLimit > 0 && batchProcessor.LastBlockHeight() >= cfg.zkCfg.DebugLimit {
				endLoop = true
				break
			}
			// Highest Verified Batch Limit + configurable amount of unverified batches
			if cfg.zkCfg.SyncLimitVerifiedEnabled && batchProcessor.HighestSeenBatchNumber() >= batchProcessor.HighestVerifiedBatchNumber()+cfg.zkCfg.SyncLimitUnverifiedCount {
				endLoop = true
				break
			}
			if endLoop, err = batchProcessor.ProcessEntry(entry); err != nil {
				// if we triggered an unwind somewhere we need to return from the stage
				if err == ErrorTriggeredUnwind {
					return nil
				}
				return fmt.Errorf("ProcessEntry: %w", err)
			}
			dsClientProgress.Store(batchProcessor.LastBlockHeight())
		case <-ctx.Done():
			log.Warn(fmt.Sprintf("[%s] Context done", logPrefix))
			endLoop = true
		default:
			time.Sleep(10 * time.Millisecond)
		}

		if endLoop {
			log.Info(fmt.Sprintf("[%s] Total blocks written: %d", logPrefix, batchProcessor.TotalBlocksWritten()))
			break
		}

		// this can be after the loop break because we save progress at the end of stage anyways. no need to do it twice
		// commit progress from time to time
		if batchProcessor.TotalBlocksWritten() != prevAmountBlocksWritten && batchProcessor.TotalBlocksWritten()%STAGE_PROGRESS_SAVE == 0 {
			if err = saveStageProgress(tx, logPrefix, batchProcessor.HighestHashableL2BlockNo(), batchProcessor.HighestSeenBatchNumber(), batchProcessor.LastBlockHeight(), batchProcessor.LastForkId()); err != nil {
				return fmt.Errorf("saveStageProgress: %w", err)
			}
			if err := hermezDb.WriteBlockL1InfoTreeIndexProgress(batchProcessor.LastBlockHeight(), highestL1InfoTreeIndex); err != nil {
				return fmt.Errorf("WriteBlockL1InfoTreeIndexProgress: %w", err)
			}

			if freshTx {
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("failed to commit tx, %w", err)
				}

				if tx, err = cfg.db.BeginRw(ctx); err != nil {
					return fmt.Errorf("failed to open tx, %w", err)
				}
				defer tx.Rollback()
				hermezDb.SetNewTx(tx)
				eriDb.SetNewTx(tx)
				batchProcessor.SetNewTx(tx)
			}
			prevAmountBlocksWritten = batchProcessor.TotalBlocksWritten()
		}

	}

	// no new progress, nothing to save
	if batchProcessor.LastBlockHeight() == stageProgressBlockNo {
		return nil
	}

	if err = saveStageProgress(tx, logPrefix, batchProcessor.HighestHashableL2BlockNo(), batchProcessor.HighestSeenBatchNumber(), batchProcessor.LastBlockHeight(), batchProcessor.LastForkId()); err != nil {
		return fmt.Errorf("saveStageProgress: %w", err)
	}
	if err := hermezDb.WriteBlockL1InfoTreeIndexProgress(batchProcessor.LastBlockHeight(), highestL1InfoTreeIndex); err != nil {
		return fmt.Errorf("WriteBlockL1InfoTreeIndexProgress: %w", err)
	}

	// stop printing blocks written progress routine
	elapsed := time.Since(startSyncTime)
	log.Info(fmt.Sprintf("[%s] Finished writing blocks", logPrefix), "blocksWritten", batchProcessor.TotalBlocksWritten(), "elapsed", elapsed)

	if freshTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("tx.Commit: %w", err)
		}
	}

	return nil
}

func saveStageProgress(tx kv.RwTx, logPrefix string, highestHashableL2BlockNo, highestSeenBatchNo, lastBlockHeight, lastForkId uint64) error {
	var err error
	// store the highest hashable block number
	if err := stages.SaveStageProgress(tx, stages.HighestHashableL2BlockNo, highestHashableL2BlockNo); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, highestSeenBatchNo); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	// store the highest seen forkid
	if err := stages.SaveStageProgress(tx, stages.ForkId, lastForkId); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	// save the latest verified batch number as well just in case this node is upgraded
	// to a sequencer in the future
	if err := stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, highestSeenBatchNo); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "lastBlockHeight", lastBlockHeight)
	if err := stages.SaveStageProgress(tx, stages.Batches, lastBlockHeight); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	return nil
}

func UnwindBatchesStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg BatchesCfg, ctx context.Context) (err error) {
	logPrefix := u.LogPrefix()

	useExternalTx := tx != nil
	if !useExternalTx {
		if tx, err = cfg.db.BeginRw(ctx); err != nil {
			return fmt.Errorf("cfg.db.BeginRw: %w", err)
		}
		defer tx.Rollback()
	}

	fromBlock := u.UnwindPoint + 1
	toBlock := u.CurrentBlockNumber
	log.Info(fmt.Sprintf("[%s] Unwinding batches stage from block number", logPrefix), "fromBlock", fromBlock, "toBlock", toBlock)
	defer log.Info(fmt.Sprintf("[%s] Unwinding batches complete", logPrefix))

	hermezDb := hermez_db.NewHermezDb(tx)

	//////////////////////////////////
	// delete batch connected stuff //
	//////////////////////////////////
	highestVerifiedBatch, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	fromBatchPrev, err := hermezDb.GetBatchNoByL2Block(fromBlock - 1)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return fmt.Errorf("GetBatchNoByL2Block: %w", err)
	}
	fromBatch, err := hermezDb.GetBatchNoByL2Block(fromBlock)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return fmt.Errorf("GetBatchNoByL2Block: %w", err)
	}
	toBatch, err := hermezDb.GetBatchNoByL2Block(toBlock)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return fmt.Errorf("GetBatchNoByL2Block: %w", err)
	}

	// if previous block has different batch, delete the "fromBlock" one
	// since it is written first in this block
	// otherwise don't delete it and start from the next batch
	if fromBatchPrev == fromBatch && fromBatch != 0 {
		fromBatch++
	}

	if fromBatch <= toBatch {
		if err := hermezDb.DeleteForkIds(fromBatch, toBatch); err != nil {
			return fmt.Errorf("DeleteForkIds: %w", err)
		}
		if err := hermezDb.DeleteBatchGlobalExitRoots(fromBatch); err != nil {
			return fmt.Errorf("DeleteBatchGlobalExitRoots: %w", err)
		}
	}

	if highestVerifiedBatch >= fromBatch {
		if err := rawdb.DeleteForkchoiceFinalized(tx); err != nil {
			return fmt.Errorf("DeleteForkchoiceFinalized: %w", err)
		}
	}
	/////////////////////////////////////////
	// finish delete batch connected stuff //
	/////////////////////////////////////////

	// cannot unwind EffectiveGasPricePercentage here although it is written in stage batches, because we have already deleted the transactions

	if err := hermezDb.DeleteStateRoots(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteStateRoots: %w", err)
	}
	if err := hermezDb.DeleteIntermediateTxStateRoots(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteIntermediateTxStateRoots: %w", err)
	}
	if err = rawdb.TruncateBlocks(ctx, tx, fromBlock); err != nil {
		return fmt.Errorf("TruncateBlocks: %w", err)
	}
	if err := hermezDb.DeleteBlockBatches(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteBlockBatches: %w", err)
	}
	if err := hermezDb.DeleteForkIdBlock(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteForkIdBlock: %w", err)
	}

	//////////////////////////////////////////////////////
	// get gers and l1BlockHashes before deleting them				    //
	// so we can delete them in the other table as well //
	//////////////////////////////////////////////////////
	gers, err := hermezDb.GetBlockGlobalExitRoots(fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("GetBlockGlobalExitRoots: %w", err)
	}

	if err := hermezDb.DeleteGlobalExitRoots(&gers); err != nil {
		return fmt.Errorf("DeleteGlobalExitRoots: %w", err)
	}

	if err = hermezDb.DeleteLatestUsedGers(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteLatestUsedGers: %w", err)
	}

	if err := hermezDb.DeleteBlockGlobalExitRoots(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteBlockGlobalExitRoots: %w", err)
	}

	if err := hermezDb.DeleteBlockL1BlockHashes(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteBlockL1BlockHashes: %w", err)
	}

	if err = hermezDb.DeleteReusedL1InfoTreeIndexes(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteReusedL1InfoTreeIndexes: %w", err)
	}

	if err = hermezDb.DeleteBatchEnds(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteBatchEnds: %w", err)
	}
	///////////////////////////////////////////////////////

	log.Info(fmt.Sprintf("[%s] Deleted headers, bodies, forkIds and blockBatches.", logPrefix))

	stageprogress := uint64(0)
	if fromBlock > 1 {
		stageprogress = fromBlock - 1
	}
	if err := stages.SaveStageProgress(tx, stages.Batches, stageprogress); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "fromBlock", stageprogress)

	/////////////////////////////////////////////
	// store the highest hashable block number //
	/////////////////////////////////////////////
	// iterate until a block with lower batch number is found
	// this is the last block of the previous batch and the highest hashable block for verifications
	lastBatchHighestBlock, _, err := hermezDb.GetHighestBlockInBatch(fromBatchPrev - 1)
	if err != nil {
		return fmt.Errorf("GetHighestBlockInBatch: %w", err)
	}

	if err := stages.SaveStageProgress(tx, stages.HighestHashableL2BlockNo, lastBatchHighestBlock); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, fromBatchPrev); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	/////////////////////////////////////////////////////
	// finish storing the highest hashable block number//
	/////////////////////////////////////////////////////

	//////////////////////////////////
	// store the highest seen forkid//
	//////////////////////////////////
	forkId, err := hermezDb.GetForkId(fromBatchPrev)
	if err != nil {
		return fmt.Errorf("GetForkId: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.ForkId, forkId); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}
	/////////////////////////////////////////
	// finish store the highest seen forkid//
	/////////////////////////////////////////

	/////////////////////////////////////////
	// store the highest used l1 info index//
	/////////////////////////////////////////

	if err := hermezDb.DeleteBlockL1InfoTreeIndexesProgress(fromBlock, toBlock); err != nil {
		return nil
	}

	if err := hermezDb.DeleteBlockL1InfoTreeIndexes(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteBlockL1InfoTreeIndexes: %w", err)
	}

	////////////////////////////////////////////////
	// finish store the highest used l1 info index//
	////////////////////////////////////////////////

	if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, fromBatchPrev); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("u.Done: %w", err)
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("tx.Commit: %w", err)
		}
	}
	return nil
}

func PruneBatchesStage(s *stagedsync.PruneState, tx kv.RwTx, cfg BatchesCfg, ctx context.Context) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("cfg.db.BeginRw: %w", err)
		}
		defer tx.Rollback()
	}

	log.Info(fmt.Sprintf("[%s] Pruning batches...", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Unwinding batches complete", logPrefix))

	hermezDb := hermez_db.NewHermezDb(tx)

	toBlock, err := stages.GetStageProgress(tx, stages.Batches)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	if err = rawdb.TruncateBlocks(ctx, tx, 1); err != nil {
		return fmt.Errorf("TruncateBlocks: %w", err)
	}

	if err := hermezDb.DeleteForkIds(0, toBlock); err != nil {
		return fmt.Errorf("DeleteForkIds: %w", err)
	}
	if err := hermezDb.DeleteBlockBatches(0, toBlock); err != nil {
		return fmt.Errorf("DeleteBlockBatches: %w", err)
	}
	if hermezDb.DeleteBlockGlobalExitRoots(0, toBlock); err != nil {
		return fmt.Errorf("DeleteBlockGlobalExitRoots: %w", err)
	}

	log.Info(fmt.Sprintf("[%s] Deleted headers, bodies, forkIds and blockBatches.", logPrefix))
	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "stageProgress", 0)
	if err := stages.SaveStageProgress(tx, stages.Batches, 0); err != nil {
		return fmt.Errorf("SaveStageProgress: %v", err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("tx.Commit: %w", err)
		}
	}
	return nil
}

// rollback performs the unwinding of blocks:
// 1. queries the latest common ancestor for datastream and db,
// 2. resolves the unwind block (as the latest block in the previous batch, comparing to the found ancestor block)
// 3. triggers the unwinding
func rollback(
	ctx context.Context,
	cfg BatchesCfg,
	logPrefix string,
	eriDb *erigon_db.ErigonDb,
	hermezDb *hermez_db.HermezDb,
	latestDSBlockNum uint64,
	latestFork uint16,
	tx kv.RwTx,
	u stagedsync.Unwinder,
) (uint64, error) {
	dsClient := buildNewStreamClient(ctx, cfg, latestFork)
	if err := dsClient.Start(); err != nil {
		return 0, err
	}
	defer func() {
		if err := dsClient.Stop(); err != nil {
			log.Error(fmt.Sprintf("[%s] Failed to stop datastream client whilst rolling back", logPrefix), "error", err)
		}
	}()
	ancestorBlockNum, ancestorBlockHash, err := findCommonAncestor(cfg, eriDb, hermezDb, l2BlockReaderRpc{}, latestDSBlockNum)
	if err != nil {
		return 0, fmt.Errorf("findCommonAncestor: %w", err)
	}
	log.Debug(fmt.Sprintf("[%s] The common ancestor for datastream and db is block %d (%s)", logPrefix, ancestorBlockNum, ancestorBlockHash))

	unwindBlockNum, unwindBlockHash, batchNum, err := getUnwindPoint(eriDb, hermezDb, ancestorBlockNum, ancestorBlockHash)
	if err != nil {
		return 0, fmt.Errorf("getUnwindPoint: %w", err)
	}

	if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, batchNum-1); err != nil {
		return 0, fmt.Errorf("SaveStageProgress: %w", err)
	}

	if cfg.zkCfg.PanicOnReorg {
		panic(fmt.Sprintf("Reorg detected: Datastream block number: %d", latestDSBlockNum))
	}

	log.Warn(fmt.Sprintf("[%s] Unwinding to block %d (%s)", logPrefix, unwindBlockNum, unwindBlockHash))

	u.UnwindTo(unwindBlockNum, stagedsync.BadBlock(unwindBlockHash, fmt.Errorf("unwind to block %d", unwindBlockNum)))
	return unwindBlockNum, nil
}

type L2BlockReaderRpc interface {
	GetZKBlockByNumberHash(url string, blockNum uint64) (common.Hash, error)
	GetBatchNumberByBlockNumber(url string, blockNum uint64) (uint64, error)
}

// findCommonAncestor searches the latest common ancestor block number and hash between the data stream and the local db.
// The common ancestor block is the one that matches both l2 block hash and batch number.
func findCommonAncestor(
	cfg BatchesCfg,
	db erigon_db.ReadOnlyErigonDb,
	hermezDb state.ReadOnlyHermezDb,
	blockReaderRpc L2BlockReaderRpc,
	latestBlockNum uint64,
) (uint64, common.Hash, error) {
	var (
		startBlockNum = uint64(0)
		endBlockNum   = latestBlockNum
		blockNumber   *uint64
		blockHash     common.Hash
	)

	if latestBlockNum == 0 {
		return 0, emptyHash, ErrFailedToFindCommonAncestor
	}

	for startBlockNum <= endBlockNum {
		if endBlockNum == 0 {
			return 0, emptyHash, ErrFailedToFindCommonAncestor
		}

		midBlockNum := (startBlockNum + endBlockNum) / 2
		headerHash, err := blockReaderRpc.GetZKBlockByNumberHash(cfg.zkCfg.L2RpcUrl, midBlockNum)
		if err != nil {
			return 0, emptyHash, fmt.Errorf("ZkBlockHash: failed to get header for block %d: %w", midBlockNum, err)
		}

		blockBatch, err := blockReaderRpc.GetBatchNumberByBlockNumber(cfg.zkCfg.L2RpcUrl, midBlockNum)
		if err != nil {
			return 0, emptyHash, fmt.Errorf("GetBatchNumberByBlockNumber: failed to get batch number for block %d: %w", midBlockNum, err)
		}

		midBlockDbHash, err := db.ReadCanonicalHash(midBlockNum)
		if err != nil {
			return 0, emptyHash, fmt.Errorf("ReadCanonicalHash block %d: %w", midBlockNum, err)
		}

		dbBatchNum, err := hermezDb.GetBatchNoByL2Block(midBlockNum)
		if err != nil {
			return 0, emptyHash, fmt.Errorf("GetBatchNoByL2Block block %d: %w", midBlockNum, err)
		}

		if headerHash != (common.Hash{}) &&
			headerHash == midBlockDbHash &&
			blockBatch == dbBatchNum {
			startBlockNum = midBlockNum + 1

			blockNumber = &midBlockNum
			blockHash = midBlockDbHash
		} else {
			endBlockNum = midBlockNum - 1
		}
	}

	if blockNumber == nil {
		return 0, emptyHash, ErrFailedToFindCommonAncestor
	}

	return *blockNumber, blockHash, nil
}

// getUnwindPoint resolves the unwind block as the latest block in the previous batch, relative to the provided block.
func getUnwindPoint(eriDb erigon_db.ReadOnlyErigonDb, hermezDb state.ReadOnlyHermezDb, blockNum uint64, blockHash common.Hash) (uint64, common.Hash, uint64, error) {
	batchNum, err := hermezDb.GetBatchNoByL2Block(blockNum)
	if err != nil {
		return 0, emptyHash, 0, fmt.Errorf("GetBatchNoByL2Block: block %d (%s): %w", blockNum, blockHash, err)
	}

	if batchNum == 0 {
		return 0, emptyHash, 0,
			fmt.Errorf("failed to find batch number for the block %d (%s)", blockNum, blockHash)
	}

	unwindBlockNum, _, err := hermezDb.GetHighestBlockInBatch(batchNum - 1)
	if err != nil {
		return 0, emptyHash, 0, fmt.Errorf("GetHighestBlockInBatch batch %d: %w", batchNum-1, err)
	}

	unwindBlockHash, err := eriDb.ReadCanonicalHash(unwindBlockNum)
	if err != nil {
		return 0, emptyHash, 0, fmt.Errorf("ReadCanonicalHash block %d: %w", unwindBlockNum, err)
	}

	return unwindBlockNum, unwindBlockHash, batchNum, nil
}

// newStreamClient instantiates new datastreamer client and starts it.
func newStreamClient(ctx context.Context, cfg BatchesCfg, latestForkId uint64) (dsClient DatastreamClient, stopFn func(), err error) {
	if cfg.dsQueryClientCreator != nil {
		dsClient, err = cfg.dsQueryClientCreator(ctx, cfg.zkCfg, latestForkId)
		if err != nil {
			return nil, nil, fmt.Errorf("dsQueryClientCreator: %w", err)
		}
		if err := dsClient.Start(); err != nil {
			return nil, nil, fmt.Errorf("dsClient.Start: %w", err)
		}
		stopFn = func() {
			if err := dsClient.Stop(); err != nil {
				log.Warn("Failed to stop datastream client", "err", err)
			}
		}
	} else {
		dsClient = cfg.dsClient
		stopFn = func() {}
	}

	return dsClient, stopFn, nil
}

func getHighestDSL2Block(ctx context.Context, batchCfg BatchesCfg, latestFork uint16) (uint64, error) {
	cfg := batchCfg.zkCfg

	// first try the sequencer rpc endpoint, it might not have been upgraded to the
	// latest version yet so if we get an error back from this call we can try the older
	// method of calling the datastream directly
	highestBlock, err := GetSequencerHighestDataStreamBlock(cfg.L2RpcUrl)
	if err == nil {
		return highestBlock, nil
	}
	log.Warn("problem getting highest ds l2 block from sequencer rpc", "err", err)

	// so something went wrong with the rpc call, let's try the older method,
	// but we're going to open a new connection rather than use the one for syncing blocks.
	// This is so we can keep the logic simple and just dispose of the connection when we're done
	// greatly simplifying state juggling of the connection if it errors
	dsClient := buildNewStreamClient(ctx, batchCfg, latestFork)
	if err := dsClient.Start(); err != nil {
		return 0, err
	}
	defer func() {
		if err := dsClient.Stop(); err != nil {
			log.Error("problem stopping datastream client looking up latest ds l2 block", "err", err)
		}
	}()
	fullBlock, err := dsClient.GetLatestL2Block()
	if err != nil {
		return 0, err
	}

	return fullBlock.L2BlockNumber, nil
}

func buildNewStreamClient(ctx context.Context, batchesCfg BatchesCfg, latestFork uint16) *client.StreamClient {
	cfg := batchesCfg.zkCfg
	return client.NewClient(ctx, cfg.L2DataStreamerUrl, cfg.L2DataStreamerUseTLS, cfg.L2DataStreamerTimeout, latestFork, client.DefaultEntryChannelSize)
}
