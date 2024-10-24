package stages

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon-lib/kv"

	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/sequencer"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/datastream/client"
	"github.com/ledgerwatch/log/v3"
)

const (
	HIGHEST_KNOWN_FORK     = 12
	STAGE_PROGRESS_SAVE    = 3000000
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
	ReadAllEntriesToChannel() error
	GetEntryChan() *chan interface{}
	GetL2BlockByNumber(blockNum uint64) (*types.FullL2Block, int, error)
	GetLatestL2Block() (*types.FullL2Block, error)
	GetStreamingAtomic() *atomic.Bool
	GetProgressAtomic() *atomic.Uint64
	EnsureConnected() (bool, error)
	Start() error
	Stop()
}

type DatastreamReadRunner interface {
	StartRead()
	StopRead()
	RestartReadFromBlock(fromBlock uint64)
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
			return fmt.Errorf("failed to open tx, %w", err)
		}
		defer tx.Rollback()
	}

	eriDb := erigon_db.NewErigonDb(tx)
	hermezDb := hermez_db.NewHermezDb(tx)

	stageProgressBlockNo, err := stages.GetStageProgress(tx, stages.Batches)
	if err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	//// BISECT ////
	if cfg.zkCfg.DebugLimit > 0 && stageProgressBlockNo > cfg.zkCfg.DebugLimit {
		return nil
	}

	// get batch for batches progress
	stageProgressBatchNo, err := hermezDb.GetBatchNoByL2Block(stageProgressBlockNo)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return fmt.Errorf("get batch no by l2 block error: %v", err)
	}

	startSyncTime := time.Now()

	latestForkId, err := stages.GetStageProgress(tx, stages.ForkId)
	if err != nil {
		return err
	}

	dsQueryClient, err := newStreamClient(ctx, cfg, latestForkId)
	if err != nil {
		log.Warn(fmt.Sprintf("[%s] %s", logPrefix, err))
		return err
	}
	defer dsQueryClient.Stop()

	highestDSL2Block, err := dsQueryClient.GetLatestL2Block()
	if err != nil {
		return fmt.Errorf("failed to retrieve the latest datastream l2 block: %w", err)
	}

	if highestDSL2Block.L2BlockNumber < stageProgressBlockNo {
		stageProgressBlockNo = highestDSL2Block.L2BlockNumber
	}

	log.Debug(fmt.Sprintf("[%s] Highest block in db and datastream", logPrefix), "datastreamBlock", highestDSL2Block.L2BlockNumber, "dbBlock", stageProgressBlockNo)

	dsClientProgress := cfg.dsClient.GetProgressAtomic()
	dsClientProgress.Store(stageProgressBlockNo)

	// start a routine to print blocks written progress
	progressChan, stopProgressPrinter := zk.ProgressPrinterWithoutTotal(fmt.Sprintf("[%s] Downloaded blocks from datastream progress", logPrefix))
	defer stopProgressPrinter()

	_, highestL1InfoTreeIndex, err := hermezDb.GetLatestBlockL1InfoTreeIndexProgress()
	if err != nil {
		return fmt.Errorf("failed to get highest used l1 info index, %w", err)
	}

	stageExecProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return fmt.Errorf("failed to get stage exec progress, %w", err)
	}

	// just exit the stage early if there is more execution work to do
	if stageExecProgress < stageProgressBlockNo {
		log.Info(fmt.Sprintf("[%s] Execution behind, skipping stage", logPrefix))
		return nil
	}

	log.Info(fmt.Sprintf("[%s] Reading blocks from the datastream.", logPrefix))

	unwindFn := func(unwindBlock uint64) error {
		return rollback(logPrefix, eriDb, hermezDb, dsQueryClient, unwindBlock, tx, u)
	}

	batchProcessor, err := NewBatchesProcessor(ctx, logPrefix, tx, hermezDb, eriDb, cfg.zkCfg.SyncLimit, cfg.zkCfg.DebugLimit, cfg.zkCfg.DebugStepAfter, cfg.zkCfg.DebugStep, stageProgressBlockNo, stageProgressBatchNo, dsQueryClient, progressChan, cfg.chainConfig, cfg.miningConfig, unwindFn)
	if err != nil {
		return err
	}

	// start routine to download blocks and push them in a channel
	dsClientRunner := NewDatastreamClientRunner(cfg.dsClient, logPrefix)
	dsClientRunner.StartRead()
	defer dsClientRunner.StopRead()

	entryChan := cfg.dsClient.GetEntryChan()

	prevAmountBlocksWritten, restartDatastreamBlock := uint64(0), uint64(0)
	endLoop := false
	unwound := false

	for {
		// get batch start and use to update forkid
		// get block
		// if no blocks available should block
		// if download routine finished, should continue to read from channel until it's empty
		// if both download routine stopped and channel empty - stop loop
		select {
		case entry := <-*entryChan:
			if restartDatastreamBlock, endLoop, unwound, err = batchProcessor.ProcessEntry(entry); err != nil {
				return err
			}
			dsClientProgress.Store(batchProcessor.LastBlockHeight())

			if restartDatastreamBlock > 0 {
				if err = dsClientRunner.RestartReadFromBlock(restartDatastreamBlock); err != nil {
					return err
				}
			}

			// if we triggered an unwind somewhere we need to return from the stage
			if unwound {
				return nil
			}
		case <-ctx.Done():
			log.Warn(fmt.Sprintf("[%s] Context done", logPrefix))
			endLoop = true
		default:
			time.Sleep(1 * time.Second)
		}

		// if ds end reached check again for new blocks in the stream
		// if there are too many new blocks get them as well before ending stage
		if batchProcessor.LastBlockHeight() >= highestDSL2Block.L2BlockNumber {
			newLatestDSL2Block, err := dsQueryClient.GetLatestL2Block()
			if err != nil {
				return fmt.Errorf("failed to retrieve the latest datastream l2 block: %w", err)
			}
			if newLatestDSL2Block.L2BlockNumber > highestDSL2Block.L2BlockNumber+NEW_BLOCKS_ON_DS_LIMIT {
				highestDSL2Block = newLatestDSL2Block
			} else {
				endLoop = true
			}
		}

		if endLoop {
			log.Info(fmt.Sprintf("[%s] Total blocks written: %d", logPrefix, batchProcessor.TotalBlocksWritten()))
			break
		}

		// this can be after the loop break because we save progress at the end of stage anyways. no need to do it twice
		// commit progress from time to time
		if batchProcessor.TotalBlocksWritten() != prevAmountBlocksWritten && batchProcessor.TotalBlocksWritten()%STAGE_PROGRESS_SAVE == 0 {
			if err = saveStageProgress(tx, logPrefix, batchProcessor.HighestHashableL2BlockNo(), batchProcessor.HighestSeenBatchNumber(), batchProcessor.LastBlockHeight(), batchProcessor.LastForkId()); err != nil {
				return err
			}
			if err := hermezDb.WriteBlockL1InfoTreeIndexProgress(batchProcessor.LastBlockHeight(), highestL1InfoTreeIndex); err != nil {
				return err
			}

			if freshTx {
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("failed to commit tx, %w", err)
				}

				if tx, err = cfg.db.BeginRw(ctx); err != nil {
					return fmt.Errorf("failed to open tx, %w", err)
				}
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
		return err
	}
	if err := hermezDb.WriteBlockL1InfoTreeIndexProgress(batchProcessor.LastBlockHeight(), highestL1InfoTreeIndex); err != nil {
		return err
	}

	// stop printing blocks written progress routine
	elapsed := time.Since(startSyncTime)
	log.Info(fmt.Sprintf("[%s] Finished writing blocks", logPrefix), "blocksWritten", batchProcessor.TotalBlocksWritten(), "elapsed", elapsed)

	if freshTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit tx, %w", err)
		}
	}

	return nil
}

func saveStageProgress(tx kv.RwTx, logPrefix string, highestHashableL2BlockNo, highestSeenBatchNo, lastBlockHeight, lastForkId uint64) error {
	var err error
	// store the highest hashable block number
	if err := stages.SaveStageProgress(tx, stages.HighestHashableL2BlockNo, highestHashableL2BlockNo); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, highestSeenBatchNo); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	// store the highest seen forkid
	if err := stages.SaveStageProgress(tx, stages.ForkId, lastForkId); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	// save the latest verified batch number as well just in case this node is upgraded
	// to a sequencer in the future
	if err := stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, highestSeenBatchNo); err != nil {
		return fmt.Errorf("save stage progress error: %w", err)
	}

	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "lastBlockHeight", lastBlockHeight)
	if err := stages.SaveStageProgress(tx, stages.Batches, lastBlockHeight); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	return nil
}

func UnwindBatchesStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg BatchesCfg, ctx context.Context) (err error) {
	logPrefix := u.LogPrefix()

	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
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
		return errors.New("could not retrieve l1 verifications batch no progress")
	}

	fromBatchPrev, err := hermezDb.GetBatchNoByL2Block(fromBlock - 1)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return fmt.Errorf("get batch no by l2 block error: %v", err)
	}
	fromBatch, err := hermezDb.GetBatchNoByL2Block(fromBlock)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return fmt.Errorf("get fromBatch no by l2 block error: %v", err)
	}
	toBatch, err := hermezDb.GetBatchNoByL2Block(toBlock)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return fmt.Errorf("get toBatch no by l2 block error: %v", err)
	}

	// if previous block has different batch, delete the "fromBlock" one
	// since it is written first in this block
	// otherwise don't delete it and start from the next batch
	if fromBatchPrev == fromBatch && fromBatch != 0 {
		fromBatch++
	}

	if fromBatch <= toBatch {
		if err := hermezDb.DeleteForkIds(fromBatch, toBatch); err != nil {
			return fmt.Errorf("delete fork ids error: %v", err)
		}
		if err := hermezDb.DeleteBatchGlobalExitRoots(fromBatch); err != nil {
			return fmt.Errorf("delete batch global exit roots error: %v", err)
		}
	}

	if highestVerifiedBatch >= fromBatch {
		if err := rawdb.DeleteForkchoiceFinalized(tx); err != nil {
			return fmt.Errorf("delete forkchoice finalized error: %v", err)
		}
	}
	/////////////////////////////////////////
	// finish delete batch connected stuff //
	/////////////////////////////////////////

	// cannot unwind EffectiveGasPricePercentage here although it is written in stage batches, because we have already deleted the transactions

	if err := hermezDb.DeleteStateRoots(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete state roots error: %v", err)
	}
	if err := hermezDb.DeleteIntermediateTxStateRoots(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete intermediate tx state roots error: %v", err)
	}
	if err = rawdb.TruncateBlocks(ctx, tx, fromBlock); err != nil {
		return fmt.Errorf("delete blocks: %w", err)
	}
	if err := hermezDb.DeleteBlockBatches(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete block batches error: %v", err)
	}
	if err := hermezDb.DeleteForkIdBlock(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete fork id block error: %v", err)
	}

	//////////////////////////////////////////////////////
	// get gers and l1BlockHashes before deleting them				    //
	// so we can delete them in the other table as well //
	//////////////////////////////////////////////////////
	gers, err := hermezDb.GetBlockGlobalExitRoots(fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("get block global exit roots error: %v", err)
	}

	if err := hermezDb.DeleteGlobalExitRoots(&gers); err != nil {
		return fmt.Errorf("delete global exit roots error: %v", err)
	}

	if err = hermezDb.DeleteLatestUsedGers(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete latest used gers error: %v", err)
	}

	if err := hermezDb.DeleteBlockGlobalExitRoots(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete block global exit roots error: %v", err)
	}

	if err := hermezDb.DeleteBlockL1BlockHashes(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete block l1 block hashes error: %v", err)
	}

	if err = hermezDb.DeleteReusedL1InfoTreeIndexes(fromBlock, toBlock); err != nil {
		return fmt.Errorf("write reused l1 info tree index error: %w", err)
	}

	if err = hermezDb.DeleteBatchEnds(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete batch ends error: %v", err)
	}
	///////////////////////////////////////////////////////

	log.Info(fmt.Sprintf("[%s] Deleted headers, bodies, forkIds and blockBatches.", logPrefix))

	stageprogress := uint64(0)
	if fromBlock > 1 {
		stageprogress = fromBlock - 1
	}
	if err := stages.SaveStageProgress(tx, stages.Batches, stageprogress); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "fromBlock", stageprogress)

	/////////////////////////////////////////////
	// store the highest hashable block number //
	/////////////////////////////////////////////
	// iterate until a block with lower batch number is found
	// this is the last block of the previous batch and the highest hashable block for verifications
	lastBatchHighestBlock, _, err := hermezDb.GetHighestBlockInBatch(fromBatchPrev - 1)
	if err != nil {
		return fmt.Errorf("get batch highest block error: %w", err)
	}

	if err := stages.SaveStageProgress(tx, stages.HighestHashableL2BlockNo, lastBatchHighestBlock); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, fromBatchPrev); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	/////////////////////////////////////////////////////
	// finish storing the highest hashable block number//
	/////////////////////////////////////////////////////

	//////////////////////////////////
	// store the highest seen forkid//
	//////////////////////////////////
	forkId, err := hermezDb.GetForkId(fromBatchPrev)
	if err != nil {
		return fmt.Errorf("get fork id error: %v", err)
	}
	if err := stages.SaveStageProgress(tx, stages.ForkId, forkId); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
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
		return fmt.Errorf("delete block l1 block hashes error: %v", err)
	}

	////////////////////////////////////////////////
	// finish store the highest used l1 info index//
	////////////////////////////////////////////////

	if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, fromBatchPrev); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	if err := u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
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
			return err
		}
		defer tx.Rollback()
	}

	log.Info(fmt.Sprintf("[%s] Pruning batches...", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Unwinding batches complete", logPrefix))

	hermezDb := hermez_db.NewHermezDb(tx)

	toBlock, err := stages.GetStageProgress(tx, stages.Batches)
	if err != nil {
		return fmt.Errorf("get stage datastream progress error: %v", err)
	}

	if err = rawdb.TruncateBlocks(ctx, tx, 1); err != nil {
		return fmt.Errorf("delete blocks: %w", err)
	}

	hermezDb.DeleteForkIds(0, toBlock)
	hermezDb.DeleteBlockBatches(0, toBlock)
	hermezDb.DeleteBlockGlobalExitRoots(0, toBlock)

	log.Info(fmt.Sprintf("[%s] Deleted headers, bodies, forkIds and blockBatches.", logPrefix))
	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "stageProgress", 0)
	if err := stages.SaveStageProgress(tx, stages.Batches, 0); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// rollback performs the unwinding of blocks:
// 1. queries the latest common ancestor for datastream and db,
// 2. resolves the unwind block (as the latest block in the previous batch, comparing to the found ancestor block)
// 3. triggers the unwinding
func rollback(logPrefix string, eriDb *erigon_db.ErigonDb, hermezDb *hermez_db.HermezDb,
	dsQueryClient DatastreamClient, latestDSBlockNum uint64, tx kv.RwTx, u stagedsync.Unwinder) error {
	ancestorBlockNum, ancestorBlockHash, err := findCommonAncestor(eriDb, hermezDb, dsQueryClient, latestDSBlockNum)
	if err != nil {
		return err
	}
	log.Debug(fmt.Sprintf("[%s] The common ancestor for datastream and db is block %d (%s)", logPrefix, ancestorBlockNum, ancestorBlockHash))

	unwindBlockNum, unwindBlockHash, batchNum, err := getUnwindPoint(eriDb, hermezDb, ancestorBlockNum, ancestorBlockHash)
	if err != nil {
		return err
	}

	if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, batchNum-1); err != nil {
		return err
	}
	log.Warn(fmt.Sprintf("[%s] Unwinding to block %d (%s)", logPrefix, unwindBlockNum, unwindBlockHash))
	u.UnwindTo(unwindBlockNum, stagedsync.BadBlock(unwindBlockHash, fmt.Errorf("unwind to block %d", unwindBlockNum)))
	return nil
}

// findCommonAncestor searches the latest common ancestor block number and hash between the data stream and the local db.
// The common ancestor block is the one that matches both l2 block hash and batch number.
func findCommonAncestor(
	db erigon_db.ReadOnlyErigonDb,
	hermezDb state.ReadOnlyHermezDb,
	dsClient DatastreamClient,
	latestBlockNum uint64) (uint64, common.Hash, error) {
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
		midBlockDataStream, errCode, err := dsClient.GetL2BlockByNumber(midBlockNum)
		if err != nil &&
			// the required block might not be in the data stream, so ignore that error
			errCode != types.CmdErrBadFromBookmark {
			return 0, emptyHash, err
		}

		midBlockDbHash, err := db.ReadCanonicalHash(midBlockNum)
		if err != nil {
			return 0, emptyHash, err
		}

		dbBatchNum, err := hermezDb.GetBatchNoByL2Block(midBlockNum)
		if err != nil {
			return 0, emptyHash, err
		}

		if midBlockDataStream != nil &&
			midBlockDataStream.L2Blockhash == midBlockDbHash &&
			midBlockDataStream.BatchNumber == dbBatchNum {
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
		return 0, emptyHash, 0, err
	}

	if batchNum == 0 {
		return 0, emptyHash, 0,
			fmt.Errorf("failed to find batch number for the block %d (%s)", blockNum, blockHash)
	}

	unwindBlockNum, _, err := hermezDb.GetHighestBlockInBatch(batchNum - 1)
	if err != nil {
		return 0, emptyHash, 0, err
	}

	unwindBlockHash, err := eriDb.ReadCanonicalHash(unwindBlockNum)
	if err != nil {
		return 0, emptyHash, 0, err
	}

	return unwindBlockNum, unwindBlockHash, batchNum, nil
}

// newStreamClient instantiates new datastreamer client and starts it.
func newStreamClient(ctx context.Context, cfg BatchesCfg, latestForkId uint64) (DatastreamClient, error) {
	var (
		dsClient DatastreamClient
		err      error
	)

	if cfg.dsQueryClientCreator != nil {
		dsClient, err = cfg.dsQueryClientCreator(ctx, cfg.zkCfg, latestForkId)
		if err != nil {
			return nil, fmt.Errorf("failed to create a datastream client. Reason: %w", err)
		}
	} else {
		zkCfg := cfg.zkCfg
		dsClient = client.NewClient(ctx, zkCfg.L2DataStreamerUrl, zkCfg.DatastreamVersion, zkCfg.L2DataStreamerTimeout, uint16(latestForkId))
	}

	if err := dsClient.Start(); err != nil {
		return nil, fmt.Errorf("failed to start a datastream client. Reason: %w", err)
	}

	return dsClient, nil
}
