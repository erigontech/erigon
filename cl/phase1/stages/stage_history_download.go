package stages

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/log/v3"
)

type StageHistoryReconstructionCfg struct {
	genesisCfg        *clparams.GenesisConfig
	beaconCfg         *clparams.BeaconChainConfig
	downloader        *network.BackwardBeaconDownloader
	startingRoot      libcommon.Hash
	startingSlot      uint64
	backFillingAmount uint64
	tmpdir            string
	db                persistence.BeaconChainDatabase
	engine            execution_client.ExecutionEngine
	logger            log.Logger
}

const logIntervalTime = 30 * time.Second

func StageHistoryReconstruction(downloader *network.BackwardBeaconDownloader, db persistence.BeaconChainDatabase, engine execution_client.ExecutionEngine, genesisCfg *clparams.GenesisConfig, beaconCfg *clparams.BeaconChainConfig, backFillingAmount uint64, startingRoot libcommon.Hash, startinSlot uint64, tmpdir string, logger log.Logger) StageHistoryReconstructionCfg {
	return StageHistoryReconstructionCfg{
		genesisCfg:        genesisCfg,
		beaconCfg:         beaconCfg,
		downloader:        downloader,
		startingRoot:      startingRoot,
		tmpdir:            tmpdir,
		startingSlot:      startinSlot,
		logger:            logger,
		backFillingAmount: backFillingAmount,
		db:                db,
		engine:            engine,
	}
}

func waitForExecutionEngineToBeReady(ctx context.Context, engine execution_client.ExecutionEngine) error {
	if engine == nil {
		return nil
	}
	checkInterval := time.NewTicker(200 * time.Millisecond)
	for {
		select {
		case <-checkInterval.C:
			ready, err := engine.Ready()
			if err != nil {
				return err
			}
			if ready {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// SpawnStageBeaconsForward spawn the beacon forward stage
func SpawnStageHistoryDownload(cfg StageHistoryReconstructionCfg, ctx context.Context, logger log.Logger) error {
	// Wait for execution engine to be ready.
	if err := waitForExecutionEngineToBeReady(ctx, cfg.engine); err != nil {
		return err
	}
	blockRoot := cfg.startingRoot
	destinationSlot := uint64(0)
	currentSlot := cfg.startingSlot
	if currentSlot > cfg.backFillingAmount {
		destinationSlot = currentSlot - cfg.backFillingAmount
	}

	executionBlocksCollector := etl.NewCollector("SpawnStageHistoryDownload", cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer executionBlocksCollector.Close()
	// Start the procedure
	logger.Info("Downloading History", "from", currentSlot)
	// Setup slot and block root
	cfg.downloader.SetSlotToDownload(currentSlot)
	cfg.downloader.SetExpectedRoot(blockRoot)
	foundLatestEth1ValidHash := false
	if cfg.engine == nil || !cfg.engine.SupportInsertion() {
		foundLatestEth1ValidHash = true // skip this if we are not using an engine supporting direct insertion
	}

	var currEth1Progress atomic.Int64

	// Set up onNewBlock callback
	cfg.downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlock) (finished bool, err error) {
		if blk.Version() >= clparams.BellatrixVersion {
			currEth1Progress.Store(int64(blk.Block.Body.ExecutionPayload.BlockNumber))
		}
		if !foundLatestEth1ValidHash {
			payload := blk.Block.Body.ExecutionPayload
			encodedPayload, err := payload.EncodeSSZ(nil)
			if err != nil {
				return false, fmt.Errorf("error encoding execution payload during download: %s", err)
			}
			// Use snappy compression that the temporary files do not take too much disk.
			encodedPayload = utils.CompressSnappy(append(encodedPayload, byte(blk.Version())))
			if err := executionBlocksCollector.Collect(dbutils.BlockBodyKey(payload.BlockNumber, payload.BlockHash), encodedPayload); err != nil {
				return false, fmt.Errorf("error collecting execution payload during download: %s", err)
			}

			bodyChainHeader, err := cfg.engine.GetBodiesByHashes([]libcommon.Hash{payload.BlockHash})
			if err != nil {
				return false, fmt.Errorf("error retrieving whether execution payload is present: %s", err)
			}
			foundLatestEth1ValidHash = len(bodyChainHeader) > 0
		}

		slot := blk.Block.Slot
		return slot <= destinationSlot && foundLatestEth1ValidHash, cfg.db.WriteBlock(ctx, blk, true)
	})
	prevProgress := cfg.downloader.Progress()

	logInterval := time.NewTicker(logIntervalTime)
	finishCh := make(chan struct{})
	// Start logging thread
	go func() {
		for {
			select {
			case <-logInterval.C:
				logArgs := []interface{}{}
				currProgress := cfg.downloader.Progress()
				speed := float64(prevProgress-currProgress) / float64(logIntervalTime/time.Second)
				prevProgress = currProgress
				peerCount, err := cfg.downloader.Peers()
				if err != nil {
					return
				}
				logArgs = append(logArgs,
					"slot", currProgress,
					"blockNumber", currEth1Progress.Load(),
					"blk/sec", fmt.Sprintf("%.1f", speed),
					"peers", peerCount)
				logger.Info("Downloading History", logArgs...)
			case <-finishCh:
				return
			case <-ctx.Done():

			}
		}
	}()
	for !cfg.downloader.Finished() {
		cfg.downloader.RequestMore(ctx)
	}
	close(finishCh)
	// If i do not give it a database, erigon lib starts to cry uncontrollably
	db := memdb.New(cfg.tmpdir)
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	blockBatch := []*types.Block{}
	blockBatchMaxSize := 1000

	if err := executionBlocksCollector.Load(tx, kv.Headers, func(k, vComp []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if cfg.engine == nil || !cfg.engine.SupportInsertion() {
			return next(k, nil, nil)
		}
		var err error
		var v []byte
		if v, err = utils.DecompressSnappy(vComp); err != nil {
			return fmt.Errorf("error decompressing dump during collection: %s", err)
		}

		version := clparams.StateVersion(v[len(v)-1])
		executionPayload := cltypes.NewEth1Block(version, cfg.beaconCfg)
		if err := executionPayload.DecodeSSZ(v[:len(v)-1], int(version)); err != nil {
			return fmt.Errorf("error decoding execution payload during collection: %s", err)
		}
		body := executionPayload.Body()
		header, err := executionPayload.RlpHeader()
		if err != nil {
			return fmt.Errorf("error parsing rlp header during collection: %s", err)
		}

		txs, err := types.DecodeTransactions(body.Transactions)
		if err != nil {
			return err
		}

		block := types.NewBlockFromStorage(executionPayload.BlockHash, header, txs, nil, body.Withdrawals)
		blockBatch = append(blockBatch, block)
		if len(blockBatch) >= blockBatchMaxSize {
			if err := cfg.engine.InsertBlocks(blockBatch); err != nil {
				return fmt.Errorf("error inserting block during collection: %s", err)
			}
			blockBatch = blockBatch[:0]
		}
		return next(k, nil, nil)
	}, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if cfg.engine != nil && cfg.engine.SupportInsertion() {
		if err := cfg.engine.InsertBlocks(blockBatch); err != nil {
			return fmt.Errorf("error doing last block insertion during collection: %s", err)
		}
	}
	return nil
}
