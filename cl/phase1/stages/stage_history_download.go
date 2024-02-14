package stages

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv/dbutils"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/log/v3"
)

type StageHistoryReconstructionCfg struct {
	genesisCfg               *clparams.GenesisConfig
	beaconCfg                *clparams.BeaconChainConfig
	downloader               *network.BackwardBeaconDownloader
	sn                       *freezeblocks.CaplinSnapshots
	startingRoot             libcommon.Hash
	backfilling              bool
	waitForAllRoutines       bool
	startingSlot             uint64
	tmpdir                   string
	indiciesDB               kv.RwDB
	engine                   execution_client.ExecutionEngine
	antiquary                *antiquary.Antiquary
	logger                   log.Logger
	executionBlocksCollector *etl.Collector
	backfillingThrottling    time.Duration
}

const logIntervalTime = 30 * time.Second

func StageHistoryReconstruction(downloader *network.BackwardBeaconDownloader, antiquary *antiquary.Antiquary, sn *freezeblocks.CaplinSnapshots, indiciesDB kv.RwDB, engine execution_client.ExecutionEngine, genesisCfg *clparams.GenesisConfig, beaconCfg *clparams.BeaconChainConfig, backfilling, waitForAllRoutines bool, startingRoot libcommon.Hash, startinSlot uint64, tmpdir string, backfillingThrottling time.Duration, executionBlocksCollector *etl.Collector, logger log.Logger) StageHistoryReconstructionCfg {
	return StageHistoryReconstructionCfg{
		genesisCfg:               genesisCfg,
		beaconCfg:                beaconCfg,
		downloader:               downloader,
		startingRoot:             startingRoot,
		tmpdir:                   tmpdir,
		startingSlot:             startinSlot,
		waitForAllRoutines:       waitForAllRoutines,
		logger:                   logger,
		backfilling:              backfilling,
		indiciesDB:               indiciesDB,
		antiquary:                antiquary,
		engine:                   engine,
		sn:                       sn,
		backfillingThrottling:    backfillingThrottling,
		executionBlocksCollector: executionBlocksCollector,
	}
}

// SpawnStageBeaconsForward spawn the beacon forward stage
func SpawnStageHistoryDownload(cfg StageHistoryReconstructionCfg, ctx context.Context, logger log.Logger) error {
	// Wait for execution engine to be ready.
	blockRoot := cfg.startingRoot
	currentSlot := cfg.startingSlot

	if !clparams.SupportBackfilling(cfg.beaconCfg.DepositNetworkID) {
		cfg.backfilling = false // disable backfilling if not on a supported network
	}

	// Start the procedure
	logger.Info("Starting downloading History", "from", currentSlot)
	// Setup slot and block root
	cfg.downloader.SetSlotToDownload(currentSlot)
	cfg.downloader.SetExpectedRoot(blockRoot)
	foundLatestEth1ValidBlock := &atomic.Bool{}
	foundLatestEth1ValidBlock.Store(false)
	if cfg.engine == nil || !cfg.engine.SupportInsertion() {
		foundLatestEth1ValidBlock.Store(true) // skip this if we are not using an engine supporting direct insertion
	}

	var currEth1Progress atomic.Int64

	bytesReadInTotal := atomic.Uint64{}
	// Set up onNewBlock callback
	cfg.downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlock) (finished bool, err error) {
		tx, err := cfg.indiciesDB.BeginRw(ctx)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
		if blk.Version() >= clparams.BellatrixVersion {
			currEth1Progress.Store(int64(blk.Block.Body.ExecutionPayload.BlockNumber))
		}

		destinationSlot := cfg.sn.SegmentsMax()
		bytesReadInTotal.Add(uint64(blk.EncodingSizeSSZ()))

		slot := blk.Block.Slot
		if destinationSlot <= blk.Block.Slot {
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, blk, true); err != nil {
				return false, err
			}
		}
		if !foundLatestEth1ValidBlock.Load() && blk.Version() >= clparams.BellatrixVersion {
			payload := blk.Block.Body.ExecutionPayload
			payloadRoot, err := payload.HashSSZ()
			if err != nil {
				return false, fmt.Errorf("error hashing execution payload during download: %s", err)
			}
			encodedPayload, err := payload.EncodeSSZ(nil)
			if err != nil {
				return false, fmt.Errorf("error encoding execution payload during download: %s", err)
			}
			// Use snappy compression that the temporary files do not take too much disk.
			encodedPayload = utils.CompressSnappy(append([]byte{byte(blk.Version())}, append(blk.Block.ParentRoot[:], encodedPayload...)...))
			if err := cfg.executionBlocksCollector.Collect(dbutils.BlockBodyKey(payload.BlockNumber, payloadRoot), encodedPayload); err != nil {
				return false, fmt.Errorf("error collecting execution payload during download: %s", err)
			}
			if currEth1Progress.Load()%100 == 0 {
				return false, tx.Commit()
			}

			bodyChainHeader, err := cfg.engine.GetBodiesByHashes([]libcommon.Hash{payload.BlockHash})
			if err != nil {
				return false, fmt.Errorf("error retrieving whether execution payload is present: %s", err)
			}
			foundLatestEth1ValidBlock.Store((len(bodyChainHeader) > 0 && bodyChainHeader[0] != nil) || cfg.engine.FrozenBlocks() > payload.BlockNumber)
		}
		if blk.Version() <= clparams.AltairVersion {
			foundLatestEth1ValidBlock.Store(true)
		}

		return foundLatestEth1ValidBlock.Load() && (!cfg.backfilling || slot <= destinationSlot), tx.Commit()
	})
	prevProgress := cfg.downloader.Progress()

	finishCh := make(chan struct{})
	// Start logging thread

	go func() {
		logInterval := time.NewTicker(logIntervalTime)
		defer logInterval.Stop()
		for {
			select {
			case <-logInterval.C:
				logTime := logIntervalTime
				// if we found the latest valid hash extend ticker to 10 times the normal amout
				if foundLatestEth1ValidBlock.Load() {
					logTime = 20 * logIntervalTime
					logInterval.Reset(logTime)
				}

				if cfg.engine != nil && cfg.engine.SupportInsertion() {
					if ready, err := cfg.engine.Ready(); !ready {
						if err != nil {
							log.Warn("could not log progress", "err", err)
						}
						continue
					}

				}
				logArgs := []interface{}{}
				currProgress := cfg.downloader.Progress()
				blockProgress := float64(prevProgress - currProgress)
				ratio := float64(logTime / time.Second)
				speed := blockProgress / ratio
				prevProgress = currProgress
				peerCount, err := cfg.downloader.Peers()
				if err != nil {
					return
				}
				logArgs = append(logArgs,
					"slot", currProgress,
					"blockNumber", currEth1Progress.Load(),
					"blk/sec", fmt.Sprintf("%.1f", speed),
					"mbps/sec", fmt.Sprintf("%.4f", float64(bytesReadInTotal.Load())/(1000*1000*ratio)),
					"peers", peerCount,
					"snapshots", cfg.sn.SegmentsMax(),
					"reconnected", foundLatestEth1ValidBlock.Load(),
				)
				bytesReadInTotal.Store(0)
				logger.Info("Downloading History", logArgs...)
			case <-finishCh:
				return
			case <-ctx.Done():
			}
		}
	}()

	go func() {
		for !cfg.downloader.Finished() {
			if err := cfg.downloader.RequestMore(ctx); err != nil {
				log.Debug("closing backfilling routine", "err", err)
				return
			}
		}
		cfg.antiquary.NotifyBackfilled()
		log.Info("Backfilling finished")

		close(finishCh)
	}()
	// Lets wait for the latestValidHash to be turned on
	for !foundLatestEth1ValidBlock.Load() || (cfg.waitForAllRoutines && !cfg.downloader.Finished()) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	cfg.downloader.SetThrottle(cfg.backfillingThrottling) // throttle to 0.6 second for backfilling
	cfg.downloader.SetNeverSkip(false)
	// If i do not give it a database, erigon lib starts to cry uncontrollably
	db2 := memdb.New(cfg.tmpdir)
	defer db2.Close()
	tx2, err := db2.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx2.Rollback()

	cfg.logger.Info("Ready to insert history, waiting for sync cycle to finish")

	return nil
}
