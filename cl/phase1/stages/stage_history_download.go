package stages

import (
	"context"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clpersist"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/spf13/afero"

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
	dataDirFs         afero.Fs
	logger            log.Logger
}

const logIntervalTime = 30 * time.Second

func StageHistoryReconstruction(downloader *network.BackwardBeaconDownloader, dataDirFs afero.Fs, genesisCfg *clparams.GenesisConfig, beaconCfg *clparams.BeaconChainConfig, backFillingAmount uint64, startingRoot libcommon.Hash, startinSlot uint64, tmpdir string, logger log.Logger) StageHistoryReconstructionCfg {
	return StageHistoryReconstructionCfg{
		genesisCfg:        genesisCfg,
		beaconCfg:         beaconCfg,
		downloader:        downloader,
		startingRoot:      startingRoot,
		tmpdir:            tmpdir,
		startingSlot:      startinSlot,
		logger:            logger,
		backFillingAmount: backFillingAmount,
		dataDirFs:         dataDirFs,
	}
}

// SpawnStageBeaconsForward spawn the beacon forward stage
func SpawnStageHistoryDownload(cfg StageHistoryReconstructionCfg, ctx context.Context, logger log.Logger) error {

	blockRoot := cfg.startingRoot
	destinationSlot := uint64(0)
	currentSlot := cfg.startingSlot
	if currentSlot > cfg.backFillingAmount {
		destinationSlot = currentSlot - cfg.backFillingAmount
	}

	// Start the procedure
	logger.Info("Downloading History", "from", currentSlot, "to", destinationSlot)
	// Setup slot and block root
	cfg.downloader.SetSlotToDownload(currentSlot)
	cfg.downloader.SetExpectedRoot(blockRoot)
	foundLatestEth1ValidHash := false

	fs := afero.NewBasePathFs(cfg.dataDirFs, "caplin/beacon")
	// Set up onNewBlock callback
	cfg.downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlock) (finished bool, err error) {
		slot := blk.Block.Slot
		return slot <= destinationSlot && foundLatestEth1ValidHash, clpersist.SaveBlockWithConfig(fs, blk, cfg.beaconCfg)
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
					"progress", currProgress,
					"blk/sec", fmt.Sprintf("%.1f", speed),
					"peers", peerCount)
				if currentSlot > destinationSlot {
					logArgs = append(logArgs, "remaining", currProgress-destinationSlot)
				}
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

	return nil
}
