package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/log/v3"
)

type StageHistoryReconstructionCfg struct {
	db         kv.RwDB
	genesisCfg *clparams.GenesisConfig
	beaconCfg  *clparams.BeaconChainConfig
	downloader *network.BackwardBeaconDownloader
	state      *state.BeaconState
}

const RecEnabled = true
const DestinationSlot = 5100000
const logIntervalTime = 30 * time.Second

func StageHistoryReconstruction(db kv.RwDB, downloader *network.BackwardBeaconDownloader, genesisCfg *clparams.GenesisConfig, beaconCfg *clparams.BeaconChainConfig, state *state.BeaconState) StageHistoryReconstructionCfg {
	return StageHistoryReconstructionCfg{
		db:         db,
		genesisCfg: genesisCfg,
		beaconCfg:  beaconCfg,
		downloader: downloader,
		state:      state,
	}
}

// SpawnStageBeaconsForward spawn the beacon forward stage
func SpawnStageHistoryReconstruction(cfg StageHistoryReconstructionCfg, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context) error {
	// This stage must be done only once.
	progress := s.BlockNumber
	if progress != 0 || !RecEnabled {
		return nil
	}

	useExternalTx := tx != nil
	var err error
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	blockRoot, err := cfg.state.BlockRoot()
	if err != nil {
		return err
	}
	log.Info("[History Reconstruction Phase] Reconstructing", "from", cfg.state.LatestBlockHeader().Slot, "to", DestinationSlot)
	// Setup slot and block root
	cfg.downloader.SetSlotToDownload(cfg.state.LatestBlockHeader().Slot)
	cfg.downloader.SetExpectedRoot(blockRoot)
	// Set up onNewBlock callback
	cfg.downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlockBellatrix) (finished bool, err error) {
		/*if err := rawdb.WriteBeaconBlock(tx, blk); err != nil {
			return false, err
		}*/
		// will arbitratly stop at slot 5.1M for testing reasons
		return blk.Block.Slot == 5100000, nil
	})
	prevProgress := cfg.downloader.Progress()

	logInterval := time.NewTicker(30 * time.Second)
	finishCh := make(chan struct{})
	// Start logging thread
	go func() {
		for {
			select {
			case <-logInterval.C:
				currProgress := cfg.downloader.Progress()
				speed := (float64(prevProgress) - float64(currProgress)) / (float64(logIntervalTime) / float64(time.Second))
				prevProgress = currProgress
				peerCount, err := cfg.downloader.Peers()
				if err != nil {
					return
				}
				log.Info("[History Reconstruction Phase] Backwards downloading phase",
					"progress", currProgress,
					"blk/sec", fmt.Sprintf("%.1f", speed),
					"peers", peerCount,
				)
			case <-finishCh:
				return
			case <-ctx.Done():

			}
		}
	}()
	for !cfg.downloader.Finished() {
		cfg.downloader.RequestMore()
	}
	close(finishCh)
	if err := s.Update(tx, 1); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
