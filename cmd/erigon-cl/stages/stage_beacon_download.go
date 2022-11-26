package stages

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/log/v3"
)

type StageBeaconForwardCfg struct {
	downloader *network.ForwardBeaconDownloader
	genesisCfg *clparams.GenesisConfig
	beaconCfg  *clparams.BeaconChainConfig
	state      *cltypes.BeaconState
}

func StageBeaconForward(downloader *network.ForwardBeaconDownloader, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, state *cltypes.BeaconState) StageBeaconForwardCfg {
	return StageBeaconForwardCfg{
		downloader: downloader,
		genesisCfg: genesisCfg,
		beaconCfg:  beaconCfg,
		state:      state,
	}
}

// SpawnStageBeaconForward spawn the beacon forward stage
func SpawnStageBeaconForward(cfg StageBeaconForwardCfg /*s *stagedsync.StageState,*/, tx kv.RwTx, ctx context.Context) error {
	// For now just collect the blocks downloaded in an array
	blocks := []*cltypes.SignedBeaconBlockBellatrix{}
	progress := cfg.state.LatestBlockHeader.Slot
	// We add one so that we wait for Gossiped blocks if we are on chain tip.
	targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) + 1
	log.Info("[Beacon Downloading] Started", "start", progress, "target", targetSlot)
	cfg.downloader.SetHighestProcessSlot(progress)
	cfg.downloader.SetLimitSegmentsLength(1024)
	// On new blocks we just check slot sequencing for now :)
	cfg.downloader.SetProcessFunction(func(
		highestSlotProcessed uint64,
		newBlocks []*cltypes.SignedBeaconBlockBellatrix) (newHighestSlotProcessed uint64, err error) {
		newHighestSlotProcessed = highestSlotProcessed
		for _, block := range newBlocks {
			if block.Block.Slot != newHighestSlotProcessed+1 || block.Block.Slot > targetSlot {
				continue
			}
			newHighestSlotProcessed++
			blocks = append(blocks, block)
		}
		return
	})
	cfg.downloader.SetIsDownloading(true)
	logInterval := time.NewTicker(30 * time.Second)
	defer logInterval.Stop()
	triggerInterval := time.NewTicker(200 * time.Millisecond)
	defer triggerInterval.Stop()
	// Process blocks until we reach our target
	for highestProcessed := cfg.downloader.GetHighestProcessedSlot(); targetSlot > highestProcessed; highestProcessed = cfg.downloader.GetHighestProcessedSlot() {
		headSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
		// If we are on chain tip, just wait for gossip.
		if headSlot != highestProcessed+1 {
			// Send 5 requests every 200 Millisecond
			cfg.downloader.RequestMore(10)
		}

		if err := cfg.downloader.ProcessBlocks(); err != nil {
			return err
		}
		select {
		case <-logInterval.C:
			log.Info("[Beacon Downloading] Progress", "slot", cfg.downloader.GetHighestProcessedSlot())
		case <-triggerInterval.C:
		}
	}
	log.Info("Processed and collected blocks", "count", len(blocks))
	return nil
}
