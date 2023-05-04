package stages

import (
	"context"
	"runtime"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/execution_client"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/forkchoice"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

type StageForkChoiceCfg struct {
	db              kv.RwDB
	downloader      *network.ForwardBeaconDownloader
	genesisCfg      *clparams.GenesisConfig
	beaconCfg       *clparams.BeaconChainConfig
	executionClient *execution_client.ExecutionClient
	state           *state.BeaconState
	gossipManager   *network.GossipManager
	forkChoice      *forkchoice.ForkChoiceStore
}

const minPeersForDownload = 3

func StageForkChoice(db kv.RwDB, downloader *network.ForwardBeaconDownloader, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, state *state.BeaconState, executionClient *execution_client.ExecutionClient, gossipManager *network.GossipManager, forkChoice *forkchoice.ForkChoiceStore) StageForkChoiceCfg {
	return StageForkChoiceCfg{
		db:              db,
		downloader:      downloader,
		genesisCfg:      genesisCfg,
		beaconCfg:       beaconCfg,
		state:           state,
		executionClient: executionClient,
		gossipManager:   gossipManager,
		forkChoice:      forkChoice,
	}
}

// StageForkChoice enables the fork choice state. it is never supposed to exit this stage once it gets in.
func SpawnStageForkChoice(cfg StageForkChoiceCfg, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context) error {
	/*useExternalTx := tx != nil
	var err error
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}*/
	// Start download service
	log.Info("Started Ethereum 2.0 Gossip Service")
	// We start gossip management.
	go cfg.gossipManager.Start()
	go onTickService(ctx, cfg)
	go func() {
		logIntervalPeers := time.NewTicker(1 * time.Minute)
		for {
			select {
			case <-logIntervalPeers.C:
				if peerCount, err := cfg.downloader.Peers(); err == nil {
					log.Info("[Caplin] P2P", "peers", peerCount)

				}
			case <-ctx.Done():
				return
			}

		}
	}()
	startDownloadService(s, cfg)
	/*if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}*/
	return nil
}

func startDownloadService(s *stagedsync.StageState, cfg StageForkChoiceCfg) {
	cfg.downloader.SetHighestProcessedRoot(libcommon.Hash{})
	cfg.downloader.SetHighestProcessedSlot(cfg.state.Slot())
	cfg.downloader.SetProcessFunction(func(highestSlotProcessed uint64, _ libcommon.Hash, newBlocks []*cltypes.SignedBeaconBlock) (uint64, libcommon.Hash, error) {
		for _, block := range newBlocks {
			sendForckchoice :=
				utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) == block.Block.Slot
			if err := cfg.forkChoice.OnBlock(block, false, true); err != nil {
				log.Warn("Could not download block", "reason", err)
				return highestSlotProcessed, libcommon.Hash{}, err
			}
			if sendForckchoice {
				// Import the head
				headRoot, headSlot, err := cfg.forkChoice.GetHead()
				if err != nil {
					log.Debug("Could not fetch head data", "err", err)
					continue
				}
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				log.Debug("New block imported",
					"slot", block.Block.Slot, "head", headSlot, "headRoot", headRoot,
					"alloc", libcommon.ByteCount(m.Alloc))

				// Do forkchoice if possible
				if cfg.forkChoice.Engine() != nil {
					finalizedCheckpoint := cfg.forkChoice.FinalizedCheckpoint()
					// Run forkchoice
					if err := cfg.forkChoice.Engine().ForkChoiceUpdate(
						cfg.forkChoice.GetEth1Hash(finalizedCheckpoint.Root),
						cfg.forkChoice.GetEth1Hash(headRoot),
					); err != nil {
						log.Warn("Could send not forkchoice", "err", err)
					}
				}
			}
			highestSlotProcessed = utils.Max64(block.Block.Slot, highestSlotProcessed)
		}
		// Checks done, update all internals accordingly
		return highestSlotProcessed, libcommon.Hash{}, nil
	})
	maxBlockBehindBeforeDownload := int64(32)
	overtimeMargin := uint64(6) // how much time has passed before trying download the next block in seconds
MainLoop:
	for {
		targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
		overtime := utils.GetCurrentSlotOverTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
		seenSlot := cfg.forkChoice.HighestSeen()
		if targetSlot == seenSlot || (targetSlot == seenSlot+1 && overtime < overtimeMargin) {
			time.Sleep(time.Second)
			continue
		}
		peersCount, err := cfg.downloader.Peers()
		if err != nil {
			continue
		}
		waitWhenNotEnoughPeers := 5 * time.Second
		if peersCount < minPeersForDownload {
			log.Debug("Cannot sync up caplin, not enough peers", "have", peersCount, "needed", minPeersForDownload, "retryIn", waitWhenNotEnoughPeers)
			time.Sleep(waitWhenNotEnoughPeers)
			continue
		}

		cfg.downloader.SetHighestProcessedRoot(libcommon.Hash{})
		cfg.downloader.SetHighestProcessedSlot(
			utils.Max64(cfg.forkChoice.HighestSeen()-uint64(maxBlockBehindBeforeDownload), cfg.forkChoice.AnchorSlot()))

		// Wait small time
		log.Debug("Caplin may have missed some slots, started downloading chain")
		// Process blocks until we reach our target
		for highestProcessed := cfg.downloader.GetHighestProcessedSlot(); utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) > highestProcessed; highestProcessed = cfg.downloader.GetHighestProcessedSlot() {
			cfg.downloader.RequestMore()
			peersCount, err = cfg.downloader.Peers()
			if err != nil {
				break
			}
			if peersCount < minPeersForDownload {
				log.Debug("lost too many peers, restarting sync...")
				continue MainLoop
			}
		}
		log.Debug("Finished catching up", "slot", cfg.downloader.GetHighestProcessedSlot())
	}
}

func onTickService(ctx context.Context, cfg StageForkChoiceCfg) {
	tickInterval := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-tickInterval.C:
			cfg.forkChoice.OnTick(uint64(time.Now().Unix()))
		case <-ctx.Done():
			return
		}
	}
}
