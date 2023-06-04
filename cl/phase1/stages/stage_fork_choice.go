package stages

import (
	"context"
	"runtime"
	"time"

	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

type StageForkChoiceCfg struct {
	db              kv.RwDB
	downloader      *network2.ForwardBeaconDownloader
	genesisCfg      *clparams.GenesisConfig
	beaconCfg       *clparams.BeaconChainConfig
	executionClient *execution_client.ExecutionClient
	state           *state.BeaconState
	gossipManager   *network2.GossipManager
	forkChoice      *forkchoice.ForkChoiceStore
	caplinFreezer   freezer.Freezer
}

const minPeersForDownload = 2
const minPeersForSyncStart = 4

var (
	freezerNameSpacePrefix = ""
	blockObjectName        = "singedBeaconBlock"
	stateObjectName        = "beaconState"
	gossipAction           = "gossip"
)

func StageForkChoice(db kv.RwDB, downloader *network2.ForwardBeaconDownloader, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, state *state.BeaconState, executionClient *execution_client.ExecutionClient, gossipManager *network2.GossipManager,
	forkChoice *forkchoice.ForkChoiceStore, caplinFreezer freezer.Freezer) StageForkChoiceCfg {
	return StageForkChoiceCfg{
		db:              db,
		downloader:      downloader,
		genesisCfg:      genesisCfg,
		beaconCfg:       beaconCfg,
		state:           state,
		executionClient: executionClient,
		gossipManager:   gossipManager,
		forkChoice:      forkChoice,
		caplinFreezer:   caplinFreezer,
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
			if err := freezer.PutObjectSSZIntoFreezer("downloaded_signedBeaconBlock", "caplin_core", block.Block.Slot, block, cfg.caplinFreezer); err != nil {
				return highestSlotProcessed, libcommon.Hash{}, err
			}

			sendForckchoice :=
				utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) == block.Block.Slot
			if err := cfg.forkChoice.OnBlock(block, false, true); err != nil {
				log.Warn("Could not download block", "reason", err, "slot", block.Block.Slot)
				return highestSlotProcessed, libcommon.Hash{}, err
			}
			highestSlotProcessed = utils.Max64(block.Block.Slot, highestSlotProcessed)
			if sendForckchoice {
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				// Import the head
				headRoot, headSlot, err := cfg.forkChoice.GetHead()

				log.Debug("New block imported",
					"slot", block.Block.Slot,
					"head", headSlot,
					"headRoot", headRoot,
					"alloc/sys", libcommon.ByteCount(m.Alloc)+"/"+libcommon.ByteCount(m.Sys),
					"numGC", m.NumGC,
				)
				if err != nil {
					log.Debug("Could not fetch head data",
						"slot", block.Block.Slot,
						"err", err)
					continue
				}

				// Do forkchoice if possible
				if cfg.forkChoice.Engine() != nil {
					finalizedCheckpoint := cfg.forkChoice.FinalizedCheckpoint()
					// Run forkchoice
					if err := cfg.forkChoice.Engine().ForkChoiceUpdate(
						cfg.forkChoice.GetEth1Hash(finalizedCheckpoint.BlockRoot()),
						cfg.forkChoice.GetEth1Hash(headRoot),
					); err != nil {
						log.Warn("Could send not forkchoice", "err", err)
					}
				}
			}
		}
		// Checks done, update all internals accordingly
		return highestSlotProcessed, libcommon.Hash{}, nil
	})
	maxBlockBehindBeforeDownload := int64(32)
	overtimeMargin := uint64(6) // how much time has passed before trying download the next block in seconds
	ctx := context.TODO()
	isDownloading := false
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
		if !isDownloading {
			isDownloading = peersCount >= minPeersForSyncStart
		}
		if isDownloading {
			isDownloading = peersCount >= minPeersForDownload
			if !isDownloading {
				log.Debug("[Caplin] Lost too many peers", "have", peersCount, "needed", minPeersForDownload)
			}
		}
		if !isDownloading {
			log.Debug("[Caplin] Waiting For Peers", "have", peersCount, "needed", minPeersForSyncStart, "retryIn", waitWhenNotEnoughPeers)
			time.Sleep(waitWhenNotEnoughPeers)
			continue
		}
		highestSeen := cfg.forkChoice.HighestSeen()
		startDownloadSlot := highestSeen - uint64(maxBlockBehindBeforeDownload)
		// Detect underflow
		if startDownloadSlot > highestSeen {
			startDownloadSlot = 0
		}

		cfg.downloader.SetHighestProcessedRoot(libcommon.Hash{})
		cfg.downloader.SetHighestProcessedSlot(
			utils.Max64(startDownloadSlot, cfg.forkChoice.FinalizedSlot()))

		// Wait small time
		log.Debug("Caplin may have missed some slots, started downloading chain")
		// Process blocks until we reach our target
		for highestProcessed := cfg.downloader.GetHighestProcessedSlot(); utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) > highestProcessed; highestProcessed = cfg.downloader.GetHighestProcessedSlot() {
			ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
			cfg.downloader.RequestMore(ctx)
			cancel()
			peersCount, err = cfg.downloader.Peers()
			if err != nil {
				break
			}
			if utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot) == cfg.forkChoice.HighestSeen() {
				break
			}
			if peersCount < minPeersForDownload {
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
