package caplin1

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/phase1/stages"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/spf13/afero"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

func RunCaplinPhase1(ctx context.Context, sentinel sentinel.SentinelClient, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig,
	engine execution_client.ExecutionEngine, state *state.CachingBeaconState,
	caplinFreezer freezer.Freezer, datadir string) error {
	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, genesisConfig)
	downloader := network.NewForwardBeaconDownloader(ctx, beaconRpc)

	logger := log.New("app", "caplin")

	if caplinFreezer != nil {
		if err := freezer.PutObjectSSZIntoFreezer("beaconState", "caplin_core", 0, state, caplinFreezer); err != nil {
			return err
		}
	}
	forkChoice, err := forkchoice.NewForkChoiceStore(state, engine, caplinFreezer, true)
	if err != nil {
		logger.Error("Could not create forkchoice", "err", err)
		return err
	}
	bls.SetEnabledCaching(true)
	state.ForEachValidator(func(v solid.Validator, idx, total int) bool {
		pk := v.PublicKey()
		if err := bls.LoadPublicKeyIntoCache(pk[:], false); err != nil {
			panic(err)
		}
		return true
	})
	gossipManager := network.NewGossipReceiver(ctx, sentinel, forkChoice, beaconConfig, genesisConfig, caplinFreezer)
	dataDirFs := afero.NewBasePathFs(afero.NewOsFs(), datadir)

	{ // start the gossip manager
		go gossipManager.Start()
		logger.Info("Started Ethereum 2.0 Gossip Service")
	}

	{ // start logging peers
		go func() {
			logIntervalPeers := time.NewTicker(1 * time.Minute)
			for {
				select {
				case <-logIntervalPeers.C:
					if peerCount, err := downloader.Peers(); err == nil {
						logger.Info("P2P", "peers", peerCount)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	{ // start ticking forkChoice
		go func() {
			tickInterval := time.NewTicker(50 * time.Millisecond)
			for {
				select {
				case <-tickInterval.C:
					forkChoice.OnTick(uint64(time.Now().Unix()))
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// start the downloader service
	go initDownloader(downloader, genesisConfig, beaconConfig, state, nil, gossipManager, forkChoice, caplinFreezer, dataDirFs)

	forkChoiceConfig := stages.StageForkChoice(nil, downloader, genesisConfig, beaconConfig, state, nil, gossipManager, forkChoice, caplinFreezer, dataDirFs)

	sync := stagedsync.New(
		stages.ConsensusStages(
			ctx,
			forkChoiceConfig,
		),
		stages.ConsensusUnwindOrder,
		stages.ConsensusPruneOrder,
		logger,
	)
	db := memdb.New(os.TempDir())
	txn, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	var notFirst atomic.Bool
	for {
		err = sync.Run(db, txn, notFirst.CompareAndSwap(false, true))
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			logger.Error("Caplin Sync Stage Fail", "err", err.Error())
		}
	}
	return err
	// return stages.SpawnStageForkChoice(
	//
	//	forkChoiceConfig,
	//	&stagedsync.StageState{ID: "Caplin"}, nil, ctx)
}

func initDownloader(downloader *network.ForwardBeaconDownloader, genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig, beaconState *state.CachingBeaconState, executionClient *execution_client.ExecutionClient, gossipManager *network.GossipManager,
	forkChoice *forkchoice.ForkChoiceStore, caplinFreezer freezer.Freezer,
	dataDirFs afero.Fs,
) {
	downloader.SetHighestProcessedRoot(common.Hash{})
	downloader.SetHighestProcessedSlot(beaconState.Slot())
	downloader.SetProcessFunction(func(highestSlotProcessed uint64, _ common.Hash, newBlocks []*cltypes.SignedBeaconBlock) (uint64, common.Hash, error) {
		for _, block := range newBlocks {
			if err := freezer.PutObjectSSZIntoFreezer("signedBeaconBlock", "caplin_core", block.Block.Slot, block, caplinFreezer); err != nil {
				return highestSlotProcessed, common.Hash{}, err
			}
			// we send fork choice if we are in the same slot...
			// except we should only send within 4 seconds.... so.... TODO: that
			sendForkChoice := utils.GetCurrentSlot(genesisCfg.GenesisTime, beaconCfg.SecondsPerSlot) == block.Block.Slot
			if err := forkChoice.OnBlock(block, sendForkChoice, true); err != nil {
				log.Warn("Could not download block", "reason", err, "slot", block.Block.Slot)
				return highestSlotProcessed, common.Hash{}, err
			}
			highestSlotProcessed = utils.Max64(block.Block.Slot, highestSlotProcessed)
			if sendForkChoice {
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				// Import the head
				headRoot, headSlot, err := forkChoice.GetHead()

				log.Debug("New block imported",
					"slot", block.Block.Slot,
					"head", headSlot,
					"headRoot", headRoot,
					"alloc/sys", common.ByteCount(m.Alloc)+"/"+common.ByteCount(m.Sys),
					"numGC", m.NumGC,
				)
				if err != nil {
					log.Debug("Could not fetch head data",
						"slot", block.Block.Slot,
						"err", err)
					continue
				}

				// Do forkchoice if possible
				if forkChoice.Engine() != nil {
					finalizedCheckpoint := forkChoice.FinalizedCheckpoint()
					log.Info("Caplin is sending forkchoice")
					// Run forkchoice
					if err := forkChoice.Engine().ForkChoiceUpdate(
						forkChoice.GetEth1Hash(finalizedCheckpoint.BlockRoot()),
						forkChoice.GetEth1Hash(headRoot),
					); err != nil {
						log.Warn("Could not set forkchoice", "err", err)
					}
				}
			}
		}
		// Checks done, update all internals accordingly
		return highestSlotProcessed, common.Hash{}, nil
	})

}
