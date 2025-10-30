// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stages

import (
	"context"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clstages"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/execution_client/block_collector"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type Cfg struct {
	rpc                     *rpc.BeaconRpcP2P
	ethClock                eth_clock.EthereumClock
	beaconCfg               *clparams.BeaconChainConfig
	executionClient         execution_client.ExecutionEngine
	state                   *state.CachingBeaconState
	gossipManager           *network2.GossipManager
	forkChoice              *forkchoice.ForkChoiceStore
	indiciesDB              kv.RwDB
	dirs                    datadir.Dirs
	blockReader             freezeblocks.BeaconSnapshotReader
	antiquary               *antiquary.Antiquary
	syncedData              *synced_data.SyncedDataManager
	emitter                 *beaconevents.EventEmitter
	blockCollector          block_collector.BlockCollector
	sn                      *freezeblocks.CaplinSnapshots
	blobStore               blob_storage.BlobStorage
	peerDas                 das.PeerDas
	attestationDataProducer attestation_producer.AttestationDataProducer
	caplinConfig            clparams.CaplinConfig
	hasDownloaded           bool
}

type Args struct {
	peers uint64

	targetEpoch, seenEpoch uint64
	targetSlot, seenSlot   uint64

	hasDownloaded bool
}

func ClStagesCfg(
	rpc *rpc.BeaconRpcP2P,
	antiquary *antiquary.Antiquary,
	ethClock eth_clock.EthereumClock,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.CachingBeaconState,
	executionClient execution_client.ExecutionEngine,
	gossipManager *network2.GossipManager,
	forkChoice *forkchoice.ForkChoiceStore,
	indiciesDB kv.RwDB,
	sn *freezeblocks.CaplinSnapshots,
	blockReader freezeblocks.BeaconSnapshotReader,
	dirs datadir.Dirs,
	syncBackLoopLimit uint64,
	caplinConfig clparams.CaplinConfig,
	syncedData *synced_data.SyncedDataManager,
	emitters *beaconevents.EventEmitter,
	blobStore blob_storage.BlobStorage,
	attestationDataProducer attestation_producer.AttestationDataProducer,
	peerDas das.PeerDas,
) *Cfg {
	return &Cfg{
		rpc:                     rpc,
		antiquary:               antiquary,
		ethClock:                ethClock,
		caplinConfig:            caplinConfig,
		beaconCfg:               beaconCfg,
		state:                   state,
		executionClient:         executionClient,
		gossipManager:           gossipManager,
		forkChoice:              forkChoice,
		dirs:                    dirs,
		indiciesDB:              indiciesDB,
		sn:                      sn,
		blockReader:             blockReader,
		peerDas:                 peerDas,
		syncedData:              syncedData,
		emitter:                 emitters,
		blobStore:               blobStore,
		blockCollector:          block_collector.NewBlockCollector(log.Root(), executionClient, beaconCfg, syncBackLoopLimit, dirs.Tmp),
		attestationDataProducer: attestationDataProducer,
	}
}

type StageName = string

const (
	ForwardSync              StageName = "ForwardSync"
	ChainTipSync             StageName = "ChainTipSync"
	ForkChoice               StageName = "ForkChoice"
	CleanupAndPruning        StageName = "CleanupAndPruning"
	SleepForSlot             StageName = "SleepForSlot"
	DownloadHistoricalBlocks StageName = "DownloadHistoricalBlocks"
)

func MetaCatchingUp(args Args) StageName {
	if !args.hasDownloaded {
		return DownloadHistoricalBlocks
	}
	if args.seenEpoch < args.targetEpoch {
		return ForwardSync
	}
	if args.seenSlot < args.targetSlot {
		return ChainTipSync
	}

	return ""
}

func processBlock(ctx context.Context, cfg *Cfg, db kv.RwDB, block *cltypes.SignedBeaconBlock, newPayload, fullValidation, checkDataAvaiability bool) error {
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := beacon_indicies.WriteHighestFinalized(tx, cfg.forkChoice.FinalizedSlot()); err != nil {
			return err
		}
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
	}); err != nil {
		return err
	}

	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}

	_, hasHeaderInFCU := cfg.forkChoice.GetHeader(blockRoot)
	if !checkDataAvaiability && hasHeaderInFCU {
		return nil
	}

	return cfg.forkChoice.OnBlock(ctx, block, newPayload, fullValidation, checkDataAvaiability)
}

/*

this graph describes the state transitions for cl

digraph {
    compound=true;
    subgraph cluster_0 {
        label="syncing";
        DownloadHistoricalBlocks;
        ForwardSync;
    }



    subgraph cluster_1 {
        label="head";
        ChainTipSync; ForkChoice; CleanupAndPruning; SleepForSlot;
    }


    DownloadHistoricalBlocks -> ForwardSync
    ForwardSync -> ChainTipSync
    ChainTipSync -> ForkChoice;
    ForkChoice -> CleanupAndPruning;
    ForkChoice -> NotInSync
    NotInSync -> ForwardSync
    SleepForSlot -> ChainTipSync

    CleanupAndPruning -> SleepForSlot
}

*/

// ConsensusClStages creates a stage loop container to be used to run caplin
func ConsensusClStages(ctx context.Context,
	cfg *Cfg,
) *clstages.StageGraph[*Cfg, Args] {

	// clstages run in a single thread - so we don't need to worry about any synchronization.
	return &clstages.StageGraph[*Cfg, Args]{
		// the ArgsFunc is run after every stage. It is passed into the transition function, and the same args are passed into the next stage.
		ArgsFunc: func(ctx context.Context, cfg *Cfg) (args Args) {
			var err error
			args.peers, err = cfg.rpc.Peers()
			if err != nil {
				log.Error("failed to get sentinel peer count", "err", err)
				args.peers = 0
			}
			args.hasDownloaded = cfg.hasDownloaded
			args.seenSlot = cfg.forkChoice.HighestSeen()
			args.seenEpoch = args.seenSlot / cfg.beaconCfg.SlotsPerEpoch
			args.targetSlot = cfg.ethClock.GetCurrentSlot()
			// Note that the target epoch is always one behind. this is because we are always behind in the current epoch, so it would not be very useful
			args.targetEpoch = cfg.ethClock.GetCurrentEpoch() - 1
			return
		},
		Stages: map[string]clstages.Stage[*Cfg, Args]{
			DownloadHistoricalBlocks: {
				Description: "Download historical blocks",
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return ChainTipSync
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					if err := saveHeadStateOnDiskIfNeeded(cfg, cfg.state); err != nil {
						return err
					}
					// We only download historical blocks once
					cfg.hasDownloaded = true
					startingRoot, err := cfg.state.BlockRoot()
					if err != nil {
						return err
					}
					if cfg.state.Slot() == 0 {
						cfg.state = nil // Release the state
						return nil
					}

					startingSlot := cfg.state.LatestBlockHeader().Slot
					downloader := network2.NewBackwardBeaconDownloader(ctx, cfg.rpc, cfg.sn, cfg.executionClient, cfg.indiciesDB)

					if err := SpawnStageHistoryDownload(StageHistoryReconstruction(downloader, cfg.antiquary, cfg.sn, cfg.indiciesDB, cfg.executionClient, cfg.beaconCfg, cfg.caplinConfig, false, startingRoot, startingSlot, cfg.dirs.Tmp, 600*time.Millisecond, cfg.blockCollector, cfg.blockReader, cfg.blobStore, logger, cfg.forkChoice), context.Background(), logger); err != nil {
						cfg.hasDownloaded = false
						return err
					}
					cfg.state = nil // Release the state
					return nil
				},
			},
			ForwardSync: {
				Description: `if we are 1 or more epochs behind, we download in parallel by epoch`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return ChainTipSync
				},
				ActionFunc: forwardSync,
			},
			ChainTipSync: {
				Description: `if we are within the epoch but not at head, we run catchupblocks`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return ForkChoice
				},
				ActionFunc: chainTipSync,
			},
			ForkChoice: {
				Description: `fork choice stage. We will send all fork choise things here
				also, we will wait up to delay seconds to deal with attestations + side forks`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return SleepForSlot
				},
				ActionFunc: doForkchoiceRoutine,
			},
			CleanupAndPruning: {
				Description: `cleanup and pruning is done here`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return SleepForSlot
				},
				ActionFunc: cleanupAndPruning,
			},
			SleepForSlot: {
				Description: `sleep until the next slot`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return ChainTipSync
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					nextSlot := args.seenSlot + 1
					nextSlotTime := cfg.ethClock.GetSlotTime(nextSlot)
					time.Sleep(time.Until(nextSlotTime))
					return nil
				},
			},
		},
	}
}
