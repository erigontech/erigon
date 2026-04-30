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
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clstages"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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
	"github.com/erigontech/erigon/common/log/v3"
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
	blobDownloader          *network2.BlobHistoryDownloader
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
	ctx context.Context,
	rpc *rpc.BeaconRpcP2P,
	antiquary *antiquary.Antiquary,
	ethClock eth_clock.EthereumClock,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.CachingBeaconState,
	executionClient execution_client.ExecutionEngine,
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
	blobDownloader := network2.NewBlobHistoryDownloader(
		ctx,
		beaconCfg,
		rpc,
		indiciesDB,
		blobStore,
		blockReader,
		sn,
		forkChoice,
		forkChoice,
		caplinConfig.ArchiveBlobs,
		caplinConfig.ImmediateBlobsBackfilling,
		log.Root(),
	)

	return &Cfg{
		rpc:                     rpc,
		antiquary:               antiquary,
		ethClock:                ethClock,
		caplinConfig:            caplinConfig,
		beaconCfg:               beaconCfg,
		state:                   state,
		executionClient:         executionClient,
		forkChoice:              forkChoice,
		dirs:                    dirs,
		indiciesDB:              indiciesDB,
		sn:                      sn,
		blockReader:             blockReader,
		peerDas:                 peerDas,
		blobDownloader:          blobDownloader,
		syncedData:              syncedData,
		emitter:                 emitters,
		blobStore:               blobStore,
		blockCollector:          block_collector.NewPersistentBlockCollector(log.Root(), executionClient, beaconCfg, syncBackLoopLimit, dirs.CaplinHistory),
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
	WaitForPeers             StageName = "WaitForPeers"
	DownloadHistoricalBlocks StageName = "DownloadHistoricalBlocks"
)

func MetaCatchingUp(args Args) StageName {
	if !args.hasDownloaded {
		return DownloadHistoricalBlocks
	}
	// If we have no peers, skip sync stages and go directly to ForkChoice.
	// Sync stages (ForwardSync, ChainTipSync) require peers to download blocks
	// and will just timeout. ForkChoice still needs to run so the head advances
	// from blocks received via the beacon API (solo validator / single node).
	if args.peers == 0 {
		return ""
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
			// Note that the target epoch is always one behind. this is because we are always behind in the current epoch, so it would not be very useful.
			// Guard against uint64 underflow at genesis (GetCurrentEpoch() == 0).
			if currentEpoch := cfg.ethClock.GetCurrentEpoch(); currentEpoch > 0 {
				args.targetEpoch = currentEpoch - 1
			}
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
						// Initialize syncedData so Syncing() returns false at genesis.
						// Without this, the VC gets 503 "beacon node is syncing" forever.
						if err := cfg.syncedData.OnHeadState(cfg.state); err != nil {
							return fmt.Errorf("failed to set genesis head state: %w", err)
						}
						// Mark forkchoice as synced so OnAttestation processes attestations.
						cfg.forkChoice.SetSynced(true)
						// Write genesis beacon block to DB so block production can find the parent block.
						if err := writeGenesisBeaconBlock(ctx, cfg); err != nil {
							return fmt.Errorf("failed to write genesis beacon block: %w", err)
						}
						cfg.state = nil // Release the state
						return nil
					}

					startingSlot := cfg.state.LatestBlockHeader().Slot
					downloader := network2.NewBackwardBeaconDownloader(ctx, cfg.rpc, cfg.sn, cfg.executionClient, cfg.indiciesDB)

					if err := SpawnStageHistoryDownload(StageHistoryReconstruction(downloader, cfg.antiquary, cfg.sn, cfg.indiciesDB, cfg.executionClient, cfg.beaconCfg, cfg.caplinConfig, false, startingRoot, startingSlot, cfg.dirs.Tmp, 600*time.Millisecond, cfg.blockCollector, cfg.blockReader, cfg.blobStore, logger, cfg.forkChoice, cfg.blobDownloader), ctx, logger); err != nil {
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
					return CleanupAndPruning
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
			WaitForPeers: {
				Description: `brief wait for peer discovery when no peers are available`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return ChainTipSync
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					// Wait 1 second before re-checking peers. Short enough to react
					// quickly once peers appear, long enough to avoid busy-looping.
					select {
					case <-time.After(1 * time.Second):
					case <-ctx.Done():
					}
					return nil
				},
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

// writeGenesisBeaconBlock writes a synthetic genesis beacon block to the DB.
// This allows block production to find the parent block when starting from genesis (slot 0).
// The genesis beacon block body is reconstructed from the genesis state:
//   - All lists are empty (no deposits, attestations, etc.)
//   - ExecutionPayload is reconstructed from the genesis state's LatestExecutionPayloadHeader
//   - SyncAggregate is all-zero (for Altair+)
//
// This works because for genesis the execution payload has empty transactions/withdrawals,
// so Eth1Block.HashSSZ() == Eth1Header.HashSSZ() and body_root matches exactly.
func writeGenesisBeaconBlock(ctx context.Context, cfg *Cfg) error {
	if cfg.state == nil {
		return nil
	}
	version := cfg.state.Version()
	genesisBlock := cltypes.NewSignedBeaconBlock(cfg.beaconCfg, version)
	blk := genesisBlock.Block
	header := cfg.state.LatestBlockHeader()
	blk.Slot = header.Slot
	blk.ProposerIndex = header.ProposerIndex
	blk.ParentRoot = header.ParentRoot
	// header.Root (LatestBlockHeader.StateRoot) is zeroed during state transitions.
	// We must fill in the actual state root so the block root matches syncedData.HeadRoot().
	stateRoot, err := cfg.state.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to compute genesis state root: %w", err)
	}
	blk.StateRoot = stateRoot

	// The genesis block body is BeaconBlockBody() with all default/empty values per the spec.
	// SyncAggregate needs preset-aware bitvector sizing; ExecutionPayload sub-fields
	// must be initialized (non-nil) for SSZ encoding.
	body := blk.Body
	if version >= clparams.AltairVersion {
		body.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(cfg.beaconCfg.SyncCommitteeSize) / 8)
	}
	if version >= clparams.BellatrixVersion {
		body.ExecutionPayload.Extra = solid.NewExtraData()
		body.ExecutionPayload.Transactions = &solid.TransactionsSSZ{}
		if version >= clparams.CapellaVersion {
			body.ExecutionPayload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.beaconCfg.MaxWithdrawalsPerPayload), 44)
		}
	}

	return cfg.indiciesDB.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, genesisBlock, true)
	})
}
