package stages

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clstages"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/blob_storage"
	"github.com/ledgerwatch/erigon/cl/persistence/db_config"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client/block_collector"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/ledgerwatch/erigon/cl/validator/attestation_producer"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	"github.com/ledgerwatch/log/v3"

	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
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
	tmpdir                  string
	dbConfig                db_config.DatabaseConfiguration
	blockReader             freezeblocks.BeaconSnapshotReader
	antiquary               *antiquary.Antiquary
	syncedData              *synced_data.SyncedDataManager
	emitter                 *beaconevents.Emitters
	blockCollector          block_collector.BlockCollector
	sn                      *freezeblocks.CaplinSnapshots
	blobStore               blob_storage.BlobStorage
	attestationDataProducer attestation_producer.AttestationDataProducer

	hasDownloaded, backfilling, blobBackfilling bool
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
	tmpdir string,
	dbConfig db_config.DatabaseConfiguration,
	backfilling bool,
	blobBackfilling bool,
	syncedData *synced_data.SyncedDataManager,
	emitters *beaconevents.Emitters,
	blobStore blob_storage.BlobStorage,
	attestationDataProducer attestation_producer.AttestationDataProducer,
) *Cfg {
	return &Cfg{
		rpc:                     rpc,
		antiquary:               antiquary,
		ethClock:                ethClock,
		beaconCfg:               beaconCfg,
		state:                   state,
		executionClient:         executionClient,
		gossipManager:           gossipManager,
		forkChoice:              forkChoice,
		tmpdir:                  tmpdir,
		indiciesDB:              indiciesDB,
		dbConfig:                dbConfig,
		sn:                      sn,
		blockReader:             blockReader,
		backfilling:             backfilling,
		syncedData:              syncedData,
		emitter:                 emitters,
		blobStore:               blobStore,
		blockCollector:          block_collector.NewBlockCollector(log.Root(), executionClient, beaconCfg, tmpdir),
		blobBackfilling:         blobBackfilling,
		attestationDataProducer: attestationDataProducer,
	}
}

type StageName = string

const (
	ForwardSync              StageName = "ForwardSync"
	CatchUpBlocks            StageName = "CatchUpBlocks"
	ForkChoice               StageName = "ForkChoice"
	ListenForForks           StageName = "ListenForForks"
	CleanupAndPruning        StageName = "CleanupAndPruning"
	SleepForSlot             StageName = "SleepForSlot"
	DownloadHistoricalBlocks StageName = "DownloadHistoricalBlocks"
)

const (
	minPeersForDownload = uint64(4)
)

func MetaCatchingUp(args Args) StageName {
	if !args.hasDownloaded {
		return DownloadHistoricalBlocks
	}
	if args.seenEpoch < args.targetEpoch {
		return ForwardSync
	}
	if args.seenSlot < args.targetSlot {
		return CatchUpBlocks
	}

	return ""
}

/*

this graph describes the state transitions for cl

digraph {
    compound=true;
    subgraph cluster_0 {
        label="syncing";
        WaitForPeers;
        CatchUpBlocks;
        ForwardSync;
    }

    subgraph cluster_3 {
        label="if behind (transition function)"
        MetaCatchingUp;
    }

    subgraph cluster_1 {
        label="head";
        ForkChoice; CleanupAndPruning; ListenForForks; SleepForSlot;
    }

    MetaCatchingUp -> WaitForPeers
    MetaCatchingUp -> ForwardSync
    MetaCatchingUp -> CatchUpBlocks

    WaitForPeers -> MetaCatchingUp[lhead=cluster_3]
    ForwardSync -> MetaCatchingUp[lhead=cluster_3]
    CatchUpBlocks -> MetaCatchingUp[lhead=cluster_3]
    CleanupAndPruning -> MetaCatchingUp[lhead=cluster_3]
    ListenForForks -> MetaCatchingUp[lhead=cluster_3]
    ForkChoice -> MetaCatchingUp[lhead=cluster_3]

    CatchUpBlocks -> ForkChoice
    ForkChoice -> ListenForForks

    SleepForSlot -> WaitForPeers

    ListenForForks -> ForkChoice
    ListenForForks -> SleepForSlot
    ListenForForks -> CleanupAndPruning
    CleanupAndPruning -> SleepForSlot
}
*/

// ConsensusClStages creates a stage loop container to be used to run caplin
func ConsensusClStages(ctx context.Context,
	cfg *Cfg,
) *clstages.StageGraph[*Cfg, Args] {

	rpcSource := persistence.NewBeaconRpcSource(cfg.rpc)
	processBlock := func(db kv.RwDB, block *cltypes.SignedBeaconBlock, newPayload, fullValidation, checkDataAvaiability bool) error {
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			if err := beacon_indicies.WriteHighestFinalized(tx, cfg.forkChoice.FinalizedSlot()); err != nil {
				return err
			}
			return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
		}); err != nil {
			return err
		}

		return cfg.forkChoice.OnBlock(ctx, block, newPayload, fullValidation, checkDataAvaiability)
	}

	// TODO: this is an ugly hack, but it works! Basically, we want shared state in the clstages.
	// Probably the correct long term solution is to create a third generic parameter that defines shared state
	// but for now, all it would have are the two gossip sources and the forkChoicesSinceReorg, so i don't think its worth it (yet).
	shouldForkChoiceSinceReorg := false

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
					return CatchUpBlocks
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					cfg.hasDownloaded = true
					startingRoot, err := cfg.state.BlockRoot()
					if err != nil {
						return err
					}
					// This stage is special so use context.Background() TODO(Giulio2002): make the context be passed in
					startingSlot := cfg.state.LatestBlockHeader().Slot
					downloader := network2.NewBackwardBeaconDownloader(context.Background(), cfg.rpc, cfg.executionClient, cfg.indiciesDB)

					if err := SpawnStageHistoryDownload(StageHistoryReconstruction(downloader, cfg.antiquary, cfg.sn, cfg.indiciesDB, cfg.executionClient, cfg.beaconCfg, cfg.backfilling, cfg.blobBackfilling, false, startingRoot, startingSlot, cfg.tmpdir, 600*time.Millisecond, cfg.blockCollector, cfg.blockReader, cfg.blobStore, logger), context.Background(), logger); err != nil {
						cfg.hasDownloaded = false
						return err
					}
					return nil
				},
			},
			ForwardSync: {
				Description: `if we are 1 or more epochs behind, we download in parallel by epoch`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return CatchUpBlocks
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					shouldInsert := cfg.executionClient != nil && cfg.executionClient.SupportInsertion()

					downloader := network2.NewForwardBeaconDownloader(ctx, cfg.rpc)
					finalizedCheckpoint := cfg.forkChoice.FinalizedCheckpoint()
					var currentSlot atomic.Uint64
					currentSlot.Store(finalizedCheckpoint.Epoch() * cfg.beaconCfg.SlotsPerEpoch)
					secsPerLog := 30
					logTicker := time.NewTicker(time.Duration(secsPerLog) * time.Second)
					// Always start from the current finalized checkpoint
					downloader.SetHighestProcessedRoot(finalizedCheckpoint.BlockRoot())
					downloader.SetHighestProcessedSlot(currentSlot.Load())
					downloader.SetProcessFunction(func(highestSlotProcessed uint64, highestBlockRootProcessed common.Hash, blocks []*cltypes.SignedBeaconBlock) (newHighestSlotProcessed uint64, newHighestBlockRootProcessed common.Hash, err error) {
						initialHighestSlotProcessed := highestSlotProcessed
						initialHighestBlockRootProcessed := highestBlockRootProcessed
						sort.Slice(blocks, func(i, j int) bool {
							return blocks[i].Block.Slot < blocks[j].Block.Slot
						})

						for i, block := range blocks {
							if err := processBlock(cfg.indiciesDB, block, false, true, false); err != nil {
								log.Warn("bad blocks segment received", "err", err)
								blocks = blocks[i:]
								break
							}
							if shouldInsert && block.Version() >= clparams.BellatrixVersion {
								if err := cfg.blockCollector.AddBlock(block.Block); err != nil {
									logger.Warn("failed to add block to collector", "err", err)
									blocks = blocks[i:]
									break
								}
							}

							if highestSlotProcessed < block.Block.Slot {
								currentSlot.Store(block.Block.Slot)
								highestSlotProcessed = block.Block.Slot
								highestBlockRootProcessed, err = block.Block.HashSSZ()
								if err != nil {
									blocks = blocks[i:]
									logger.Warn("failed to hash block", "err", err)
									break
								}
							}
						}
						// Do the DA now, first of all see what blobs to retrieve
						ids, err := network2.BlobsIdentifiersFromBlocks(blocks)
						if err != nil {
							logger.Warn("failed to get blob identifiers", "err", err)
							return initialHighestSlotProcessed, initialHighestBlockRootProcessed, err
						}
						if ids.Len() == 0 { // no blobs, no DA.
							return highestSlotProcessed, highestBlockRootProcessed, nil
						}
						blobs, err := network2.RequestBlobsFrantically(ctx, cfg.rpc, ids)
						if err != nil {
							logger.Warn("failed to get blobs", "err", err)
							return initialHighestSlotProcessed, initialHighestBlockRootProcessed, err
						}
						var highestProcessed, inserted uint64
						if highestProcessed, inserted, err = blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, cfg.blobStore, ids, blobs.Responses, nil); err != nil {
							logger.Warn("failed to get verify blobs", "err", err)
							cfg.rpc.BanPeer(blobs.Peer)
							return initialHighestSlotProcessed, initialHighestBlockRootProcessed, err
						}
						if inserted == uint64(ids.Len()) {
							return highestSlotProcessed, highestBlockRootProcessed, nil
						}

						if highestProcessed <= initialHighestSlotProcessed {
							return initialHighestSlotProcessed, initialHighestBlockRootProcessed, nil
						}
						return highestProcessed - 1, highestBlockRootProcessed, err
					})
					chainTipSlot := cfg.ethClock.GetCurrentSlot()
					logger.Info("[Caplin] Forward Sync", "from", currentSlot.Load(), "to", chainTipSlot)
					prevProgress := currentSlot.Load()
					for downloader.GetHighestProcessedSlot() < chainTipSlot {
						downloader.RequestMore(ctx)

						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-logTicker.C:
							progressMade := chainTipSlot - currentSlot.Load()
							distFromChainTip := time.Duration(progressMade*cfg.beaconCfg.SecondsPerSlot) * time.Second
							timeProgress := currentSlot.Load() - prevProgress
							estimatedTimeRemaining := 999 * time.Hour
							if timeProgress > 0 {
								estimatedTimeRemaining = time.Duration(float64(progressMade)/(float64(currentSlot.Load()-prevProgress)/float64(secsPerLog))) * time.Second
							}
							prevProgress = currentSlot.Load()
							logger.Info("[Caplin] Forward Sync", "progress", currentSlot.Load(), "distance-from-chain-tip", distFromChainTip, "estimated-time-remaining", estimatedTimeRemaining)
						default:
						}
					}

					return nil
				},
			},
			CatchUpBlocks: {
				Description: `if we are within the epoch but not at head, we run catchupblocks`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return ForkChoice
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					totalRequest := args.targetSlot - args.seenSlot
					readyTimeout := time.NewTimer(10 * time.Second)
					readyInterval := time.NewTimer(50 * time.Millisecond)
					defer readyTimeout.Stop()
					defer readyInterval.Stop()
					if cfg.executionClient != nil {
					ReadyLoop:
						for { // if the client does not support insertion, then skip
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-readyTimeout.C:
								return nil
							case <-readyInterval.C:
								ready, err := cfg.executionClient.Ready(ctx)
								if err != nil {
									return err
								}
								if ready {
									break ReadyLoop
								}
							}
						}
					}

					tmpDB := memdb.New(cfg.tmpdir)
					defer tmpDB.Close()
					tx, err := tmpDB.BeginRw(ctx)
					if err != nil {
						return err
					}
					defer tx.Rollback()

					if cfg.executionClient != nil && cfg.executionClient.SupportInsertion() {
						if err := cfg.blockCollector.Flush(context.Background()); err != nil {
							return err
						}
					}
					tx.Rollback()

					logger.Debug("waiting for blocks...",
						"seenSlot", args.seenSlot,
						"targetSlot", args.targetSlot,
						"requestedSlots", totalRequest,
					)
					respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1024)
					errCh := make(chan error)

					// 15 seconds is a good timeout for this
					ctx, cn := context.WithTimeout(ctx, 25*time.Second)
					defer cn()

					go func() {
						select {
						case <-time.After((time.Duration(cfg.beaconCfg.SecondsPerSlot) * time.Second) / 2):
						case <-ctx.Done():
							return
						}

						for {
							var blocks *peers.PeeredObject[[]*cltypes.SignedBeaconBlock]
							var err error
							from := cfg.forkChoice.HighestSeen() - 2
							currentSlot := cfg.ethClock.GetCurrentSlot()
							count := (currentSlot - from) + 4
							if cfg.forkChoice.HighestSeen() >= args.targetSlot {
								return
							}
							blocks, err = rpcSource.GetRange(ctx, nil, from, count)
							if err != nil {
								errCh <- err
								return
							}
							if len(blocks.Data) == 0 {
								continue
							}
							ids, err := network2.BlobsIdentifiersFromBlocks(blocks.Data)
							if err != nil {
								errCh <- err
								return
							}
							var inserted uint64

							for inserted != uint64(ids.Len()) {
								select {
								case <-ctx.Done():
									return
								default:
								}
								blobs, err := network2.RequestBlobsFrantically(ctx, cfg.rpc, ids)
								if err != nil {
									errCh <- err
									return
								}
								if _, inserted, err = blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, cfg.blobStore, ids, blobs.Responses, nil); err != nil {
									errCh <- err
									return
								}
							}
							select {
							case respCh <- blocks:
							case <-ctx.Done():
								return
							case <-time.After(time.Second): // take a smol pause
							}
						}
					}()

					logTimer := time.NewTicker(30 * time.Second)
					defer logTimer.Stop()
					// blocks may be scheduled for later execution outside of the catch-up flow
					presenceTicker := time.NewTicker(20 * time.Millisecond)
					defer presenceTicker.Stop()
					seenBlockRoots := make(map[common.Hash]struct{})
				MainLoop:
					for {
						select {
						case <-presenceTicker.C:
							if cfg.forkChoice.HighestSeen() >= args.targetSlot {
								break MainLoop
							}
						case <-ctx.Done():
							return errors.New("timeout waiting for blocks")
						case err := <-errCh:
							return err
						case blocks := <-respCh:
							for _, block := range blocks.Data {

								if _, ok := cfg.forkChoice.GetHeader(block.Block.ParentRoot); !ok {
									time.Sleep(time.Millisecond)
									continue
								}
								// we can ignore this error because the block would not process if the hashssz failed
								blockRoot, _ := block.Block.HashSSZ()
								if _, ok := cfg.forkChoice.GetHeader(blockRoot); ok {
									if block.Block.Slot >= args.targetSlot {
										break MainLoop
									}
									continue
								}
								if _, ok := seenBlockRoots[blockRoot]; ok {
									continue
								}
								seenBlockRoots[blockRoot] = struct{}{}
								if err := processBlock(cfg.indiciesDB, block, true, true, true); err != nil {
									log.Debug("bad blocks segment received", "err", err)
									continue
								}

								if err := tx.Commit(); err != nil {
									return err
								}

								// publish block to event handler
								cfg.emitter.Publish("block", map[string]any{
									"slot":                 strconv.Itoa(int(block.Block.Slot)),
									"block":                common.Hash(blockRoot),
									"execution_optimistic": false, // TODO: i don't know what to put here. i see other places doing false, leaving flase for now
								})
								if block.Block.Slot >= args.targetSlot {
									break MainLoop
								}
							}
						case <-logTimer.C:
							logger.Info("[Caplin] Progress", "progress", cfg.forkChoice.HighestSeen(), "from", args.seenSlot, "to", args.targetSlot)
						}
					}

					return nil
				},
			},
			ForkChoice: {
				Description: `fork choice stage. We will send all fork choise things here
				also, we will wait up to delay seconds to deal with attestations + side forks`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return ListenForForks
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {

					// Now check the head
					headRoot, headSlot, err := cfg.forkChoice.GetHead()
					if err != nil {
						return fmt.Errorf("failed to get head: %w", err)
					}

					// Do forkchoice if possible
					if cfg.forkChoice.Engine() != nil {
						finalizedCheckpoint := cfg.forkChoice.FinalizedCheckpoint()
						logger.Debug("Caplin is sending forkchoice")
						// Run forkchoice
						if _, err := cfg.forkChoice.Engine().ForkChoiceUpdate(
							ctx,
							cfg.forkChoice.GetEth1Hash(finalizedCheckpoint.BlockRoot()),
							cfg.forkChoice.GetEth1Hash(headRoot), nil,
						); err != nil {
							logger.Warn("Could not set forkchoice", "err", err)
							return err
						}
					}
					if err := cfg.rpc.SetStatus(cfg.forkChoice.FinalizedCheckpoint().BlockRoot(),
						cfg.forkChoice.FinalizedCheckpoint().Epoch(),
						headRoot, headSlot); err != nil {
						logger.Warn("Could not set status", "err", err)
					}
					tx, err := cfg.indiciesDB.BeginRw(ctx)
					if err != nil {
						return fmt.Errorf("failed to begin transaction: %w", err)
					}
					defer tx.Rollback()

					type canonicalEntry struct {
						slot uint64
						root common.Hash
					}

					currentRoot := headRoot
					currentSlot := headSlot
					currentCanonical, err := beacon_indicies.ReadCanonicalBlockRoot(tx, currentSlot)
					if err != nil {
						return fmt.Errorf("failed to read canonical block root: %w", err)
					}
					reconnectionRoots := []canonicalEntry{{currentSlot, currentRoot}}

					for currentRoot != currentCanonical {
						var newFoundSlot *uint64

						if currentRoot, err = beacon_indicies.ReadParentBlockRoot(ctx, tx, currentRoot); err != nil {
							return fmt.Errorf("failed to read parent block root: %w", err)
						}
						if newFoundSlot, err = beacon_indicies.ReadBlockSlotByBlockRoot(tx, currentRoot); err != nil {
							return fmt.Errorf("failed to read block slot by block root: %w", err)
						}
						if newFoundSlot == nil {
							break
						}
						currentSlot = *newFoundSlot
						currentCanonical, err = beacon_indicies.ReadCanonicalBlockRoot(tx, currentSlot)
						if err != nil {
							return fmt.Errorf("failed to read canonical block root: %w", err)
						}
						reconnectionRoots = append(reconnectionRoots, canonicalEntry{currentSlot, currentRoot})
					}
					if err := beacon_indicies.TruncateCanonicalChain(ctx, tx, currentSlot); err != nil {
						return fmt.Errorf("failed to truncate canonical chain: %w", err)
					}
					for i := len(reconnectionRoots) - 1; i >= 0; i-- {
						if err := beacon_indicies.MarkRootCanonical(ctx, tx, reconnectionRoots[i].slot, reconnectionRoots[i].root); err != nil {
							return fmt.Errorf("failed to mark root canonical: %w", err)
						}
					}
					if err := beacon_indicies.MarkRootCanonical(ctx, tx, headSlot, headRoot); err != nil {
						return fmt.Errorf("failed to mark root canonical: %w", err)
					}

					// Increment validator set
					headState, err := cfg.forkChoice.GetStateAtBlockRoot(headRoot, false)
					if err != nil {
						return fmt.Errorf("failed to get state at block root: %w", err)
					}
					cfg.forkChoice.SetSynced(true)
					if err := cfg.syncedData.OnHeadState(headState); err != nil {
						return fmt.Errorf("failed to set head state: %w", err)
					}
					start := time.Now()

					copiedHeadState := cfg.syncedData.HeadState() // it is just copied, so we can use it without worrying about concurrency

					if _, err = cfg.attestationDataProducer.ProduceAndCacheAttestationData(copiedHeadState, copiedHeadState.Slot(), 0); err != nil {
						logger.Warn("failed to produce and cache attestation data", "err", err)
					}
					// Incement some stuff here
					preverifiedValidators := cfg.forkChoice.PreverifiedValidator(headState.FinalizedCheckpoint().BlockRoot())
					preverifiedHistoricalSummary := cfg.forkChoice.PreverifiedHistoricalSummaries(headState.FinalizedCheckpoint().BlockRoot())
					preverifiedHistoricalRoots := cfg.forkChoice.PreverifiedHistoricalRoots(headState.FinalizedCheckpoint().BlockRoot())
					if err := state_accessors.IncrementPublicKeyTable(tx, headState, preverifiedValidators); err != nil {
						return fmt.Errorf("failed to increment public key table: %w", err)
					}
					if err := state_accessors.IncrementHistoricalSummariesTable(tx, headState, preverifiedHistoricalSummary); err != nil {
						return fmt.Errorf("failed to increment historical summaries table: %w", err)
					}
					if err := state_accessors.IncrementHistoricalRootsTable(tx, headState, preverifiedHistoricalRoots); err != nil {
						return fmt.Errorf("failed to increment historical roots table: %w", err)
					}
					log.Debug("Incremented state history", "elapsed", time.Since(start), "preverifiedValidators", preverifiedValidators)

					stateRoot, err := headState.HashSSZ()
					if err != nil {
						return fmt.Errorf("failed to hash ssz: %w", err)
					}

					headEpoch := headSlot / cfg.beaconCfg.SlotsPerEpoch
					previous_duty_dependent_root, err := headState.GetBlockRootAtSlot((headEpoch-1)*cfg.beaconCfg.SlotsPerEpoch - 1)
					if err != nil {
						return fmt.Errorf("failed to get block root at slot for previous_duty_dependent_root: %w", err)
					}
					current_duty_dependent_root, err := headState.GetBlockRootAtSlot(headEpoch*cfg.beaconCfg.SlotsPerEpoch - 1)
					if err != nil {
						return fmt.Errorf("failed to get block root at slot for current_duty_dependent_root: %w", err)
					}
					// emit the head event
					cfg.emitter.Publish("head", map[string]any{
						"slot":                         strconv.Itoa(int(headSlot)),
						"block":                        headRoot,
						"state":                        common.Hash(stateRoot),
						"epoch_transition":             true,
						"previous_duty_dependent_root": previous_duty_dependent_root,
						"current_duty_dependent_root":  current_duty_dependent_root,
						"execution_optimistic":         false,
					})

					var m runtime.MemStats
					dbg.ReadMemStats(&m)
					logger.Debug("Imported chain segment",
						"hash", headRoot, "slot", headSlot,
						"alloc", common.ByteCount(m.Alloc),
						"sys", common.ByteCount(m.Sys))
					if err := tx.Commit(); err != nil {
						return err
					}
					return nil
				},
			},
			ListenForForks: {
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					defer func() {
						shouldForkChoiceSinceReorg = false
					}()
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					if shouldForkChoiceSinceReorg {
						return ForkChoice
					}
					return CleanupAndPruning

				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					return nil // Remove completely in a subsequent refactor
				},
			},
			CleanupAndPruning: {
				Description: `cleanup and pruning is done here`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return SleepForSlot
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					tx, err := cfg.indiciesDB.BeginRw(ctx)
					if err != nil {
						return err
					}
					defer tx.Rollback()
					pruneDistance := uint64(1_000_000)

					if !cfg.backfilling {
						if err := beacon_indicies.PruneBlocks(ctx, tx, args.seenSlot-pruneDistance); err != nil {
							return err
						}
					}

					if err := tx.Commit(); err != nil {
						return err
					}
					return cfg.blobStore.Prune()

				},
			},
			SleepForSlot: {
				Description: `sleep until the next slot`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return ListenForForks
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					nextSlot := args.seenSlot + 1
					nextSlotTime := cfg.ethClock.GetSlotTime(nextSlot)
					nextSlotDur := nextSlotTime.Sub(time.Now())
					logger.Debug("sleeping until next slot", "slot", nextSlot, "time", nextSlotTime, "dur", nextSlotDur)
					time.Sleep(nextSlotDur)
					return nil
				},
			},
		},
	}
}
