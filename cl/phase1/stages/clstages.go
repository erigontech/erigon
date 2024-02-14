package stages

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clstages"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/db_config"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

type Cfg struct {
	rpc             *rpc.BeaconRpcP2P
	genesisCfg      *clparams.GenesisConfig
	beaconCfg       *clparams.BeaconChainConfig
	executionClient execution_client.ExecutionEngine
	state           *state.CachingBeaconState
	gossipManager   *network2.GossipManager
	forkChoice      *forkchoice.ForkChoiceStore
	indiciesDB      kv.RwDB
	tmpdir          string
	dbConfig        db_config.DatabaseConfiguration
	sn              *freezeblocks.CaplinSnapshots
	antiquary       *antiquary.Antiquary
	syncedData      *synced_data.SyncedDataManager
	emitter         *beaconevents.Emitters
	prebuffer       *etl.Collector
	gossipSource    persistence.BlockSource

	hasDownloaded, backfilling bool
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
	genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.CachingBeaconState,
	executionClient execution_client.ExecutionEngine,
	gossipManager *network2.GossipManager,
	forkChoice *forkchoice.ForkChoiceStore,
	indiciesDB kv.RwDB,
	sn *freezeblocks.CaplinSnapshots,
	tmpdir string,
	dbConfig db_config.DatabaseConfiguration,
	backfilling bool,
	syncedData *synced_data.SyncedDataManager,
	emitters *beaconevents.Emitters,
	gossipSource persistence.BlockSource,
) *Cfg {
	return &Cfg{
		rpc:             rpc,
		antiquary:       antiquary,
		genesisCfg:      genesisCfg,
		beaconCfg:       beaconCfg,
		state:           state,
		executionClient: executionClient,
		gossipManager:   gossipManager,
		forkChoice:      forkChoice,
		tmpdir:          tmpdir,
		indiciesDB:      indiciesDB,
		dbConfig:        dbConfig,
		sn:              sn,
		backfilling:     backfilling,
		syncedData:      syncedData,
		emitter:         emitters,
		prebuffer:       etl.NewCollector("Caplin-blocks", tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()),
		gossipSource:    gossipSource,
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
	processBlock := func(tx kv.RwTx, block *cltypes.SignedBeaconBlock, newPayload, fullValidation bool) error {
		if err := cfg.forkChoice.OnBlock(block, newPayload, fullValidation); err != nil {
			log.Warn("fail to process block", "reason", err, "slot", block.Block.Slot)
			return err
		}
		if err := beacon_indicies.WriteHighestFinalized(tx, cfg.forkChoice.FinalizedSlot()); err != nil {
			return err
		}
		// Write block to database optimistically if we are very behind.
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
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
			args.targetSlot = utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
			// Note that the target epoch is always one behind. this is because we are always behind in the current epoch, so it would not be very useful
			args.targetEpoch = utils.GetCurrentEpoch(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, cfg.beaconCfg.SlotsPerEpoch) - 1
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
					downloader := network2.NewBackwardBeaconDownloader(context.Background(), cfg.rpc, cfg.indiciesDB)

					if err := SpawnStageHistoryDownload(StageHistoryReconstruction(downloader, cfg.antiquary, cfg.sn, cfg.indiciesDB, cfg.executionClient, cfg.genesisCfg, cfg.beaconCfg, cfg.backfilling, false, startingRoot, startingSlot, cfg.tmpdir, 600*time.Millisecond, cfg.prebuffer, logger), context.Background(), logger); err != nil {
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
					tx, err := cfg.indiciesDB.BeginRw(ctx)
					if err != nil {
						return err
					}
					defer tx.Rollback()
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
						for _, block := range blocks {

							if err := processBlock(tx, block, false, true); err != nil {
								log.Warn("bad blocks segment received", "err", err)
								return highestSlotProcessed, highestBlockRootProcessed, err
							}
							if shouldInsert && block.Version() >= clparams.BellatrixVersion {
								if cfg.prebuffer == nil {
									cfg.prebuffer = etl.NewCollector("Caplin-blocks", cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root())
									cfg.prebuffer.LogLvl(log.LvlDebug)
								}
								executionPayload := block.Block.Body.ExecutionPayload
								executionPayloadRoot, err := executionPayload.HashSSZ()
								if err != nil {
									return highestSlotProcessed, highestBlockRootProcessed, err
								}
								versionByte := byte(block.Version())
								enc, err := executionPayload.EncodeSSZ(nil)
								if err != nil {
									return highestSlotProcessed, highestBlockRootProcessed, err
								}
								enc = append([]byte{versionByte}, append(block.Block.ParentRoot[:], enc...)...)
								enc = utils.CompressSnappy(enc)

								if err := cfg.prebuffer.Collect(dbutils.BlockBodyKey(executionPayload.BlockNumber, executionPayloadRoot), enc); err != nil {
									return highestSlotProcessed, highestBlockRootProcessed, err
								}
							}

							if highestSlotProcessed < block.Block.Slot {
								currentSlot.Store(block.Block.Slot)
								highestSlotProcessed = block.Block.Slot
								highestBlockRootProcessed, err = block.Block.HashSSZ()
								if err != nil {
									return highestSlotProcessed, highestBlockRootProcessed, err
								}
							}
						}

						return highestSlotProcessed, highestBlockRootProcessed, nil
					})
					chainTipSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
					logger.Info("[Caplin] Forward Sync", "from", currentSlot.Load(), "to", chainTipSlot)
					prevProgress := currentSlot.Load()
					for currentSlot.Load()+4 < chainTipSlot {
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
					if err := tx.Commit(); err != nil {
						return err
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
								time.Sleep(10 * time.Second)
								return nil
							case <-readyInterval.C:
								ready, err := cfg.executionClient.Ready()
								if err != nil {
									return err
								}
								if ready {
									break ReadyLoop
								}
							}
						}
					}

					tx, err := cfg.indiciesDB.BeginRw(ctx)
					if err != nil {
						return err
					}
					defer tx.Rollback()

					blocksBatch := []*types.Block{}
					blocksBatchLimit := 10_000
					if cfg.executionClient != nil && cfg.prebuffer != nil && cfg.executionClient.SupportInsertion() {
						if err := cfg.prebuffer.Load(tx, kv.Headers, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
							if len(v) == 0 {
								return nil
							}
							v, err = utils.DecompressSnappy(v)
							if err != nil {
								return err
							}
							version := clparams.StateVersion(v[0])
							parentRoot := common.BytesToHash(v[1:33])
							v = v[33:]
							executionPayload := cltypes.NewEth1Block(version, cfg.beaconCfg)
							if err := executionPayload.DecodeSSZ(v, int(version)); err != nil {
								return err
							}
							body := executionPayload.Body()
							txs, err := types.DecodeTransactions(body.Transactions)
							if err != nil {
								log.Warn("bad blocks segment received", "err", err)
								return err
							}
							header, err := executionPayload.RlpHeader(&parentRoot)
							if err != nil {
								log.Warn("bad blocks segment received", "err", err)
								return err
							}
							blocksBatch = append(blocksBatch, types.NewBlockFromStorage(executionPayload.BlockHash, header, txs, nil, body.Withdrawals))
							if len(blocksBatch) >= blocksBatchLimit {
								if err := cfg.executionClient.InsertBlocks(blocksBatch, true); err != nil {
									logger.Warn("failed to insert blocks", "err", err)
								}
								logger.Info("[Caplin] Inserted blocks", "progress", blocksBatch[len(blocksBatch)-1].NumberU64())
								blocksBatch = []*types.Block{}
							}
							return next(k, nil, nil)
						}, etl.TransformArgs{}); err != nil {
							return err
						}
						if len(blocksBatch) > 0 {
							if err := cfg.executionClient.InsertBlocks(blocksBatch, true); err != nil {
								logger.Warn("failed to insert blocks", "err", err)
							}
						}
						cfg.prebuffer.Close()
						cfg.prebuffer = nil

					}

					logger.Debug("waiting for blocks...",
						"seenSlot", args.seenSlot,
						"targetSlot", args.targetSlot,
						"requestedSlots", totalRequest,
					)
					respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1024)
					errCh := make(chan error)
					sources := []persistence.BlockSource{cfg.gossipSource, rpcSource}

					// if we are more than one block behind, we request the rpc source as well
					if totalRequest > 2 {
						sources = append(sources, rpcSource)
					}
					// 15 seconds is a good timeout for this
					ctx, cn := context.WithTimeout(ctx, 25*time.Second)
					defer cn()

					// we go ask all the sources and see who gets back to us first. whoever does is the winner!!
					for _, v := range sources {
						sourceFunc := v.GetRange
						go func(source persistence.BlockSource) {
							if _, ok := source.(*persistence.BeaconRpcSource); ok {
								var blocks *peers.PeeredObject[[]*cltypes.SignedBeaconBlock]

								select {
								case <-time.After((time.Duration(cfg.beaconCfg.SecondsPerSlot) * time.Second) / 4):
								case <-ctx.Done():
									return
								}

								for {
									var err error
									from := cfg.forkChoice.HighestSeen() - 2
									currentSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
									count := (currentSlot - from) + 4
									if cfg.forkChoice.HighestSeen() >= args.targetSlot {
										return
									}
									blocks, err = sourceFunc(ctx, nil, from, count)
									if err != nil {
										errCh <- err
										return
									}
									if len(blocks.Data) == 0 {
										continue
									}
									select {
									case respCh <- blocks:
									case <-ctx.Done():
										return
									case <-time.After(time.Second): // take a smol pause
									}
								}
							}
							ticker := time.NewTicker(10 * time.Millisecond)
							defer ticker.Stop()
							for {
								if cfg.forkChoice.HighestSeen() >= args.targetSlot {
									return
								}
								blocks, err := sourceFunc(ctx, nil, args.seenSlot+1, 11)
								if err != nil {
									errCh <- err
									return
								}

								select {
								case <-ctx.Done():
									return
								case respCh <- blocks:
								}
								<-ticker.C

							}
						}(v)
					}
					logTimer := time.NewTicker(30 * time.Second)
					defer logTimer.Stop()
				MainLoop:
					for {
						select {
						case <-ctx.Done():
							return errors.New("timeout waiting for blocks")
						case err := <-errCh:
							return err
						case blocks := <-respCh:
							for _, block := range blocks.Data {
								// we can ignore this error because the block would not process if the hashssz failed
								blockRoot, _ := block.Block.HashSSZ()
								if _, ok := cfg.forkChoice.GetHeader(blockRoot); ok {
									continue
								}
								if err := processBlock(tx, block, true, true); err != nil {
									log.Error("bad blocks segment received", "err", err)
									continue
								}

								// publish block to event handler
								cfg.emitter.Publish("block", map[string]any{
									"slot":                 strconv.Itoa(int(block.Block.Slot)),
									"block":                common.Hash(blockRoot),
									"execution_optimistic": false, // TODO: i don't know what to put here. i see other places doing false, leaving flase for now
								})
								// Attestations processing can take some time if they are not cached in properly.
								go func() {
									block.Block.Body.Attestations.Range(func(idx int, a *solid.Attestation, total int) bool {
										// emit attestation
										cfg.emitter.Publish("attestation", a)
										if err = cfg.forkChoice.OnAttestation(a, true, false); err != nil {
											log.Debug("bad attestation received", "err", err)
										}
										return true
									})
								}()
								// emit the other stuff
								block.Block.Body.VoluntaryExits.Range(func(index int, value *cltypes.SignedVoluntaryExit, length int) bool {
									cfg.emitter.Publish("voluntary-exit", value)
									return true
								})
								if block.Block.Slot >= args.targetSlot {
									break MainLoop
								}
							}
						case <-logTimer.C:
							logger.Info("[Caplin] Progress", "progress", cfg.forkChoice.HighestSeen(), "from", args.seenSlot, "to", args.targetSlot)
						}
					}
					if err := tx.Commit(); err != nil {
						return err
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
						if err := cfg.forkChoice.Engine().ForkChoiceUpdate(
							cfg.forkChoice.GetEth1Hash(finalizedCheckpoint.BlockRoot()),
							cfg.forkChoice.GetEth1Hash(headRoot),
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
					reconnectionRoots := make([]canonicalEntry, 0, 1)

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
					slotTime := utils.GetSlotTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, args.targetSlot).Add(
						time.Duration(cfg.beaconCfg.SecondsPerSlot) * (time.Second / 3),
					)
					waitDur := slotTime.Sub(time.Now())
					ctx, cn := context.WithTimeout(ctx, waitDur)
					defer cn()
					tx, err := cfg.indiciesDB.BeginRw(ctx)
					if err != nil {
						return err
					}
					defer tx.Rollback()
					// try to get the current block TODO: Completely remove
					// blocks, err := cfg.gossipSource.GetRange(ctx, tx, args.seenSlot, 1)
					// if err != nil {
					// 	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// 		return nil
					// 	}
					// 	return err
					// }

					// for _, block := range blocks.Data {
					// 	err := processBlock(tx, block, true, true)
					// 	if err != nil {
					// 		// its okay if block processing fails
					// 		logger.Warn("extra block failed validation", "err", err)
					// 		return nil
					// 	}
					// 	shouldForkChoiceSinceReorg = true
					// 	logger.Debug("extra block received", "slot", args.seenSlot)
					// }
					return tx.Commit()
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
					// clean up some old ranges
					err = cfg.gossipSource.PurgeRange(ctx, tx, 1, args.seenSlot-cfg.beaconCfg.SlotsPerEpoch*16)
					if err != nil {
						return err
					}
					// TODO(Giulio2002): schedule snapshots retirement if needed.
					if !cfg.backfilling {
						if err := beacon_indicies.PruneBlocks(ctx, tx, args.seenSlot-pruneDistance); err != nil {
							return err
						}
					}

					return tx.Commit()
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
					nextSlotTime := utils.GetSlotTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, nextSlot)
					nextSlotDur := nextSlotTime.Sub(time.Now())
					logger.Debug("sleeping until next slot", "slot", nextSlot, "time", nextSlotTime, "dur", nextSlotDur)
					time.Sleep(nextSlotDur)
					return nil
				},
			},
		},
	}
}
