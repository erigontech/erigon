package stages

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
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
	beaconDB        persistence.BeaconChainDatabase
	indiciesDB      kv.RwDB
	tmpdir          string
	dbConfig        db_config.DatabaseConfiguration
	sn              *freezeblocks.CaplinSnapshots
	antiquary       *antiquary.Antiquary
	syncedData      *synced_data.SyncedDataManager
	emitter         *beaconevents.Emitters

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
	beaconDB persistence.BeaconChainDatabase,
	indiciesDB kv.RwDB,
	sn *freezeblocks.CaplinSnapshots,
	tmpdir string,
	dbConfig db_config.DatabaseConfiguration,
	backfilling bool,
	syncedData *synced_data.SyncedDataManager,
	emitters *beaconevents.Emitters,
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
		beaconDB:        beaconDB,
		indiciesDB:      indiciesDB,
		dbConfig:        dbConfig,
		sn:              sn,
		backfilling:     backfilling,
		syncedData:      syncedData,
		emitter:         emitters,
	}
}

type StageName = string

const (
	CatchUpEpochs            StageName = "CatchUpEpochs"
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
		return CatchUpEpochs
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
        CatchUpEpochs;
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
    MetaCatchingUp -> CatchUpEpochs
    MetaCatchingUp -> CatchUpBlocks

    WaitForPeers -> MetaCatchingUp[lhead=cluster_3]
    CatchUpEpochs -> MetaCatchingUp[lhead=cluster_3]
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
	gossipSource := persistence.NewGossipSource(ctx, cfg.gossipManager)
	processBlock := func(tx kv.RwTx, block *cltypes.SignedBeaconBlock, newPayload, fullValidation bool) error {
		if err := cfg.forkChoice.OnBlock(block, newPayload, fullValidation); err != nil {
			log.Warn("fail to process block", "reason", err, "slot", block.Block.Slot)
			return err
		}
		if err := beacon_indicies.WriteHighestFinalized(tx, cfg.forkChoice.FinalizedSlot()); err != nil {
			return err
		}
		// Write block to database optimistically if we are very behind.
		return cfg.beaconDB.WriteBlock(ctx, tx, block, false)
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

					if err := SpawnStageHistoryDownload(StageHistoryReconstruction(downloader, cfg.antiquary, cfg.sn, cfg.beaconDB, cfg.indiciesDB, cfg.executionClient, cfg.genesisCfg, cfg.beaconCfg, cfg.backfilling, false, startingRoot, startingSlot, cfg.tmpdir, 600*time.Millisecond, logger), context.Background(), logger); err != nil {
						cfg.hasDownloaded = false
						return err
					}
					return nil
				},
			},
			CatchUpEpochs: {
				Description: `if we are 1 or more epochs behind, we download in parallel by epoch`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if x := MetaCatchingUp(args); x != "" {
						return x
					}
					return CatchUpBlocks
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					logger.Info("[Caplin] Downloading epochs from reqresp", "from", args.seenEpoch, "to", args.targetEpoch)
					currentEpoch := args.seenEpoch
					blockBatch := []*types.Block{}
					shouldInsert := cfg.executionClient != nil && cfg.executionClient.SupportInsertion()
					tx, err := cfg.indiciesDB.BeginRw(ctx)
					if err != nil {
						return err
					}
					defer tx.Rollback()
				MainLoop:
					for currentEpoch <= args.targetEpoch+1 {
						startBlock := currentEpoch * cfg.beaconCfg.SlotsPerEpoch
						blocks, err := rpcSource.GetRange(ctx, tx, startBlock, cfg.beaconCfg.SlotsPerEpoch)
						if err != nil {
							return err
						}
						// If we got an empty packet ban the peer
						if len(blocks.Data) == 0 {
							cfg.rpc.BanPeer(blocks.Peer)
							log.Debug("no data received from peer in epoch download")
							continue MainLoop
						}

						logger.Info("[Caplin] Epoch downloaded", "epoch", currentEpoch)
						for _, block := range blocks.Data {

							if shouldInsert && block.Version() >= clparams.BellatrixVersion {
								executionPayload := block.Block.Body.ExecutionPayload
								body := executionPayload.Body()
								txs, err := types.DecodeTransactions(body.Transactions)
								if err != nil {
									log.Warn("bad blocks segment received", "err", err)
									cfg.rpc.BanPeer(blocks.Peer)
									currentEpoch = utils.Max64(args.seenEpoch, currentEpoch-1)
									continue MainLoop
								}
								parentRoot := &block.Block.ParentRoot
								header, err := executionPayload.RlpHeader(parentRoot)
								if err != nil {
									log.Warn("bad blocks segment received", "err", err)
									cfg.rpc.BanPeer(blocks.Peer)
									currentEpoch = utils.Max64(args.seenEpoch, currentEpoch-1)
									continue MainLoop
								}
								blockBatch = append(blockBatch, types.NewBlockFromStorage(executionPayload.BlockHash, header, txs, nil, body.Withdrawals))
							}
							if err := processBlock(tx, block, false, true); err != nil {
								log.Warn("bad blocks segment received", "err", err)
								cfg.rpc.BanPeer(blocks.Peer)
								currentEpoch = utils.Max64(args.seenEpoch, currentEpoch-1)
								continue MainLoop
							}
						}
						if len(blockBatch) > 0 {
							if err := cfg.executionClient.InsertBlocks(blockBatch); err != nil {
								log.Warn("bad blocks segment received", "err", err)
								currentEpoch = utils.Max64(args.seenEpoch, currentEpoch-1)
								blockBatch = blockBatch[:0]
								continue MainLoop
							}
							blockBatch = blockBatch[:0]
						}
						currentEpoch++
					}
					return tx.Commit()
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
					logger.Debug("waiting for blocks...",
						"seenSlot", args.seenSlot,
						"targetSlot", args.targetSlot,
						"requestedSlots", totalRequest,
					)
					respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock])
					errCh := make(chan error)
					sources := []persistence.BlockSource{gossipSource, rpcSource}

					// if we are more than one block behind, we request the rpc source as well
					if totalRequest > 2 {
						sources = append(sources, rpcSource)
					}
					// 15 seconds is a good timeout for this
					ctx, cn := context.WithTimeout(ctx, 15*time.Second)
					defer cn()

					// we go ask all the sources and see who gets back to us first. whoever does is the winner!!
					for _, v := range sources {
						sourceFunc := v.GetRange
						go func(source persistence.BlockSource) {
							if _, ok := source.(*persistence.BeaconRpcSource); ok {
								time.Sleep(2 * time.Second)
								var blocks *peers.PeeredObject[[]*cltypes.SignedBeaconBlock]
							Loop:
								for {
									var err error
									from := args.seenSlot - 2
									currentSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
									count := (currentSlot - from) + 2
									if currentSlot <= cfg.forkChoice.HighestSeen() {
										time.Sleep(100 * time.Millisecond)
										continue
									}
									blocks, err = sourceFunc(ctx, nil, from, count)
									if err != nil {
										errCh <- err
										return
									}
									for _, block := range blocks.Data {
										if block.Block.Slot >= currentSlot {
											break Loop
										}
									}
								}
								respCh <- blocks
								return
							}
							blocks, err := sourceFunc(ctx, nil, args.seenSlot+1, totalRequest)
							if err != nil {
								errCh <- err
								return
							}
							respCh <- blocks
						}(v)
					}
					tx, err := cfg.indiciesDB.BeginRw(ctx)
					if err != nil {
						return err
					}
					defer tx.Rollback()

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
								if err := processBlock(tx, block, true, true); err != nil {
									log.Error("bad blocks segment received", "err", err)
									cfg.rpc.BanPeer(blocks.Peer)
									continue MainLoop
								}
								// we can ignore this error because the block would not process if the hashssz failed
								blockRoot, _ := block.HashSSZ()
								// publish block to event handler
								cfg.emitter.Publish("block", map[string]any{
									"slot":                 strconv.Itoa(int(block.Block.Slot)),
									"block":                common.Hash(blockRoot),
									"execution_optimistic": false, // TODO: i don't know what to put here. i see other places doing false, leaving flase for now
								})
								block.Block.Body.Attestations.Range(func(idx int, a *solid.Attestation, total int) bool {
									// emit attestation
									cfg.emitter.Publish("attestation", a)
									if err = cfg.forkChoice.OnAttestation(a, true, false); err != nil {
										log.Debug("bad attestation received", "err", err)
									}
									return true
								})
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
					return tx.Commit()
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
					return tx.Commit()
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
					// try to get the current block
					blocks, err := gossipSource.GetRange(ctx, tx, args.seenSlot, 1)
					if err != nil {
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return nil
						}
						return err
					}

					for _, block := range blocks.Data {
						err := processBlock(tx, block, true, true)
						if err != nil {
							// its okay if block processing fails
							logger.Warn("extra block failed validation", "err", err)
							return nil
						}
						shouldForkChoiceSinceReorg = true
						logger.Debug("extra block received", "slot", args.seenSlot)
					}
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
					// clean up some old ranges
					err = gossipSource.PurgeRange(ctx, tx, 1, args.seenSlot-cfg.beaconCfg.SlotsPerEpoch*16)
					if err != nil {
						return err
					}
					// TODO(Giulio2002): schedule snapshots retirement if needed.
					if !cfg.backfilling {
						if err := cfg.beaconDB.PurgeRange(ctx, tx, 1, cfg.forkChoice.HighestSeen()-100_000); err != nil {
							return err
						}
						if err := beacon_indicies.PruneBlockRoots(ctx, tx, 0, cfg.forkChoice.HighestSeen()-100_000); err != nil {
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
