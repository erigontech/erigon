package caplin1

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc/credentials"

	"golang.org/x/sync/semaphore"

	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloaderproto"
	"github.com/ledgerwatch/erigon/cl/aggregation"
	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/beacon"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/beacon/handler"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel"
	"github.com/ledgerwatch/erigon/cl/sentinel/service"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/ledgerwatch/erigon/cl/validator/attestation_producer"
	"github.com/ledgerwatch/erigon/cl/validator/committee_subscription"
	"github.com/ledgerwatch/erigon/cl/validator/sync_contribution_pool"
	"github.com/ledgerwatch/erigon/cl/validator/validator_params"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	"github.com/spf13/afero"

	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/blob_storage"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/persistence/state/historical_states_reader"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/phase1/network/services"
	"github.com/ledgerwatch/erigon/cl/phase1/stages"
	"github.com/ledgerwatch/erigon/cl/pool"

	"github.com/Giulio2002/bls"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/clparams"
)

func OpenCaplinDatabase(ctx context.Context,
	beaconConfig *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	dbPath string,
	blobDir string,
	engine execution_client.ExecutionEngine,
	wipeout bool,
	blobPruneDistance uint64,
) (kv.RwDB, blob_storage.BlobStorage, error) {
	dataDirIndexer := path.Join(dbPath, "beacon_indicies")
	blobDbPath := path.Join(blobDir, "chaindata")

	if wipeout {
		os.RemoveAll(dataDirIndexer)
		os.RemoveAll(blobDbPath)
	}

	os.MkdirAll(dbPath, 0700)
	os.MkdirAll(dataDirIndexer, 0700)

	db := mdbx.MustOpen(dataDirIndexer)
	blobDB := mdbx.MustOpen(blobDbPath)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	{ // start ticking forkChoice
		go func() {
			<-ctx.Done()
			db.Close()     // close sql database here
			blobDB.Close() // close blob database here
		}()
	}
	return db, blob_storage.NewBlobStore(blobDB, afero.NewBasePathFs(afero.NewOsFs(), blobDir), blobPruneDistance, beaconConfig, ethClock), nil
}

func RunCaplinPhase1(ctx context.Context, engine execution_client.ExecutionEngine, config *ethconfig.Config, networkConfig *clparams.NetworkConfig,
	beaconConfig *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock, state *state.CachingBeaconState, dirs datadir.Dirs, eth1Getter snapshot_format.ExecutionBlockReaderByNumber,
	snDownloader proto_downloader.DownloaderClient, backfilling, blobBackfilling bool, states bool, indexDB kv.RwDB, blobStorage blob_storage.BlobStorage, creds credentials.TransportCredentials, snBuildSema *semaphore.Weighted) error {
	ctx, cn := context.WithCancel(ctx)
	defer cn()

	logger := log.New("app", "caplin")

	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, beaconConfig, dirs, logger)
	rcsn := freezeblocks.NewBeaconSnapshotReader(csn, eth1Getter, beaconConfig)

	pool := pool.NewOperationsPool(beaconConfig)
	attestationProducer := attestation_producer.New(beaconConfig)

	caplinFcuPath := path.Join(dirs.Tmp, "caplin-forkchoice")
	os.RemoveAll(caplinFcuPath)
	err := os.MkdirAll(caplinFcuPath, 0o755)
	if err != nil {
		return err
	}
	fcuFs := afero.NewBasePathFs(afero.NewOsFs(), caplinFcuPath)
	syncedDataManager := synced_data.NewSyncedDataManager(true, beaconConfig)

	syncContributionPool := sync_contribution_pool.NewSyncContributionPool(beaconConfig)
	emitters := beaconevents.NewEmitters()
	aggregationPool := aggregation.NewAggregationPool(ctx, beaconConfig, networkConfig, ethClock)
	forkChoice, err := forkchoice.NewForkChoiceStore(ethClock, state, engine, pool, fork_graph.NewForkGraphDisk(state, fcuFs, config.BeaconRouter), emitters, syncedDataManager, blobStorage)
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

	forkDigest, err := ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}
	activeIndicies := state.GetActiveValidatorsIndices(state.Slot() / beaconConfig.SlotsPerEpoch)

	sentinel, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:         config.CaplinDiscoveryAddr,
		Port:           int(config.CaplinDiscoveryPort),
		TCPPort:        uint(config.CaplinDiscoveryTCPPort),
		NetworkConfig:  networkConfig,
		BeaconConfig:   beaconConfig,
		TmpDir:         dirs.Tmp,
		EnableBlocks:   true,
		ActiveIndicies: uint64(len(activeIndicies)),
	}, rcsn, blobStorage, indexDB, &service.ServerConfig{
		Network:   "tcp",
		Addr:      fmt.Sprintf("%s:%d", config.SentinelAddr, config.SentinelPort),
		Creds:     creds,
		Validator: config.BeaconRouter.Validator,
		InitialStatus: &cltypes.Status{
			ForkDigest:     forkDigest,
			FinalizedRoot:  state.FinalizedCheckpoint().BlockRoot(),
			FinalizedEpoch: state.FinalizedCheckpoint().Epoch(),
			HeadSlot:       state.FinalizedCheckpoint().Epoch() * beaconConfig.SlotsPerEpoch,
			HeadRoot:       state.FinalizedCheckpoint().BlockRoot(),
		},
	}, ethClock, forkChoice, logger)
	if err != nil {
		return err
	}
	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, ethClock)
	committeeSub := committee_subscription.NewCommitteeSubscribeManagement(ctx, indexDB, beaconConfig, networkConfig, ethClock, sentinel, state, aggregationPool, syncedDataManager)
	// Define gossip services
	blockService := services.NewBlockService(ctx, indexDB, forkChoice, syncedDataManager, ethClock, beaconConfig, emitters)
	blobService := services.NewBlobSidecarService(ctx, beaconConfig, forkChoice, syncedDataManager, ethClock, false)
	syncCommitteeMessagesService := services.NewSyncCommitteeMessagesService(beaconConfig, ethClock, syncedDataManager, syncContributionPool, false)
	attestationService := services.NewAttestationService(ctx, forkChoice, committeeSub, ethClock, syncedDataManager, beaconConfig, networkConfig)
	syncContributionService := services.NewSyncContributionService(syncedDataManager, beaconConfig, syncContributionPool, ethClock, emitters, false)
	aggregateAndProofService := services.NewAggregateAndProofService(ctx, syncedDataManager, forkChoice, beaconConfig, pool, false)
	voluntaryExitService := services.NewVoluntaryExitService(pool, emitters, syncedDataManager, beaconConfig, ethClock)
	blsToExecutionChangeService := services.NewBLSToExecutionChangeService(pool, emitters, syncedDataManager, beaconConfig)
	proposerSlashingService := services.NewProposerSlashingService(pool, syncedDataManager, beaconConfig, ethClock)
	// Create the gossip manager
	gossipManager := network.NewGossipReceiver(sentinel, forkChoice, beaconConfig, ethClock, emitters, committeeSub,
		blockService, blobService, syncCommitteeMessagesService, syncContributionService, aggregateAndProofService,
		attestationService, voluntaryExitService, blsToExecutionChangeService, proposerSlashingService)
	{ // start ticking forkChoice
		go func() {
			tickInterval := time.NewTicker(2 * time.Millisecond)
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

	{ // start the gossip manager
		go gossipManager.Start(ctx)
		logger.Info("Started Ethereum 2.0 Gossip Service")
	}

	{ // start logging peers
		go func() {
			logIntervalPeers := time.NewTicker(1 * time.Minute)
			for {
				select {
				case <-logIntervalPeers.C:
					if peerCount, err := beaconRpc.Peers(); err == nil {
						logger.Info("P2P", "peers", peerCount)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	tx, err := indexDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := state_accessors.InitializeStaticTables(tx, state); err != nil {
		return err
	}
	if err := beacon_indicies.WriteHighestFinalized(tx, 0); err != nil {
		return err
	}

	vTables := state_accessors.NewStaticValidatorTable()
	// Read the current table
	if states {
		if err := state_accessors.ReadValidatorsTable(tx, vTables); err != nil {
			return err
		}
	}
	// get the initial state
	genesisState, err := initial_state.GetGenesisState(clparams.NetworkType(beaconConfig.DepositNetworkID))
	if err != nil {
		return err
	}
	antiq := antiquary.NewAntiquary(ctx, blobStorage, genesisState, vTables, beaconConfig, dirs, snDownloader, indexDB, csn, rcsn, logger, states, backfilling, blobBackfilling, snBuildSema)
	// Create the antiquary
	go func() {
		if err := antiq.Loop(); err != nil {
			logger.Error("Antiquary failed", "err", err)
		}
	}()

	if err := tx.Commit(); err != nil {
		return err
	}

	statesReader := historical_states_reader.NewHistoricalStatesReader(beaconConfig, rcsn, vTables, genesisState)
	validatorParameters := validator_params.NewValidatorParams()
	if config.BeaconRouter.Active {
		apiHandler := handler.NewApiHandler(
			logger,
			networkConfig,
			ethClock,
			beaconConfig,
			indexDB,
			forkChoice,
			pool,
			rcsn,
			syncedDataManager,
			statesReader,
			sentinel,
			params.GitTag,
			&config.BeaconRouter,
			emitters,
			blobStorage,
			csn,
			validatorParameters,
			attestationProducer,
			engine,
			syncContributionPool,
			committeeSub,
			aggregationPool,
			syncCommitteeMessagesService,
			syncContributionService,
			aggregateAndProofService,
			attestationService,
			voluntaryExitService,
			blsToExecutionChangeService,
			proposerSlashingService,
		)
		go beacon.ListenAndServe(&beacon.LayeredBeaconHandler{
			ArchiveApi: apiHandler,
		}, config.BeaconRouter)
		log.Info("Beacon API started", "addr", config.BeaconRouter.Address)
	}

	stageCfg := stages.ClStagesCfg(beaconRpc, antiq, ethClock, beaconConfig, state, engine, gossipManager, forkChoice, indexDB, csn, rcsn, dirs.Tmp, uint64(config.LoopBlockLimit), backfilling, blobBackfilling, syncedDataManager, emitters, blobStorage, attestationProducer)
	sync := stages.ConsensusClStages(ctx, stageCfg)

	logger.Info("[Caplin] starting clstages loop")
	err = sync.StartWithStage(ctx, "DownloadHistoricalBlocks", logger, stageCfg)
	logger.Info("[Caplin] exiting clstages loop")
	if err != nil {
		return err
	}
	return err
}
