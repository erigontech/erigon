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

package caplin1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"time"

	"github.com/spf13/afero"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/credentials"

	"github.com/erigontech/erigon-lib/common/dir"
	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/beacon"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/handler"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/cl/persistence/genesisdb"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/phase1/stages"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/cl/sentinel/service"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/cl/validator/committee_subscription"
	"github.com/erigontech/erigon/cl/validator/sync_contribution_pool"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/eth/ethconfig"
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
		dir.RemoveAll(dataDirIndexer)
		dir.RemoveAll(blobDbPath)
	}

	os.MkdirAll(dbPath, 0700)
	os.MkdirAll(dataDirIndexer, 0700)

	db := mdbx.New(kv.CaplinDB, log.New()).Path(dbPath).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { //TODO: move Caplin tables to own tables cofig
			return kv.ChaindataTablesCfg
		}).MustOpen()
	blobDB := mdbx.New(kv.CaplinDB, log.New()).Path(blobDbPath).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
			return kv.ChaindataTablesCfg
		}).MustOpen()

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
			db.Close()
			blobDB.Close() // close blob database here
		}()
	}
	return db, blob_storage.NewBlobStore(blobDB, afero.NewBasePathFs(afero.NewOsFs(), blobDir), blobPruneDistance, beaconConfig, ethClock), nil
}

func RunCaplinService(ctx context.Context, engine execution_client.ExecutionEngine, config clparams.CaplinConfig,
	dirs datadir.Dirs, eth1Getter snapshot_format.ExecutionBlockReaderByNumber,
	snDownloader proto_downloader.DownloaderClient, creds credentials.TransportCredentials, snBuildSema *semaphore.Weighted) error {

	var networkConfig *clparams.NetworkConfig
	var beaconConfig *clparams.BeaconChainConfig

	var err error

	var genesisState *state.CachingBeaconState
	var genesisDb genesisdb.GenesisDB

	if config.IsDevnet() {
		config.NetworkId = clparams.CustomNetwork // Force custom network
		if config.HaveInvalidDevnetParams() {
			return errors.New("devnet config and genesis state paths must be set together")
		}
		customBeaconCfg, customNetworkCfg, err := clparams.CustomConfig(config.CustomConfigPath)
		if err != nil {
			return err
		}
		beaconConfig = &customBeaconCfg
		networkConfig = &customNetworkCfg
		genesisDb = genesisdb.NewGenesisDB(beaconConfig, dirs.CaplinGenesis)
		stateBytes, err := os.ReadFile(config.CustomGenesisStatePath)
		if err != nil {
			return fmt.Errorf("could not read provided genesis state file: %s", err)
		}
		genesisState = state.New(beaconConfig)

		if err := genesisState.DecodeSSZ(stateBytes, int(beaconConfig.GetCurrentStateVersion(beaconConfig.GenesisEpoch))); err != nil {
			return fmt.Errorf("could not decode genesis state: %s", err)
		}
	} else {
		networkConfig, beaconConfig = clparams.GetConfigsByNetwork(config.NetworkId)
		genesisDb = genesisdb.NewGenesisDB(beaconConfig, dirs.CaplinGenesis)

		isGenesisDBInitialized, err := genesisDb.IsInitialized()
		if err != nil {
			return err
		}

		// If genesis state is provided and is hardcoded, use it
		if initial_state.IsGenesisStateSupported(config.NetworkId) && !isGenesisDBInitialized {
			genesisState, err = initial_state.GetGenesisState(config.NetworkId)
			if err != nil {
				return err
			}
		}
	}

	// init the current beacon config for global access
	clparams.InitGlobalStaticConfig(beaconConfig, &config)

	if config.NetworkId == clparams.CustomNetwork {
		config.NetworkId = clparams.NetworkType(beaconConfig.DepositNetworkID)
	}

	if len(config.BootstrapNodes) > 0 {
		networkConfig.BootNodes = config.BootstrapNodes
	}

	if len(config.StaticPeers) > 0 {
		networkConfig.StaticPeers = config.StaticPeers
	}
	if genesisState != nil {
		genesisDb.Initialize(genesisState)
	} else {
		genesisState, err = genesisDb.ReadGenesisState()
		if err != nil {
			return err
		}
	}

	state, err := checkpoint_sync.ReadOrFetchLatestBeaconState(ctx, dirs, beaconConfig, config, genesisDb)
	if err != nil {
		return err
	}
	ethClock := eth_clock.NewEthereumClock(state.GenesisTime(), state.GenesisValidatorsRoot(), beaconConfig)

	pruneBlobDistance := uint64(128600)
	if config.ArchiveBlobs || config.BlobPruningDisabled {
		pruneBlobDistance = math.MaxUint64
	}

	indexDB, blobStorage, err := OpenCaplinDatabase(ctx, beaconConfig, ethClock, dirs.CaplinIndexing, dirs.CaplinBlobs, engine, false, pruneBlobDistance)
	if err != nil {
		return err
	}

	caplinOptions := []CaplinOption{}
	if config.BeaconAPIRouter.Builder {
		if config.RelayUrlExist() {
			caplinOptions = append(caplinOptions, WithBuilder(config.MevRelayUrl, beaconConfig))
		} else {
			log.Warn("builder api enable but relay url not set. Skipping builder mode")
			config.BeaconAPIRouter.Builder = false
		}
	}
	log.Info("Starting caplin")

	if eth1Getter != nil {
		eth1Getter.SetBeaconChainConfig(beaconConfig)
	}

	ctx, cn := context.WithCancel(ctx)
	defer cn()

	option := &option{}
	for _, opt := range caplinOptions {
		opt(option)
	}

	logger := log.New("app", "caplin")

	config.NetworkId = clparams.CustomNetwork // Force custom network
	freezeCfg := ethconfig.Defaults.Snapshot
	freezeCfg.ChainName = beaconConfig.ConfigName
	csn := freezeblocks.NewCaplinSnapshots(freezeCfg, beaconConfig, dirs, logger)
	rcsn := freezeblocks.NewBeaconSnapshotReader(csn, eth1Getter, beaconConfig)

	pool := pool.NewOperationsPool(beaconConfig)
	attestationProducer := attestation_producer.New(ctx, beaconConfig)

	caplinFcuPath := path.Join(dirs.Tmp, "caplin-forkchoice")
	dir.RemoveAll(caplinFcuPath)
	err = os.MkdirAll(caplinFcuPath, 0o755)
	if err != nil {
		return err
	}
	fcuFs := afero.NewBasePathFs(afero.NewOsFs(), caplinFcuPath)
	syncedDataManager := synced_data.NewSyncedDataManager(beaconConfig, true)

	syncContributionPool := sync_contribution_pool.NewSyncContributionPool(beaconConfig)
	emitters := beaconevents.NewEventEmitter()
	aggregationPool := aggregation.NewAggregationPool(ctx, beaconConfig, networkConfig, ethClock)
	doLMDSampling := len(state.GetActiveValidatorsIndices(state.Slot()/beaconConfig.SlotsPerEpoch)) >= 20_000

	// create the public keys registry
	pksRegistry := public_keys_registry.NewHeadViewPublicKeysRegistry(syncedDataManager)
	validatorParameters := validator_params.NewValidatorParams()
	forkChoice, err := forkchoice.NewForkChoiceStore(
		ethClock, state, engine, pool, fork_graph.NewForkGraphDisk(state, syncedDataManager, fcuFs, config.BeaconAPIRouter, emitters),
		emitters, syncedDataManager, blobStorage, pksRegistry, validatorParameters, doLMDSampling)
	if err != nil {
		logger.Error("Could not create forkchoice", "err", err)
		return err
	}
	bls.SetEnabledCaching(true)

	forkDigest, err := ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}
	activeIndicies := state.GetActiveValidatorsIndices(state.Slot() / beaconConfig.SlotsPerEpoch)

	peerDasState := peerdasstate.NewPeerDasState(beaconConfig, networkConfig)
	columnStorage := blob_storage.NewDataColumnStore(afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinColumnData), pruneBlobDistance, beaconConfig, ethClock, emitters)
	sentinel, localNode, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:                       config.CaplinDiscoveryAddr,
		Port:                         int(config.CaplinDiscoveryPort),
		TCPPort:                      uint(config.CaplinDiscoveryTCPPort),
		EnableUPnP:                   config.EnableUPnP,
		MaxInboundTrafficPerPeer:     config.MaxInboundTrafficPerPeer,
		MaxOutboundTrafficPerPeer:    config.MaxOutboundTrafficPerPeer,
		AdaptableTrafficRequirements: config.AdptableTrafficRequirements,
		SubscribeAllTopics:           config.SubscribeAllTopics,
		NetworkConfig:                networkConfig,
		BeaconConfig:                 beaconConfig,
		TmpDir:                       dirs.Tmp,
		EnableBlocks:                 true,
		ActiveIndicies:               uint64(len(activeIndicies)),
		MaxPeerCount:                 config.MaxPeerCount,
	}, rcsn, blobStorage, indexDB, &service.ServerConfig{
		Network: "tcp",
		Addr:    fmt.Sprintf("%s:%d", config.SentinelAddr, config.SentinelPort),
		Creds:   creds,
		InitialStatus: &cltypes.Status{
			ForkDigest:     forkDigest,
			FinalizedRoot:  state.FinalizedCheckpoint().Root,
			FinalizedEpoch: state.FinalizedCheckpoint().Epoch,
			HeadSlot:       state.FinalizedCheckpoint().Epoch * beaconConfig.SlotsPerEpoch,
			HeadRoot:       state.FinalizedCheckpoint().Root,
		},
	}, ethClock, forkChoice, columnStorage, peerDasState, logger)
	if err != nil {
		return err
	}
	peerDasState.SetLocalNodeID(localNode)
	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, ethClock, state)
	peerDas := das.NewPeerDas(ctx, beaconRpc, beaconConfig, &config, columnStorage, blobStorage, sentinel, localNode.ID(), ethClock, peerDasState)
	forkChoice.InitPeerDas(peerDas) // hack init
	committeeSub := committee_subscription.NewCommitteeSubscribeManagement(ctx, indexDB, beaconConfig, networkConfig, ethClock, sentinel, aggregationPool, syncedDataManager)
	batchSignatureVerifier := services.NewBatchSignatureVerifier(ctx, sentinel)
	// Define gossip services
	blockService := services.NewBlockService(ctx, indexDB, forkChoice, syncedDataManager, ethClock, beaconConfig, emitters)
	blobService := services.NewBlobSidecarService(ctx, beaconConfig, forkChoice, syncedDataManager, ethClock, emitters, false)
	dataColumnSidecarService := services.NewDataColumnSidecarService(beaconConfig, ethClock, forkChoice, syncedDataManager, columnStorage, emitters)
	syncCommitteeMessagesService := services.NewSyncCommitteeMessagesService(beaconConfig, ethClock, syncedDataManager, syncContributionPool, batchSignatureVerifier, false)
	attestationService := services.NewAttestationService(ctx, forkChoice, committeeSub, ethClock, syncedDataManager, beaconConfig, networkConfig, emitters, batchSignatureVerifier)
	syncContributionService := services.NewSyncContributionService(syncedDataManager, beaconConfig, syncContributionPool, ethClock, emitters, batchSignatureVerifier, false)
	aggregateAndProofService := services.NewAggregateAndProofService(ctx, syncedDataManager, forkChoice, beaconConfig, pool, false, batchSignatureVerifier)
	voluntaryExitService := services.NewVoluntaryExitService(pool, emitters, syncedDataManager, beaconConfig, ethClock, batchSignatureVerifier)
	blsToExecutionChangeService := services.NewBLSToExecutionChangeService(pool, emitters, syncedDataManager, beaconConfig, batchSignatureVerifier)
	proposerSlashingService := services.NewProposerSlashingService(pool, syncedDataManager, beaconConfig, ethClock, emitters)

	{
		go batchSignatureVerifier.Start()
	}

	// Create the gossip manager
	gossipManager := network.NewGossipReceiver(sentinel, forkChoice, beaconConfig, networkConfig, ethClock, emitters, committeeSub,
		blockService, blobService, dataColumnSidecarService, syncCommitteeMessagesService, syncContributionService, aggregateAndProofService,
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
					} else {
						logger.Error("P2P", "err", err)
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

	if err := beacon_indicies.WriteHighestFinalized(tx, 0); err != nil {
		return err
	}

	vTables := state_accessors.NewStaticValidatorTable()
	// Read the current table
	if config.ArchiveStates {
		if err := state_accessors.ReadValidatorsTable(tx, vTables); err != nil {
			return err
		}
	}
	stateSnapshots := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: beaconConfig.ConfigName}, beaconConfig, dirs, snapshotsync.MakeCaplinStateSnapshotsTypes(indexDB), logger)
	antiq := antiquary.NewAntiquary(ctx, blobStorage, genesisState, vTables, beaconConfig, dirs, snDownloader, indexDB, stateSnapshots, csn, rcsn, syncedDataManager, logger, config.ArchiveStates, config.ArchiveBlocks, config.ArchiveBlobs, config.SnapshotGenerationEnabled, snBuildSema)
	// Create the antiquary
	go func() {
		keepGoing := true
		for keepGoing {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				if err := antiq.Loop(); err != nil {
					logger.Debug("Antiquary failed", "err", err)
				} else {
					keepGoing = false
				}
			}
		}
	}()

	if err := tx.Commit(); err != nil {
		return err
	}

	statesReader := historical_states_reader.NewHistoricalStatesReader(beaconConfig, rcsn, vTables, genesisState, stateSnapshots, syncedDataManager)
	if config.BeaconAPIRouter.Active {
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
			version.GitTag,
			&config.BeaconAPIRouter,
			emitters,
			blobStorage,
			columnStorage,
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
			option.builderClient,
			stateSnapshots,
			true,
		)
		go beacon.ListenAndServe(&beacon.LayeredBeaconHandler{
			ArchiveApi: apiHandler,
		}, config.BeaconAPIRouter)
		log.Info("Beacon API started", "addr", config.BeaconAPIRouter.Address)
	}

	stageCfg := stages.ClStagesCfg(
		beaconRpc,
		antiq,
		ethClock,
		beaconConfig,
		state,
		engine,
		gossipManager,
		forkChoice,
		indexDB,
		csn,
		rcsn,
		dirs,
		config.LoopBlockLimit,
		config,
		syncedDataManager,
		emitters,
		blobStorage,
		attestationProducer,
		peerDas,
	)
	sync := stages.ConsensusClStages(ctx, stageCfg)

	logger.Info("[Caplin] starting clstages loop")
	err = sync.StartWithStage(ctx, "DownloadHistoricalBlocks", logger, stageCfg)
	logger.Info("[Caplin] exiting clstages loop")
	if err != nil {
		return err
	}
	return err
}
