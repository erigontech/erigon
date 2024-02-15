package caplin1

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/beacon"
	"github.com/ledgerwatch/erigon/cl/beacon/beacon_router_configuration"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/beacon/handler"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/beacon/validatorapi"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/freezer"
	freezer2 "github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel"
	"github.com/ledgerwatch/erigon/cl/sentinel/service"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"google.golang.org/grpc/credentials"

	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/persistence/db_config"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/persistence/state/historical_states_reader"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/phase1/stages"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/spf13/afero"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/log/v3"
)

func OpenCaplinDatabase(ctx context.Context,
	databaseConfig db_config.DatabaseConfiguration,
	beaconConfig *clparams.BeaconChainConfig,
	dbPath string,
	engine execution_client.ExecutionEngine,
	wipeout bool,
) (kv.RwDB, error) {
	dataDirIndexer := path.Join(dbPath, "beacon_indicies")
	if wipeout {
		os.RemoveAll(dataDirIndexer)
	}

	os.MkdirAll(dbPath, 0700)

	db := mdbx.MustOpen(dataDirIndexer)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if err := db_config.WriteConfigurationIfNotExist(ctx, tx, databaseConfig); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	{ // start ticking forkChoice
		go func() {
			<-ctx.Done()
			db.Close() // close sql database here
		}()
	}
	return db, nil
}

func RunCaplinPhase1(ctx context.Context, engine execution_client.ExecutionEngine, config *ethconfig.Config, networkConfig *clparams.NetworkConfig,
	beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, state *state.CachingBeaconState,
	caplinFreezer freezer.Freezer, dirs datadir.Dirs, cfg beacon_router_configuration.RouterConfiguration, eth1Getter snapshot_format.ExecutionBlockReaderByNumber,
	snDownloader proto_downloader.DownloaderClient, backfilling bool, states bool, indexDB kv.RwDB, creds credentials.TransportCredentials) error {
	ctx, cn := context.WithCancel(ctx)
	defer cn()

	logger := log.New("app", "caplin")

	csn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{}, beaconConfig, dirs.Snap, logger)
	rcsn := freezeblocks.NewBeaconSnapshotReader(csn, eth1Getter, beaconConfig)

	if caplinFreezer != nil {
		if err := freezer2.PutObjectSSZIntoFreezer("beaconState", "caplin_core", 0, state, caplinFreezer); err != nil {
			return err
		}
	}

	pool := pool.NewOperationsPool(beaconConfig)

	caplinFcuPath := path.Join(dirs.Tmp, "caplin-forkchoice")
	os.RemoveAll(caplinFcuPath)
	err := os.MkdirAll(caplinFcuPath, 0o755)
	if err != nil {
		return err
	}
	fcuFs := afero.NewBasePathFs(afero.NewOsFs(), caplinFcuPath)

	emitters := beaconevents.NewEmitters()
	forkChoice, err := forkchoice.NewForkChoiceStore(ctx, state, engine, caplinFreezer, pool, fork_graph.NewForkGraphDisk(state, fcuFs), emitters)
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

	forkDigest, err := fork.ComputeForkDigest(beaconConfig, genesisConfig)
	if err != nil {
		return err
	}

	sentinel, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:        config.LightClientDiscoveryAddr,
		Port:          int(config.LightClientDiscoveryPort),
		TCPPort:       uint(config.LightClientDiscoveryTCPPort),
		GenesisConfig: genesisConfig,
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		TmpDir:        dirs.Tmp,
		EnableBlocks:  true,
	}, rcsn, indexDB, &service.ServerConfig{Network: "tcp", Addr: fmt.Sprintf("%s:%d", config.SentinelAddr, config.SentinelPort)}, creds, &cltypes.Status{
		ForkDigest:     forkDigest,
		FinalizedRoot:  state.FinalizedCheckpoint().BlockRoot(),
		FinalizedEpoch: state.FinalizedCheckpoint().Epoch(),
		HeadSlot:       state.FinalizedCheckpoint().Epoch() * beaconConfig.SlotsPerEpoch,
		HeadRoot:       state.FinalizedCheckpoint().BlockRoot(),
	}, forkChoice, logger)
	if err != nil {
		return err
	}

	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, genesisConfig)
	gossipSource := persistence.NewGossipSource(ctx)

	gossipManager := network.NewGossipReceiver(sentinel, forkChoice, beaconConfig, genesisConfig, caplinFreezer, emitters, gossipSource)
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

	dbConfig, err := db_config.ReadConfiguration(ctx, tx)
	if err != nil {
		return err
	}

	if err := state_accessors.InitializeStaticTables(tx, state); err != nil {
		return err
	}
	if err := beacon_indicies.WriteHighestFinalized(tx, 0); err != nil {
		return err
	}

	vTables := state_accessors.NewStaticValidatorTable()
	// Read the the current table
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
	af := afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinHistory)
	antiq := antiquary.NewAntiquary(ctx, genesisState, vTables, beaconConfig, dirs, snDownloader, indexDB, csn, rcsn, logger, states, backfilling, af)
	// Create the antiquary
	go func() {
		if err := antiq.Loop(); err != nil {
			logger.Error("Antiquary failed", "err", err)
		}
	}()

	if err := tx.Commit(); err != nil {
		return err
	}

	statesReader := historical_states_reader.NewHistoricalStatesReader(beaconConfig, rcsn, vTables, af, genesisState)
	syncedDataManager := synced_data.NewSyncedDataManager(cfg.Active, beaconConfig)
	if cfg.Active {
		apiHandler := handler.NewApiHandler(genesisConfig, beaconConfig, indexDB, forkChoice, pool, rcsn, syncedDataManager, statesReader, sentinel, params.GitTag)
		headApiHandler := &validatorapi.ValidatorApiHandler{
			FC:             forkChoice,
			BeaconChainCfg: beaconConfig,
			GenesisCfg:     genesisConfig,
			Emitters:       emitters,
		}
		go beacon.ListenAndServe(&beacon.LayeredBeaconHandler{
			ValidatorApi: headApiHandler,
			ArchiveApi:   apiHandler,
		}, cfg)
		log.Info("Beacon API started", "addr", cfg.Address)
	}

	forkChoice.StartAttestationsRTT()

	stageCfg := stages.ClStagesCfg(beaconRpc, antiq, genesisConfig, beaconConfig, state, engine, gossipManager, forkChoice, indexDB, csn, dirs.Tmp, dbConfig, backfilling, syncedDataManager, emitters, gossipSource)
	sync := stages.ConsensusClStages(ctx, stageCfg)

	logger.Info("[Caplin] starting clstages loop")
	err = sync.StartWithStage(ctx, "DownloadHistoricalBlocks", logger, stageCfg)
	logger.Info("[Caplin] exiting clstages loop")
	if err != nil {
		return err
	}
	return err
}
