package caplin1

import (
	"context"
	"os"
	"path"
	"time"

	"github.com/ledgerwatch/erigon/cl/beacon"
	"github.com/ledgerwatch/erigon/cl/beacon/handler"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	freezer2 "github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/persistence"
	persistence2 "github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/db_config"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/phase1/stages"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/spf13/afero"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/log/v3"
)

func OpenCaplinDatabase(ctx context.Context,
	databaseConfig db_config.DatabaseConfiguration,
	beaconConfig *clparams.BeaconChainConfig,
	rawBeaconChain persistence2.RawBeaconBlockChain,
	dbPath string,
	engine execution_client.ExecutionEngine,
	wipeout bool,
) (persistence.BeaconChainDatabase, kv.RwDB, error) {
	dataDirIndexer := path.Join(dbPath, "beacon_indicies")
	if wipeout {
		os.RemoveAll(dataDirIndexer)
	}

	os.MkdirAll(dbPath, 0700)

	db := mdbx.MustOpen(dataDirIndexer)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	if err := db_config.WriteConfigurationIfNotExist(ctx, tx, databaseConfig); err != nil {
		return nil, nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	{ // start ticking forkChoice
		go func() {
			<-ctx.Done()
			db.Close() // close sql database here
		}()
	}
	return persistence2.NewBeaconChainDatabaseFilesystem(rawBeaconChain, engine, beaconConfig), db, nil
}

func RunCaplinPhase1(ctx context.Context, sentinel sentinel.SentinelClient, engine execution_client.ExecutionEngine,
	beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, state *state.CachingBeaconState,
	caplinFreezer freezer.Freezer, dirs datadir.Dirs, cfg beacon.RouterConfiguration) error {
	rawDB := persistence.AferoRawBeaconBlockChainFromOsPath(beaconConfig, dirs.CaplinHistory)
	beaconDB, sqlDB, err := OpenCaplinDatabase(ctx, db_config.DefaultDatabaseConfiguration, beaconConfig, rawDB, dirs.CaplinIndexing, engine, true)
	if err != nil {
		return err
	}
	ctx, cn := context.WithCancel(ctx)
	defer cn()

	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, genesisConfig)

	logger := log.New("app", "caplin")

	if caplinFreezer != nil {
		if err := freezer2.PutObjectSSZIntoFreezer("beaconState", "caplin_core", 0, state, caplinFreezer); err != nil {
			return err
		}
	}
	pool := pool.NewOperationsPool(beaconConfig)

	caplinFcuPath := path.Join(dirs.Tmp, "caplin-forkchoice")
	os.RemoveAll(caplinFcuPath)
	err = os.MkdirAll(caplinFcuPath, 0o755)
	if err != nil {
		return err
	}
	fcuFs := afero.NewBasePathFs(afero.NewOsFs(), caplinFcuPath)

	forkChoice, err := forkchoice.NewForkChoiceStore(ctx, state, engine, caplinFreezer, pool, fork_graph.NewForkGraphDisk(state, fcuFs))
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
	gossipManager := network.NewGossipReceiver(sentinel, forkChoice, beaconConfig, genesisConfig, caplinFreezer)
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

	if cfg.Active {
		apiHandler := handler.NewApiHandler(genesisConfig, beaconConfig, rawDB, sqlDB, forkChoice, pool)
		go beacon.ListenAndServe(apiHandler, &cfg)
		log.Info("Beacon API started", "addr", cfg.Address)
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

	tx, err := sqlDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	dbConfig, err := db_config.ReadConfiguration(ctx, tx)
	if err != nil {
		return err
	}
	tx.Rollback()

	stageCfg := stages.ClStagesCfg(beaconRpc, genesisConfig, beaconConfig, state, engine, gossipManager, forkChoice, beaconDB, sqlDB, dirs.Tmp, dbConfig)
	sync := stages.ConsensusClStages(ctx, stageCfg)

	logger.Info("[Caplin] starting clstages loop")
	err = sync.StartWithStage(ctx, "WaitForPeers", logger, stageCfg)
	logger.Info("[Caplin] exiting clstages loop")
	if err != nil {
		return err
	}
	return err
}
