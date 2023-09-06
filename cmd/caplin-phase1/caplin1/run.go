package caplin1

import (
	"context"
	"database/sql"
	"os"
	"path"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon/cl/beacon"
	"github.com/ledgerwatch/erigon/cl/beacon/handler"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/db_config"
	"github.com/ledgerwatch/erigon/cl/persistence/sql_migrations"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/phase1/stages"
	"github.com/spf13/afero"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/log/v3"
)

func RunCaplinPhase1(ctx context.Context, sentinel sentinel.SentinelClient,
	beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig,
	engine execution_client.ExecutionEngine, state *state.CachingBeaconState,
	caplinFreezer freezer.Freezer, dirs datadir.Dirs, cfg beacon.RouterConfiguration) error {
	ctx, cn := context.WithCancel(ctx)
	defer cn()

	database_config := db_config.DefaultDatabaseConfiguration

	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, genesisConfig)

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
	gossipManager := network.NewGossipReceiver(sentinel, forkChoice, beaconConfig, genesisConfig, caplinFreezer)
	dataDirFs := afero.NewBasePathFs(afero.NewOsFs(), dirs.DataDir)
	dataDirIndexer := path.Join(dirs.DataDir, "beacon_indicies")

	os.Remove(dataDirIndexer)
	db, err := sql.Open("sqlite", dataDirIndexer)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := sql_migrations.ApplyMigrations(ctx, tx); err != nil {
		return err
	}
	if err := db_config.WriteConfigurationIfNotExist(ctx, tx, database_config); err != nil {
		return err
	}
	var haveDatabaseConfig db_config.DatabaseConfiguration
	if haveDatabaseConfig, err = db_config.ReadConfiguration(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	logger.Info("Caplin Pruning",
		"pruning provided", database_config.PruneDepth, "effective pruning", haveDatabaseConfig.PruneDepth,
		"fullBlocks", haveDatabaseConfig.FullBlocks)
	logger.Info("Disclaimer: This Caplin will run with database parameters with which it was started the first time, e.g pruning.")
	{ // start ticking forkChoice
		go func() {
			tickInterval := time.NewTicker(50 * time.Millisecond)
			for {
				select {
				case <-tickInterval.C:
					forkChoice.OnTick(uint64(time.Now().Unix()))
				case <-ctx.Done():
					db.Close() // close sql database here
					return
				}
			}
		}()
	}
	beaconDB := persistence.NewBeaconChainDatabaseFilesystem(afero.NewBasePathFs(dataDirFs, dirs.DataDir), engine, haveDatabaseConfig.FullBlocks, beaconConfig, db)

	if cfg.Active {
		apiHandler := handler.NewApiHandler(genesisConfig, beaconConfig, beaconDB, db, forkChoice)
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

	stageCfg := stages.ClStagesCfg(beaconRpc, genesisConfig, beaconConfig, state, engine, gossipManager, forkChoice, beaconDB, dirs, db, haveDatabaseConfig)
	sync := stages.ConsensusClStages(ctx, stageCfg)

	logger.Info("[Caplin] starting clstages loop")
	err = sync.StartWithStage(ctx, "WaitForPeers", logger, stageCfg)
	logger.Info("[Caplin] exiting clstages loop")
	if err != nil {
		return err
	}
	return err
}
