package caplin1

import (
	"context"
	"database/sql"
	"os"
	"path"
	"time"

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
	"github.com/ledgerwatch/erigon/cl/pool"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/log/v3"
)

func OpenCaplinDatabase(ctx context.Context,
	databaseConfig db_config.DatabaseConfiguration,
	beaconConfig *clparams.BeaconChainConfig,
	rawBeaconChain persistence.RawBeaconBlockChain,
	dbPath string,
	engine execution_client.ExecutionEngine,
) (persistence.BeaconChainDatabase, *sql.DB, error) {
	dataDirIndexer := path.Join(dbPath, "beacon_indicies")
	os.Remove(dataDirIndexer)
	os.MkdirAll(dbPath, 0700)

	db, err := sql.Open("sqlite", dataDirIndexer)
	if err != nil {
		return nil, nil, err
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	if err := sql_migrations.ApplyMigrations(ctx, tx); err != nil {
		return nil, nil, err
	}
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
	return persistence.NewBeaconChainDatabaseFilesystem(rawBeaconChain, engine, beaconConfig), db, nil
}

func RunCaplinPhase1(ctx context.Context, sentinel sentinel.SentinelClient, engine execution_client.ExecutionEngine,
	beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, state *state.CachingBeaconState,
	caplinFreezer freezer.Freezer, db *sql.DB, rawDB persistence.RawBeaconBlockChain, beaconDB persistence.BeaconChainDatabase, tmpdir string, cfg beacon.RouterConfiguration) error {
	ctx, cn := context.WithCancel(ctx)
	defer cn()

	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, genesisConfig)

	logger := log.New("app", "caplin")

	if caplinFreezer != nil {
		if err := freezer.PutObjectSSZIntoFreezer("beaconState", "caplin_core", 0, state, caplinFreezer); err != nil {
			return err
		}
	}
	pool := pool.NewOperationsPool(beaconConfig)

	forkChoice, err := forkchoice.NewForkChoiceStore(ctx, state, engine, caplinFreezer, pool, true)
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

	if cfg.Active {
		apiHandler := handler.NewApiHandler(genesisConfig, beaconConfig, rawDB, db, forkChoice, pool)
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

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	dbConfig, err := db_config.ReadConfiguration(ctx, tx)
	if err != nil {
		return err
	}
	tx.Rollback()

	stageCfg := stages.ClStagesCfg(beaconRpc, genesisConfig, beaconConfig, state, engine, gossipManager, forkChoice, beaconDB, db, tmpdir, dbConfig)
	sync := stages.ConsensusClStages(ctx, stageCfg)

	logger.Info("[Caplin] starting clstages loop")
	err = sync.StartWithStage(ctx, "WaitForPeers", logger, stageCfg)
	logger.Info("[Caplin] exiting clstages loop")
	if err != nil {
		return err
	}
	return err
}
