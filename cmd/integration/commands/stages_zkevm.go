package commands

import (
	"context"
	"math/big"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/zk/zk_config"
	"github.com/erigontech/erigon/zk/zk_config/cfg_dynamic_genesis"

	"github.com/c2h5oh/datasize"
	chain3 "github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcfg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/turbo/shards"
	stages2 "github.com/erigontech/erigon/turbo/stages"
	"github.com/erigontech/erigon/zk/sequencer"
	stages3 "github.com/erigontech/erigon/zk/stages"
)

func newSyncZk(ctx context.Context, db kv.RwDB) (consensus.Engine, *vm.Config, *stagedsync.Sync) {
	historyV3, pm := kvcfg.HistoryV3.FromDB(db), fromdb.PruneMode(db)

	vmConfig := &vm.Config{}

	var genesis *types.Genesis

	if strings.HasPrefix(chain, "dynamic") {
		if config == "" {
			panic("Config file is required for dynamic chain")
		}
		zk_config.ZKDynamicConfigPath = filepath.Dir(config)

		genesis = core.GenesisBlockByChainName(chain)

		dConf := cfg_dynamic_genesis.NewDynamicGenesisConfig(chain)

		genesis.Timestamp = dConf.Timestamp
		genesis.GasLimit = dConf.GasLimit
		genesis.Difficulty = big.NewInt(dConf.Difficulty)
	} else {
		genesis = core.GenesisBlockByChainName(chain)
	}

	chainConfig, genesisBlock, genesisErr := core.CommitGenesisBlock(db, genesis, "", log.New())
	if _, ok := genesisErr.(*chain3.ConfigCompatError); genesisErr != nil && !ok {
		panic(genesisErr)
	}
	//log.Info("Initialised chain configuration", "config", chainConfig)

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	cfg := ethconfig.Defaults
	cfg.HistoryV3 = historyV3
	cfg.Prune = pm
	cfg.BatchSize = batchSize
	cfg.DeprecatedTxPool.Disable = true
	cfg.Genesis = core.GenesisBlockByChainName(chain)
	cfg.Dirs = datadir.New(datadirCli)

	logger := log.New()
	br, _ := blocksIO(db, logger)
	engine, _ := initConsensusEngine(ctx, chainConfig, cfg.Dirs.DataDir, db, br, logger)
	allSn, _, agg := allSnapshots(ctx, db, logger)
	cfg.Snapshot = allSn.Cfg()

	statusDataProvider := sentry.NewStatusDataProvider(
		db,
		chainConfig,
		genesisBlock,
		chainConfig.ChainID.Uint64(),
		logger,
	)

	maxBlockBroadcastPeers := func(header *types.Header) uint { return 0 }

	sentryControlServer, err := sentry_multi_client.NewMultiClient(
		db,
		chainConfig,
		engine,
		nil,
		ethconfig.Defaults.Sync,
		br,
		blockBufferSize,
		statusDataProvider,
		false,
		maxBlockBroadcastPeers,
		false,
		logger,
	)
	if err != nil {
		panic(err)
	}

	isSequencer := sequencer.IsSequencer()
	var stages []*stagedsync.Stage

	if isSequencer {
		stages = stages2.NewSequencerZkStages(
			context.Background(),
			db,
			&cfg,
			sentryControlServer,
			&shards.Notifications{},
			nil,
			allSn,
			agg,
			nil,
			engine,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
	} else {
		stages = stages2.NewDefaultZkStages(
			context.Background(),
			db,
			&cfg,
			sentryControlServer,
			&shards.Notifications{},
			nil,
			allSn,
			agg,
			nil,
			engine,
			nil,
			nil,
			nil,
			nil)
	}

	// set the unwind order depending on whether sequencer or synchronizer (ensure to set ENV VAR!)
	unwindOrder := stages3.ZkUnwindOrder
	if isSequencer {
		unwindOrder = stages3.ZkSequencerUnwindOrder
	}

	sync := stagedsync.New(cfg.Sync, stages, unwindOrder, stagedsync.DefaultPruneOrder, logger)

	return engine, vmConfig, sync
}
