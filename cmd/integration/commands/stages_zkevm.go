package commands

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/c2h5oh/datasize"
	chain3 "github.com/gateway-fm/cdk-erigon-lib/chain"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/shards"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	stages3 "github.com/ledgerwatch/erigon/zk/stages"
)

func newSyncZk(ctx context.Context, db kv.RwDB) (consensus.Engine, *vm.Config, *stagedsync.Sync) {
	historyV3, pm := kvcfg.HistoryV3.FromDB(db), fromdb.PruneMode(db)

	vmConfig := &vm.Config{}

	var genesis *types.Genesis

	if strings.HasPrefix(chain, "dynamic") {
		if config == "" {
			panic("Config file is required for dynamic chain")
		}

		params.DynamicChainConfigPath = filepath.Dir(config)
		genesis = core.GenesisBlockByChainName(chain)
		filename := path.Join(params.DynamicChainConfigPath, chain+"-conf.json")

		dConf := utils.DynamicConfig{}

		if _, err := os.Stat(filename); err == nil {
			dConfBytes, err := os.ReadFile(filename)
			if err != nil {
				panic(err)
			}
			if err := json.Unmarshal(dConfBytes, &dConf); err != nil {
				panic(err)
			}
		}

		genesis.Timestamp = dConf.Timestamp
		genesis.GasLimit = dConf.GasLimit
		genesis.Difficulty = big.NewInt(dConf.Difficulty)
	} else {
		genesis = core.GenesisBlockByChainName(chain)
	}

	chainConfig, genesisBlock, genesisErr := core.CommitGenesisBlock(db, genesis, "")
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

	engine := initConsensusEngine(chainConfig, cfg.Dirs.DataDir, db)

	br := getBlockReader(db)
	allSn, agg := allSnapshots(ctx, db)
	cfg.Snapshot = allSn.Cfg()

	sentryControlServer, err := sentry.NewMultiClient(
		db,
		"",
		chainConfig,
		genesisBlock.Hash(),
		engine,
		1,
		nil,
		ethconfig.Defaults.Sync,
		br,
		false,
		nil,
		ethconfig.Defaults.DropUselessPeers,
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

	sync := stagedsync.New(stages, unwindOrder, stagedsync.DefaultPruneOrder)

	return engine, vmConfig, sync
}
