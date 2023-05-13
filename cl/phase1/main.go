package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon/cl/phase1/core"
	rawdb2 "github.com/ledgerwatch/erigon/cl/phase1/core/rawdb"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"
	stages2 "github.com/ledgerwatch/erigon/cl/phase1/stages"
	"os"

	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/rpc"
	lcCli "github.com/ledgerwatch/erigon/cmd/sentinel/cli"

	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/service"
	sentinelapp "github.com/ledgerwatch/erigon/turbo/app"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

func main() {
	app := sentinelapp.MakeApp("erigon-cl", runConsensusLayerNode, flags.CLDefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runConsensusLayerNode(cliCtx *cli.Context) error {
	var logger log.Logger
	var err error
	if logger, err = debug.Setup(cliCtx, true /* root logger */); err != nil {
		return err
	}

	ctx := context.Background()
	cfg, _ := lcCli.SetupConsensusClientCfg(cliCtx)
	var db kv.RwDB
	if cfg.Chaindata == "" {
		db, err = mdbx.NewTemporaryMdbx()
	} else {
		db, err = mdbx.Open(cfg.Chaindata, log.Root(), false)
	}
	if err != nil {
		log.Error("Error opening database", "err", err)
	}
	defer db.Close()
	if err := checkAndStoreBeaconDataConfigWithDB(ctx, db, cfg.BeaconDataCfg); err != nil {
		log.Error("Could load beacon data configuration", "err", err)
		return err
	}

	tmpdir := "/tmp"
	executionClient, err := execution_client.NewExecutionClient(ctx, "127.0.0.1:8989")
	if err != nil {
		log.Warn("Could not connect to execution client", "err", err)
		return err
	}

	if cfg.TransitionChain {
		state, err := initial_state.GetGenesisState(cfg.NetworkType)
		if err != nil {
			return err
		}
		// Execute from genesis to whatever we have.
		return stages2.SpawnStageBeaconState(stages2.StageBeaconState(db, cfg.BeaconCfg, state, executionClient), nil, ctx)
	}

	// Fetch the checkpoint state.
	cpState, err := getCheckpointState(ctx, db, cfg.BeaconCfg, cfg.GenesisCfg, cfg.CheckpointUri)
	if err != nil {
		log.Error("Could not get checkpoint", "err", err)
		return err
	}

	log.Info("Starting sync from checkpoint.")
	// Start the sentinel service
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(cfg.LogLvl), log.StderrHandler))
	log.Info("[Sentinel] running sentinel with configuration", "cfg", cfg)
	s, err := startSentinel(cliCtx, *cfg, cpState)
	if err != nil {
		log.Error("Could not start sentinel service", "err", err)
	}

	genesisCfg := cfg.GenesisCfg
	beaconConfig := cfg.BeaconCfg
	beaconRpc := rpc.NewBeaconRpcP2P(ctx, s, beaconConfig, genesisCfg)
	downloader := network2.NewForwardBeaconDownloader(ctx, beaconRpc)
	bdownloader := network2.NewBackwardBeaconDownloader(ctx, beaconRpc)

	forkChoice, err := forkchoice.NewForkChoiceStore(cpState, nil, true)
	if err != nil {
		log.Error("Could not start forkchoice service", "err", err)
		return nil
	}
	gossipManager := network2.NewGossipReceiver(ctx, s, forkChoice, beaconConfig, genesisCfg)
	stageloop, err := stages2.NewConsensusStagedSync(ctx, db, downloader, bdownloader, genesisCfg, beaconConfig, cpState,
		tmpdir, executionClient, cfg.BeaconDataCfg, gossipManager, forkChoice, logger)
	if err != nil {
		return err
	}
Loop:
	for {
		if err := stageloop.Run(db, nil, false); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			break Loop
		default:
		}
	}
	return nil
}

func startSentinel(cliCtx *cli.Context, cfg lcCli.ConsensusClientCliCfg, beaconState *state.BeaconState) (sentinelrpc.SentinelClient, error) {
	forkDigest, err := fork.ComputeForkDigest(cfg.BeaconCfg, cfg.GenesisCfg)
	if err != nil {
		return nil, err
	}
	s, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:        cfg.Addr,
		Port:          int(cfg.Port),
		TCPPort:       cfg.ServerTcpPort,
		GenesisConfig: cfg.GenesisCfg,
		NetworkConfig: cfg.NetworkCfg,
		BeaconConfig:  cfg.BeaconCfg,
		NoDiscovery:   cfg.NoDiscovery,
	}, nil, &service.ServerConfig{Network: cfg.ServerProtocol, Addr: cfg.ServerAddr}, nil, &cltypes.Status{
		ForkDigest:     forkDigest,
		FinalizedRoot:  beaconState.FinalizedCheckpoint().Root,
		FinalizedEpoch: beaconState.FinalizedCheckpoint().Epoch,
		HeadSlot:       beaconState.FinalizedCheckpoint().Epoch * cfg.BeaconCfg.SlotsPerEpoch,
		HeadRoot:       beaconState.FinalizedCheckpoint().Root,
	})
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
		return nil, err
	}
	log.Info("Sentinel started", "addr", cfg.ServerAddr)
	return s, nil
}

func getCheckpointState(ctx context.Context, db kv.RwDB, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, uri string) (*state.BeaconState, error) {
	state, err := core.RetrieveBeaconState(ctx, beaconConfig, genesisConfig, uri)
	if err != nil {
		log.Error("[Checkpoint Sync] Failed", "reason", err)
		return nil, err
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		log.Error("[DB] Failed", "reason", err)
		return nil, err
	}
	defer tx.Rollback()

	if err := rawdb2.WriteBeaconState(tx, state); err != nil {
		log.Error("[DB] Failed", "reason", err)
		return nil, err
	}
	log.Info("Checkpoint sync successful: hurray!")
	return state, tx.Commit()
}

func checkAndStoreBeaconDataConfigWithDB(ctx context.Context, db kv.RwDB, provided *rawdb2.BeaconDataConfig) error {
	tx, err := db.BeginRw(ctx)
	if err != nil {
		log.Error("[DB] Failed", "reason", err)
		return err
	}
	defer tx.Rollback()
	if provided == nil {
		return errors.New("no valid beacon data config found")
	}
	stored, err := rawdb2.ReadBeaconDataConfig(tx)
	if err != nil {
		return err
	}
	if stored != nil {
		if err := checkBeaconDataConfig(provided, stored); err != nil {
			return err
		}
	}
	return rawdb2.WriteBeaconDataConfig(tx, provided)
}

func checkBeaconDataConfig(provided *rawdb2.BeaconDataConfig, stored *rawdb2.BeaconDataConfig) error {
	if provided.BackFillingAmount != stored.BackFillingAmount {
		return fmt.Errorf("mismatching backfilling amount, provided %d, stored %d", provided.BackFillingAmount, stored.BackFillingAmount)
	}
	if provided.SlotPerRestorePoint != stored.SlotPerRestorePoint {
		return fmt.Errorf("mismatching sprp, provided %d, stored %d", provided.SlotPerRestorePoint, stored.SlotPerRestorePoint)
	}
	return nil
}
