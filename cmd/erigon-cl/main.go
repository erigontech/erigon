package main

import (
	"context"
	"fmt"
	"os"

	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/stages"
	lcCli "github.com/ledgerwatch/erigon/cmd/sentinel/cli"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/handshake"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/service"
	sentinelapp "github.com/ledgerwatch/erigon/turbo/app"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

func main() {
	app := sentinelapp.MakeApp(runConsensusLayerNode, flags.CLDefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runConsensusLayerNode(cliCtx *cli.Context) error {
	ctx := context.Background()
	cfg, _ := lcCli.SetupConsensusClientCfg(cliCtx)

	db, err := mdbx.NewTemporaryMdbx()
	if err != nil {
		log.Error("Error opening database", "err", err)
	}
	defer db.Close()
	// Fetch the checkpoint state.
	cpState, err := getCheckpointState(ctx, db)
	if err != nil {
		log.Error("Could not get checkpoint", "err", err)
		return err
	}

	log.Info("Starting sync from checkpoint.")

	// Start the sentinel service
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	log.Info("[Sentinel] running sentinel with configuration", "cfg", cfg)
	s, err := startSentinel(cliCtx, *cfg, cpState)
	if err != nil {
		log.Error("Could not start sentinel service", "err", err)
	}

	genesisCfg, _, beaconConfig := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	downloader := network.NewForwardBeaconDownloader(ctx, s)
	gossipManager := network.NewGossipReceiver(ctx, s)
	gossipManager.AddReceiver(sentinelrpc.GossipType_BeaconBlockGossipType, downloader)
	go gossipManager.Loop()
	stageloop, err := stages.NewConsensusStagedSync(ctx, db, downloader, genesisCfg, beaconConfig, cpState, nil, false)
	if err != nil {
		return err
	}
Loop:
	for {
		if err := stageloop.Run(db, nil, false, true); err != nil {
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
		HeadSlot:       beaconState.FinalizedCheckpoint().Epoch * 32,
		HeadRoot:       beaconState.FinalizedCheckpoint().Root,
	}, handshake.FullClientRule)
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
		return nil, err
	}
	log.Info("Sentinel started", "addr", cfg.ServerAddr)
	return s, nil
}

func getCheckpointState(ctx context.Context, db kv.RwDB) (*state.BeaconState, error) {

	uri := clparams.GetCheckpointSyncEndpoint(clparams.MainnetNetwork)

	state, err := core.RetrieveBeaconState(ctx, uri)
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

	if err := rawdb.WriteBeaconState(tx, state); err != nil {
		log.Error("[DB] Failed", "reason", err)
		return nil, err
	}
	log.Info("Checkpoint sync successful: hurray!")
	return state, tx.Commit()
}
