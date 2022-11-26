package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	cldb "github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/stages"
	lcCli "github.com/ledgerwatch/erigon/cmd/sentinel/cli"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/service"
	sentinelapp "github.com/ledgerwatch/erigon/turbo/app"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

func main() {
	app := sentinelapp.MakeApp(runConsensusLayerNode, flags.LightClientDefaultFlags)
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
	lcCfg, _ := lcCli.SetUpLightClientCfg(cliCtx)

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
	log.Info("[Sentinel] running sentinel with configuration", "cfg", lcCfg)
	s, err := startSentinel(cliCtx, *lcCfg)
	if err != nil {
		log.Error("Could not start sentinel service", "err", err)
	}

	genesisCfg, _, beaconConfig := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	downloader := network.NewForwardBeaconDownloader(ctx, s)

	if err := stages.SpawnStageBeaconsBlocks(stages.StageBeaconsBlock(db, downloader, genesisCfg, beaconConfig, cpState), nil, ctx); err != nil {
		return err
	}
	return stages.SpawnStageBeaconState(stages.StageBeaconState(db, genesisCfg, beaconConfig, cpState), nil, ctx)
}

func startSentinel(cliCtx *cli.Context, lcCfg lcCli.LightClientCliCfg) (consensusrpc.SentinelClient, error) {
	s, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:        lcCfg.Addr,
		Port:          int(lcCfg.Port),
		TCPPort:       lcCfg.ServerTcpPort,
		GenesisConfig: lcCfg.GenesisCfg,
		NetworkConfig: lcCfg.NetworkCfg,
		BeaconConfig:  lcCfg.BeaconCfg,
		NoDiscovery:   lcCfg.NoDiscovery,
	}, nil, &service.ServerConfig{Network: lcCfg.ServerProtocol, Addr: lcCfg.ServerAddr}, nil)
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
		return nil, err
	}
	log.Info("Sentinel started", "addr", lcCfg.ServerAddr)
	return s, nil
}

func getCheckpointState(ctx context.Context, db kv.RwDB) (*cltypes.BeaconState, error) {

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
	if _, err = cldb.ReadBeaconState(tx, state.Slot); err != nil {
		log.Error("[DB] Failed", "reason", err)
		return nil, err
	}
	log.Info("Checkpoint sync successful: hurray!")
	return state, tx.Commit()
}
