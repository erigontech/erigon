package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/fork"
	clcore "github.com/ledgerwatch/erigon/cmd/erigon-cl/cl-core"
	cldb "github.com/ledgerwatch/erigon/cmd/erigon-cl/cl-core/cl-db"
	lcCli "github.com/ledgerwatch/erigon/cmd/sentinel/cli"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/service"
	sentinelapp "github.com/ledgerwatch/erigon/turbo/app"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli"
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

func runConsensusLayerNode(cliCtx *cli.Context) {
	lcCfg, _ := lcCli.SetUpLightClientCfg(cliCtx)
	ctx := context.Background()

	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	log.Info("[Sentinel] running sentinel with configuration", "cfg", lcCfg)
	s, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:        lcCfg.Addr,
		Port:          int(lcCfg.Port),
		TCPPort:       lcCfg.ServerTcpPort,
		GenesisConfig: lcCfg.GenesisCfg,
		NetworkConfig: lcCfg.NetworkCfg,
		BeaconConfig:  lcCfg.BeaconCfg,
		NoDiscovery:   lcCfg.NoDiscovery,
	}, &service.ServerConfig{Network: lcCfg.ServerProtocol, Addr: lcCfg.ServerAddr}, nil)
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
		return
	}
	log.Info("Sentinel started", "addr", lcCfg.ServerAddr)

	digest, err := fork.ComputeForkDigest(lcCfg.BeaconCfg, lcCfg.GenesisCfg)
	if err != nil {
		log.Error("Could not compute fork digeest", "err", err)
		return
	}
	log.Info("Fork digest", "data", digest)

	db, err := mdbx.NewTemporaryMdbx()

	// Checkpoint sync.
	if err != nil {
		log.Error("Error opening database", "err", err)
	}
	defer db.Close()
	uri := clparams.GetCheckpointSyncEndpoint(clparams.MainnetNetwork)

	state, err := clcore.RetrieveBeaconState(ctx, uri)

	if err != nil {
		log.Error("[Checkpoint Sync] Failed", "reason", err)
		return
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		log.Error("[DB] Failed", "reason", err)
		return
	}
	defer tx.Rollback()

	if err := cldb.WriteBeaconState(tx, state); err != nil {
		log.Error("[DB] Failed", "reason", err)
		return
	}
	if _, err = cldb.ReadBeaconState(tx, state.Slot); err != nil {
		log.Error("[DB] Failed", "reason", err)
		return
	}
	log.Info("Checkpoint sync successful: hurray!")

	log.Info("Starting sync from checkpoint.")
	blocks, err := clcore.GetBlocksFromCheckpoint(ctx, &s, state, digest)
	if err != nil {
		log.Error("[Head sync] Failed", "reason", err)
		return
	}
	log.Info("Retrieved most recent %d blocks.", len(blocks))
}
