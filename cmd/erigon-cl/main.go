package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
	clcore "github.com/ledgerwatch/erigon/cmd/erigon-cl/cl-core"
	cldb "github.com/ledgerwatch/erigon/cmd/erigon-cl/cl-core/cl-db"
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

	// Fetch the checkpoint state.
	cpState, err := getCheckpointState(ctx)
	if err != nil {
		log.Error("Could not get checkpoint", "err", err)
		return err
	}

	log.Info("Starting sync from checkpoint.")

	// Compute the fork digest of the chain.
	digest, err := fork.ComputeForkDigest(lcCfg.BeaconCfg, lcCfg.GenesisCfg)
	if err != nil {
		log.Error("Could not compute fork digest", "err", err)
		return err
	}
	log.Info("Fork digest", "data", digest)

	// Start the sentinel service
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	log.Info("[Sentinel] running sentinel with configuration", "cfg", lcCfg)
	s, err := startSentinel(cliCtx, *lcCfg)
	if err != nil {
		log.Error("Could not start sentinel service", "err", err)
	}

	// Get checkpoint block by root.
	cpBlock, err := clcore.GetCheckpointBlock(ctx, s, cpState)
	if err != nil {
		log.Error("[Head sync] Failed", "reason", err)
		return err
	}
	log.Info("Retrieved root block.", "block", cpBlock)

	// Get status object.
	status, err := clcore.GetStatus(ctx, s, &cltypes.Status{
		ForkDigest:     digest,
		FinalizedRoot:  cpState.FinalizedCheckpoint.Root,
		FinalizedEpoch: cpState.FinalizedCheckpoint.Epoch,
		HeadRoot:       cpState.FinalizedCheckpoint.Root,
		HeadSlot:       cpBlock.Block.Slot,
	})
	if err != nil {
		log.Error("[Head sync] Failed, status not fetched", "reason", err)
		return err
	}
	log.Info("Retrieved status.", "status", status)
	log.Info("Current finalized root.", "root", hex.EncodeToString(status.FinalizedRoot[:]))
	log.Info("Current finalized epoch.", "epoch", status.FinalizedEpoch)
	log.Info("Current head root.", "root", hex.EncodeToString(status.HeadRoot[:]))
	log.Info("Current head slot.", "slot", status.HeadSlot)
	return nil
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

func getCheckpointState(ctx context.Context) (*cltypes.BeaconState, error) {
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
		return nil, err
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		log.Error("[DB] Failed", "reason", err)
		return nil, err
	}
	defer tx.Rollback()

	if err := cldb.WriteBeaconState(tx, state); err != nil {
		log.Error("[DB] Failed", "reason", err)
		return nil, err
	}
	if _, err = cldb.ReadBeaconState(tx, state.Slot); err != nil {
		log.Error("[DB] Failed", "reason", err)
		return nil, err
	}
	log.Info("Checkpoint sync successful: hurray!")
	return state, nil
}
