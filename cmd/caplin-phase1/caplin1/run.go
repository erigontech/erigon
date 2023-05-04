package caplin1

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/execution_client"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/forkchoice"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/stages"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

func RunCaplinPhase1(ctx context.Context, sentinel sentinel.SentinelClient, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, engine execution_client.ExecutionEngine, state *state.BeaconState) error {
	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, genesisConfig)
	downloader := network.NewForwardBeaconDownloader(ctx, beaconRpc)

	forkChoice, err := forkchoice.NewForkChoiceStore(state, engine, true)
	if err != nil {
		log.Error("Could not create forkchoice", "err", err)
		return err
	}
	gossipManager := network.NewGossipReceiver(ctx, sentinel, forkChoice, beaconConfig, genesisConfig)
	return stages.SpawnStageForkChoice(stages.StageForkChoice(nil, downloader, genesisConfig, beaconConfig, state, nil, gossipManager, forkChoice), &stagedsync.StageState{ID: "Caplin"}, nil, ctx)
}
