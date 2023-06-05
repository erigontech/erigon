package caplin1

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/phase1/stages"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

func RunCaplinPhase1(ctx context.Context, sentinel sentinel.SentinelClient, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig,
	engine execution_client.ExecutionEngine, state *state.BeaconState, caplinFreezer freezer.Freezer) error {
	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, genesisConfig)
	downloader := network2.NewForwardBeaconDownloader(ctx, beaconRpc)

	forkChoice, err := forkchoice.NewForkChoiceStore(state, engine, caplinFreezer, true)
	if err != nil {
		log.Error("Could not create forkchoice", "err", err)
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
	gossipManager := network2.NewGossipReceiver(ctx, sentinel, forkChoice, beaconConfig, genesisConfig, caplinFreezer)
	return stages.SpawnStageForkChoice(stages.StageForkChoice(nil, downloader, genesisConfig, beaconConfig, state, nil, gossipManager, forkChoice, caplinFreezer), &stagedsync.StageState{ID: "Caplin"}, nil, ctx)
}
