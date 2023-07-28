package stages

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon/cl/clpersist"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/spf13/afero"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

type StageForkChoiceCfg struct {
	db              kv.RwDB
	rpc             *rpc.BeaconRpcP2P
	source          clpersist.BlockSource
	genesisCfg      *clparams.GenesisConfig
	beaconCfg       *clparams.BeaconChainConfig
	executionClient *execution_client.ExecutionClient
	state           *state.CachingBeaconState
	gossipManager   *network2.GossipManager
	forkChoice      *forkchoice.ForkChoiceStore
	caplinFreezer   freezer.Freezer
	dataDirFs       afero.Fs
}

const minPeersForDownload = 2
const minPeersForSyncStart = 4

var (
	freezerNameSpacePrefix = ""
	blockObjectName        = "singedBeaconBlock"
	stateObjectName        = "beaconState"
	gossipAction           = "gossip"
)

func StageForkChoice(db kv.RwDB,
	rpc *rpc.BeaconRpcP2P,
	source clpersist.BlockSource,
	genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.CachingBeaconState,
	executionClient *execution_client.ExecutionClient,
	gossipManager *network2.GossipManager,
	forkChoice *forkchoice.ForkChoiceStore,
	caplinFreezer freezer.Freezer,
	dataDirFs afero.Fs,
) StageForkChoiceCfg {
	return StageForkChoiceCfg{
		db:              db,
		rpc:             rpc,
		genesisCfg:      genesisCfg,
		beaconCfg:       beaconCfg,
		source:          source,
		state:           state,
		executionClient: executionClient,
		gossipManager:   gossipManager,
		forkChoice:      forkChoice,
		caplinFreezer:   caplinFreezer,
	}
}

func SpawnStageWaitForPeers(cfg StageForkChoiceCfg, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context) error {
	peersCount, err := cfg.rpc.Peers()
	if err != nil {
		return nil
	}
	waitWhenNotEnoughPeers := 3 * time.Second
	for {
		if peersCount > minPeersForDownload {
			break
		}
		log.Debug("[Caplin] Waiting For Peers", "have", peersCount, "needed", minPeersForSyncStart, "retryIn", waitWhenNotEnoughPeers)
		time.Sleep(waitWhenNotEnoughPeers)
		peersCount, err = cfg.rpc.Peers()
		if err != nil {
			peersCount = 0
		}
	}
	return nil
}
