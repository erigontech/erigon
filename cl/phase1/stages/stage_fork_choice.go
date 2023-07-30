package stages

import (
	"github.com/ledgerwatch/erigon/cl/clpersist"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/spf13/afero"

	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/cl/clparams"
)

type CaplinStagedSyncCfg struct {
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

func CaplinStagedSync(db kv.RwDB,
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
) CaplinStagedSyncCfg {
	return CaplinStagedSyncCfg{
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
		dataDirFs:       dataDirFs,
	}
}
