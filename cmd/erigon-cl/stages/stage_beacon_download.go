package stages

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/network"
)

type StageBeaconForwardCfg struct {
	ctx        context.Context
	downloader network.ForwardBeaconDownloader
	genesisCfg *clparams.GenesisConfig
	beaconCfg  *clparams.BeaconChainConfig
}
