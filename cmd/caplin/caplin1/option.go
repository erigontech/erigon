package caplin1

import (
	"github.com/ledgerwatch/erigon/cl/beacon/builder"
	"github.com/ledgerwatch/erigon/cl/clparams"
)

type option struct {
	builderClient builder.BuilderClient
}

type CaplinOption func(*option)

func WithBuilder(mevRelayUrl string, beaconConfig *clparams.BeaconChainConfig) CaplinOption {
	return func(o *option) {
		o.builderClient = builder.NewBlockBuilderClient(mevRelayUrl, beaconConfig)
	}
}
