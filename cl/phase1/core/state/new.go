package state

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltrace"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/raw"
)

func New(cfg *clparams.BeaconChainConfig) abstract.BeaconState {
	return &cltrace.BeaconStateProxy{
		Handler: cltrace.InvocationHandlerFunc(func(method string, args []any) (retvals []any, intercept bool) {
			return nil, false
		}),
		Underlying: NewCaching(cfg),
	}
}

func NewCaching(cfg *clparams.BeaconChainConfig) *CachingBeaconState {
	state := &CachingBeaconState{
		BeaconState: raw.New(cfg),
	}
	state.initBeaconState()
	return state
}

func NewFromRaw(r *raw.BeaconState) *CachingBeaconState {
	state := &CachingBeaconState{
		BeaconState: r,
	}
	state.initBeaconState()
	return state
}
