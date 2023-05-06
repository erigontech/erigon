package state

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/raw"
)

func GetEmptyBeaconState() *BeaconState {
	return NewFromRaw(raw.GetEmptyBeaconState())
}

func GetEmptyBeaconStateWithVersion(v clparams.StateVersion) *BeaconState {
	return NewFromRaw(raw.GetEmptyBeaconStateWithVersion(v))
}
