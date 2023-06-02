package raw

import (
	_ "embed"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
)

//go:embed testdata/state.ssz_snappy
var denebState []byte

func GetTestState() *BeaconState {
	state := New(&clparams.MainnetBeaconConfig)
	utils.DecodeSSZSnappy(state, denebState, int(clparams.DenebVersion))
	return state

}
