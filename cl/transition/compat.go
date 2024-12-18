package transition

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	machine2 "github.com/erigontech/erigon/cl/transition/machine"

	"github.com/erigontech/erigon/cl/cltypes"
)

var _ machine2.Interface = (*eth2.Impl)(nil)

var DefaultMachine = &eth2.Impl{}
var ValidatingMachine = &eth2.Impl{FullValidation: true}

func TransitionState(s abstract.BeaconState, block *cltypes.SignedBeaconBlock, blockRewardsCollector *eth2.BlockRewardsCollector, fullValidation bool) error {
	cvm := &eth2.Impl{FullValidation: fullValidation, BlockRewardsCollector: blockRewardsCollector}
	return machine2.TransitionState(cvm, s, block)
}
