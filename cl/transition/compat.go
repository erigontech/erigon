package transition

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	machine2 "github.com/ledgerwatch/erigon/cl/transition/machine"

	"github.com/ledgerwatch/erigon/cl/cltypes"
)

var _ machine2.Interface = (*eth2.Impl)(nil)

var DefaultMachine = &eth2.Impl{}
var ValidatingMachine = &eth2.Impl{FullValidation: true}

func TransitionState(s abstract.BeaconState, block *cltypes.SignedBeaconBlock, fullValidation bool) error {
	cvm := &eth2.Impl{FullValidation: fullValidation}
	return machine2.TransitionState(cvm, s, block)
}
