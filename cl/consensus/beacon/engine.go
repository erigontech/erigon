package beacon

import (
	"github.com/erigontech/erigon/cl/consensus"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/transition/machine"
)

// Engine is the L1 beacon chain consensus engine. It wraps the existing
// eth2.Impl state machine and enables full data availability checking
// and Casper FFG finality.
type Engine struct{}

var _ consensus.Engine = (*Engine)(nil)

func (e *Engine) Type() consensus.EngineType {
	return consensus.BeaconChainEngineType
}

func (e *Engine) Machine(fullValidation bool, blockRewardsCollector *eth2.BlockRewardsCollector) machine.Interface {
	return &eth2.Impl{
		FullValidation:        fullValidation,
		BlockRewardsCollector: blockRewardsCollector,
	}
}

func (e *Engine) ShouldVerifyDataAvailability() bool {
	return true
}

func (e *Engine) FinalityMode() consensus.FinalityMode {
	return consensus.FinalityCasperFFG
}
