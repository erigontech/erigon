package rollup

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/consensus"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/transition/machine"
)

// Engine is the L2 based rollup consensus engine. It reuses the core
// beacon state machine (slot processing, block header, randao, fork
// choice) but skips L1-specific operations:
//   - eth1 data voting
//   - sync aggregate processing
//   - validator deposits (old-style and EL-triggered)
//
// Data availability (PeerDAS/blobs) is not required since L2 blocks
// are derived from L1 and don't carry sidecars.
type Engine struct {
	devMode bool
}

var _ consensus.Engine = (*Engine)(nil)

func NewEngine() *Engine {
	return &Engine{}
}

func NewDevEngine() *Engine {
	return &Engine{devMode: true}
}

func (e *Engine) Type() consensus.EngineType {
	if e.devMode {
		return consensus.DevEngineType
	}
	return consensus.RollupEngineType
}

func (e *Engine) Machine(fullValidation bool, blockRewardsCollector *eth2.BlockRewardsCollector) machine.Interface {
	return &rollupMachine{
		Impl: eth2.Impl{
			FullValidation:        fullValidation,
			BlockRewardsCollector: blockRewardsCollector,
		},
	}
}

func (e *Engine) ShouldVerifyDataAvailability() bool {
	return false
}

func (e *Engine) FinalityMode() consensus.FinalityMode {
	if e.devMode {
		return consensus.FinalityInstant
	}
	return consensus.FinalityL1Anchor
}

// rollupMachine embeds the L1 state machine and overrides L1-specific
// operations as no-ops. All generic operations (block header, randao,
// execution payload, attestations, slashings, withdrawals, slot
// processing) delegate to the embedded eth2.Impl.
type rollupMachine struct {
	eth2.Impl
}

var _ machine.Interface = (*rollupMachine)(nil)

func (m *rollupMachine) ProcessEth1Data(state abstract.BeaconState, eth1Data *cltypes.Eth1Data) error {
	return nil
}

func (m *rollupMachine) ProcessSyncAggregate(s abstract.BeaconState, sync *cltypes.SyncAggregate) error {
	return nil
}

func (m *rollupMachine) ProcessDeposit(s abstract.BeaconState, deposit *cltypes.Deposit) error {
	return nil
}

func (m *rollupMachine) ProcessDepositRequest(s abstract.BeaconState, depositRequest *solid.DepositRequest) error {
	return nil
}
