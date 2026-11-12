package vm

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// StatefulPrecompile is a precompile that reads and writes state. RequiredGas is
// the flat charge taken before RunStateful; RunStateful charges the rest from
// env.Gas. Its error reverts every state change of the call.
type StatefulPrecompile interface {
	PrecompiledContract
	RunStateful(env *PrecompileEnv, input []byte) ([]byte, error)
}

type PrecompileEnv struct {
	EVM    *EVM
	Self   accounts.Address
	Caller accounts.Address
	Value  uint256.Int
	Gas    *mdgas.MdGas
	// ReadOnly is true inside a STATICCALL or any read-only ancestor frame.
	ReadOnly bool
	// DirectCall is false for CALLCODE and DELEGATECALL.
	DirectCall bool
}

var (
	statefulEnabled func(*chain.Rules) bool
	statefulResolve func(accounts.Address) (StatefulPrecompile, bool)
)

// SetStatefulPrecompiles installs stateful precompiles that are not in the
// precompile map, so they are neither warm nor listed as active. resolve is
// consulted only under rules for which enabled is true. Call it once, at init.
func SetStatefulPrecompiles(enabled func(*chain.Rules) bool, resolve func(accounts.Address) (StatefulPrecompile, bool)) {
	statefulEnabled, statefulResolve = enabled, resolve
}

func statefulPrecompilesOn(rules *chain.Rules) bool {
	return statefulEnabled != nil && statefulEnabled(rules)
}

func (evm *EVM) runStatefulPrecompile(p StatefulPrecompile, env PrecompileEnv, input []byte, gas mdgas.MdGas, tracer *tracing.Hooks) ([]byte, mdgas.MdGas, error) {
	remaining, err := chargePrecompile(p, input, gas, tracer)
	if err != nil {
		return nil, remaining, err
	}
	env.EVM = evm
	env.Gas = &remaining
	output, err := p.RunStateful(&env, input)
	return output, remaining, err
}
