// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

// These tests sit outside package vm on purpose: a chain integrating against
// erigon has only the exported surface, so anything they need that vm does not
// export is a gap in the seam rather than a gap in the test.
package runtime

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// recordingStatefulPrecompile implements vm.StatefulPrecompile and records
// the PrecompileContext of every RunStateful call for assertion.
type recordingStatefulPrecompile struct {
	vm.NoStatelessRun
	calls []*vm.PrecompileContext
}

var _ vm.StatefulPrecompile = (*recordingStatefulPrecompile)(nil)

func (r *recordingStatefulPrecompile) Name() string { return "RECORDING" }

func (r *recordingStatefulPrecompile) RunStateful(input []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	r.calls = append(r.calls, ctx)
	if !gas.ChargeExecution(111) {
		return nil, vm.ErrOutOfGas
	}
	return []byte{0x2a}, nil
}

func prepareStatefulCall(t *testing.T, cfg *Config, precompileAddr accounts.Address) *vm.EVM {
	t.Helper()
	vmenv := NewEnv(cfg)
	rules := vmenv.ChainRules()
	cfg.State.Prepare(rules, cfg.Origin, cfg.Coinbase, precompileAddr, vm.ActivePrecompiles(rules), nil)
	require.NoError(t, cfg.State.CreateAccount(cfg.Origin, false))
	require.NoError(t, cfg.State.AddBalance(cfg.Origin, *uint256.NewInt(1_000_000), tracing.BalanceChangeUnspecified))
	return vmenv
}

type funcPrecompile struct {
	vm.NoStatelessRun
	name string
	run  func(input []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error)
}

func (f funcPrecompile) Name() string { return f.name }

func (f funcPrecompile) RunStateful(input []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	return f.run(input, gas, ctx)
}

func registerPrecompiles(t *testing.T, chainID uint64, set vm.PrecompiledContracts) {
	t.Helper()
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts { return set })
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })
}

func TestStatefulPrecompileDispatch(t *testing.T) {
	const chainID = 900401
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x88}))
	rec := &recordingStatefulPrecompile{}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: rec})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	gas := mdgas.MdGas{Execution: 100000}
	value := *uint256.NewInt(7)

	ret, remaining, _, err := vmenv.Call(cfg.Origin, precompileAddr, []byte{0x01}, gas, value, false)
	require.NoError(t, err)
	require.Equal(t, []byte{0x2a}, ret)
	require.Equal(t, gas.Execution-111, remaining.Execution)

	require.Len(t, rec.calls, 1)
	got := rec.calls[0]
	require.Equal(t, cfg.Origin, got.Caller)
	require.Equal(t, precompileAddr, got.Self)
	require.Equal(t, precompileAddr, got.ActingAs)
	gotValue := got.Value
	require.True(t, gotValue.Eq(&value))
	require.False(t, got.ReadOnly)

	cfg2 := newL2TestConfig(t, chainID)
	vmenv2 := prepareStatefulCall(t, cfg2, precompileAddr)

	_, _, _, err = vmenv2.StaticCall(cfg2.Origin, precompileAddr, []byte{0x01}, gas)
	require.NoError(t, err)
	require.Len(t, rec.calls, 2)
	require.True(t, rec.calls[1].ReadOnly, "STATICCALL must reach the precompile with ctx.ReadOnly=true")
}

func TestStatefulPrecompileDelegateCallIdentity(t *testing.T) {
	const chainID = 900402
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x89}))
	rec := &recordingStatefulPrecompile{}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: rec})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	delegator := accounts.InternAddress(common.HexToAddress("0xde1e"))
	value := *uint256.NewInt(3)

	_, _, _, err := vmenv.DelegateCall(delegator, cfg.Origin, precompileAddr, []byte{0x01}, value, mdgas.MdGas{Execution: 100000})
	require.NoError(t, err)

	require.Len(t, rec.calls, 1)
	got := rec.calls[0]
	require.Equal(t, precompileAddr, got.Self)
	require.Equal(t, delegator, got.ActingAs, "DELEGATECALL runs the precompile as the delegating contract")
	require.Equal(t, cfg.Origin, got.Caller, "DELEGATECALL preserves the delegating frame's caller")
	require.False(t, got.ReadOnly)
}

// TestStatefulPrecompileReentryHitsDepthLimit pins that a stateful precompile
// re-entering the EVM through ctx.EVM counts against CallCreateDepth.
func TestStatefulPrecompileReentryHitsDepthLimit(t *testing.T) {
	const chainID = 900403
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8a}))
	calls := 0
	rec := funcPrecompile{name: "REENTER",
		run: func(input []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			calls++
			if calls > 1100 {
				return nil, nil
			}
			_, err := ctx.Call(gas, precompileAddr, input, gas.Remaining().Execution, uint256.Int{})
			return nil, err
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: rec})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, _, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 1_000_000}, uint256.Int{}, false)
	require.ErrorIs(t, err, vm.ErrDepth)
	require.LessOrEqual(t, calls, 1030, "recursion must be cut off by the depth limit")
}

// TestStatefulPrecompileStateGasAttribution pins that State-dimension gas a
// stateful precompile consumes is reported as State usage, not folded into
// Execution.
func TestStatefulPrecompileStateGasAttribution(t *testing.T) {
	const chainID = 900404
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8b}))
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: funcPrecompile{name: "STATEGAS",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if !gas.ChargeExecution(100) || !gas.ChargeState(40) {
				return nil, vm.ErrOutOfGas
			}
			return nil, nil
		}}})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 10_000, State: 500}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Equal(t, uint64(9_900), remaining.Execution)
	require.Equal(t, uint64(460), remaining.State)
	require.Equal(t, int64(40), gasUsed.State, "State consumption must be attributed to the State dimension")
	require.Equal(t, uint64(100), gasUsed.Execution, "Execution usage must not absorb the State spend")
}

// TestStatefulPrecompileStaticContextInherited pins that a nested call made
// through ctx.EVM from inside a STATICCALL'd precompile keeps write
// protection, like nested bytecode frames do.
func TestStatefulPrecompileStaticContextInherited(t *testing.T) {
	const chainID = 900405
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8c}))
	storeAddr := accounts.InternAddress(common.HexToAddress("0x5570"))
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: funcPrecompile{name: "NESTED",
		run: func(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			_, err := ctx.Call(gas, storeAddr, nil, gas.Remaining().Execution, uint256.Int{})
			return nil, err
		}}})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)
	require.NoError(t, cfg.State.CreateAccount(storeAddr, true))
	require.NoError(t, cfg.State.SetCode(storeAddr, []byte{0x60, 0x01, 0x60, 0x01, 0x55, 0x00}, tracing.CodeChangeUnspecified)) // PUSH1 1 PUSH1 1 SSTORE STOP

	_, _, _, err := vmenv.StaticCall(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 1_000_000})
	require.ErrorIs(t, err, vm.ErrWriteProtection)
}

// TestStatefulPrecompileCallCodeIdentity pins the third frameIdentity branch,
// the one where ActingAs and Caller are both the caller and neither is Self.
// A CALLCODE'd precompile has to write to the calling contract's address, not
// its own.
func TestStatefulPrecompileCallCodeIdentity(t *testing.T) {
	const chainID = 900406
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x91}))
	rec := &recordingStatefulPrecompile{}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: rec})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	value := *uint256.NewInt(5)
	_, _, _, err := vmenv.CallCode(cfg.Origin, precompileAddr, []byte{0x01}, mdgas.MdGas{Execution: 100000}, value)
	require.NoError(t, err)

	require.Len(t, rec.calls, 1)
	got := rec.calls[0]
	require.Equal(t, precompileAddr, got.Self, "Self stays the precompile's own code address")
	require.Equal(t, cfg.Origin, got.ActingAs, "CALLCODE runs the precompile as the calling contract")
	require.Equal(t, cfg.Origin, got.Caller, "CALLCODE leaves the caller as its own frame")
	require.False(t, got.ReadOnly)
}

// TestStatefulPrecompileStateGasSpill pins the attribution when a state charge
// outruns the reservoir: the whole charge counts as State usage and the part
// that came out of execution gas is reported as spill, rather than the charge
// being read back off the reservoir alone.
func TestStatefulPrecompileStateGasSpill(t *testing.T) {
	const chainID = 900407
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8e}))
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: funcPrecompile{name: "SPILL",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if !gas.ChargeState(40) {
				return nil, vm.ErrOutOfGas
			}
			return nil, nil
		}}})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 10_000, State: 10}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Equal(t, uint64(9_970), remaining.Execution, "the 30 gas the reservoir could not cover comes out of execution gas")
	require.Equal(t, uint64(0), remaining.State)
	require.Equal(t, int64(40), gasUsed.State, "the whole charge is State usage, not just the reservoir's share")
	require.Equal(t, uint64(30), gasUsed.StateSpill)
	require.Equal(t, uint64(0), gasUsed.Execution, "spilled state gas must not be reported as execution usage")
}

// TestStatefulPrecompileSpillRestoredOnRevert pins that handleFrameRevert can
// see the spill. EIP-8037 returns state gas to the parent on revert, so the 30
// that spilled into execution gas comes back while the 100 charged as
// execution gas stays spent.
func TestStatefulPrecompileSpillRestoredOnRevert(t *testing.T) {
	const chainID = 900408
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8f}))
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: funcPrecompile{name: "REVERTSPILL",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if !gas.ChargeExecution(100) || !gas.ChargeState(40) {
				return nil, vm.ErrOutOfGas
			}
			return nil, vm.ErrExecutionReverted
		}}})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 10_000, State: 10}, uint256.Int{}, false)
	require.ErrorIs(t, err, vm.ErrExecutionReverted)
	require.Equal(t, uint64(9_900), remaining.Execution, "the 30 spilled into execution gas is restored, the 100 charged to it is not")
	require.Equal(t, uint64(10), remaining.State, "the reservoir is restored to what the frame was handed")
}

// TestStatefulPrecompileNetStateRefundSucceeds pins that ending a frame with
// more state gas than it was handed is a valid result, not gas minting. State
// usage is signed for exactly this case, and the frame's execution usage still
// derives correctly from it.
func TestStatefulPrecompileNetStateRefundSucceeds(t *testing.T) {
	const chainID = 900409
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x90}))
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: funcPrecompile{name: "CLEAR",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if !gas.ChargeState(40) {
				return nil, vm.ErrOutOfGas
			}
			gas.RefundState(100)
			return nil, nil
		}}})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 10_000, State: 50}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Equal(t, uint64(10_000), remaining.Execution)
	require.Equal(t, uint64(110), remaining.State, "50 handed, 40 charged, 100 refunded")
	require.Equal(t, int64(-60), gasUsed.State)
	require.Equal(t, uint64(0), gasUsed.Execution, "a net state refund must not inflate execution usage")
}

// TestStatefulPrecompileCannotEscapeStaticContext pins the value transfer and
// the account creation, which the interpreter refuses while charging gas for
// CALL and CREATE — a path ctx.EVM skips entirely.
func TestStatefulPrecompileCannotEscapeStaticContext(t *testing.T) {
	const chainID = 900410
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8f}))
	target := accounts.InternAddress(common.HexToAddress("0x7a49"))
	var callErr, createErr error
	p := funcPrecompile{name: "ESCAPE",
		run: func(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			_, callErr = ctx.Call(gas, target, nil, gas.Remaining().Execution, *uint256.NewInt(5))
			_, _, createErr = ctx.Create(gas, []byte{0x00}, gas.Remaining().Execution, uint256.Int{}, nil)
			return nil, nil
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: p})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)
	require.NoError(t, cfg.State.AddBalance(precompileAddr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified))

	_, _, _, err := vmenv.StaticCall(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 1_000_000, State: 1_000_000})
	require.NoError(t, err)
	require.ErrorIs(t, callErr, vm.ErrWriteProtection, "a value-bearing CALL out of a static frame must be refused")
	require.ErrorIs(t, createErr, vm.ErrWriteProtection, "CREATE out of a static frame must be refused")

	balance, err := cfg.State.GetBalance(target)
	require.NoError(t, err)
	require.True(t, balance.IsZero(), "no value may leave a static frame")

	// Control arm: the same precompile under a plain CALL still moves value,
	// so the gate is scoped to the static context and not to ctx.EVM.
	callErr, createErr = nil, nil
	cfg2 := newL2TestConfig(t, chainID)
	vmenv2 := prepareStatefulCall(t, cfg2, precompileAddr)
	require.NoError(t, cfg2.State.AddBalance(precompileAddr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified))

	_, _, _, err = vmenv2.Call(cfg2.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}, uint256.Int{}, false)
	require.NoError(t, err)
	require.NoError(t, callErr)
	require.NoError(t, createErr, "CREATE out of a writable frame must be allowed")
	balance, err = cfg2.State.GetBalance(target)
	require.NoError(t, err)
	require.Equal(t, uint64(5), balance.Uint64())
}

func TestStatefulPrecompileCreateDeploysAndMovesEndowment(t *testing.T) {
	const chainID = 900442
	const endowment = uint64(9)
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xb4}))
	initCode := []byte{0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x01, 0x60, 0x1f, 0xf3}
	var created accounts.Address
	var createErr error
	p := funcPrecompile{name: "DEPLOYER",
		run: func(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			_, created, createErr = ctx.Create(gas, initCode, gas.Remaining().Execution, *uint256.NewInt(endowment), nil)
			return nil, createErr
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: p})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)
	require.NoError(t, cfg.State.AddBalance(precompileAddr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified))

	_, remaining, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil,
		mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}, uint256.Int{}, false)
	require.NoError(t, err)
	require.NoError(t, createErr)
	require.NotEqual(t, accounts.Address{}, created, "the wrapper has to hand back the deployed address")

	code, err := cfg.State.GetCode(created)
	require.NoError(t, err)
	require.Equal(t, []byte{0x01}, code, "the init code's return value is the deployed runtime code")

	balance, err := cfg.State.GetBalance(created)
	require.NoError(t, err)
	require.Equal(t, endowment, balance.Uint64(), "the endowment has to reach the new account")

	require.Less(t, remaining.Execution, uint64(1_000_000), "the deployment has to cost the frame gas")
}

type reservoirChargeStatefulPrecompile struct {
	vm.NoStatelessRun
	amount uint64
}

func (reservoirChargeStatefulPrecompile) Name() string { return "RESERVOIR" }

func (p reservoirChargeStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
	if !gas.ChargeState(p.amount) {
		return nil, vm.ErrOutOfGas
	}
	return nil, nil
}

// TestStatefulPrecompileStateChargeIsTraced pins the gas-event stream against
// the interpreter's useMdGas: a state charge the EIP-8037 reservoir covers in
// full reports the state dimension, so reading only the execution figures
// drops the event entirely.
func TestStatefulPrecompileStateChargeIsTraced(t *testing.T) {
	const chainID = 900411
	const reservoir, charge = uint64(500), uint64(40)
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x90}))
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: reservoirChargeStatefulPrecompile{amount: charge}})

	type gasEvent struct{ from, to uint64 }
	var events []gasEvent

	cfg := newL2TestConfig(t, chainID)
	cfg.EVMConfig.Tracer = &tracing.Hooks{
		OnGasChange: func(from, to uint64, _ tracing.GasChangeReason) {
			events = append(events, gasEvent{from, to})
		},
	}
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, _, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil,
		mdgas.MdGas{Execution: 10_000, State: reservoir}, uint256.Int{}, false)
	require.NoError(t, err)
	// The surrounding frame-enter and frame-exit events report execution gas;
	// only the charge itself is in the state dimension.
	require.Contains(t, events, gasEvent{reservoir, reservoir - charge},
		"a reservoir-covered state charge must still reach the tracer")
}

// TestStatefulPrecompileCannotMintExecutionGas pins the bound on
// RefundExecution. Execution gas only comes back from a charge this frame
// made, so an unbounded refill underflows used.Execution and returns the
// caller more gas than it handed in — evm.call validates nothing after
// RunStateful.
func TestStatefulPrecompileCannotMintExecutionGas(t *testing.T) {
	const chainID = 900412
	const handed = uint64(100_000)
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x91}))
	refundErr := errors.New("the precompile never ran")
	p := funcPrecompile{name: "OVERREFUND",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if gas.RefundExecution(1) {
				refundErr = errors.New("refund accepted with nothing charged")
				return nil, nil
			}
			if !gas.ChargeExecution(100) {
				return nil, vm.ErrOutOfGas
			}
			if gas.RefundExecution(101) {
				refundErr = errors.New("refund accepted above the charged total")
				return nil, nil
			}
			if !gas.RefundExecution(100) {
				refundErr = errors.New("refund of the charged total was rejected")
				return nil, nil
			}
			refundErr = nil
			return nil, nil
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: p})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: handed}, uint256.Int{}, false)
	require.NoError(t, err)
	require.NoError(t, refundErr)
	require.Equal(t, handed, remaining.Execution, "the frame must not end holding more execution gas than it was handed")
	require.Zero(t, gasUsed.Execution)
}

// TestStatefulPrecompileWrappedRevertKeepsFrameGas pins the classification of a
// revert wrapped the way Go idiomatically wraps a sentinel. handleFrameRevert
// compares the bare value, so an unnormalized wrap burns the frame's leftover
// gas while the receipt still reads as reverted.
func TestStatefulPrecompileWrappedRevertKeepsFrameGas(t *testing.T) {
	const chainID = 900413
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x92}))
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: funcPrecompile{name: "WRAPREVERT",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if !gas.ChargeExecution(100) {
				return nil, vm.ErrOutOfGas
			}
			return nil, fmt.Errorf("precompile failed: %w", vm.ErrExecutionReverted)
		}}})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil,
		mdgas.MdGas{Execution: 10_000}, uint256.Int{}, false)
	require.ErrorIs(t, err, vm.ErrExecutionReverted)
	require.Equal(t, uint64(9_900), remaining.Execution, "a revert keeps the frame's leftover gas")
}

func TestStatefulPrecompileMultiWrappedExceptionalBurnsFrameGas(t *testing.T) {
	const chainID = 900415
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x94}))
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: funcPrecompile{name: "MULTIWRAP",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if !gas.ChargeExecution(100) {
				return nil, vm.ErrOutOfGas
			}
			return nil, fmt.Errorf("%w: %w", vm.ErrOutOfGas, vm.ErrExecutionReverted)
		}}})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil,
		mdgas.MdGas{Execution: 10_000}, uint256.Int{}, false)
	require.ErrorIs(t, err, vm.ErrOutOfGas)
	require.Zero(t, remaining.Execution, "an exceptional failure burns the frame's leftover gas")
}

// TestStatefulPrecompileRefusedCallLeavesNoFrameTrace pins where the static
// write-protection guard sits. The equivalent opcode is rejected while gas is
// charged and never reaches the frame, so a refused re-entrant CALL must not
// record an address access (consensus-relevant under EIP-7928) or a tracer
// Enter/Exit pair of its own.
func TestStatefulPrecompileRefusedCallLeavesNoFrameTrace(t *testing.T) {
	const chainID = 900414
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x93}))
	target := accounts.InternAddress(common.HexToAddress("0x7a50"))
	var callErr error
	p := funcPrecompile{name: "REFUSED",
		run: func(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			_, callErr = ctx.Call(gas, target, nil, gas.Remaining().Execution, *uint256.NewInt(5))
			return nil, nil
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: p})

	var entered []accounts.Address
	cfg := newL2TestConfig(t, chainID)
	cfg.EVMConfig.Tracer = &tracing.Hooks{
		OnEnter: func(_ int, _ byte, _ accounts.Address, to accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
			entered = append(entered, to)
		},
	}
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)
	require.NoError(t, cfg.State.AddBalance(precompileAddr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified))

	_, _, _, err := vmenv.StaticCall(cfg.Origin, precompileAddr, nil,
		mdgas.MdGas{Execution: 1_000_000, State: 1_000_000})
	require.NoError(t, err)
	require.ErrorIs(t, callErr, vm.ErrWriteProtection)
	require.NotContains(t, entered, target, "a refused call must not open a frame on the target")
}

// TestSetPrecompilesNilRestoresChainSet pins both halves of the override
// contract: an empty non-nil map disables every precompile, nil means the
// chain's own set. Resolving nil lazily instead would put registryMu on the
// call path.
func TestSetPrecompilesNilRestoresChainSet(t *testing.T) {
	const chainID = 900415
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x94}))
	rec := &recordingStatefulPrecompile{}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: rec})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)
	gas := mdgas.MdGas{Execution: 10_000}

	vmenv.SetPrecompiles(vm.PrecompiledContracts{})
	_, _, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil, gas, uint256.Int{}, false)
	require.NoError(t, err)
	require.Empty(t, rec.calls, "an empty non-nil map disables every precompile")

	vmenv.SetPrecompiles(nil)
	_, _, _, err = vmenv.Call(cfg.Origin, precompileAddr, nil, gas, uint256.Int{}, false)
	require.NoError(t, err)
	require.Len(t, rec.calls, 1, "nil restores the chain's own set")
}

type reservoirHandoffPrecompile struct {
	vm.NoStatelessRun
	inner   accounts.Address
	kind    string
	callErr error
}

func (*reservoirHandoffPrecompile) Name() string { return "HANDOFF" }

func (p *reservoirHandoffPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	switch p.kind {
	case "staticcall":
		_, p.callErr = ctx.StaticCall(gas, p.inner, nil, 50_000)
	case "delegatecall":
		_, p.callErr = ctx.DelegateCall(gas, p.inner, nil, 50_000)
	default:
		_, p.callErr = ctx.Call(gas, p.inner, nil, 50_000, uint256.Int{})
	}
	return nil, p.callErr
}

// TestStatefulPrecompileNestedCallMovesTheReservoir pins the EIP-8037 handoff.
// MdGas passes by value, so a nested call handed gas.Remaining() directly would
// leave the reservoir standing in both frames and let each nesting level spend
// it again.
func TestStatefulPrecompileNestedCallMovesTheReservoir(t *testing.T) {
	const chainID = 900416
	const reservoir, charge = uint64(5_000), uint64(400)
	outerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x95}))
	innerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x96}))
	outer := &reservoirHandoffPrecompile{inner: innerAddr}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{outerAddr: outer, innerAddr: reservoirChargeStatefulPrecompile{amount: charge}})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, outerAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, outerAddr, nil,
		mdgas.MdGas{Execution: 100_000, State: reservoir}, uint256.Int{}, false)
	require.NoError(t, err)
	require.NoError(t, outer.callErr)
	require.Equal(t, reservoir-charge, remaining.State,
		"the child's charge must come out of the one reservoir, not a copy of it")
	require.Equal(t, int64(charge), gasUsed.State)
}

// TestStatefulPrecompileStateChargePreAmsterdamIsExecutionGas pins where a state
// charge lands before Amsterdam. There is no reservoir then, so charging the
// state dimension would spill into execution gas but record itself in
// used.State, which pre-Amsterdam transaction accounting drops — taking the gas
// off the frame without it reaching the receipt or the block.
func TestStatefulPrecompileStateChargePreAmsterdamIsExecutionGas(t *testing.T) {
	const chainID = 900417
	const charge = uint64(40)
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x97}))
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: reservoirChargeStatefulPrecompile{amount: charge}})

	cfg := newL2TestConfig(t, chainID)
	cfg.ChainConfig.AmsterdamTime = nil
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil,
		mdgas.MdGas{Execution: 10_000}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Equal(t, uint64(10_000-charge), remaining.Execution)
	require.Equal(t, uint64(charge), gasUsed.Execution,
		"pre-Amsterdam the charge has to reach the execution dimension the receipt reads")
	require.Equal(t, int64(0), gasUsed.State)
}

// TestStatefulPrecompileHandoffCoversEveryReentryKind pins the EIP-8037 handoff
// on the re-entry kinds beyond plain CALL. Each keeps its own caller identity,
// so they need their own helper, but the reservoir has to move the same way:
// handing the child gas.Remaining() would leave it standing in both frames.
func TestStatefulPrecompileHandoffCoversEveryReentryKind(t *testing.T) {
	const reservoir, charge = uint64(5_000), uint64(400)
	for i, kind := range []string{"call", "staticcall", "delegatecall"} {
		t.Run(kind, func(t *testing.T) {
			chainID := uint64(900420 + i)
			outerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xa0, byte(i)}))
			innerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xa1, byte(i)}))
			outer := &reservoirHandoffPrecompile{inner: innerAddr, kind: kind}
			registerPrecompiles(t, chainID, vm.PrecompiledContracts{outerAddr: outer, innerAddr: reservoirChargeStatefulPrecompile{amount: charge}})

			cfg := newL2TestConfig(t, chainID)
			vmenv := prepareStatefulCall(t, cfg, outerAddr)

			_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, outerAddr, nil,
				mdgas.MdGas{Execution: 100_000, State: reservoir}, uint256.Int{}, false)
			require.NoError(t, err)
			require.NoError(t, outer.callErr)
			require.Equal(t, reservoir-charge, remaining.State,
				"the child's charge must come out of the one reservoir, not a copy of it")
			require.Equal(t, int64(charge), gasUsed.State)
		})
	}
}

// TestStatefulPrecompileDelegateCallKeepsFrameValue pins that a nested
// DELEGATECALL out of a precompile preserves the calling frame's msg.value.
// DELEGATECALL takes no value operand, so a helper that let the caller supply
// one would hand the delegate frame a value the opcode never could.
func TestStatefulPrecompileDelegateCallKeepsFrameValue(t *testing.T) {
	const chainID = 900430
	outerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xa8}))
	innerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xa9}))
	inner := &recordingStatefulPrecompile{}
	outer := &reservoirHandoffPrecompile{inner: innerAddr, kind: "delegatecall"}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{outerAddr: outer, innerAddr: inner})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, outerAddr)

	value := *uint256.NewInt(7)
	_, _, _, err := vmenv.Call(cfg.Origin, outerAddr, nil,
		mdgas.MdGas{Execution: 100_000}, value, false)
	require.NoError(t, err)
	require.NoError(t, outer.callErr)
	require.Len(t, inner.calls, 1)
	delegateValue := inner.calls[0].Value
	require.True(t, delegateValue.Eq(&value),
		"the delegate frame must observe the calling frame's value")
}

// TestStatefulPrecompilePanicReleasesTheGasHandle pins the handle's lifetime
// against a panic. Erigon recovers execution panics on versioned state, so a
// release that only runs on the normal path leaves a stashed handle pointing at
// the dead frame's counters and still accepting charges.
func TestStatefulPrecompilePanicReleasesTheGasHandle(t *testing.T) {
	const chainID = 900431
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xaa}))
	var stashed *vm.PrecompileGas
	p := funcPrecompile{name: "PANIC",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			stashed = gas
			panic("precompile blew up")
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: p})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	func() {
		defer func() {
			require.NotNil(t, recover(), "the precompile has to have panicked")
		}()
		_, _, _, _ = vmenv.Call(cfg.Origin, precompileAddr, nil,
			mdgas.MdGas{Execution: 100_000, State: 5_000}, uint256.Int{}, false)
	}()

	require.NotNil(t, stashed)
	require.Equal(t, mdgas.MdGas{}, stashed.Remaining(),
		"a handle that outlived its frame must report nothing")
	require.False(t, stashed.ChargeExecution(1), "and must refuse to charge")
}

// TestVersionGatedPrecompileIsPrecompiledToTracers pins that an address the EVM
// activates at an L2 version is a precompile to the tracers too. They rebuild
// Rules from the VMContext instead of reading the EVM's, so a dropped L2Version
// makes 4byte record precompile input as a contract selector, flat traces keep
// calls they should filter, and JS isPrecompiled return false.
func TestVersionGatedPrecompileIsPrecompiledToTracers(t *testing.T) {
	const chainID = 900432
	const activeAt = 30
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x99}))
	plainAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x9a}))
	rec := &recordingStatefulPrecompile{}
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(l2Version uint64) vm.PrecompiledContracts {
		if l2Version < activeAt {
			return nil
		}
		return vm.PrecompiledContracts{precompileAddr: rec}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newL2TestConfig(t, chainID)
	cfg.ChainConfig.L2 = l2VersionRules{}
	cfg.L2Version = activeAt
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	input := []byte{0x01, 0x02, 0x03, 0x04}
	_, _, _, err := vmenv.Call(cfg.Origin, precompileAddr, input,
		mdgas.MdGas{Execution: 100_000}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Len(t, rec.calls, 1, "the EVM has to dispatch the gated address as a precompile")

	tracer, err := tracers.New("4byteTracer", nil, nil)
	require.NoError(t, err)
	tracer.OnTxStart(vmenv.GetVMContext(), nil, cfg.Origin)

	tracer.OnEnter(1, byte(vm.CALL), cfg.Origin, precompileAddr, true, input, 0, uint256.Int{}, nil)
	res, err := tracer.GetResult()
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(res), "a call into the gated precompile is not a contract selector")

	tracer.OnEnter(1, byte(vm.CALL), cfg.Origin, plainAddr, false, input, 0, uint256.Int{}, nil)
	res, err = tracer.GetResult()
	require.NoError(t, err)
	require.JSONEq(t, `{"0x01020304-0":1}`, string(res), "a plain call still records its selector")
}

func TestStatefulPrecompileCannotOverflowStateRefund(t *testing.T) {
	const chainID = 900418
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x9c}))
	refundErr := errors.New("the precompile never ran")
	p := funcPrecompile{name: "OVERFLOWREFUND",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if !gas.ChargeState(10) {
				return nil, vm.ErrOutOfGas
			}
			if gas.RefundState(math.MaxUint64) {
				refundErr = errors.New("refund of MaxUint64 accepted")
				return nil, nil
			}
			if gas.RefundState(math.MaxInt64 + 1) {
				refundErr = errors.New("refund above MaxInt64 accepted")
				return nil, nil
			}
			if !gas.RefundState(10) {
				refundErr = errors.New("refund of the charged total was rejected")
				return nil, nil
			}
			refundErr = nil
			return nil, nil
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: p})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 10_000, State: 50}, uint256.Int{}, false)
	require.NoError(t, err)
	require.NoError(t, refundErr)
	require.Equal(t, uint64(50), remaining.State, "an unrepresentable refund must leave the reservoir alone")
	require.Zero(t, gasUsed.State, "an unrepresentable refund must not move state usage")
	require.Equal(t, uint64(10_000), remaining.Execution)
}

func TestStatefulPrecompileNestedUsageFoldCannotWrap(t *testing.T) {
	const chainID = 900419
	outerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x9d}))
	innerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x9e}))
	var accepted [2]bool
	inner := funcPrecompile{name: "DEEPREFUNDINNER",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			accepted[0] = gas.RefundState(math.MaxInt64)
			accepted[1] = gas.RefundState(1)
			return nil, nil
		}}
	var callErr error
	outer := funcPrecompile{name: "DEEPREFUNDOUTER",
		run: func(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			if !gas.RefundState(1) {
				return nil, errors.New("the outer refund was rejected")
			}
			_, callErr = ctx.Call(gas, innerAddr, nil, 50_000, uint256.Int{})
			return nil, callErr
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{outerAddr: outer, innerAddr: inner})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, outerAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, outerAddr, nil, mdgas.MdGas{Execution: 100_000, State: 50}, uint256.Int{}, false)
	require.Equal(t, [2]bool{true, true}, accepted, "each refund is representable in the child's own usage")
	require.ErrorIs(t, callErr, vm.ErrGasUintOverflow, "a child usage the parent cannot adopt must fail the nested call")
	require.ErrorIs(t, err, vm.ErrGasUintOverflow)
	require.Equal(t, int64(-1), gasUsed.State, "only the parent's own refund reaches its usage")
	require.Zero(t, gasUsed.StateClamped(), "a frame that only refunded must not report state gas at the block level")
	require.Equal(t, uint64(50), remaining.State, "the entry reservoir goes back to the caller")
}

var unadoptedChildSlot = accounts.InternKey(common.BytesToHash([]byte{0x77}))

func TestStatefulPrecompileCannotSwallowUnadoptableChildUsage(t *testing.T) {
	const chainID = 900420
	outerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x9f}))
	innerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xa0}))
	cfg := newL2TestConfig(t, chainID)
	var refunds [2]bool
	inner := funcPrecompile{name: "UNADOPTABLEWRITEINNER",
		run: func(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			if err := ctx.EVM.IntraBlockState().SetState(cfg.Origin, unadoptedChildSlot, *uint256.NewInt(42)); err != nil {
				return nil, err
			}
			refunds[0] = gas.RefundState(math.MaxInt64)
			refunds[1] = gas.RefundState(1)
			return []byte{0xff}, nil
		}}
	var callRet []byte
	var callErr error
	outer := funcPrecompile{name: "SWALLOWINGOUTER",
		run: func(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			if !gas.RefundState(1) {
				return nil, errors.New("the outer refund was rejected")
			}
			callRet, callErr = ctx.Call(gas, innerAddr, nil, 50_000, uint256.Int{})
			return nil, nil
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{outerAddr: outer, innerAddr: inner})

	vmenv := prepareStatefulCall(t, cfg, outerAddr)
	ret, remaining, _, err := vmenv.Call(cfg.Origin, outerAddr, nil, mdgas.MdGas{Execution: 100_000, State: 50}, uint256.Int{}, false)
	require.Equal(t, [2]bool{true, true}, refunds, "each refund is representable in the child's own usage")
	require.ErrorIs(t, callErr, vm.ErrGasUintOverflow, "a child usage the parent cannot adopt must fail the nested call")
	require.Nil(t, callRet, "a failed nested call returns no data")

	slot, readErr := cfg.State.GetState(cfg.Origin, unadoptedChildSlot)
	require.NoError(t, readErr)
	require.True(t, slot.IsZero(), "the child's writes must not survive a usage the parent cannot adopt")

	require.ErrorIs(t, err, vm.ErrGasUintOverflow, "swallowing the nested error must not let the frame commit")
	require.Nil(t, ret, "an aborted frame returns no data")
	require.Equal(t, uint64(50), remaining.State, "the frame's entry reservoir goes back to its caller, not the child's minted one")
	require.Zero(t, remaining.Execution, "an unrepresentable frame consumes its execution gas")
}

func TestStatefulPrecompileNestedCallAbsorbsMisplacedStateGas(t *testing.T) {
	const chainID = 900440
	const charge = uint64(10)
	outerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xb0}))
	innerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xb1}))
	inner := funcPrecompile{name: "RESERVOIRREFUND",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if !gas.RefundState(charge) {
				return nil, errors.New("the child refund was rejected")
			}
			return nil, nil
		}}
	var afterCall mdgas.MdGas
	var callErr error
	outer := funcPrecompile{name: "SPILLTHENCALL",
		run: func(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			if !gas.ChargeState(charge) {
				return nil, vm.ErrOutOfGas
			}
			_, callErr = ctx.Call(gas, innerAddr, nil, 50_000, uint256.Int{})
			afterCall = gas.Remaining()
			return nil, callErr
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{outerAddr: outer, innerAddr: inner})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, outerAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, outerAddr, nil,
		mdgas.MdGas{Execution: 100_000, State: 0}, uint256.Int{}, false)
	require.NoError(t, err)
	require.NoError(t, callErr)
	require.Zero(t, afterCall.State,
		"state gas the child left in the reservoir must move back to execution gas, not stay parked")
	require.Equal(t, uint64(100_000), afterCall.Execution,
		"the frame is whole again: the 10 it spilled came back through the child's refund")
	require.Zero(t, gasUsed.StateSpill, "the absorbed spill must not be reported upward")
	require.Equal(t, uint64(100_000), remaining.Execution)
	require.Zero(t, remaining.State)
}

func TestStatefulPrecompileCannotRefundWhatAChildBurned(t *testing.T) {
	const chainID = 900441
	const burn = uint64(40_000)
	outerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xb2}))
	innerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xb3}))
	inner := funcPrecompile{name: "BURNER",
		run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
			if !gas.ChargeExecution(burn) {
				return nil, vm.ErrOutOfGas
			}
			return nil, nil
		}}
	var reclaimed, ownRefund bool
	var callErr error
	outer := funcPrecompile{name: "RECLAIMER",
		run: func(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			_, callErr = ctx.Call(gas, innerAddr, nil, 50_000, uint256.Int{})
			reclaimed = gas.RefundExecution(burn)
			if !gas.ChargeExecution(100) {
				return nil, vm.ErrOutOfGas
			}
			ownRefund = gas.RefundExecution(100)
			return nil, nil
		}}
	registerPrecompiles(t, chainID, vm.PrecompiledContracts{outerAddr: outer, innerAddr: inner})

	cfg := newL2TestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, outerAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, outerAddr, nil,
		mdgas.MdGas{Execution: 100_000}, uint256.Int{}, false)
	require.NoError(t, err)
	require.NoError(t, callErr)
	require.False(t, reclaimed, "gas a nested frame burned must not be refundable by this frame")
	require.True(t, ownRefund, "the frame's own charge stays refundable")
	require.Equal(t, uint64(100_000-burn), remaining.Execution,
		"the child's burn stays spent")
	require.Equal(t, burn, gasUsed.Execution)
}
