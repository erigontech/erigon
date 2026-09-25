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

// Outside package vm on purpose: an integrating chain sees only the exported surface.
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

// EIP-8037 returns state gas to the parent on revert.
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
	// Frame enter/exit emit gas events too, in the execution dimension.
	require.Contains(t, events, gasEvent{reservoir, reservoir - charge},
		"a reservoir-covered state charge must still reach the tracer")
}

// Nothing downstream of RunStateful re-validates the frame's gas, so the bound has to hold here.
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

// The revert check compares the bare value, so a wrapped sentinel must still classify as a revert.
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
	for i, exceptional := range []error{vm.ErrOutOfGas, vm.ErrGasUintOverflow, vm.ErrMaxCodeSizeExceeded} {
		t.Run(exceptional.Error(), func(t *testing.T) {
			chainID := uint64(900415 + i)
			precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{byte(0x94 + i)}))
			registerPrecompiles(t, chainID, vm.PrecompiledContracts{precompileAddr: funcPrecompile{name: "MULTIWRAP",
				run: func(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
					if !gas.ChargeExecution(100) {
						return nil, vm.ErrOutOfGas
					}
					return nil, fmt.Errorf("%w: %w", exceptional, vm.ErrExecutionReverted)
				}}})

			cfg := newL2TestConfig(t, chainID)
			vmenv := prepareStatefulCall(t, cfg, precompileAddr)

			_, remaining, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil,
				mdgas.MdGas{Execution: 10_000}, uint256.Int{}, false)
			require.ErrorIs(t, err, exceptional)
			require.Zero(t, remaining.Execution, "an exceptional failure burns the frame's leftover gas")
		})
	}
}

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

// No reservoir before Amsterdam, and pre-Amsterdam accounting drops used.State entirely.
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

// Erigon recovers execution panics, so a gas handle can outlive its frame.
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

// Tracers rebuild Rules from the VMContext, so a dropped L2Version misclassifies the gated address.
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

func TestStatefulPrecompileCannotEscapeStaticContextThroughEVM(t *testing.T) {
	const chainID = 900410
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8f}))
	target := accounts.InternAddress(common.HexToAddress("0x7a49"))
	var callErr, createErr error
	p := funcPrecompile{name: "ESCAPE",
		run: func(_ []byte, _ *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
			handed := mdgas.MdGas{Execution: 100_000}
			_, _, _, callErr = ctx.EVM.Call(ctx.ActingAs, target, nil, handed, *uint256.NewInt(5), false)
			_, _, _, _, createErr = ctx.EVM.Create(ctx.ActingAs, []byte{0x00}, handed, uint256.Int{}, nil, false)
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

	_, _, _, err := vmenv.StaticCall(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 1_000_000, State: 1_000_000})
	require.NoError(t, err)
	require.ErrorIs(t, callErr, vm.ErrWriteProtection, "a value-bearing CALL out of a static frame must be refused")
	require.NotContains(t, entered, target, "a refused call must not open a frame on the target")
	require.ErrorIs(t, createErr, vm.ErrWriteProtection, "CREATE out of a static frame must be refused")

	balance, err := cfg.State.GetBalance(target)
	require.NoError(t, err)
	require.True(t, balance.IsZero(), "no value may leave a static frame")

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
	require.Equal(t, uint64(5), balance.Uint64(), "the gate is the static context, not ctx.EVM")
}
