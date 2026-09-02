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
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
	"github.com/erigontech/erigon/execution/types"
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

func newStatefulTestConfig(t *testing.T, chainID uint64) *Config {
	t.Helper()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	st := state.New(state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	t.Cleanup(st.Close)

	cfg := &Config{
		ChainConfig: &chain.Config{
			ChainID:               uint256.NewInt(chainID),
			HomesteadBlock:        new(uint64),
			TangerineWhistleBlock: new(uint64),
			SpuriousDragonBlock:   new(uint64),
			ByzantiumBlock:        new(uint64),
			ConstantinopleBlock:   new(uint64),
			PetersburgBlock:       new(uint64),
			IstanbulBlock:         new(uint64),
			MuirGlacierBlock:      new(uint64),
			BerlinBlock:           new(uint64),
			LondonBlock:           new(uint64),
			ArrowGlacierBlock:     new(uint64),
			GrayGlacierBlock:      new(uint64),
			ShanghaiTime:          new(uint64),
			CancunTime:            new(uint64),
			PragueTime:            new(uint64),
			OsakaTime:             new(uint64),
			AmsterdamTime:         new(uint64),
		},
		Origin: accounts.InternAddress(common.HexToAddress("0xcafe")),
		State:  st,
	}
	setDefaults(cfg)
	return cfg
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

func TestStatefulPrecompileDispatch(t *testing.T) {
	const chainID = 900401
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x88}))
	rec := &recordingStatefulPrecompile{}
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: rec}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
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
	require.True(t, got.Value.Eq(&value))
	require.False(t, got.ReadOnly)

	cfg2 := newStatefulTestConfig(t, chainID)
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
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: rec}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
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

type reenteringStatefulPrecompile struct {
	vm.NoStatelessRun
	self  accounts.Address
	calls int
}

func (r *reenteringStatefulPrecompile) Name() string { return "REENTER" }

func (r *reenteringStatefulPrecompile) RunStateful(input []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	r.calls++
	if r.calls > 1100 {
		return nil, nil
	}
	_, err := ctx.Call(gas, r.self, input, gas.Remaining().Execution, nil)
	return nil, err
}

// TestStatefulPrecompileReentryHitsDepthLimit pins that a stateful precompile
// re-entering the EVM through ctx.EVM counts against CallCreateDepth.
func TestStatefulPrecompileReentryHitsDepthLimit(t *testing.T) {
	const chainID = 900403
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8a}))
	rec := &reenteringStatefulPrecompile{self: precompileAddr}
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: rec}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, _, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 1_000_000}, uint256.Int{}, false)
	require.ErrorIs(t, err, vm.ErrDepth)
	require.LessOrEqual(t, rec.calls, 1030, "recursion must be cut off by the depth limit")
}

type stateGasStatefulPrecompile struct{ vm.NoStatelessRun }

func (stateGasStatefulPrecompile) Name() string { return "STATEGAS" }

func (stateGasStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
	if !gas.ChargeExecution(100) || !gas.ChargeState(40) {
		return nil, vm.ErrOutOfGas
	}
	return nil, nil
}

// TestStatefulPrecompileStateGasAttribution pins that State-dimension gas a
// stateful precompile consumes is reported as State usage, not folded into
// Execution.
func TestStatefulPrecompileStateGasAttribution(t *testing.T) {
	const chainID = 900404
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8b}))
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: stateGasStatefulPrecompile{}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 10_000, State: 500}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Equal(t, uint64(9_900), remaining.Execution)
	require.Equal(t, uint64(460), remaining.State)
	require.Equal(t, int64(40), gasUsed.State, "State consumption must be attributed to the State dimension")
	require.Equal(t, uint64(100), gasUsed.Execution, "Execution usage must not absorb the State spend")
}

type nestedCallStatefulPrecompile struct {
	vm.NoStatelessRun
	target accounts.Address
}

func (nestedCallStatefulPrecompile) Name() string { return "NESTED" }

func (p nestedCallStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	_, err := ctx.Call(gas, p.target, nil, gas.Remaining().Execution, nil)
	return nil, err
}

// TestStatefulPrecompileStaticContextInherited pins that a nested call made
// through ctx.EVM from inside a STATICCALL'd precompile keeps write
// protection, like nested bytecode frames do.
func TestStatefulPrecompileStaticContextInherited(t *testing.T) {
	const chainID = 900405
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8c}))
	storeAddr := accounts.InternAddress(common.HexToAddress("0x5570"))
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: nestedCallStatefulPrecompile{target: storeAddr}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
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
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: rec}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
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

// spillingStatefulPrecompile charges more state gas than the frame's EIP-8037
// reservoir holds, so the excess spills into execution gas.
type spillingStatefulPrecompile struct {
	vm.NoStatelessRun
	execution, state uint64
}

func (spillingStatefulPrecompile) Name() string { return "SPILL" }

func (p spillingStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
	if !gas.ChargeExecution(p.execution) || !gas.ChargeState(p.state) {
		return nil, vm.ErrOutOfGas
	}
	return nil, nil
}

// TestStatefulPrecompileStateGasSpill pins the attribution when a state charge
// outruns the reservoir: the whole charge counts as State usage and the part
// that came out of execution gas is reported as spill, rather than the charge
// being read back off the reservoir alone.
func TestStatefulPrecompileStateGasSpill(t *testing.T) {
	const chainID = 900407
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8e}))
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: spillingStatefulPrecompile{state: 40}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 10_000, State: 10}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Equal(t, uint64(9_970), remaining.Execution, "the 30 gas the reservoir could not cover comes out of execution gas")
	require.Equal(t, uint64(0), remaining.State)
	require.Equal(t, int64(40), gasUsed.State, "the whole charge is State usage, not just the reservoir's share")
	require.Equal(t, uint64(30), gasUsed.StateSpill)
	require.Equal(t, uint64(0), gasUsed.Execution, "spilled state gas must not be reported as execution usage")
}

// revertingSpillPrecompile spills state gas and then reverts.
type revertingSpillPrecompile struct{ vm.NoStatelessRun }

func (revertingSpillPrecompile) Name() string { return "REVERTSPILL" }

func (revertingSpillPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
	if !gas.ChargeExecution(100) || !gas.ChargeState(40) {
		return nil, vm.ErrOutOfGas
	}
	return nil, vm.ErrExecutionReverted
}

// TestStatefulPrecompileSpillRestoredOnRevert pins that handleFrameRevert can
// see the spill. EIP-8037 returns state gas to the parent on revert, so the 30
// that spilled into execution gas comes back while the 100 charged as
// execution gas stays spent.
func TestStatefulPrecompileSpillRestoredOnRevert(t *testing.T) {
	const chainID = 900408
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8f}))
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: revertingSpillPrecompile{}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 10_000, State: 10}, uint256.Int{}, false)
	require.ErrorIs(t, err, vm.ErrExecutionReverted)
	require.Equal(t, uint64(9_900), remaining.Execution, "the 30 spilled into execution gas is restored, the 100 charged to it is not")
	require.Equal(t, uint64(10), remaining.State, "the reservoir is restored to what the frame was handed")
}

// clearingStatefulPrecompile refunds more state gas than it charged, the way a
// frame that clears state an ancestor created does.
type clearingStatefulPrecompile struct{ vm.NoStatelessRun }

func (clearingStatefulPrecompile) Name() string { return "CLEAR" }

func (clearingStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
	if !gas.ChargeState(40) {
		return nil, vm.ErrOutOfGas
	}
	gas.RefundState(100)
	return nil, nil
}

// TestStatefulPrecompileNetStateRefundSucceeds pins that ending a frame with
// more state gas than it was handed is a valid result, not gas minting. State
// usage is signed for exactly this case, and the frame's execution usage still
// derives correctly from it.
func TestStatefulPrecompileNetStateRefundSucceeds(t *testing.T) {
	const chainID = 900409
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x90}))
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: clearingStatefulPrecompile{}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 10_000, State: 50}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Equal(t, uint64(10_000), remaining.Execution)
	require.Equal(t, uint64(110), remaining.State, "50 handed, 40 charged, 100 refunded")
	require.Equal(t, int64(-60), gasUsed.State)
	require.Equal(t, uint64(0), gasUsed.Execution, "a net state refund must not inflate execution usage")
}

// staticEscapePrecompile reaches back into the EVM the way an integrator's
// precompile would, and records what each entry point returned.
type staticEscapePrecompile struct {
	vm.NoStatelessRun
	target    accounts.Address
	callErr   error
	createErr error
}

func (*staticEscapePrecompile) Name() string { return "ESCAPE" }

func (p *staticEscapePrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	_, p.callErr = ctx.Call(gas, p.target, nil, gas.Remaining().Execution, uint256.NewInt(5))
	_, _, p.createErr = ctx.Create(gas, []byte{0x00}, gas.Remaining().Execution, nil, nil)
	return nil, nil
}

// TestStatefulPrecompileCannotEscapeStaticContext pins the value transfer and
// the account creation, which the interpreter refuses while charging gas for
// CALL and CREATE — a path ctx.EVM skips entirely.
func TestStatefulPrecompileCannotEscapeStaticContext(t *testing.T) {
	const chainID = 900410
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8f}))
	target := accounts.InternAddress(common.HexToAddress("0x7a49"))
	p := &staticEscapePrecompile{target: target}
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: p}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)
	require.NoError(t, cfg.State.AddBalance(precompileAddr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified))

	_, _, _, err := vmenv.StaticCall(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 1_000_000, State: 1_000_000})
	require.NoError(t, err)
	require.ErrorIs(t, p.callErr, vm.ErrWriteProtection, "a value-bearing CALL out of a static frame must be refused")
	require.ErrorIs(t, p.createErr, vm.ErrWriteProtection, "CREATE out of a static frame must be refused")

	balance, err := cfg.State.GetBalance(target)
	require.NoError(t, err)
	require.True(t, balance.IsZero(), "no value may leave a static frame")

	// Control arm: the same precompile under a plain CALL still moves value,
	// so the gate is scoped to the static context and not to ctx.EVM.
	p.callErr, p.createErr = nil, nil
	cfg2 := newStatefulTestConfig(t, chainID)
	vmenv2 := prepareStatefulCall(t, cfg2, precompileAddr)
	require.NoError(t, cfg2.State.AddBalance(precompileAddr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified))

	_, _, _, err = vmenv2.Call(cfg2.Origin, precompileAddr, nil, mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}, uint256.Int{}, false)
	require.NoError(t, err)
	require.NoError(t, p.callErr)
	balance, err = cfg2.State.GetBalance(target)
	require.NoError(t, err)
	require.Equal(t, uint64(5), balance.Uint64())
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
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: reservoirChargeStatefulPrecompile{amount: charge}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	type gasEvent struct{ from, to uint64 }
	var events []gasEvent

	cfg := newStatefulTestConfig(t, chainID)
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

type overRefundStatefulPrecompile struct {
	vm.NoStatelessRun
	refundErr error
}

func (*overRefundStatefulPrecompile) Name() string { return "OVERREFUND" }

func (p *overRefundStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
	if gas.RefundExecution(1) {
		p.refundErr = errors.New("refund accepted with nothing charged")
		return nil, nil
	}
	if !gas.ChargeExecution(100) {
		return nil, vm.ErrOutOfGas
	}
	if gas.RefundExecution(101) {
		p.refundErr = errors.New("refund accepted above the charged total")
		return nil, nil
	}
	if !gas.RefundExecution(100) {
		p.refundErr = errors.New("refund of the charged total was rejected")
	}
	return nil, nil
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
	p := &overRefundStatefulPrecompile{}
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: p}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Execution: handed}, uint256.Int{}, false)
	require.NoError(t, err)
	require.NoError(t, p.refundErr)
	require.Equal(t, handed, remaining.Execution, "the frame must not end holding more execution gas than it was handed")
	require.Zero(t, gasUsed.Execution)
}

type wrappedRevertStatefulPrecompile struct{ vm.NoStatelessRun }

func (wrappedRevertStatefulPrecompile) Name() string { return "WRAPREVERT" }

func (wrappedRevertStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
	if !gas.ChargeExecution(100) {
		return nil, vm.ErrOutOfGas
	}
	return nil, fmt.Errorf("precompile failed: %w", vm.ErrExecutionReverted)
}

// TestStatefulPrecompileWrappedRevertKeepsFrameGas pins the classification of a
// revert wrapped the way Go idiomatically wraps a sentinel. handleFrameRevert
// compares the bare value, so an unnormalized wrap burns the frame's leftover
// gas while the receipt still reads as reverted.
func TestStatefulPrecompileWrappedRevertKeepsFrameGas(t *testing.T) {
	const chainID = 900413
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x92}))
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: wrappedRevertStatefulPrecompile{}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil,
		mdgas.MdGas{Execution: 10_000}, uint256.Int{}, false)
	require.ErrorIs(t, err, vm.ErrExecutionReverted)
	require.Equal(t, uint64(9_900), remaining.Execution, "a revert keeps the frame's leftover gas")
}

type refusedCallStatefulPrecompile struct {
	vm.NoStatelessRun
	target  accounts.Address
	callErr error
}

func (*refusedCallStatefulPrecompile) Name() string { return "REFUSED" }

func (p *refusedCallStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	_, p.callErr = ctx.Call(gas, p.target, nil, gas.Remaining().Execution, uint256.NewInt(5))
	return nil, nil
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
	p := &refusedCallStatefulPrecompile{target: target}
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: p}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	var entered []accounts.Address
	cfg := newStatefulTestConfig(t, chainID)
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
	require.ErrorIs(t, p.callErr, vm.ErrWriteProtection)
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
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: rec}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
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
	inner         accounts.Address
	kind          string
	mutateValueTo uint64
	callErr       error
}

func (*reservoirHandoffPrecompile) Name() string { return "HANDOFF" }

func (p *reservoirHandoffPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	if p.mutateValueTo != 0 {
		ctx.Value.SetUint64(p.mutateValueTo)
	}
	switch p.kind {
	case "staticcall":
		_, p.callErr = ctx.StaticCall(gas, p.inner, nil, 50_000)
	case "callcode":
		_, p.callErr = ctx.CallCode(gas, p.inner, nil, 50_000, nil)
	case "delegatecall":
		_, p.callErr = ctx.DelegateCall(gas, p.inner, nil, 50_000)
	default:
		_, p.callErr = ctx.Call(gas, p.inner, nil, 50_000, nil)
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
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{
			outerAddr: outer,
			innerAddr: reservoirChargeStatefulPrecompile{amount: charge},
		}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
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
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: reservoirChargeStatefulPrecompile{amount: charge}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
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
	for i, kind := range []string{"call", "staticcall", "callcode", "delegatecall"} {
		t.Run(kind, func(t *testing.T) {
			chainID := uint64(900420 + i)
			outerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xa0, byte(i)}))
			innerAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xa1, byte(i)}))
			outer := &reservoirHandoffPrecompile{inner: innerAddr, kind: kind}
			vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
				return vm.PrecompiledContracts{
					outerAddr: outer,
					innerAddr: reservoirChargeStatefulPrecompile{amount: charge},
				}
			})
			t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

			cfg := newStatefulTestConfig(t, chainID)
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
	// Writing through the exported pointer must not reach the delegate frame.
	outer := &reservoirHandoffPrecompile{inner: innerAddr, kind: "delegatecall", mutateValueTo: 99}
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{outerAddr: outer, innerAddr: inner}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, outerAddr)

	value := *uint256.NewInt(7)
	_, _, _, err := vmenv.Call(cfg.Origin, outerAddr, nil,
		mdgas.MdGas{Execution: 100_000}, value, false)
	require.NoError(t, err)
	require.NoError(t, outer.callErr)
	require.Len(t, inner.calls, 1)
	require.True(t, inner.calls[0].Value.Eq(&value),
		"the delegate frame must observe the calling frame's value")
}

type panickingStatefulPrecompile struct {
	vm.NoStatelessRun
	stashed *vm.PrecompileGas
}

func (*panickingStatefulPrecompile) Name() string { return "PANIC" }

func (p *panickingStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, _ *vm.PrecompileContext) ([]byte, error) {
	p.stashed = gas
	panic("precompile blew up")
}

// TestStatefulPrecompilePanicReleasesTheGasHandle pins the handle's lifetime
// against a panic. Erigon recovers execution panics on versioned state, so a
// release that only runs on the normal path leaves a stashed handle pointing at
// the dead frame's counters and still accepting charges.
func TestStatefulPrecompilePanicReleasesTheGasHandle(t *testing.T) {
	const chainID = 900431
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xaa}))
	p := &panickingStatefulPrecompile{}
	vm.RegisterPrecompiles(uint256.NewInt(chainID), func(uint64) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: p}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chainID)) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	func() {
		defer func() {
			require.NotNil(t, recover(), "the precompile has to have panicked")
		}()
		_, _, _, _ = vmenv.Call(cfg.Origin, precompileAddr, nil,
			mdgas.MdGas{Execution: 100_000, State: 5_000}, uint256.Int{}, false)
	}()

	require.NotNil(t, p.stashed)
	require.Equal(t, mdgas.MdGas{}, p.stashed.Remaining(),
		"a handle that outlived its frame must report nothing")
	require.False(t, p.stashed.ChargeExecution(1), "and must refuse to charge")
}

// l2VersionRules stands in for an L2 stack's version oracle: it stamps the
// block context's version onto the resolved rules, which is what gates a
// registered provider.
type l2VersionRules struct{}

func (l2VersionRules) Name() string { return "test-l2" }

func (l2VersionRules) ResolveRules(l2Version, _, _ uint64, rules *chain.Rules) {
	rules.L2Version = l2Version
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

	cfg := newStatefulTestConfig(t, chainID)
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

// runtime.Execute and runtime.Call start the tracer themselves, so a partial
// VMContext there is invisible to a test that drives OnTxStart directly. The
// 4byte, flat-call and JS tracers rebuild Rules from this context: a dropped
// L2Version misclassifies a version-gated precompile, and a nil ChainConfig
// panics them outright.
func TestRuntimeStartsTracerWithFullVMContext(t *testing.T) {
	const chainID = 900434
	const activeAt = 30

	newCfg := func(t *testing.T) (*Config, *tracing.VMContext) {
		t.Helper()
		cfg := newStatefulTestConfig(t, chainID)
		cfg.ChainConfig.L2 = l2VersionRules{}
		cfg.L2Version = activeAt
		var got tracing.VMContext
		cfg.EVMConfig.Tracer = &tracing.Hooks{
			OnTxStart: func(vmctx *tracing.VMContext, _ types.Transaction, _ accounts.Address) {
				if vmctx != nil {
					got = *vmctx
				}
			},
		}
		return cfg, &got
	}

	assertFull := func(t *testing.T, got *tracing.VMContext) {
		t.Helper()
		require.NotNil(t, got.IntraBlockState, "the tracer must be started at all")
		require.NotNil(t, got.ChainConfig, "a nil ChainConfig panics the 4byte and flat tracers")
		require.Equal(t, uint64(activeAt), got.L2Version,
			"a dropped L2Version evaluates version-gated providers at 0")
	}

	t.Run("Call", func(t *testing.T) {
		cfg, got := newCfg(t)
		_, _, err := Call(accounts.InternAddress(common.BytesToAddress([]byte{0x9b})), nil, cfg)
		require.NoError(t, err)
		assertFull(t, got)
	})

	t.Run("Execute", func(t *testing.T) {
		cfg, got := newCfg(t)
		_, _, err := Execute([]byte{byte(vm.STOP)}, nil, cfg, t.TempDir())
		require.NoError(t, err)
		assertFull(t, got)
	})
}
