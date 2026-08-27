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
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// recordingStatefulPrecompile implements vm.StatefulPrecompile and records
// the PrecompileContext of every RunStateful call for assertion.
type recordingStatefulPrecompile struct {
	calls []*vm.PrecompileContext
}

var _ vm.StatefulPrecompile = (*recordingStatefulPrecompile)(nil)

func (r *recordingStatefulPrecompile) RequiredGas([]byte) uint64        { return 0 }
func (r *recordingStatefulPrecompile) Run(input []byte) ([]byte, error) { return nil, nil }
func (r *recordingStatefulPrecompile) Name() string                     { return "RECORDING" }

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

	// Value transfer to the not-yet-existing precompile account triggers the
	// EIP-2780 top-level NEW_ACCOUNT state charge; budget it in the State
	// dimension so it doesn't spill into Execution.
	gas := mdgas.MdGas{Execution: 100000, State: params.StateGasNewAccount}
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
	self  accounts.Address
	calls int
}

func (r *reenteringStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (r *reenteringStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (r *reenteringStatefulPrecompile) Name() string               { return "REENTER" }

func (r *reenteringStatefulPrecompile) RunStateful(input []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	r.calls++
	if r.calls > 1100 {
		return nil, nil
	}
	handed := gas.Remaining()
	if !gas.ChargeExecution(handed.Execution) {
		return nil, vm.ErrOutOfGas
	}
	_, leftover, _, err := ctx.Evm.Call(ctx.Caller, r.self, input, handed, uint256.Int{}, false)
	gas.RefundExecution(leftover.Execution)
	return nil, err
}

// TestStatefulPrecompileReentryHitsDepthLimit pins that a stateful precompile
// re-entering the EVM through ctx.Evm counts against CallCreateDepth.
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

type stateGasStatefulPrecompile struct{}

func (stateGasStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (stateGasStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (stateGasStatefulPrecompile) Name() string               { return "STATEGAS" }

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
	target accounts.Address
}

func (nestedCallStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (nestedCallStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (nestedCallStatefulPrecompile) Name() string               { return "NESTED" }

func (p nestedCallStatefulPrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	handed := gas.Remaining()
	if !gas.ChargeExecution(handed.Execution) {
		return nil, vm.ErrOutOfGas
	}
	_, leftover, _, err := ctx.Evm.Call(ctx.Caller, p.target, nil, handed, uint256.Int{}, false)
	gas.RefundExecution(leftover.Execution)
	return nil, err
}

// TestStatefulPrecompileStaticContextInherited pins that a nested call made
// through ctx.Evm from inside a STATICCALL'd precompile keeps write
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
	const chainID = 900410
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
type spillingStatefulPrecompile struct{ execution, state uint64 }

func (spillingStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (spillingStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (spillingStatefulPrecompile) Name() string               { return "SPILL" }

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
type revertingSpillPrecompile struct{}

func (revertingSpillPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (revertingSpillPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (revertingSpillPrecompile) Name() string               { return "REVERTSPILL" }

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
type clearingStatefulPrecompile struct{}

func (clearingStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (clearingStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (clearingStatefulPrecompile) Name() string               { return "CLEAR" }

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
	target    accounts.Address
	callErr   error
	createErr error
}

func (*staticEscapePrecompile) RequiredGas([]byte) uint64  { return 0 }
func (*staticEscapePrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (*staticEscapePrecompile) Name() string               { return "ESCAPE" }

func (p *staticEscapePrecompile) RunStateful(_ []byte, gas *vm.PrecompileGas, ctx *vm.PrecompileContext) ([]byte, error) {
	handed := gas.Remaining()
	_, _, _, p.callErr = ctx.Evm.Call(ctx.Self, p.target, nil, handed, *uint256.NewInt(5), false)
	_, _, _, _, p.createErr = ctx.Evm.Create(ctx.Self, []byte{0x00}, handed, uint256.Int{}, nil, false)
	return nil, nil
}

// TestStatefulPrecompileCannotEscapeStaticContext pins the value transfer and
// the account creation, which the interpreter refuses while charging gas for
// CALL and CREATE — a path ctx.Evm skips entirely.
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
	// so the gate is scoped to the static context and not to ctx.Evm.
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

type reservoirChargeStatefulPrecompile struct{ amount uint64 }

func (reservoirChargeStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (reservoirChargeStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (reservoirChargeStatefulPrecompile) Name() string               { return "RESERVOIR" }

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

type overRefundStatefulPrecompile struct{ refundErr error }

func (*overRefundStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (*overRefundStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (*overRefundStatefulPrecompile) Name() string               { return "OVERREFUND" }

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
