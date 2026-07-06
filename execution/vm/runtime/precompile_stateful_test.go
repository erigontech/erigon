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

package runtime

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// recordingStatefulPrecompile implements vm.StatefulPrecompile and records
// the PrecompileContext of every RunStateful call for assertion.
type recordingStatefulPrecompile struct {
	calls []*vm.PrecompileContext
}

func (r *recordingStatefulPrecompile) RequiredGas([]byte) uint64        { return 0 }
func (r *recordingStatefulPrecompile) Run(input []byte) ([]byte, error) { return nil, nil }
func (r *recordingStatefulPrecompile) Name() string                     { return "RECORDING" }

func (r *recordingStatefulPrecompile) RunStateful(input []byte, gas mdgas.MdGas, ctx *vm.PrecompileContext) ([]byte, mdgas.MdGas, error) {
	r.calls = append(r.calls, ctx)
	remaining := gas
	remaining.Regular -= 111
	return []byte{0x2a}, remaining, nil
}

func newStatefulTestConfig(t *testing.T, chainID uint64) *Config {
	t.Helper()
	db := testutil.TemporalDB(t)
	tx, domains := testutil.TemporalTxSD(t, db)
	st := state.New(state.NewReaderV3(domains.AsGetter(tx)))

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
	require.NoError(t, cfg.State.Prepare(rules, cfg.Origin, cfg.Coinbase, precompileAddr, vm.ActivePrecompiles(rules), nil, nil))
	require.NoError(t, cfg.State.CreateAccount(cfg.Origin, false))
	require.NoError(t, cfg.State.AddBalance(cfg.Origin, *uint256.NewInt(1_000_000), tracing.BalanceChangeUnspecified))
	return vmenv
}

func TestStatefulPrecompileDispatch(t *testing.T) {
	const chainID = 900401
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x88}))
	rec := &recordingStatefulPrecompile{}
	vm.RegisterPrecompiles(chainID, func(*chain.Rules) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: rec}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(chainID) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	gas := mdgas.MdGas{Regular: 100000}
	value := *uint256.NewInt(7)

	ret, remaining, _, err := vmenv.Call(cfg.Origin, precompileAddr, []byte{0x01}, gas, value, false)
	require.NoError(t, err)
	require.Equal(t, []byte{0x2a}, ret)
	require.Equal(t, gas.Regular-111, remaining.Regular)

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
	vm.RegisterPrecompiles(chainID, func(*chain.Rules) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: rec}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(chainID) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	delegator := accounts.InternAddress(common.HexToAddress("0xde1e"))
	value := *uint256.NewInt(3)

	_, _, _, err := vmenv.DelegateCall(delegator, cfg.Origin, precompileAddr, []byte{0x01}, value, mdgas.MdGas{Regular: 100000})
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

func (r *reenteringStatefulPrecompile) RunStateful(input []byte, gas mdgas.MdGas, ctx *vm.PrecompileContext) ([]byte, mdgas.MdGas, error) {
	r.calls++
	if r.calls > 1100 {
		return nil, gas, nil
	}
	_, remaining, _, err := ctx.Evm.Call(ctx.Caller, r.self, input, gas, uint256.Int{}, false)
	return nil, remaining, err
}

// TestStatefulPrecompileReentryHitsDepthLimit pins that a stateful precompile
// re-entering the EVM through ctx.Evm counts against CallCreateDepth.
func TestStatefulPrecompileReentryHitsDepthLimit(t *testing.T) {
	const chainID = 900403
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8a}))
	rec := &reenteringStatefulPrecompile{self: precompileAddr}
	vm.RegisterPrecompiles(chainID, func(*chain.Rules) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: rec}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(chainID) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, _, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Regular: 1_000_000}, uint256.Int{}, false)
	require.ErrorIs(t, err, vm.ErrDepth)
	require.LessOrEqual(t, rec.calls, 1030, "recursion must be cut off by the depth limit")
}

type stateGasStatefulPrecompile struct{}

func (stateGasStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (stateGasStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (stateGasStatefulPrecompile) Name() string               { return "STATEGAS" }

func (stateGasStatefulPrecompile) RunStateful(_ []byte, gas mdgas.MdGas, _ *vm.PrecompileContext) ([]byte, mdgas.MdGas, error) {
	gas.Regular -= 100
	gas.State -= 40
	return nil, gas, nil
}

// TestStatefulPrecompileStateGasAttribution pins that State-dimension gas a
// stateful precompile consumes is reported as State usage, not folded into
// Regular.
func TestStatefulPrecompileStateGasAttribution(t *testing.T) {
	const chainID = 900404
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8b}))
	vm.RegisterPrecompiles(chainID, func(*chain.Rules) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: stateGasStatefulPrecompile{}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(chainID) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, remaining, gasUsed, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Regular: 10_000, State: 500}, uint256.Int{}, false)
	require.NoError(t, err)
	require.Equal(t, uint64(9_900), remaining.Regular)
	require.Equal(t, uint64(460), remaining.State)
	require.Equal(t, int64(40), gasUsed.State, "State consumption must be attributed to the State dimension")
	require.Equal(t, uint64(100), gasUsed.Regular, "Regular usage must not absorb the State spend")
}

type nestedCallStatefulPrecompile struct {
	target accounts.Address
}

func (nestedCallStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (nestedCallStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (nestedCallStatefulPrecompile) Name() string               { return "NESTED" }

func (p nestedCallStatefulPrecompile) RunStateful(_ []byte, gas mdgas.MdGas, ctx *vm.PrecompileContext) ([]byte, mdgas.MdGas, error) {
	_, remaining, _, err := ctx.Evm.Call(ctx.Caller, p.target, nil, gas, uint256.Int{}, false)
	return nil, remaining, err
}

// TestStatefulPrecompileStaticContextInherited pins that a nested call made
// through ctx.Evm from inside a STATICCALL'd precompile keeps write
// protection, like nested bytecode frames do.
func TestStatefulPrecompileStaticContextInherited(t *testing.T) {
	const chainID = 900405
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8c}))
	storeAddr := accounts.InternAddress(common.HexToAddress("0x5570"))
	vm.RegisterPrecompiles(chainID, func(*chain.Rules) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: nestedCallStatefulPrecompile{target: storeAddr}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(chainID) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)
	require.NoError(t, cfg.State.CreateAccount(storeAddr, true))
	require.NoError(t, cfg.State.SetCode(storeAddr, []byte{0x60, 0x01, 0x60, 0x01, 0x55, 0x00}, tracing.CodeChangeUnspecified)) // PUSH1 1 PUSH1 1 SSTORE STOP

	_, _, _, err := vmenv.StaticCall(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Regular: 1_000_000})
	require.ErrorIs(t, err, vm.ErrWriteProtection)
}

type gasMintingStatefulPrecompile struct{}

func (gasMintingStatefulPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (gasMintingStatefulPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (gasMintingStatefulPrecompile) Name() string               { return "MINT" }

func (gasMintingStatefulPrecompile) RunStateful(_ []byte, gas mdgas.MdGas, _ *vm.PrecompileContext) ([]byte, mdgas.MdGas, error) {
	gas.Regular += 1_000_000
	return nil, gas, nil
}

// TestStatefulPrecompileCannotMintGas pins that a stateful precompile
// returning more gas than it was given fails the call instead of corrupting
// frame accounting.
func TestStatefulPrecompileCannotMintGas(t *testing.T) {
	const chainID = 900406
	precompileAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x8d}))
	vm.RegisterPrecompiles(chainID, func(*chain.Rules) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{precompileAddr: gasMintingStatefulPrecompile{}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(chainID) })

	cfg := newStatefulTestConfig(t, chainID)
	vmenv := prepareStatefulCall(t, cfg, precompileAddr)

	_, _, _, err := vmenv.Call(cfg.Origin, precompileAddr, nil, mdgas.MdGas{Regular: 10_000}, uint256.Int{}, false)
	require.Error(t, err)
}
