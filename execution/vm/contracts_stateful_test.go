package vm

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type probePrecompile struct {
	flat   uint64
	inside uint64
	err    error
	got    PrecompileEnv
}

func (p *probePrecompile) Name() string                    { return "Probe" }
func (p *probePrecompile) RequiredGas(input []byte) uint64 { return p.flat }
func (p *probePrecompile) Run([]byte) ([]byte, error) {
	return nil, errors.New("stateless dispatch of a stateful precompile")
}

func (p *probePrecompile) RunStateful(env *PrecompileEnv, input []byte) ([]byte, error) {
	p.got = *env
	p.got.Gas = nil
	env.Gas.Execution -= p.inside
	key := accounts.InternKey(common.Hash{1})
	if err := env.EVM.IntraBlockState().SetState(env.Self, key, *uint256.NewInt(1)); err != nil {
		return nil, err
	}
	return input, p.err
}

func newProbeEVM(t *testing.T) (*EVM, *state.IntraBlockState) {
	t.Helper()
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
	return evm, ibs
}

func TestStatefulPrecompileCallContext(t *testing.T) {
	var (
		probeAddr = accounts.InternAddress(common.HexToAddress("0x0b0be0"))
		caller    = accounts.InternAddress(common.HexToAddress("0xca11e5"))
		origin    = accounts.InternAddress(common.HexToAddress("0x0416019"))
		gas       = mdgas.MdGas{Execution: 100_000}
		value     = *uint256.NewInt(7)
	)

	for _, tc := range []struct {
		name       string
		invoke     func(*EVM) error
		wantCaller accounts.Address
		wantValue  uint64
		wantDirect bool
		wantRead   bool
	}{
		{
			name:       "CALL",
			invoke:     func(evm *EVM) error { _, _, _, err := evm.Call(caller, probeAddr, nil, gas, value, true); return err },
			wantCaller: caller, wantValue: 7, wantDirect: true,
		},
		{
			name:       "STATICCALL",
			invoke:     func(evm *EVM) error { _, _, _, err := evm.StaticCall(caller, probeAddr, nil, gas); return err },
			wantCaller: caller, wantDirect: true, wantRead: true,
		},
		{
			name:       "CALLCODE",
			invoke:     func(evm *EVM) error { _, _, _, err := evm.CallCode(caller, probeAddr, nil, gas, value); return err },
			wantCaller: caller, wantValue: 7,
		},
		{
			name: "DELEGATECALL",
			invoke: func(evm *EVM) error {
				_, _, _, err := evm.DelegateCall(caller, origin, probeAddr, nil, value, gas)
				return err
			},
			wantCaller: origin, wantValue: 7,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm, ibs := newProbeEVM(t)
			require.NoError(t, ibs.AddBalance(caller, value, tracing.BalanceChangeUnspecified))
			probe := &probePrecompile{}
			evm.SetPrecompiles(PrecompiledContracts{probeAddr: probe})
			require.NoError(t, tc.invoke(evm))
			require.Same(t, evm, probe.got.EVM)
			require.Equal(t, probeAddr, probe.got.Self)
			require.Equal(t, tc.wantCaller, probe.got.Caller)
			require.Equal(t, tc.wantValue, probe.got.Value.Uint64())
			require.Equal(t, tc.wantDirect, probe.got.DirectCall)
			require.Equal(t, tc.wantRead, probe.got.ReadOnly)
		})
	}
}

func TestStatefulPrecompileGasAndRevert(t *testing.T) {
	probeAddr := accounts.InternAddress(common.HexToAddress("0x0b0be0"))
	key := accounts.InternKey(common.Hash{1})

	evm, ibs := newProbeEVM(t)
	probe := &probePrecompile{flat: 100, inside: 2_000}
	evm.SetPrecompiles(PrecompiledContracts{probeAddr: probe})
	ret, left, _, err := evm.Call(accounts.ZeroAddress, probeAddr, []byte{0xaa}, mdgas.MdGas{Execution: 10_000}, uint256.Int{}, true)
	require.NoError(t, err)
	require.Equal(t, []byte{0xaa}, ret)
	require.Equal(t, uint64(10_000-100-2_000), left.Execution)
	v, err := ibs.GetState(probeAddr, key)
	require.NoError(t, err)
	require.Equal(t, uint64(1), v.Uint64())

	evm, ibs = newProbeEVM(t)
	probe = &probePrecompile{err: ErrExecutionReverted}
	evm.SetPrecompiles(PrecompiledContracts{probeAddr: probe})
	_, left, _, err = evm.Call(accounts.ZeroAddress, probeAddr, nil, mdgas.MdGas{Execution: 10_000}, uint256.Int{}, true)
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, uint64(10_000), left.Execution)
	v, err = ibs.GetState(probeAddr, key)
	require.NoError(t, err)
	require.True(t, v.IsZero())

	evm, _ = newProbeEVM(t)
	evm.SetPrecompiles(PrecompiledContracts{probeAddr: &probePrecompile{flat: 20_000}})
	_, left, _, err = evm.Call(accounts.ZeroAddress, probeAddr, nil, mdgas.MdGas{Execution: 10_000}, uint256.Int{}, true)
	require.ErrorIs(t, err, ErrOutOfGas)
	require.Zero(t, left.Execution)
}

func TestStatefulPrecompileResolver(t *testing.T) {
	routed := accounts.InternAddress(common.HexToAddress("0xca52000000000000000000000000000000000001"))
	other := accounts.InternAddress(common.HexToAddress("0xca52000000000000000000000000000000000002"))
	probe := &probePrecompile{}
	var gotRules *chain.Rules
	prev := statefulPrecompiles
	SetStatefulPrecompiles(func(rules *chain.Rules, addr accounts.Address) (StatefulPrecompile, bool) {
		gotRules = rules
		return probe, addr == routed
	})
	t.Cleanup(func() { statefulPrecompiles = prev })

	evm, _ := newProbeEVM(t)
	_, _, _, err := evm.Call(accounts.ZeroAddress, routed, nil, mdgas.MdGas{Execution: 10_000}, uint256.Int{}, true)
	require.NoError(t, err)
	require.Same(t, evm.ChainRules(), gotRules)
	require.Equal(t, routed, probe.got.Self)
	require.NotContains(t, ActivePrecompiles(evm.ChainRules()), routed)

	probe.got = PrecompileEnv{}
	_, _, _, err = evm.Call(accounts.ZeroAddress, other, nil, mdgas.MdGas{Execution: 10_000}, uint256.Int{}, true)
	require.NoError(t, err)
	require.Nil(t, probe.got.EVM)
}
