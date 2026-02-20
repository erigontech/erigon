package vm

import (
	"testing"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/arb/osver"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func arbRules(arbosVersion uint64) *chain.Rules {
	return &chain.Rules{
		IsArbitrum:   true,
		IsStylus:     arbosVersion >= osver.ArbosVersion_Stylus,
		IsDia:        arbosVersion >= osver.ArbosVersion_Dia,
		ArbOSVersion: arbosVersion,
	}
}

func TestPrecompiles_NonArbitrum(t *testing.T) {
	rules := &chain.Rules{IsArbitrum: false, IsCancun: true}
	precompiles := Precompiles(rules)

	_, ok := precompiles[common.BytesToAddress([]byte{1})]
	require.True(t, ok, "ecrecover should exist in standard precompiles")
}

func TestPrecompiles_BeforeArbOS30(t *testing.T) {
	rules := arbRules(osver.ArbosVersion_20)
	precompiles := Precompiles(rules)
	require.Equal(t, PrecompiledContractsBeforeArbOS30, precompiles)
}

func TestPrecompiles_StartingFromArbOS30(t *testing.T) {
	rules := arbRules(osver.ArbosVersion_Stylus)
	precompiles := Precompiles(rules)
	require.Equal(t, PrecompiledContractsStartingFromArbOS30, precompiles)
}

func TestPrecompiles_StartingFromArbOS50(t *testing.T) {
	rules := arbRules(osver.ArbosVersion_50)
	precompiles := Precompiles(rules)
	require.Equal(t, PrecompiledContractsStartingFromArbOS50, precompiles)
}

func TestRunPrecompiledContract_ReturnsMultiGas(t *testing.T) {
	ecrecover := &ecrecover{}
	input := make([]byte, 128)
	suppliedGas := uint64(100000)

	_, remainingGas, usedMG, err := RunPrecompiledContract(ecrecover, input, suppliedGas, nil, nil)
	require.NoError(t, err)

	gasCost := ecrecover.RequiredGas(input)
	require.Equal(t, suppliedGas-gasCost, remainingGas)
	require.Equal(t, gasCost, usedMG.Get(multigas.ResourceKindComputation))
	require.Equal(t, gasCost, usedMG.SingleGas())
}

func TestRunPrecompiledContract_InsufficientGas(t *testing.T) {
	ecrecover := &ecrecover{}
	input := make([]byte, 128)
	suppliedGas := uint64(1)

	_, _, usedMG, err := RunPrecompiledContract(ecrecover, input, suppliedGas, nil, nil)
	require.ErrorIs(t, err, ErrOutOfGas)
	require.Equal(t, suppliedGas, usedMG.Get(multigas.ResourceKindComputation))
}

func TestRunPrecompiledContract_WithTracer(t *testing.T) {
	ecrecover := &ecrecover{}
	input := make([]byte, 128)
	suppliedGas := uint64(100000)

	var tracedOldGas, tracedNewGas uint64
	tracer := &tracing.Hooks{
		OnGasChange: func(old, new uint64, reason tracing.GasChangeReason) {
			tracedOldGas = old
			tracedNewGas = new
		},
	}

	_, _, _, err := RunPrecompiledContract(ecrecover, input, suppliedGas, tracer, nil)
	require.NoError(t, err)
	require.Equal(t, suppliedGas, tracedOldGas)
	require.Equal(t, suppliedGas-ecrecover.RequiredGas(input), tracedNewGas)
}

func TestPrecompiledContractsP256Verify(t *testing.T) {
	addr := common.BytesToAddress([]byte{0x01, 0x00})
	p, ok := PrecompiledContractsP256Verify[addr]
	require.True(t, ok)
	require.NotNil(t, p)
}
