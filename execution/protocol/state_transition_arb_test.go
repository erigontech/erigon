package protocol

import (
	"testing"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestRevertedTxGasUsed_KnownTx(t *testing.T) {
	knownTxHash := common.HexToHash("0x58df300a7f04fe31d41d24672786cbe1c58b4f3d8329d0d74392d814dd9f7e40")
	gasUsed, ok := RevertedTxGasUsed[knownTxHash]
	require.True(t, ok, "known reverted tx should be in the map")
	require.Equal(t, uint64(45174), gasUsed)
}

func TestRevertedTxGasUsed_UnknownTx(t *testing.T) {
	unknownHash := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	_, ok := RevertedTxGasUsed[unknownHash]
	require.False(t, ok)
}

func TestIntrinsicMultiGas_BasicTx(t *testing.T) {
	data := []byte{0x01, 0x02, 0x00, 0x03}
	mg, floorGas, overflow := multigas.IntrinsicMultiGas(data, 0, 0, false, true, true, false, false, false, 0)
	require.False(t, overflow)
	require.Greater(t, mg.SingleGas(), uint64(0))
	require.Equal(t, params.TxGas, floorGas)

	require.Greater(t, mg.Get(multigas.ResourceKindComputation), uint64(0))
	require.Greater(t, mg.Get(multigas.ResourceKindL2Calldata), uint64(0))
}

func TestIntrinsicMultiGas_ContractCreation(t *testing.T) {
	data := []byte{0x01, 0x02}
	mg, _, overflow := multigas.IntrinsicMultiGas(data, 0, 0, true, true, true, false, false, false, 0)
	require.False(t, overflow)
	require.GreaterOrEqual(t, mg.Get(multigas.ResourceKindComputation), params.TxGasContractCreation)
}

func TestIntrinsicMultiGas_WithAccessList(t *testing.T) {
	data := []byte{0x01}
	mg, _, overflow := multigas.IntrinsicMultiGas(data, 2, 3, false, true, true, false, false, false, 0)
	require.False(t, overflow)
	require.Greater(t, mg.Get(multigas.ResourceKindStorageAccess), uint64(0))
	expectedAccess := 2*params.TxAccessListAddressGas + 3*params.TxAccessListStorageKeyGas
	require.Equal(t, expectedAccess, mg.Get(multigas.ResourceKindStorageAccess))
}

func TestIntrinsicMultiGas_WithAuthorizations(t *testing.T) {
	data := []byte{0x01}
	mg, _, overflow := multigas.IntrinsicMultiGas(data, 0, 0, false, true, true, false, false, false, 2)
	require.False(t, overflow)
	require.Equal(t, 2*params.CallNewAccountGas, mg.Get(multigas.ResourceKindStorageGrowth))
}

func TestArbitrumRules_BlobGasSkipped(t *testing.T) {
	rules := &chain.Rules{
		IsArbitrum: true,
		IsCancun:   true,
	}
	require.True(t, rules.IsArbitrum)
	require.True(t, rules.IsCancun)
}

func TestArbitrumRules_GasLimitCapSkipped(t *testing.T) {
	rules := &chain.Rules{
		IsArbitrum: true,
		IsOsaka:    true,
	}
	require.True(t, rules.IsArbitrum)
	require.True(t, rules.IsOsaka)
}
