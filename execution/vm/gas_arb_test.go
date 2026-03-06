package vm

import (
	"testing"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/stretchr/testify/require"
)

func TestAddConstantMultiGas_Computation(t *testing.T) {
	mg := multigas.ZeroGas()
	addConstantMultiGas(&mg, 100, ADD)
	require.Equal(t, uint64(100), mg.Get(multigas.ResourceKindComputation))
	require.Equal(t, uint64(0), mg.Get(multigas.ResourceKindStorageAccess))
	require.Equal(t, uint64(100), mg.SingleGas())
}

func TestAddConstantMultiGas_AccumulatesMultipleCalls(t *testing.T) {
	mg := multigas.ZeroGas()
	addConstantMultiGas(&mg, 50, ADD)
	addConstantMultiGas(&mg, 30, MUL)
	require.Equal(t, uint64(80), mg.Get(multigas.ResourceKindComputation))
	require.Equal(t, uint64(80), mg.SingleGas())
}

func TestAddConstantMultiGas_SelfdestructEIP150(t *testing.T) {
	mg := multigas.ZeroGas()
	addConstantMultiGas(&mg, params.SelfdestructGasEIP150, SELFDESTRUCT)

	require.Equal(t, params.WarmStorageReadCostEIP2929, mg.Get(multigas.ResourceKindComputation))
	require.Equal(t, params.SelfdestructGasEIP150-params.WarmStorageReadCostEIP2929, mg.Get(multigas.ResourceKindStorageAccess))
	require.Equal(t, params.SelfdestructGasEIP150, mg.SingleGas())
}

func TestAddConstantMultiGas_SelfdestructNonEIP150Cost(t *testing.T) {
	mg := multigas.ZeroGas()
	addConstantMultiGas(&mg, 42, SELFDESTRUCT)

	require.Equal(t, uint64(42), mg.Get(multigas.ResourceKindComputation))
	require.Equal(t, uint64(0), mg.Get(multigas.ResourceKindStorageAccess))
}

func TestAddConstantMultiGas_ZeroCost(t *testing.T) {
	mg := multigas.ZeroGas()
	addConstantMultiGas(&mg, 0, STOP)
	require.Equal(t, uint64(0), mg.SingleGas())
	require.True(t, mg.IsZero())
}

func TestCategorizeDynamicGas_StorageAccess(t *testing.T) {
	for _, op := range []OpCode{BALANCE, EXTCODESIZE, EXTCODECOPY, EXTCODEHASH} {
		mg := multigas.ZeroGas()
		categorizeDynamicGas(&mg, op, 200)
		require.Equal(t, uint64(200), mg.Get(multigas.ResourceKindStorageAccess), "op=%s", op)
		require.Equal(t, uint64(0), mg.Get(multigas.ResourceKindComputation), "op=%s", op)
		require.Equal(t, uint64(200), mg.SingleGas(), "op=%s", op)
	}
}

func TestCategorizeDynamicGas_SkipsSelfCategorizing(t *testing.T) {
	for _, op := range []OpCode{SLOAD, SSTORE, CALL, CALLCODE, STATICCALL, DELEGATECALL, LOG0, LOG1, LOG2, LOG3, LOG4} {
		mg := multigas.ZeroGas()
		categorizeDynamicGas(&mg, op, 5000)
		require.True(t, mg.IsZero(), "op=%s should be skipped by categorizeDynamicGas", op)
	}
}

func TestCategorizeDynamicGas_Computation(t *testing.T) {
	for _, op := range []OpCode{CREATE, CREATE2, KECCAK256, CODECOPY, RETURNDATACOPY} {
		mg := multigas.ZeroGas()
		categorizeDynamicGas(&mg, op, 100)
		require.Equal(t, uint64(100), mg.Get(multigas.ResourceKindComputation), "op=%s", op)
		require.Equal(t, uint64(100), mg.SingleGas(), "op=%s", op)
	}
}

func TestCategorizeDynamicGas_ZeroCost(t *testing.T) {
	mg := multigas.ZeroGas()
	categorizeDynamicGas(&mg, CREATE, 0)
	require.True(t, mg.IsZero())
}

func TestCategorizeDynamicGas_Accumulates(t *testing.T) {
	mg := multigas.ZeroGas()
	categorizeDynamicGas(&mg, BALANCE, 100)
	categorizeDynamicGas(&mg, CREATE, 700)
	categorizeDynamicGas(&mg, EXTCODESIZE, 50)
	require.Equal(t, uint64(150), mg.Get(multigas.ResourceKindStorageAccess))
	require.Equal(t, uint64(700), mg.Get(multigas.ResourceKindComputation))
	require.Equal(t, uint64(850), mg.SingleGas())
}

func TestCategorizeAclSstoreGas_ColdAccess(t *testing.T) {
	mg := multigas.ZeroGas()
	totalGas := params.ColdSloadCostEIP2929 + params.SstoreSetGasEIP2200
	categorizeAclSstoreGas(&mg, params.ColdSloadCostEIP2929, totalGas)

	require.Equal(t, params.ColdSloadCostEIP2929, mg.Get(multigas.ResourceKindStorageAccess))
	require.Equal(t, params.SstoreSetGasEIP2200, mg.Get(multigas.ResourceKindStorageGrowth))
	require.Equal(t, totalGas, mg.SingleGas())
}

func TestCategorizeAclSstoreGas_WarmAccess(t *testing.T) {
	mg := multigas.ZeroGas()
	writeCost := params.WarmStorageReadCostEIP2929
	categorizeAclSstoreGas(&mg, 0, writeCost)

	require.Equal(t, uint64(0), mg.Get(multigas.ResourceKindStorageAccess))
	require.Equal(t, writeCost, mg.Get(multigas.ResourceKindStorageGrowth))
	require.Equal(t, writeCost, mg.SingleGas())
}

func TestCategorizeAclSstoreGas_ColdResetSlot(t *testing.T) {
	mg := multigas.ZeroGas()
	writeCost := params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929
	totalGas := params.ColdSloadCostEIP2929 + writeCost
	categorizeAclSstoreGas(&mg, params.ColdSloadCostEIP2929, totalGas)

	require.Equal(t, params.ColdSloadCostEIP2929, mg.Get(multigas.ResourceKindStorageAccess))
	require.Equal(t, writeCost, mg.Get(multigas.ResourceKindStorageGrowth))
	require.Equal(t, totalGas, mg.SingleGas())
}
