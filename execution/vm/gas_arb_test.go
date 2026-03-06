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
	for _, op := range []OpCode{SLOAD, BALANCE, EXTCODESIZE, EXTCODECOPY, EXTCODEHASH, SELFBALANCE} {
		mg := multigas.ZeroGas()
		categorizeDynamicGas(&mg, op, 200)
		require.Equal(t, uint64(200), mg.Get(multigas.ResourceKindStorageAccess), "op=%s", op)
		require.Equal(t, uint64(0), mg.Get(multigas.ResourceKindComputation), "op=%s", op)
		require.Equal(t, uint64(200), mg.SingleGas(), "op=%s", op)
	}
}

func TestCategorizeDynamicGas_StorageGrowth(t *testing.T) {
	mg := multigas.ZeroGas()
	categorizeDynamicGas(&mg, SSTORE, 5000)
	require.Equal(t, uint64(5000), mg.Get(multigas.ResourceKindStorageGrowth))
	require.Equal(t, uint64(0), mg.Get(multigas.ResourceKindComputation))
	require.Equal(t, uint64(5000), mg.SingleGas())
}

func TestCategorizeDynamicGas_HistoryGrowth(t *testing.T) {
	for _, op := range []OpCode{LOG0, LOG1, LOG2, LOG3, LOG4} {
		mg := multigas.ZeroGas()
		categorizeDynamicGas(&mg, op, 375)
		require.Equal(t, uint64(375), mg.Get(multigas.ResourceKindHistoryGrowth), "op=%s", op)
		require.Equal(t, uint64(375), mg.SingleGas(), "op=%s", op)
	}
}

func TestCategorizeDynamicGas_Computation(t *testing.T) {
	for _, op := range []OpCode{CALL, CALLCODE, DELEGATECALL, STATICCALL, CREATE, CREATE2, KECCAK256, CODECOPY, RETURNDATACOPY} {
		mg := multigas.ZeroGas()
		categorizeDynamicGas(&mg, op, 100)
		require.Equal(t, uint64(100), mg.Get(multigas.ResourceKindComputation), "op=%s", op)
		require.Equal(t, uint64(100), mg.SingleGas(), "op=%s", op)
	}
}

func TestCategorizeDynamicGas_ZeroCost(t *testing.T) {
	mg := multigas.ZeroGas()
	categorizeDynamicGas(&mg, SLOAD, 0)
	require.True(t, mg.IsZero())
}

func TestCategorizeDynamicGas_Accumulates(t *testing.T) {
	mg := multigas.ZeroGas()
	categorizeDynamicGas(&mg, SLOAD, 100)
	categorizeDynamicGas(&mg, SSTORE, 5000)
	categorizeDynamicGas(&mg, LOG0, 375)
	categorizeDynamicGas(&mg, CALL, 700)
	require.Equal(t, uint64(100), mg.Get(multigas.ResourceKindStorageAccess))
	require.Equal(t, uint64(5000), mg.Get(multigas.ResourceKindStorageGrowth))
	require.Equal(t, uint64(375), mg.Get(multigas.ResourceKindHistoryGrowth))
	require.Equal(t, uint64(700), mg.Get(multigas.ResourceKindComputation))
	require.Equal(t, uint64(6175), mg.SingleGas())
}
