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
