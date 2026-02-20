package vm

import (
	"testing"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/holiman/uint256"
)

func newTestContract(gas uint64) *Contract {
	caller := AccountRef(common.HexToAddress("0x1"))
	addr := common.HexToAddress("0x2")
	return NewContract(caller, addr, uint256.NewInt(0), gas, NewJumpDestCache(JumpDestCacheLimit))
}

func TestUseMultiGas_TracksCorrectly(t *testing.T) {
	c := newTestContract(10000)

	mg := multigas.ComputationGas(100)
	ok := c.UseMultiGas(mg, nil, tracing.GasChangeIgnored)
	require.True(t, ok)
	require.Equal(t, uint64(100), c.UsedMultiGas.Get(multigas.ResourceKindComputation))
	require.Equal(t, uint64(9900), c.Gas)

	mg2 := multigas.StorageAccessGas(200)
	ok = c.UseMultiGas(mg2, nil, tracing.GasChangeIgnored)
	require.True(t, ok)
	require.Equal(t, uint64(100), c.UsedMultiGas.Get(multigas.ResourceKindComputation))
	require.Equal(t, uint64(200), c.UsedMultiGas.Get(multigas.ResourceKindStorageAccess))
	require.Equal(t, uint64(300), c.UsedMultiGas.SingleGas())
	require.Equal(t, uint64(9700), c.Gas)
}

func TestUseMultiGas_InsufficientGas(t *testing.T) {
	c := newTestContract(50)

	mg := multigas.ComputationGas(100)
	ok := c.UseMultiGas(mg, nil, tracing.GasChangeIgnored)
	require.False(t, ok)
	require.Equal(t, uint64(50), c.Gas)
	require.True(t, c.UsedMultiGas.IsZero())
}

func TestGetTotalUsedMultiGas_SubtractsRetained(t *testing.T) {
	c := newTestContract(10000)

	c.UsedMultiGas = multigas.ComputationGas(500)
	c.RetainedMultiGas = multigas.ComputationGas(100)

	total := c.GetTotalUsedMultiGas()
	require.Equal(t, uint64(400), total.Get(multigas.ResourceKindComputation))
	require.Equal(t, uint64(400), total.SingleGas())
}

func TestGetTotalUsedMultiGas_UnderflowClampsToZero(t *testing.T) {
	c := newTestContract(10000)

	c.UsedMultiGas = multigas.ComputationGas(100)
	c.RetainedMultiGas = multigas.ComputationGas(500)

	total := c.GetTotalUsedMultiGas()
	require.Equal(t, uint64(0), total.Get(multigas.ResourceKindComputation))
}

func TestIsDelegateOrCallcode(t *testing.T) {
	c := newTestContract(10000)
	require.False(t, c.IsDelegateOrCallcode())

	c.delegateOrCallcode = true
	require.True(t, c.IsDelegateOrCallcode())
}

func TestContract_IsDeploymentAndIsSystemCall(t *testing.T) {
	c := newTestContract(10000)
	require.False(t, c.IsDeployment)
	require.False(t, c.IsSystemCall)

	c.IsDeployment = true
	c.IsSystemCall = true
	require.True(t, c.IsDeployment)
	require.True(t, c.IsSystemCall)
}

func TestNewContract_InitializesMultiGasToZero(t *testing.T) {
	c := newTestContract(10000)
	require.True(t, c.UsedMultiGas.IsZero())
	require.True(t, c.RetainedMultiGas.IsZero())
}
