package vm

import (
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func runSelfdestructGasFn(t *testing.T, gasFn gasFunc) *state.IntraBlockState {
	t.Helper()
	self := accounts.InternAddress(common.HexToAddress("0x3333333333333333333333333333333333333333"))
	beneficiary := accounts.InternAddress(common.HexToAddress("0x4444444444444444444444444444444444444444"))
	ibs := state.NewWithVersionMap(state.NewNoopReader(), state.NewVersionMap(nil))
	t.Cleanup(func() { ibs.Release(false) })
	ibs.SetTxContext(1, 5)
	ibs.SetVersion(0)
	require.NoError(t, ibs.AddBalance(self, *uint256.NewInt(1000), 0))
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.TestChainOsakaConfig, Config{})
	scope := &CallContext{Contract: *NewContract(self, self, self, uint256.Int{})}
	scope.cacheGen++
	benVal := beneficiary.Value()
	scope.Stack.push(*new(uint256.Int).SetBytes(benVal[:]))
	_, err := gasFn(evm, scope, mdgas.MdGas{Execution: 1_000_000}, 0)
	require.NoError(t, err)
	return ibs
}

// A recorded SelfDestructPath read races the destroyer's flush under parallel
// execution, so the refund probe must not run when the refund cannot apply.
func TestGasSelfdestructNoRefunds_RecordsNoSelfDestructRead(t *testing.T) {
	t.Parallel()
	ibs := runSelfdestructGasFn(t, gasSelfdestructEIP3529)
	self := accounts.InternAddress(common.HexToAddress("0x3333333333333333333333333333333333333333"))
	reads := ibs.VersionedReads()
	_, tracked := reads.GetSelfDestruct(self)
	require.False(t, tracked)
	require.Zero(t, ibs.GetRefund())
}

func TestGasSelfdestructWithRefunds_StillRefundsAndRecordsRead(t *testing.T) {
	t.Parallel()
	ibs := runSelfdestructGasFn(t, gasSelfdestructEIP2929)
	self := accounts.InternAddress(common.HexToAddress("0x3333333333333333333333333333333333333333"))
	reads := ibs.VersionedReads()
	_, tracked := reads.GetSelfDestruct(self)
	require.True(t, tracked)
	require.Equal(t, params.SelfdestructRefundGas, ibs.GetRefund())
}

// A zero-value CALL prices the target without reading its emptiness, so gas
// records no read of it; the target is still a BAL access either way.
func TestStatefulGasCallReadsEmptinessOnlyWithValue(t *testing.T) {
	t.Parallel()
	target := accounts.InternAddress(common.HexToAddress("0x5555555555555555555555555555555555555555"))
	for _, transfersValue := range []bool{false, true} {
		ibs := state.NewWithVersionMap(state.NewNoopReader(), state.NewVersionMap(nil))
		t.Cleanup(func() { ibs.Release(false) })
		ibs.SetTxContext(1, 5)
		ibs.StartAccessRecording()
		evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.TestChainOsakaConfig, Config{})
		scope := &CallContext{}
		targetVal := target.Value()
		scope.Stack.push(*new(uint256.Int).SetBytes(targetVal[:]))
		scope.Stack.push(uint256.Int{})
		_, err := statefulGasCall(evm, scope, mdgas.MdGasCost{}, mdgas.MdGas{Execution: 1_000_000}, transfersValue)
		require.NoError(t, err)

		reads := ibs.VersionedReads()
		_, read := reads.GetAddress(target)
		require.Equal(t, transfersValue, read, "transfersValue=%v", transfersValue)

		var io state.VersionedIO
		ibs.MergeTxIOInto(&io, ibs.VersionedWrites())
		require.True(t, slices.ContainsFunc(io.AsBlockAccessList(), func(a types.AccountChanges) bool { return a.Address == target.Value() }),
			"transfersValue=%v: the target must stay a BAL access", transfersValue)
	}
}
