package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// readsUnfinishedWrite is a tx whose run reads a balance that an earlier, unfinished tx wrote.
type readsUnfinishedWrite struct {
	exec.Task
	addr accounts.Address
}

type panicsAfterDependencyRead struct {
	readsUnfinishedWrite
	cancelledBeforePanic bool
}

func (r *panicsAfterDependencyRead) Execute(evm *vm.EVM, _ rules.Engine, _ *types.Genesis, ibs *state.IntraBlockState,
	_ *chain.Config, _ rules.ChainReader, _ datadir.Dirs, _ bool,
) *exec.TxResult {
	_, _ = ibs.GetBalance(r.addr)
	r.cancelledBeforePanic = evm.Cancelled()
	panic("task panic after reading a dependency")
}

// A task that panics after its run was cancelled must not leave the worker's EVM cancelled for its next task.
func TestSpeculativeRunPanicClearsWorkerCancellation(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xce"))
	versions := state.NewVersionMap(nil)
	versions.WriteBalance(addr, state.Version{TxIndex: 1}, *uint256.NewInt(1), false)
	ibs := state.NewWithVersionMap(state.NewNoopReader(), versions)
	defer ibs.Close()
	ibs.SetTxContext(1, 2)

	evm := vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{})
	task := &panicsAfterDependencyRead{readsUnfinishedWrite: readsUnfinishedWrite{addr: addr}}
	tv := &taskVersion{execTask: &execTask{Task: task}, version: state.Version{TxIndex: 2}, versionMap: versions}

	require.Panics(t, func() {
		tv.Execute(evm, nil, nil, ibs, chain.AllProtocolChanges, nil, datadir.Dirs{}, false)
	})
	require.Equal(t, 1, ibs.DepTxIndex())
	require.True(t, task.cancelledBeforePanic, "the dependency read must cancel the run")
	require.False(t, evm.Cancelled(), "a recovered task panic must not leave the worker's EVM cancelled")
}
