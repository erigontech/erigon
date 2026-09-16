package stagedsync

import (
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func mkDetTask(txIndex int) *execTask {
	return &execTask{Task: &exec.TxTask{
		Header:  &types.Header{Number: *uint256.NewInt(1)},
		TxNum:   uint64(txIndex + 1),
		TxIndex: txIndex,
	}}
}

func newDetExecutor(t *testing.T, n int) *blockExecutor {
	t.Helper()
	tasks := make([]*execTask, n)
	reexec := make([]chan struct{}, n)
	for i := range tasks {
		tasks[i] = mkDetTask(i)
		reexec[i] = make(chan struct{}, 1)
	}
	return &blockExecutor{
		tasks:               tasks,
		blockIO:             &state.VersionedIO{},
		versionMap:          state.NewVersionMap(nil),
		readerIdx:           map[readerKey][]int{},
		finalizedResults:    map[int]*execResult{},
		coinbaseFlushedUpTo: -1,
		execFailed:          make([]int, n),
		slReexec:            reexec,
		slReexecFlag:        make([]atomic.Bool, n),
		writeChangedPrev:    map[int]*state.WriteSet{},
	}
}

// markValidated drives a task through pending -> in-progress -> complete, the
// state a tx reaches when it has been early-validated (trusting the worker
// verdict) but not yet finalized/sealed.
func markValidated(be *blockExecutor, tx int) {
	be.validateTasks.pushPending(tx)
	be.validateTasks.takeNextPending()
	be.validateTasks.markComplete(tx)
}

// TestValidated_ReaderReExecutedWhenWriterChangesAfterReaderCommits pins the
// detection when the reader is already committed at the moment the writer
// changes: revalidateCommittedDependents must un-commit and re-execute it.
func TestValidated_ReaderReExecutedWhenWriterChangesAfterReaderCommits(t *testing.T) {
	be := newDetExecutor(t, 2)
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	v1 := *uint256.NewInt(0x1111)
	v2 := *uint256.NewInt(0x2222)

	// W (tx0) writes key=v1, Validated at (0,0).
	wWrites := newWS().stor(addr, key, state.Version{TxIndex: 0, Incarnation: 0}, v1).build()
	be.blockIO.RecordWrites(state.Version{TxIndex: 0, Incarnation: 0}, wWrites)
	be.versionMap.FlushVersionedWrites(wWrites, false, "")
	be.versionMap.MarkWritesValidated(wWrites, nil)

	// R (tx1) early-broke on W's Validated value, recorded the dep at (0,0), and
	// is validated (committed) early.
	rReads := state.ReadSet{}
	rReads.SetHeader(addr, state.StoragePath, key, state.ReadHeader{Source: state.MapRead, Version: state.Version{TxIndex: 0, Incarnation: 0}})
	be.blockIO.RecordReads(state.Version{TxIndex: 1, Incarnation: 0}, rReads)
	be.indexReads(1, rReads)
	be.finalizedResults[1] = &execResult{TxResult: &exec.TxResult{Task: be.tasks[1].Task}}
	markValidated(be, 1)
	require.True(t, be.validateTasks.checkComplete(1))

	// W re-executes (the minority): key -> v2 at a new incarnation.
	newW := newWS().stor(addr, key, state.Version{TxIndex: 0, Incarnation: 1}, v2).build()
	be.blockIO.RecordWrites(state.Version{TxIndex: 0, Incarnation: 1}, newW)
	be.versionMap.WriteStorage(addr, key, state.Version{TxIndex: 0, Incarnation: 1}, v2, false)

	be.revalidateCommittedDependents(0, wWrites)

	require.False(t, be.validateTasks.checkComplete(1),
		"R was committed against W's in-flight value; W changed -> R must be un-committed")
	require.True(t, be.slReexecFlag[1].Load(), "R must be signalled for re-execution")
}

// TestValidated_ReaderCommittingAfterWriterChange pins the OTHER ordering: the
// writer changes BEFORE the reader commits, so revalidateCommittedDependents(W)
// fires while R is not yet a committed dependent and never re-checks it. This
// probes whether the early-validated reader (which recorded the stale value) is
// left committed-stale — the "why re-execution is not forced" case.
func TestValidated_ReaderCommittingAfterWriterChange(t *testing.T) {
	be := newDetExecutor(t, 2)
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	v1 := *uint256.NewInt(0x1111)
	v2 := *uint256.NewInt(0x2222)

	// W (tx0) Validated at (0,0)=v1.
	wWrites := newWS().stor(addr, key, state.Version{TxIndex: 0, Incarnation: 0}, v1).build()
	be.blockIO.RecordWrites(state.Version{TxIndex: 0, Incarnation: 0}, wWrites)
	be.versionMap.FlushVersionedWrites(wWrites, false, "")
	be.versionMap.MarkWritesValidated(wWrites, nil)

	// R executed against v1 and recorded the dep at (0,0) — but hasn't committed yet.
	rReads := state.ReadSet{}
	rReads.SetHeader(addr, state.StoragePath, key, state.ReadHeader{Source: state.MapRead, Version: state.Version{TxIndex: 0, Incarnation: 0}})
	be.blockIO.RecordReads(state.Version{TxIndex: 1, Incarnation: 0}, rReads)
	be.indexReads(1, rReads)
	be.finalizedResults[1] = &execResult{TxResult: &exec.TxResult{Task: be.tasks[1].Task}}

	// W re-executes to v2 BEFORE R commits; revalidate fires now (R not committed).
	newW := newWS().stor(addr, key, state.Version{TxIndex: 0, Incarnation: 1}, v2).build()
	be.blockIO.RecordWrites(state.Version{TxIndex: 0, Incarnation: 1}, newW)
	be.versionMap.WriteStorage(addr, key, state.Version{TxIndex: 0, Incarnation: 1}, v2, false)
	be.revalidateCommittedDependents(0, wWrites) // R is not complete yet -> not re-checked

	// R now commits early, trusting a worker verdict computed against the stale v1.
	markValidated(be, 1)

	// Is R's stale read noticed? Validate R against the current versionMap.
	got := be.versionMap.ValidateVersion(1, be.blockIO,
		func(rv, wv state.Version) state.VersionValidity {
			if rv != wv {
				return state.VersionInvalid
			}
			return state.VersionValid
		}, false, "")
	require.Equal(t, state.VersionInvalid, got,
		"R recorded v1@(0,0) but W is now v2@(0,1): R's read is stale and must be seen as invalid at any re-check")
	require.True(t, be.validateTasks.checkComplete(1),
		"R was committed early trusting the worker verdict; nothing re-checked it after the change")
}
