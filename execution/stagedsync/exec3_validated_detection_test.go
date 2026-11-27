package stagedsync

import (
	"fmt"
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
	poolTasks := make([]*poolTask, n)
	for i := range tasks {
		tasks[i] = mkDetTask(i)
		poolTasks[i] = &poolTask{}
	}
	return &blockExecutor{
		tasks:               tasks,
		blockIO:             &state.VersionedIO{},
		versionMap:          state.NewVersionMap(nil),
		readerByAddr:        map[accounts.Address][]addrReader{},
		finalizedResults:    map[int]*execResult{},
		coinbaseFlushedUpTo: -1,
		execFailed:          make([]int, n),
		poolTasks:           poolTasks,
		writeChangedPrev:    map[int]*state.WriteSet{},
		revalidate:          map[int]bool{},
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
	be.versionMap.FlushWrites(wWrites, false)
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
	require.True(t, be.poolTasks[1].pending, "R must be signalled for re-execution")
}

// TestValidated_CrossPathLifecycleWriteReExecutesReader pins item 2 at the executor
// level: a reader committed against a live account must be un-committed when a lower-
// index tx self-destructs that account, even though the reader read a DIFFERENT field
// (balance) than the destruct writes (SelfDestruct). The revalidation index must select
// the reader by address for a whole-account lifecycle write, not only by the exact
// written cell — otherwise R keeps a stale live balance and the block root is wrong.
func TestValidated_CrossPathLifecycleWriteReExecutesReader(t *testing.T) {
	be := newDetExecutor(t, 3)
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	baseBal := *uint256.NewInt(1_000)

	// Base: addr live with a balance at (0,0), Validated.
	baseW := newWS().bal(addr, state.Version{TxIndex: 0, Incarnation: 0}, baseBal).build()
	be.blockIO.RecordWrites(state.Version{TxIndex: 0, Incarnation: 0}, baseW)
	be.versionMap.FlushWrites(baseW, false)
	be.versionMap.MarkWritesValidated(baseW, nil)

	// R (tx2) read addr.BALANCE at (0,0) while live, recorded the dep, committed early.
	rReads := state.ReadSet{}
	rReads.SetHeader(addr, state.BalancePath, accounts.NilKey, state.ReadHeader{Source: state.MapRead, Version: state.Version{TxIndex: 0, Incarnation: 0}})
	be.blockIO.RecordReads(state.Version{TxIndex: 2, Incarnation: 0}, rReads)
	be.indexReads(2, rReads)
	be.finalizedResults[2] = &execResult{TxResult: &exec.TxResult{Task: be.tasks[2].Task}}
	markValidated(be, 2)
	require.True(t, be.validateTasks.checkComplete(2))

	// W1 (tx1) self-destructs addr — writes SelfDestructPath only, never BalancePath.
	sdW := newWS().selfDestruct(addr, state.Version{TxIndex: 1, Incarnation: 0}, true).build()
	be.blockIO.RecordWrites(state.Version{TxIndex: 1, Incarnation: 0}, sdW)
	be.versionMap.FlushWrites(sdW, false)
	be.versionMap.MarkWritesValidated(sdW, nil)

	be.revalidateCommittedDependents(1, nil)

	require.False(t, be.validateTasks.checkComplete(2),
		"R read a live balance; a later self-destruct of that account must un-commit R (cross-path lifecycle dep)")
	require.True(t, be.poolTasks[2].pending, "R must be signalled for re-execution")
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
	be.versionMap.FlushWrites(wWrites, false)
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

// A write that a writer's new incarnation drops may not be in the BAL, so the BAL
// skip must not hide the readers of that dropped cell.
func TestRevalCandidates_BALKeepsReadersOfDroppedWrite(t *testing.T) {
	be := newDetExecutor(t, 2)
	be.versionMap.HasBAL = true
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	key := accounts.InternKey(common.HexToHash("0x01"))

	oldW := newWS().stor(addr, key, state.Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(0x1111)).build()
	rReads := state.ReadSet{}
	rReads.SetHeader(addr, state.StoragePath, key, state.ReadHeader{Source: state.MapRead, Version: state.Version{TxIndex: 0, Incarnation: 0}})
	be.indexReads(1, rReads)

	require.Equal(t, []int{1}, be.revalCandidates(0, newWS().build(), oldW))
}

func TestFinalizeFlushFlagsTheReaderOfAChangedCell(t *testing.T) {
	be := newDetExecutor(t, 4)
	be.versionMap.HasBAL = true
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	slot := accounts.InternKey(common.HexToHash("0x01"))
	reads := state.ReadSet{}
	reads.SetHeader(addr, state.StoragePath, slot, state.ReadHeader{Source: state.StorageRead})
	be.blockIO.RecordReads(state.Version{TxIndex: 2}, reads)
	be.indexReads(2, reads)

	ws := newWS().stor(addr, slot, state.Version{TxIndex: 1}, *uint256.NewInt(1)).build()
	be.versionMap.FlushWrites(ws, true)
	be.markReadersDirty(1, ws)

	require.Equal(t, map[int]bool{2: true}, be.revalidate)
}

// A reader that read a BAL seed from the version map stays valid and clean while the writer's flush leaves the seed
// untouched, and is flagged when the writer's value differs from the seed.
func TestMarkReadersDirtyOnBALSeededAccountPaths(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	writer := state.Version{TxIndex: 1}
	slot := accounts.InternKey(common.HexToHash("0x01"))
	seedCode, otherCode := accounts.NewCode([]byte{0x60, 0x01}), accounts.NewCode([]byte{0x60, 0x02})
	codeSeed := types.AccountChanges{CodeChanges: []*types.CodeChange{{Index: 2, Bytecode: seedCode.Bytes}}}
	codeWrites := func(c accounts.Code) *state.WriteSet {
		return newWS().code(addr, writer, c).codeHash(addr, writer, c.Hash).codeSize(addr, writer, c.Len()).build()
	}
	cases := []struct {
		name         string
		path         state.AccountPath
		seed         types.AccountChanges
		same, differ *state.WriteSet
	}{
		{
			"balance", state.BalancePath,
			types.AccountChanges{BalanceChanges: []*types.BalanceChange{{Index: 2, Value: *uint256.NewInt(7)}}},
			newWS().bal(addr, writer, *uint256.NewInt(7)).build(), newWS().bal(addr, writer, *uint256.NewInt(8)).build(),
		},
		{
			"nonce", state.NoncePath,
			types.AccountChanges{NonceChanges: []*types.NonceChange{{Index: 2, Value: 7}}},
			newWS().nonce(addr, writer, 7).build(), newWS().nonce(addr, writer, 8).build(),
		},
		{"code", state.CodePath, codeSeed, codeWrites(seedCode), codeWrites(otherCode)},
		{
			"code hash", state.CodeHashPath, codeSeed,
			newWS().codeHash(addr, writer, seedCode.Hash).build(), newWS().codeHash(addr, writer, otherCode.Hash).build(),
		},
		{
			"code size", state.CodeSizePath, codeSeed,
			newWS().codeSize(addr, writer, seedCode.Len()).build(), newWS().codeSize(addr, writer, seedCode.Len()+1).build(),
		},
		{
			"storage", state.StoragePath,
			types.AccountChanges{StorageChanges: []types.SlotChanges{{Slot: slot, Changes: []*types.StorageChange{{Index: 2, Value: *uint256.NewInt(7)}}}}},
			newWS().stor(addr, slot, writer, *uint256.NewInt(7)).build(), newWS().stor(addr, slot, writer, *uint256.NewInt(8)).build(),
		},
	}
	for _, c := range cases {
		newBE := func() *blockExecutor {
			be := newDetExecutor(t, 4)
			seed := c.seed
			seed.Address = addr.Value()
			be.versionMap = state.NewVersionMap(types.BlockAccessList{seed})
			rs := state.ReadSet{}
			key := accounts.NilKey
			if c.path == state.StoragePath {
				key = slot
			}
			rs.SetHeader(addr, c.path, key, state.ReadHeader{Source: state.MapRead, Version: writer})
			be.blockIO.RecordReads(state.Version{TxIndex: 2}, rs)
			be.indexReads(2, rs)
			return be
		}
		valid := func(be *blockExecutor) state.VersionValidity {
			return be.versionMap.ValidateVersion(2, be.blockIO, func(rv, wv state.Version) state.VersionValidity {
				if rv != wv {
					return state.VersionInvalid
				}
				return state.VersionValid
			}, false, "")
		}
		t.Run(c.name+" seed left untouched", func(t *testing.T) {
			be := newBE()
			require.Equal(t, state.VersionValid, valid(be))
			be.versionMap.FlushWrites(c.same, true)
			be.markReadersDirty(1, c.same)
			require.Empty(t, be.revalidate)
			require.Equal(t, state.VersionValid, valid(be))
		})
		t.Run(c.name+" seed overwritten", func(t *testing.T) {
			be := newBE()
			be.markReadersDirty(1, c.differ)
			require.Equal(t, map[int]bool{2: true}, be.revalidate)
		})
	}
}

// A re-run that no longer writes a value the access list seeds at its tx puts the seed back instead
// of removing the cell. Without the cell, a later tx would read the committed value, and validation
// accepts a committed read of a seeded path once the seed is back. A value the access list seeds
// only below the tx is not the tx's write, so the cell is removed.
func TestSelfLoopFlushKeepsTheSeedOfADroppedWrite(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	slot := accounts.InternKey(common.HexToHash("0x01"))
	earlier := state.Version{TxIndex: 1}
	seedCode, otherCode := accounts.NewCode([]byte{0x60, 0x01}), accounts.NewCode([]byte{0x60, 0x02})
	cases := []struct {
		name    string
		seed    func(index uint32) types.AccountChanges
		earlier *state.WriteSet
		paths   []state.AccountPath
		value   func(*state.VersionMap) any
		want    any
	}{
		{
			"balance",
			func(index uint32) types.AccountChanges {
				return types.AccountChanges{BalanceChanges: []*types.BalanceChange{{Index: index, Value: *uint256.NewInt(7)}}}
			},
			newWS().bal(addr, earlier, *uint256.NewInt(8)).build(),
			[]state.AccountPath{state.BalancePath},
			func(vm *state.VersionMap) any { v, _, _ := vm.ReadBalance(addr, 2); return v }, *uint256.NewInt(7),
		},
		{
			"balance of a removed account",
			func(index uint32) types.AccountChanges {
				return types.AccountChanges{BalanceChanges: []*types.BalanceChange{{Index: index, Value: *uint256.NewInt(7)}}}
			},
			newWS().emptyRemoval(addr, earlier).build(),
			[]state.AccountPath{state.BalancePath},
			func(vm *state.VersionMap) any { v, _, _ := vm.ReadBalance(addr, 2); return v }, *uint256.NewInt(7),
		},
		{
			"nonce",
			func(index uint32) types.AccountChanges {
				return types.AccountChanges{NonceChanges: []*types.NonceChange{{Index: index, Value: 7}}}
			},
			newWS().nonce(addr, earlier, 8).build(),
			[]state.AccountPath{state.NoncePath},
			func(vm *state.VersionMap) any { v, _, _ := vm.ReadNonce(addr, 2); return v }, uint64(7),
		},
		{
			"code",
			func(index uint32) types.AccountChanges {
				return types.AccountChanges{CodeChanges: []*types.CodeChange{{Index: index, Bytecode: seedCode.Bytes}}}
			},
			newWS().code(addr, earlier, otherCode).codeHash(addr, earlier, otherCode.Hash).codeSize(addr, earlier, otherCode.Len()+1).build(),
			[]state.AccountPath{state.CodePath, state.CodeHashPath, state.CodeSizePath},
			func(vm *state.VersionMap) any {
				code, _, _ := vm.ReadCode(addr, 2)
				hash, _, _ := vm.ReadCodeHash(addr, 2)
				size, _, _ := vm.ReadCodeSize(addr, 2)
				return []any{code.Bytes, hash, size}
			},
			[]any{seedCode.Bytes, seedCode.Hash, seedCode.Len()},
		},
		{
			"storage",
			func(index uint32) types.AccountChanges {
				return types.AccountChanges{StorageChanges: []types.SlotChanges{{Slot: slot, Changes: []*types.StorageChange{{Index: index, Value: *uint256.NewInt(7)}}}}}
			},
			newWS().stor(addr, slot, earlier, *uint256.NewInt(8)).build(),
			[]state.AccountPath{state.StoragePath},
			func(vm *state.VersionMap) any { v, _, _ := vm.ReadStorage(addr, slot, 2); return v }, *uint256.NewInt(7),
		},
	}
	dropping := state.Version{TxIndex: 1, Incarnation: 1}
	for _, c := range cases {
		for _, at := range []struct {
			name  string
			index uint32
			want  state.Version
		}{
			{"seeded at the tx", 2, dropping},
			{"seeded below the tx", 1, state.Version{TxIndex: 0}},
		} {
			t.Run(c.name+" "+at.name, func(t *testing.T) {
				be := newDetExecutor(t, 3)
				seed := c.seed(at.index)
				seed.Address = addr.Value()
				be.versionMap = state.NewVersionMap(types.BlockAccessList{seed})
				run := &exec.TxResult{Task: be.tasks[1].Task, TxOut: c.earlier}
				be.selfLoopFlush(earlier, run, nil)
				be.selfLoopFlush(dropping, &exec.TxResult{Task: be.tasks[1].Task, TxOut: &state.WriteSet{}}, run.TxOut)

				key := accounts.NilKey
				if c.paths[0] == state.StoragePath {
					key = slot
				}
				for _, path := range c.paths {
					res := be.versionMap.ReadStatus(addr, path, key, 2)
					require.Equal(t, state.MVReadResultDone, res.Status(), path)
					require.Equal(t, at.want, res.Version(), path)
					require.NotEqual(t, state.ValueDeleted, res.ValStatus(), path)
				}
				require.Equal(t, c.want, c.value(be.versionMap))
			})
		}
	}
}

func TestMarkReadersDirty(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	slot1 := accounts.InternKey(common.HexToHash("0x01"))
	slot2 := accounts.InternKey(common.HexToHash("0x02"))
	read := func(be *blockExecutor, tx int, incarnation state.Incarnation, key accounts.StorageKey) {
		rs := state.ReadSet{}
		rs.SetHeader(addr, state.StoragePath, key, state.ReadHeader{Source: state.StorageRead})
		be.blockIO.RecordReads(state.Version{TxIndex: tx, Incarnation: incarnation}, rs)
		be.indexReads(tx, rs)
	}
	newBE := func() *blockExecutor {
		be := newDetExecutor(t, 4)
		read(be, 0, 0, slot1)
		read(be, 2, 0, slot1)
		read(be, 3, 0, slot2)
		return be
	}
	writer := state.Version{TxIndex: 1}

	t.Run("storage write flags only the later reader of the slot", func(t *testing.T) {
		be := newBE()
		be.markReadersDirty(1, newWS().stor(addr, slot1, writer, *uint256.NewInt(1)).build())
		require.Equal(t, map[int]bool{2: true}, be.revalidate)
	})
	t.Run("self-destruct flags every later reader of the address", func(t *testing.T) {
		be := newBE()
		be.markReadersDirty(1, newWS().selfDestruct(addr, writer, true).build())
		require.Equal(t, map[int]bool{2: true, 3: true}, be.revalidate)
	})
	t.Run("a reader whose latest incarnation no longer reads the cell is not flagged", func(t *testing.T) {
		be := newBE()
		read(be, 2, 1, slot2)
		be.markReadersDirty(1, newWS().stor(addr, slot1, writer, *uint256.NewInt(1)).build())
		require.Empty(t, be.revalidate)
	})
	seeded := types.BlockAccessList{{Address: addr.Value(), StorageChanges: []types.SlotChanges{{Slot: slot1, Changes: []*types.StorageChange{{Index: 2, Value: *uint256.NewInt(1)}}}}}}
	t.Run("a write the BAL already holds leaves the readers clean", func(t *testing.T) {
		be := newBE()
		be.versionMap = state.NewVersionMap(seeded)
		be.markReadersDirty(1, newWS().stor(addr, slot1, writer, *uint256.NewInt(1)).build())
		require.Empty(t, be.revalidate)
	})
	t.Run("a write the BAL holds leaves the readers clean after validation", func(t *testing.T) {
		be := newBE()
		be.versionMap = state.NewVersionMap(seeded)
		ws := newWS().stor(addr, slot1, writer, *uint256.NewInt(1)).build()
		be.versionMap.FlushWrites(ws, false)
		be.versionMap.MarkWritesValidated(ws, func(accounts.Address) bool { return false })
		be.markReadersDirty(1, ws)
		require.Empty(t, be.revalidate)
	})
	t.Run("a Done cell a re-run left with the BAL value flags the readers", func(t *testing.T) {
		be := newBE()
		be.versionMap = state.NewVersionMap(seeded)
		be.versionMap.WriteStorage(addr, slot1, state.Version{TxIndex: 1, Incarnation: 2}, *uint256.NewInt(1), true)
		be.markReadersDirty(1, newWS().stor(addr, slot1, state.Version{TxIndex: 1, Incarnation: 3}, *uint256.NewInt(1)).build())
		require.Equal(t, map[int]bool{2: true}, be.revalidate)
	})
	t.Run("a write the BAL does not hold flags the readers", func(t *testing.T) {
		be := newBE()
		be.versionMap = state.NewVersionMap(types.BlockAccessList{{Address: addr.Value(), StorageChanges: []types.SlotChanges{{Slot: slot2, Changes: []*types.StorageChange{{Index: 2, Value: *uint256.NewInt(1)}}}}}})
		be.markReadersDirty(1, newWS().stor(addr, slot1, writer, *uint256.NewInt(1)).build())
		require.Equal(t, map[int]bool{2: true}, be.revalidate)
	})
	t.Run("a write that differs from the BAL flags the readers", func(t *testing.T) {
		be := newBE()
		be.versionMap = state.NewVersionMap(seeded)
		be.markReadersDirty(1, newWS().stor(addr, slot1, writer, *uint256.NewInt(2)).build())
		require.Equal(t, map[int]bool{2: true}, be.revalidate)
	})
	t.Run("a BAL block still flags readers of a lifecycle write", func(t *testing.T) {
		be := newBE()
		be.versionMap = state.NewVersionMap(seeded)
		be.markReadersDirty(1, newWS().stor(addr, slot1, writer, *uint256.NewInt(1)).selfDestruct(addr, writer, true).build())
		require.Equal(t, map[int]bool{2: true, 3: true}, be.revalidate)
	})
	writes := map[state.AccountPath]*state.WriteSet{
		state.BalancePath:  newWS().bal(addr, writer, *uint256.NewInt(1)).build(),
		state.NoncePath:    newWS().nonce(addr, writer, 1).build(),
		state.CodePath:     newWS().code(addr, writer, accounts.Code{}).build(),
		state.CodeHashPath: newWS().codeHash(addr, writer, accounts.CodeHash{}).build(),
		state.CodeSizePath: newWS().codeSize(addr, writer, 1).build(),
		state.StoragePath:  newWS().stor(addr, slot1, writer, *uint256.NewInt(1)).build(),
	}
	for written, ws := range writes {
		for read := state.AddressPath; read <= state.CreateContractPath; read++ {
			t.Run(fmt.Sprintf("write %d read %d", written, read), func(t *testing.T) {
				be := newDetExecutor(t, 4)
				rs := state.ReadSet{}
				rs.SetHeader(addr, read, slot1, state.ReadHeader{Source: state.StorageRead})
				be.blockIO.RecordReads(state.Version{TxIndex: 2}, rs)
				be.indexReads(2, rs)
				be.markReadersDirty(1, ws)
				require.Equal(t, read == written, be.revalidate[2])
			})
		}
	}
	for _, written := range []state.AccountPath{state.BalancePath, state.NoncePath} {
		t.Run(fmt.Sprintf("a reader of balance and nonce is flagged by a write %d", written), func(t *testing.T) {
			be := newDetExecutor(t, 4)
			rs := state.ReadSet{}
			rs.SetHeader(addr, state.BalancePath, accounts.NilKey, state.ReadHeader{Source: state.StorageRead})
			rs.SetHeader(addr, state.NoncePath, accounts.NilKey, state.ReadHeader{Source: state.StorageRead})
			be.blockIO.RecordReads(state.Version{TxIndex: 2}, rs)
			be.indexReads(2, rs)
			be.markReadersDirty(1, writes[written])
			require.True(t, be.revalidate[2])
		})
	}
}
