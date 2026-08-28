package state

import (
	"testing"
	"unsafe"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestJournalEntrySize guards the compact union against accidental bloat,
// e.g. inlining rare *stateObject/[]byte fields that belong in journalExtra.
func TestJournalEntrySize(t *testing.T) {
	if got := unsafe.Sizeof(journalEntry{}); got > 72 {
		t.Fatalf("journalEntry grew to %d B (want <= 72)", got)
	}
}

// TestJournalDirtySymmetry pins every constructor's dirties accounting against
// dirtied(), which revert uses to undo it. A kind the two disagree about leaves
// the dirties refcount unbalanced — under-dirtying drops a modified account and
// diverges the state root, over-dirtying leaks entries across the revert.
func TestJournalDirtySymmetry(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	val := *uint256.NewInt(42)

	cases := []struct {
		name string
		kind entryKind
		call func(j *journal)
	}{
		{"createObjectChange", kindCreateObject, func(j *journal) { j.createObjectChange(addr) }},
		{"resetObjectChange", kindResetObject, func(j *journal) { j.resetObjectChange(addr, nil, nil) }},
		{"selfdestructChange", kindSelfdestruct, func(j *journal) { j.selfdestructChange(addr, false, val, false) }},
		{"selfdestructChangeVersioned", kindSelfdestruct, func(j *journal) {
			j.selfdestructChangeVersioned(addr, false, val, false, false, 0, false, val)
		}},
		{"balanceChange", kindBalance, func(j *journal) { j.balanceChange(addr, val, false) }},
		{"balanceIncrease", kindBalanceIncrease, func(j *journal) { j.balanceIncrease(addr, val) }},
		{"balanceIncreaseTransfer", kindBalanceIncreaseTransfer, func(j *journal) {
			j.balanceIncreaseTransfer(&BalanceIncrease{})
		}},
		{"nonceChange", kindNonce, func(j *journal) { j.nonceChange(addr, 1, false) }},
		{"storageChange", kindStorage, func(j *journal) { j.storageChange(addr, key, val, false) }},
		{"fakeStorageChange", kindFakeStorage, func(j *journal) { j.fakeStorageChange(addr, key, val) }},
		{"codeChange", kindCode, func(j *journal) { j.codeChange(addr, nil, accounts.CodeHash{}, false) }},
		{"refundChange", kindRefund, func(j *journal) { j.refundChange(7) }},
		{"addLogChange", kindAddLog, func(j *journal) { j.addLogChange(3) }},
		{"touchAccount", kindTouch, func(j *journal) { j.touchAccount(addr, false, val) }},
		{"accessListAddAccountChange", kindAccessListAddAccount, func(j *journal) { j.accessListAddAccountChange(addr) }},
		{"accessListAddSlotChange", kindAccessListAddSlot, func(j *journal) { j.accessListAddSlotChange(addr, key) }},
		{"transientStorageChange", kindTransientStorage, func(j *journal) { j.transientStorageChange(addr, key, val) }},
	}

	covered := map[entryKind]bool{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			j := newJournal()
			defer j.release()
			tc.call(j)

			if len(j.entries) != 1 {
				t.Fatalf("appended %d entries, want 1", len(j.entries))
			}
			if j.entries[0].kind != tc.kind {
				t.Fatalf("appended kind %d, want %d", j.entries[0].kind, tc.kind)
			}
			// revert decrements exactly once, for exactly the address dirtied()
			// names, so the constructor must have incremented exactly that.
			wantAddr, wantDirty := j.entries[0].dirtied()
			if !wantDirty {
				if len(j.dirties) != 0 {
					t.Errorf("dirtied %d accounts, but dirtied() reports the kind is clean", len(j.dirties))
				}
				return
			}
			if len(j.dirties) != 1 {
				t.Fatalf("dirtied %d accounts, want exactly 1", len(j.dirties))
			}
			if got := j.dirties[wantAddr]; got != 1 {
				t.Errorf("dirties[%v] = %d, want 1", wantAddr, got)
			}
		})
		covered[tc.kind] = true
	}

	for k := range kindEnd {
		if !covered[k] {
			t.Errorf("kind %d has no constructor case — add one so its dirty accounting stays pinned", k)
		}
	}
}

// BenchmarkJournalStorageChange measures the hot append path; it must stay at
// zero allocs per op once the entries slice has warmed up.
func BenchmarkJournalStorageChange(b *testing.B) {
	j := newJournal()
	defer j.release()
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	prev := uint256.NewInt(42)

	for range 1 << 16 {
		j.storageChange(addr, key, *prev, false)
	}
	j.Reset()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j.storageChange(addr, key, *prev, false)
		if len(j.entries) == 1<<16 {
			j.Reset()
		}
	}
}

// Reset reslices entries to zero, so anything left in the backing array stays
// reachable for as long as the journal is reused — and a kindCode entry's extra
// holds a whole contract's bytecode. The journal outlives the transaction, so
// that is a block's worth of dead code pinned by a pooled object.
func TestJournalResetDropsEntriesItReslicesPast(t *testing.T) {
	j := newJournal()
	t.Cleanup(j.release)

	addr := accounts.InternAddress(common.Address{1})
	j.codeChange(addr, make([]byte, 4096), accounts.EmptyCodeHash, false)
	j.codeChange(addr, make([]byte, 4096), accounts.EmptyCodeHash, false)
	if len(j.entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(j.entries))
	}

	j.Reset()

	tail := j.entries[:cap(j.entries)]
	for i, e := range tail {
		if e.extra != nil {
			t.Fatalf("entry %d still pins a journalExtra after Reset (prevcode %d bytes)",
				i, len(e.extra.prevcode))
		}
	}
}

// revert truncates to a snapshot, so the entries it drops outlive it; a Reset
// that clears only [0:len] would leave that stretch holding its bytecode.
func TestJournalRevertDropsTheEntriesItTruncates(t *testing.T) {
	ibs := New(NewVersionedStateReader(0, ReadSet{}, nil, nil))
	addr := accounts.InternAddress(common.Address{1})
	ibs.stateObjects[addr] = newObject(ibs, addr, &accounts.Account{}, &accounts.Account{})

	j := ibs.journal
	for range 4 {
		j.codeChange(addr, make([]byte, 4096), accounts.EmptyCodeHash, false)
	}

	j.revert(ibs, 2)
	j.Reset()

	for i, e := range j.entries[:cap(j.entries)] {
		if e.extra != nil {
			t.Fatalf("entry %d still pins a journalExtra after Reset (prevcode %d bytes)",
				i, len(e.extra.prevcode))
		}
	}
}
