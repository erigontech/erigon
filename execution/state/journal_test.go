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

// BenchmarkJournalStorageChange measures the hot append path; it must stay at
// zero allocs per op once the entries slice has warmed up.
func BenchmarkJournalStorageChange(b *testing.B) {
	j := newJournal()
	defer j.release()
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	prev := uint256.NewInt(42)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		j.storageChange(addr, key, *prev, false)
		if len(j.entries) == 1<<16 {
			j.Reset()
		}
	}
}
