package state

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

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
