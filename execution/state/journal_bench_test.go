package state

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	benchBatch    = 1024 // entries appended between resets (~ a heavy tx)
	benchHotSlots = 8    // distinct slots in the "repeated" pattern
)

var benchSink int

func benchStorageInputs(n int) (addrs []accounts.Address, keys []accounts.StorageKey, vals []uint256.Int) {
	addrs = make([]accounts.Address, n)
	keys = make([]accounts.StorageKey, n)
	vals = make([]uint256.Int, n)
	for i := range n {
		var a common.Address
		binary.BigEndian.PutUint64(a[12:], uint64(i+1))
		addrs[i] = accounts.InternAddress(a)
		var h common.Hash
		binary.BigEndian.PutUint64(h[24:], uint64(i+1))
		keys[i] = accounts.InternKey(h)
		vals[i].SetUint64(uint64(i + 1))
	}
	return addrs, keys, vals
}

// BenchmarkJournalStorageChange guards the SSTORE-style append hot path: with the
// compact tagged-union entry it is alloc-free. unique_keys appends distinct slots;
// repeated_keys cycles a few hot slots (ERC-20 / burntpix shape). The journal is
// reset every benchBatch entries to model the production sync.Pool reuse.
func BenchmarkJournalStorageChange(b *testing.B) {
	addrs, keys, vals := benchStorageInputs(benchBatch)
	for _, p := range []struct {
		name    string
		nUnique int
	}{
		{"unique_keys", benchBatch},
		{"repeated_keys", benchHotSlots},
	} {
		nUnique := p.nUnique
		b.Run(p.name, func(b *testing.B) {
			j := &journal{dirties: make(map[accounts.Address]int)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%benchBatch == 0 {
					j.Reset()
				}
				k := i % nUnique
				j.storageChange(addrs[k], keys[k], vals[k], true)
			}
			benchSink += j.length()
		})
	}
}

func TestJournalEntrySize(t *testing.T) {
	// The union is meant to stay small and pointer-light; guard against
	// accidental bloat (e.g. inlining the rare *stateObject/[]byte fields).
	if got := unsafe.Sizeof(journalEntry{}); got > 72 {
		t.Fatalf("journalEntry grew to %d B (want <= 72)", got)
	}
}
