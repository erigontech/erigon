package commitment

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common/length"
)

// buildClusteredStorageCorpus pins numAccounts to distinct top nibbles (so only
// numAccounts of the 16 top-nibble buckets are populated) and gives each many
// storage slots. The old per-nibble dispatch serializes each bucket on one
// goroutine; the DFS dispatch splits each account's storage into many leafTasks.
func buildClusteredStorageCorpus(b testing.TB, numAccounts, slotsPerAccount int) ([][]byte, []Update) {
	b.Helper()
	rnd := rand.New(rand.NewSource(99001))
	ub := NewUpdateBuilder()
	for i := 0; i < numAccounts; i++ {
		addr := findAddressForNibble(i%16, i)
		ah := hex.EncodeToString(addr)
		ub.Balance(ah, rnd.Uint64())
		for range slotsPerAccount {
			loc := make([]byte, length.Hash)
			rnd.Read(loc)
			val := make([]byte, 32)
			rnd.Read(val)
			ub.Storage(ah, hex.EncodeToString(loc), hex.EncodeToString(val))
		}
	}
	return ub.Build()
}

func Benchmark_Commitment_Clustered(b *testing.B) {
	for _, c := range []struct {
		name     string
		accounts int
		slots    int
	}{
		{"4acct-500K", 4, 125_000},
		{"8acct-500K", 8, 62_500},
	} {
		pk, updates := buildClusteredStorageCorpus(b, c.accounts, c.slots)
		b.Run(c.name+"/ModeDirect", func(b *testing.B) { runDirectBench(b, pk, updates) })
		for _, w := range []int{1, 4, 8, 18} {
			b.Run(fmt.Sprintf("%s/ModeParallel-w%d", c.name, w), func(b *testing.B) {
				runParallelBench(b, pk, updates, w)
			})
		}
	}
}
