package state

import (
	"sync"
	"testing"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/db/kv"
)

// A pending eth_call reads this batch while the frontier run-ahead merges a finished generation
// into it. Merge wrote `domains` and `storage` with no lock at all, while every reader holds
// latestStateLock — so the two raced on a plain Go map.
//
// THAT IS NOT AN ORDINARY DATA RACE. A map read concurrent with a map write is detected by the
// runtime itself and raised as `fatal error: concurrent map read and map write`, which no recover()
// can catch: the node dies. live111 died in exactly that stack — eth_call → DoCall → ApplyMessage →
// EVM → ReaderV3.readAccountData → GetLatest → getLatest — while 20 pending calls a block were
// being made against the live pre-execution state.
//
// Run under -race this fails on the unlocked version and passes on the locked one. Without -race it
// still exercises the path and can trip the runtime's own detector, which is the failure that
// matters.
func TestMergeDoesNotRaceWithGetLatest(t *testing.T) {
	mk := func() *TemporalMemBatch {
		sd := &TemporalMemBatch{
			storage:             btree2.NewMap[string, []dataWithTxNum](128),
			stepSize:            1,
			forkableWriters:     map[kv.ForkableId]kv.BufferedWriter{},
			pastForkableWriters: map[kv.ForkableId][]kv.BufferedWriter{},
		}
		for i := range sd.domains {
			sd.domains[i] = map[string][]dataWithTxNum{}
		}
		return sd
	}

	keys := make([]string, 64)
	for i := range keys {
		keys[i] = string(rune('a'+i%26)) + string(rune('0'+i%10))
	}

	sd := mk()
	// Seed the target so readers find something rather than short-circuiting on a miss.
	for _, k := range keys {
		sd.domains[kv.AccountsDomain][k] = []dataWithTxNum{{data: []byte(k), txNum: 1}}
		sd.storage.Set(k, []dataWithTxNum{{data: []byte(k), txNum: 1}})
	}

	const rounds = 200
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Two readers, as a pending eth_call would: through the public, lock-taking entry point.
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, k := range keys {
					sd.GetLatest(kv.AccountsDomain, []byte(k))
					sd.GetLatest(kv.StorageDomain, []byte(k))
				}
			}
		}()
	}

	// One writer, as the run-ahead promotion would.
	for i := 0; i < rounds; i++ {
		other := mk()
		for j, k := range keys {
			other.domains[kv.AccountsDomain][k] = []dataWithTxNum{{data: []byte(k), txNum: uint64(i*100 + j)}}
			other.storage.Set(k, []dataWithTxNum{{data: []byte(k), txNum: uint64(i*100 + j)}})
		}
		if err := sd.Merge(other, false); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("merge %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
}
