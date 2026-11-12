// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	btree2 "github.com/tidwall/btree"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/kvmetrics"
)

// A reorg unwind restores domain values from the diffset, so a domain missing
// from GetDiffset is silently never rolled back.
func TestGetDiffsetCoversAllDomains(t *testing.T) {
	cs := &changeset.StateChangeSet{}
	for d := range kv.DomainLen {
		cs.Diffs[d].DomainUpdate([]byte{byte(d)}, 0, []byte("prev"))
	}

	sd := &TemporalMemBatch{}
	blockHash := common.Hash{1}
	sd.SavePastChangesetAccumulator(blockHash, 1, cs)

	diffs, ok, err := sd.GetDiffset(nil, blockHash, 1)
	require.NoError(t, err)
	require.True(t, ok)
	for d := range kv.DomainLen {
		require.NotEmpty(t, diffs[d], "domain %s missing from GetDiffset", d)
	}
}

func TestTemporalMemBatchConcurrentDomainAccess(t *testing.T) {
	t.Parallel()
	sd := &TemporalMemBatch{
		stepSize: 16,
		storage:  btree2.NewMap[string, []dataWithTxNum](128),
		metrics:  &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
	}

	const keysPerDomain = 200
	const cutoff = uint64(keysPerDomain / 2)
	var wg sync.WaitGroup
	for d := range kv.DomainLen {
		domain := kv.Domain(d)
		wg.Go(func() {
			for i := range keysPerDomain {
				key := fmt.Sprintf("%s-%03d", domain, i)
				sd.putLatest(domain, key, []byte(key), uint64(i))
			}
		})
		wg.Go(func() {
			for i := range keysPerDomain {
				key := fmt.Sprintf("%s-%03d", domain, i)
				if v, _, ok := sd.GetLatest(domain, []byte(key)); ok && string(v) != key {
					t.Errorf("domain %s key %s: got %q", domain, key, v)
				}
			}
		})
	}
	wg.Go(func() {
		for range 50 {
			sd.Unwind(cutoff, nil)
		}
	})
	wg.Wait()

	sd.Unwind(cutoff, nil)
	for d := range kv.DomainLen {
		domain := kv.Domain(d)
		for i := range keysPerDomain {
			key := fmt.Sprintf("%s-%03d", domain, i)
			_, _, ok := sd.GetLatest(domain, []byte(key))
			require.Equal(t, uint64(i) < cutoff, ok, "domain %s key %s", domain, key)
		}
	}
}

// A published batch keeps serving RPC reads while it is flushed, so Flush must
// not exclude readers. The executor goroutine stays inside Flush until the RPC
// reader goroutine has finished its reads.
func TestFlushDoesNotBlockReaders(t *testing.T) {
	t.Parallel()
	_, tx := mdbxtest.NewTestTx(t)
	sd := &TemporalMemBatch{
		stepSize: 16,
		storage:  btree2.NewMap[string, []dataWithTxNum](128),
		metrics:  &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
	}
	sd.putLatest(kv.AccountsDomain, "acc", []byte("v"), 1)

	flushing := make(chan struct{})
	readerDone := make(chan struct{})
	flushErr := make(chan error, 1)
	go func() { // executor
		flushErr <- sd.Flush(context.Background(), tx, kv.WithFlushCallback(kv.AccountsDomain, func([]byte, []byte, kv.Step, uint64) {
			close(flushing)
			<-readerDone
		}))
	}()

	const reads = 1000
	readsDone := make(chan int, 1)
	go func() { // RPC reader
		<-flushing
		n := 0
		for range reads {
			if v, _, ok := sd.GetLatest(kv.AccountsDomain, []byte("acc")); ok && string(v) == "v" {
				n++
			}
		}
		readsDone <- n
	}()

	select {
	case n := <-readsDone:
		require.Equal(t, reads, n)
	case <-time.After(5 * time.Second):
		t.Error("RPC reader blocked by an in-progress Flush")
	}
	close(readerDone)
	require.NoError(t, <-flushErr)
}

// Commit borrows the batch's value buffers instead of copying, which is only
// sound while a later write to the same key leaves the earlier bytes alone.
func TestPutLatest_NeverRewritesValuesInPlace(t *testing.T) {
	t.Parallel()

	// inMemHistoryReads picks which arm a later txNum takes: append, or overwrite
	// the slot in place. Both must leave the earlier bytes alone.
	for _, inMemHistoryReads := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemHistoryReads=%v", inMemHistoryReads), func(t *testing.T) {
			t.Parallel()

			sd := &TemporalMemBatch{
				stepSize:          16,
				storage:           btree2.NewMap[string, []dataWithTxNum](128),
				metrics:           &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
				inMemHistoryReads: inMemHistoryReads,
			}

			// Each case is the FIRST write after the borrow, so the slot still holds
			// the borrowed buffer when the arm runs. A shorter value fits that buffer,
			// which is what an arm recycling it would overwrite.
			for _, next := range []struct {
				name  string
				val   string
				txNum uint64
			}{
				{"same txNum", "second", 1},
				{"later txNum", "third", 2},
			} {
				for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
					key := "k-" + domain.String() + "-" + next.name
					first := []byte("first-and-long-enough-to-be-reused")
					sd.putLatest(domain, key, first, 1)
					borrowed, _, ok := sd.GetLatest(domain, []byte(key))
					require.True(t, ok)
					require.Equal(t, first, borrowed)

					sd.putLatest(domain, key, []byte(next.val), next.txNum)

					require.Equal(t, first, borrowed,
						"%s/%s: a borrowed value must survive a later write to its key", domain, next.name)
				}
			}
		})
	}
}

func TestMergeRejectedBatchKeepsReceiverUnchanged(t *testing.T) {
	t.Parallel()
	newBatch := func() *TemporalMemBatch {
		return &TemporalMemBatch{
			stepSize: 16,
			storage:  btree2.NewMap[string, []dataWithTxNum](128),
			metrics:  &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
		}
	}
	for _, receiverHasAccumulator := range []bool{false, true} {
		sd, other := newBatch(), newBatch()
		other.putLatest(kv.AccountsDomain, "k", []byte("v"), 1)
		if receiverHasAccumulator {
			sd.currentChangesAccumulator = &changeset.StateChangeSet{}
		} else {
			other.currentChangesAccumulator = &changeset.StateChangeSet{}
		}

		require.Error(t, sd.Merge(other))
		_, _, ok := sd.GetLatest(kv.AccountsDomain, []byte("k"))
		require.False(t, ok, "receiverHasAccumulator=%v", receiverHasAccumulator)
	}
}

// Exec workers read the latest values while the apply loop writes them, so a read must not
// wait for a writer that holds the key's lock.
func TestGetLatestDoesNotWaitForWriterLock(t *testing.T) {
	t.Parallel()
	sd := &TemporalMemBatch{
		stepSize: 16,
		storage:  btree2.NewMap[string, []dataWithTxNum](128),
		metrics:  &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
	}
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		sd.putLatest(domain, "k", []byte("v1"), 1)

		sh := sd.shard(domain, "k")
		sh.Lock()
		got := make(chan string, 1)
		go func() {
			v, _, _ := sd.GetLatest(domain, []byte("k"))
			got <- string(v)
		}()
		select {
		case v := <-got:
			require.Equal(t, "v1", v, "domain %s", domain)
		case <-time.After(5 * time.Second):
			t.Errorf("domain %s: GetLatest waited for the writer's lock", domain)
		}
		sh.Unlock()
	}
}

// A reader sees whole published versions in write order: a later read of a key never
// returns an older value than an earlier read did.
func TestGetLatestSeesPublishedVersionsInOrder(t *testing.T) {
	t.Parallel()
	sd := &TemporalMemBatch{
		stepSize: 16,
		storage:  btree2.NewMap[string, []dataWithTxNum](128),
		metrics:  &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
	}
	const writes = 2000
	var wg sync.WaitGroup
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		wg.Go(func() {
			for i := 1; i <= writes; i++ {
				sd.putLatest(domain, "k", []byte(fmt.Sprintf("%06d", i)), uint64(i))
			}
		})
		for range 4 {
			wg.Go(func() {
				last := ""
				for range writes {
					v, _, ok := sd.GetLatest(domain, []byte("k"))
					if !ok {
						continue
					}
					if string(v) < last {
						t.Errorf("domain %s: read %s after %s", domain, v, last)
						return
					}
					last = string(v)
				}
			})
		}
	}
	wg.Wait()
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		v, _, ok := sd.GetLatest(domain, []byte("k"))
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("%06d", writes), string(v))
	}
}

func TestGetLatestDuringMergeNeverReturnsTheReplacedValue(t *testing.T) {
	newBatch := func() *TemporalMemBatch {
		return &TemporalMemBatch{
			stepSize: 16,
			storage:  btree2.NewMap[string, []dataWithTxNum](128),
			metrics:  &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
		}
	}
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		sd, other := newBatch(), newBatch()
		sd.putLatest(domain, "k", []byte("old"), 1)
		other.putLatest(domain, "k", []byte("merged"), 2)

		var during string
		mergeCopiedHook = func(b *TemporalMemBatch) {
			v, _, _ := b.GetLatest(domain, []byte("k"))
			during = string(v)
		}
		require.NoError(t, sd.Merge(other))
		mergeCopiedHook = nil

		require.Equal(t, "merged", during, "domain %s: a read inside Merge returned the replaced value", domain)
		v, _, _ := sd.GetLatest(domain, []byte("k"))
		require.Equal(t, "merged", string(v), "domain %s", domain)
	}
}
