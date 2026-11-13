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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	btree2 "github.com/tidwall/btree"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/kvmetrics"
)

type temporalMemBatchFlushTx struct {
	kv.RwTx
	aggTx *AggregatorRoTx
}

func (tx temporalMemBatchFlushTx) AggTx() any { return tx.aggTx }

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

func TestTemporalMemBatchPrevDoesNotAliasCaller(t *testing.T) {
	ctx := t.Context()
	dirs := datadir.New(t.TempDir())
	db := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, log.New()), dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)
	agg := NewTest(dirs).StepSize(4).Logger(log.New()).MustOpen(ctx)
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder(db))
	aggTx := agg.BeginFilesRo()
	t.Cleanup(aggTx.Close)
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	batch := &TemporalMemBatch{
		storage:  btree2.NewMap[string, []dataWithTxNum](128),
		metrics:  &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
		stepSize: agg.StepSize(),
	}
	for domain, d := range aggTx.d {
		batch.domainWriters[domain] = d.NewWriter(db)
	}
	defer batch.Close()
	key := []byte{0x91}
	first := []byte{0x10, 0x11}
	second := []byte{0x20, 0x21}
	preval := bytes.Clone(first)
	require.NoError(t, batch.DomainPut(kv.AccountsDomain, string(key), first, 1, nil))
	require.NoError(t, batch.DomainPut(kv.AccountsDomain, string(key), second, 2, preval))
	preval[0] = 0xff
	preval[1] = 0xee
	flushTx := temporalMemBatchFlushTx{RwTx: rwTx, aggTx: aggTx}
	require.NoError(t, batch.Flush(ctx, flushTx))

	cursor, err := flushTx.CursorDupSort(kv.TblAccountHistoryVals)
	require.NoError(t, err)
	defer cursor.Close()
	value, err := cursor.SeekBothRange(key, binary.BigEndian.AppendUint64(nil, 2))
	require.NoError(t, err)
	require.Equal(t, append([]byte{0, 0, 0, 0, 0, 0, 0, 2}, first...), value)
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
				sd.putLatest(domain, key, []byte(key), uint64(i), nil)
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
	sd.putLatest(kv.AccountsDomain, "acc", []byte("v"), 1, nil)

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

	sd := &TemporalMemBatch{
		stepSize: 16,
		storage:  btree2.NewMap[string, []dataWithTxNum](128),
		metrics:  &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
	}

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
			sd.putLatest(domain, key, first, 1, nil)
			borrowed, _, ok := sd.GetLatest(domain, []byte(key))
			require.True(t, ok)
			require.Equal(t, first, borrowed)

			sd.putLatest(domain, key, []byte(next.val), next.txNum, nil)

			require.Equal(t, first, borrowed,
				"%s/%s: a borrowed value must survive a later write to its key", domain, next.name)
		}
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
		other.putLatest(kv.AccountsDomain, "k", []byte("v"), 1, nil)
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
