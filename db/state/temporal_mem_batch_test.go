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
	"sync"
	"testing"

	btree2 "github.com/tidwall/btree"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
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

// CommitmentDomain is on its own lock, so the calculator can write commitment
// branches while workers read the state domains without contending. Run under
// -race: commitment write vs state read, state read/write, commitment
// read/write, and the both-lock Unwind path (deadlock-freedom).
func TestSplitLock_ConcurrentCommitmentWriteVsStateRead(t *testing.T) {
	sd := &TemporalMemBatch{
		stepSize: 1,
		storage:  btree2.NewMap[string, []dataWithTxNum](128),
		metrics:  kvmetrics.NewDomainMetrics(),
	}
	for i := range sd.domains {
		sd.domains[i] = map[string][]dataWithTxNum{}
	}
	const nKeys = 512
	mkKey := func(i int) string { return string([]byte{byte(i), byte(i >> 8)}) }
	for i := range nKeys {
		sd.putLatest(kv.AccountsDomain, mkKey(i), []byte{1}, uint64(i))
	}

	stop := make(chan struct{})
	var writers, readers sync.WaitGroup

	loopUntilStop := func(f func(i int)) {
		defer writers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				f(i)
			}
		}
	}
	writers.Add(3)
	go loopUntilStop(func(i int) { sd.putLatest(kv.CommitmentDomain, mkKey(i&(nKeys-1)), []byte{byte(i)}, uint64(i)) })
	go loopUntilStop(func(i int) { sd.putLatest(kv.AccountsDomain, mkKey(i&(nKeys-1)), []byte{byte(i)}, uint64(i)) })
	go loopUntilStop(func(i int) { sd.Unwind(uint64(1_000_000+i), nil) }) // both-lock path; txNum far above data so nothing is pruned

	readers.Add(8)
	for range 8 {
		go func() {
			defer readers.Done()
			for i := range 20_000 {
				sd.GetLatest(kv.AccountsDomain, []byte(mkKey(i&(nKeys-1))))
				sd.GetLatest(kv.CommitmentDomain, []byte(mkKey(i&(nKeys-1))))
			}
		}()
	}
	readers.Wait()
	close(stop)
	writers.Wait()
}
