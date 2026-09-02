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
	"fmt"
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

// Pins the locking contract of the per-domain latestStateLocks: readers and
// writers of different domains run concurrently while Unwind takes every
// lock. Meaningful under -race; also checks unwind correctness afterwards.
func TestTemporalMemBatchConcurrentDomainAccess(t *testing.T) {
	t.Parallel()
	sd := &TemporalMemBatch{
		stepSize: 16,
		storage:  btree2.NewMap[string, []dataWithTxNum](128),
		metrics:  &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
	}
	for d := range sd.domains {
		sd.domains[d] = map[string][]dataWithTxNum{}
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
				sd.HasPrefixInRAM(domain, []byte(key))
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
