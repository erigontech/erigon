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

package test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// A parallel fold whose worker views have drifted off the caller's snapshot
// reads through one serialized reader over the caller's tx. Both arms fold the
// same keys from a read-only caller; the drifted arm just has a commit landing
// between the caller's view and the workers'.
func BenchmarkSharedDomains_ParallelFold_PinnedFallback(b *testing.B) {
	// No b.RunParallel: mutates the process-global trie-selection flag.
	orig := statecfg.ExperimentalParallelCommitment
	b.Cleanup(func() { statecfg.ExperimentalParallelCommitment = orig })
	statecfg.ExperimentalParallelCommitment = true

	stepSize := uint64(100)
	db, _ := testDbAndAggregatorBench(b, stepSize)
	ctx := b.Context()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()
	writer := newSharedDomainsBench(b, db, rwTx)

	data := generateTestDataForDomainCommitment(b, length.Addr, length.Addr+length.Hash, stepSize*4, 4, 20_000)
	require.NotNil(b, data)
	var txNum uint64
	for domName, d := range data {
		dom := kv.AccountsDomain
		if domName == "storage" {
			dom = kv.StorageDomain
		}
		for key, upd := range d {
			for _, u := range upd {
				txNum = u.txNum
				require.NoError(b, writer.DomainPut(dom, rwTx, []byte(key), u.value, txNum, nil))
			}
		}
	}
	_, err = writer.ComputeCommitment(ctx, rwTx, true, 0, txNum, "", nil)
	require.NoError(b, err)
	require.NoError(b, writer.Flush(ctx, rwTx))
	writer.Close()
	require.NoError(b, rwTx.Commit())

	type keyTouch struct {
		dom kv.Domain
		key []byte
		val []byte
	}
	var touches []keyTouch
	require.NoError(b, db.View(ctx, func(tx kv.Tx) error {
		roTx := tx.(kv.TemporalTx)
		for domName, d := range data {
			dom := kv.AccountsDomain
			if domName == "storage" {
				dom = kv.StorageDomain
			}
			for key := range d {
				v, _, err := roTx.GetLatest(dom, []byte(key), kv.GetLatestOptions{})
				if err != nil {
					return err
				}
				if len(v) == 0 {
					continue
				}
				touches = append(touches, keyTouch{dom, []byte(key), append([]byte(nil), v...)})
			}
		}
		return nil
	}))
	require.NotEmpty(b, touches)

	var bump byte
	var roots [][]byte
	run := func(b *testing.B, drift, parallel bool) {
		statecfg.ExperimentalParallelCommitment = parallel
		callerTx, err := db.BeginTemporalRo(ctx)
		require.NoError(b, err)
		defer callerTx.Rollback()
		if drift {
			bump++
			require.NoError(b, db.Update(ctx, func(tx kv.RwTx) error {
				return tx.Put(kv.DatabaseInfo, []byte("bench-head-bump"), []byte{bump})
			}))
		}
		require.NoError(b, db.View(ctx, func(workerTx kv.Tx) error {
			require.Equal(b, drift, callerTx.ViewID() != workerTx.(kv.TemporalTx).ViewID(), "arm does not exercise the path it names")
			return nil
		}))
		doms := newSharedDomainsBench(b, db, callerTx)
		defer doms.Close()

		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			for _, t := range touches {
				require.NoError(b, doms.DomainPut(t.dom, callerTx, t.key, t.val, txNum, nil))
			}
			b.StartTimer()

			rh, err := doms.ComputeCommitment(ctx, callerTx, false, 0, txNum, "", nil)
			require.NoError(b, err)
			roots = append(roots, rh)
		}
	}

	b.Run("parallel_worker_views_match", func(b *testing.B) { run(b, false, true) })
	b.Run("parallel_worker_views_drifted", func(b *testing.B) { run(b, true, true) })
	b.Run("sequential_trie", func(b *testing.B) { run(b, false, false) })

	// Both arms must fold to the same root, or the timings compare different work.
	for _, rh := range roots {
		require.Equal(b, roots[0], rh)
	}
}
