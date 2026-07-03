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
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// TestDomain_MultiStepUnwindMatchesGroundTruth pins a storage-corruption bug in
// DomainRoTx.unwind: when a key is modified at several steps inside the unwound
// range, the unwind used to write a restore for every step's diff to the same
// unwindStep. In the DupSort values table that left multiple dup entries under
// the key, and getLatestFromDb returned the smallest (often an empty tombstone
// from a higher step) instead of the value as of the unwind target.
//
// The chain mirrors StateChurn: a dense "sum" key written every block plus
// sparse ring keys each written round-robin to a value in {0,1,2} (0 deletes).
// Early steps are filed and pruned, then a multi-step range above the file
// boundary is unwound; every key's restored value must equal the tracked ground
// truth, and the ring values must still sum to the sum key.
func TestDomain_MultiStepUnwindMatchesGroundTruth(t *testing.T) {
	const (
		stepSize  = uint64(2)
		nBlocks   = uint64(40) // one logical block per txNum
		ring      = 8
		fileSteps = 8          // build + prune files for steps [0, fileSteps)
		target    = uint64(25) // unwind to this txNum (above the file boundary)
	)

	logger := log.New()
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.StorageDomain, stepSize, logger)
	ctx := t.Context()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	dt := d.beginForTests()
	w := dt.NewWriter()
	defer w.Close()

	ringKey := func(i int) []byte { return fmt.Appendf(nil, "ring-%03d", i) }
	sumKey := []byte("SUM")
	enc := func(v int) []byte {
		if v == 0 {
			return nil
		}
		return []byte{byte(v)}
	}

	cur := map[string]int{} // current value per key
	sum := 0                // running sum of ring values
	blockDiffs := make([][]kv.DomainEntryDiff, nBlocks+1)
	var wantVal map[string]int // snapshot of cur at txNum==target
	var wantSum int

	for txn := uint64(1); txn <= nBlocks; txn++ {
		w.diff = &kv.DomainDiff{}

		rk := ringKey(int(txn) % ring)
		rkS := string(rk)
		newVal := int((txn*7 + 3) % 3) // pseudo-random in {0,1,2}, hits 0 often
		if newVal == 0 {
			require.NoError(t, w.DeleteWithPrev(rk, txn, enc(cur[rkS])))
		} else {
			require.NoError(t, w.PutWithPrev(rk, enc(newVal), txn, enc(cur[rkS])))
		}
		sum += newVal - cur[rkS]
		cur[rkS] = newVal

		require.NoError(t, w.PutWithPrev(sumKey, enc(sum), txn, enc(cur["SUM"])))
		cur["SUM"] = sum

		// Round-trip through (de)serialization to match the production diff path.
		blockDiffs[txn] = changeset.DeserializeDiffSet(changeset.SerializeDiffSet(w.diff.GetDiffSet(), nil))

		if txn == target {
			wantVal = maps.Clone(cur)
			wantSum = sum
		}
	}
	require.NoError(t, w.Flush(ctx, tx))
	dt.Close()

	for s := range fileSteps {
		require.NoError(t, d.collateBuildIntegrate(ctx, kv.Step(s), tx, background.NewProgressSet()))
	}
	pdt := d.beginForTests()
	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()
	for s := range uint64(fileSteps) {
		_, err := pdt.Prune(ctx, tx, kv.Step(s), s*stepSize, (s+1)*stepSize, 1<<62, logEvery)
		require.NoError(t, err)
	}
	pdt.Close()

	// Merge the per-block diffs for the unwound range (target, nBlocks], walking
	// high->low exactly as UnwindExecutionStage does.
	udt := d.beginForTests()
	var merged []kv.DomainEntryDiff
	for b := nBlocks; b > target; b-- {
		if merged == nil {
			merged = blockDiffs[b]
		} else {
			merged = changeset.MergeDiffSets(merged, blockDiffs[b])
		}
	}
	require.NoError(t, udt.unwind(ctx, tx, target/stepSize, target, uint64(udt.FirstStepNotInFiles()), merged))
	udt.Close()

	rdt := d.beginForTests()
	defer rdt.Close()
	readInt := func(k []byte) int {
		v, _, found, err := rdt.GetLatest(k, tx)
		require.NoError(t, err)
		if !found || len(v) == 0 {
			return 0
		}
		return int(v[0])
	}

	computed := 0
	for i := range ring {
		k := ringKey(i)
		got := readInt(k)
		computed += got
		require.Equalf(t, wantVal[string(k)], got, "ring key %s restored to wrong value after multi-step unwind", k)
	}
	gotSum := readInt(sumKey)
	require.Equal(t, wantSum, gotSum, "sum key must match ground truth at the unwind target")
	require.Equal(t, gotSum, computed, "invariant: sum of ring values must equal the sum key after unwind")
}
