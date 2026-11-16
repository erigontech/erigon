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
	w := dt.NewWriter(db)
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
	require.NoError(t, udt.unwind(ctx, tx, target, merged, nil))
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

func TestDomain_RepeatedUnwindKeepsVersionOrder(t *testing.T) {
	for _, cfg := range []statecfg.DomainCfg{statecfg.Schema.StorageDomain, statecfg.Schema.CodeDomain} {
		for _, firstCut := range []uint64{20, 32, 34} {
			t.Run(fmt.Sprintf("%s/cut_%d", cfg.Name, firstCut), func(t *testing.T) {
				db, d := testDbAndDomainOfStep(t, cfg, 16, log.New())
				tx, err := db.BeginRw(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				view := d.beginForTests()
				defer view.Close()
				writer := view.NewWriter(db)
				defer writer.Close()
				key := []byte("key")
				require.NoError(t, writer.PutWithPrev(key, []byte{1}, 2, nil))
				writer.diff = &kv.DomainDiff{}
				require.NoError(t, writer.PutWithPrev(key, []byte{2}, 34, []byte{1}))
				diffs := writer.diff.GetDiffSet()
				require.NoError(t, writer.Flush(t.Context(), tx))
				// The second unwind has no diff for this key: it did not change in that range.
				require.NoError(t, view.unwind(t.Context(), tx, firstCut, diffs, nil))
				require.NoError(t, view.unwind(t.Context(), tx, 10, nil, nil))
				writer.diff = nil
				require.NoError(t, writer.PutWithPrev(key, []byte{3}, 12, []byte{1}))
				require.NoError(t, writer.Flush(t.Context(), tx))
				value, _, _, err := view.GetLatest(key, tx)
				require.NoError(t, err)
				require.Equal(t, []byte{3}, value)
			})
		}
	}
}

func TestDomain_UnwindDoesNotRestoreAbsentBranch(t *testing.T) {
	for _, oldBranch := range [][]byte{{0, 1, 0, 0}, {0, 1, 0, 1, 0}} {
		t.Run(fmt.Sprintf("old_%x", oldBranch), func(t *testing.T) {
			db, d := testDbAndDomainOfStep(t, statecfg.Schema.CommitmentDomain, 16, log.New())
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			view := d.beginForTests()
			defer view.Close()
			writer := view.NewWriter(db)
			defer writer.Close()

			key := []byte("key")
			require.NoError(t, writer.PutWithPrev(key, oldBranch, 2, nil))
			writer.SetDiff(&kv.DomainDiff{})
			// A branch absent from the trie can still have an old record in storage.
			require.NoError(t, writer.PutWithPrev(key, []byte{0, 2, 0, 2, 0}, 34, []byte{}))
			diffs := writer.Diff().GetDiffSet()
			require.NoError(t, writer.Flush(t.Context(), tx))
			require.NoError(t, view.unwind(t.Context(), tx, 33, diffs, nil))
			// This key has no diff in the deeper unwound range.
			require.NoError(t, view.unwind(t.Context(), tx, 20, nil, nil))

			replacement := []byte{0, 4, 0, 4, 0}
			writer.SetDiff(nil)
			require.NoError(t, writer.PutWithPrev(key, replacement, 21, oldBranch))
			require.NoError(t, writer.Flush(t.Context(), tx))
			value, _, _, err := view.GetLatest(key, tx)
			require.NoError(t, err)
			require.Equal(t, replacement, value)
		})
	}
}

func TestDomain_UnwindRestoresExistingBranch(t *testing.T) {
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.CommitmentDomain, 16, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	view := d.beginForTests()
	defer view.Close()
	writer := view.NewWriter(db)
	defer writer.Close()

	key, oldBranch, previous := []byte("key"), []byte{0, 1, 0, 0}, []byte{0, 1, 0, 1, 0}
	require.NoError(t, writer.PutWithPrev(key, oldBranch, 2, nil))
	require.NoError(t, writer.PutWithPrev(key, previous, 33, oldBranch))
	writer.SetDiff(&kv.DomainDiff{})
	require.NoError(t, writer.PutWithPrev(key, []byte{0, 2, 0, 2, 0}, 35, previous))
	diffs := writer.Diff().GetDiffSet()
	require.NoError(t, writer.Flush(t.Context(), tx))
	require.NoError(t, view.unwind(t.Context(), tx, 35, diffs, nil))
	value, _, _, err := view.GetLatest(key, tx)
	require.NoError(t, err)
	require.Equal(t, previous, value)
}

func TestDomain_UnwindRestoresDeletionOverBranchShapedValue(t *testing.T) {
	for _, cfg := range []statecfg.DomainCfg{statecfg.Schema.StorageDomain, statecfg.Schema.CodeDomain} {
		t.Run(cfg.Name.String(), func(t *testing.T) {
			db, d := testDbAndDomainOfStep(t, cfg, 16, log.New())
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			view := d.beginForTests()
			defer view.Close()
			writer := view.NewWriter(db)
			defer writer.Close()

			// Outside the commitment domain, these four bytes are ordinary data.
			key, oldValue := []byte("key"), []byte{0xAB, 0xCD, 0, 0}
			require.NoError(t, writer.PutWithPrev(key, oldValue, 2, nil))
			require.NoError(t, writer.DeleteWithPrev(key, 33, oldValue))
			writer.SetDiff(&kv.DomainDiff{})
			require.NoError(t, writer.PutWithPrev(key, []byte{9}, 35, []byte{}))
			diffs := writer.Diff().GetDiffSet()
			require.NoError(t, writer.Flush(t.Context(), tx))
			require.NoError(t, view.unwind(t.Context(), tx, 35, diffs, nil))
			value, _, _, err := view.GetLatest(key, tx)
			require.NoError(t, err)
			require.Empty(t, value)
		})
	}
}

func TestDomain_UnwindIgnoresFiledVersions(t *testing.T) {
	const stepSize = uint64(16)
	for _, cfg := range []statecfg.DomainCfg{statecfg.Schema.StorageDomain, statecfg.Schema.CodeDomain} {
		t.Run(cfg.Name.String(), func(t *testing.T) {
			db, d := testDbAndDomainOfStep(t, cfg, stepSize, log.New())
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			key := []byte("key")
			write := func(txNum uint64, value, previous []byte) []kv.DomainEntryDiff {
				view := d.beginForTests()
				defer view.Close()
				writer := view.NewWriter(db)
				defer writer.Close()
				writer.SetDiff(&kv.DomainDiff{})
				require.NoError(t, writer.PutWithPrev(key, value, txNum, previous))
				diffs := writer.Diff().GetDiffSet()
				require.NoError(t, writer.Flush(t.Context(), tx))
				return diffs
			}
			write(3*stepSize+1, []byte("A"), nil)
			write(4*stepSize+1, []byte("B"), []byte("A"))
			for step := range kv.Step(5) {
				require.NoError(t, d.collateBuildIntegrate(t.Context(), step, tx, background.NewProgressSet()))
			}
			view := d.beginForTests()
			defer view.Close()
			logEvery := time.NewTicker(time.Hour)
			defer logEvery.Stop()
			// An interrupted prune can remove the newest filed row before the older one.
			_, err = view.Prune(t.Context(), tx, 4, 4*stepSize, 5*stepSize, 0, logEvery)
			require.NoError(t, err)
			value, _, _, err := view.GetLatest(key, tx)
			require.NoError(t, err)
			require.Equal(t, []byte("B"), value)
			write(5*stepSize+2, []byte("A"), value)
			diffs := write(5*stepSize+6, []byte("C"), []byte("A"))
			require.NoError(t, view.unwind(t.Context(), tx, 5*stepSize+6, diffs, nil))
			value, _, _, err = view.GetLatest(key, tx)
			require.NoError(t, err)
			require.Equal(t, []byte("A"), value)
		})
	}
}

func TestDomain_RepeatedUnwindRemovesRestoredVersions(t *testing.T) {
	for _, cfg := range []statecfg.DomainCfg{statecfg.Schema.StorageDomain, statecfg.Schema.CodeDomain} {
		for _, restored := range [][]byte{nil, {7}} {
			t.Run(fmt.Sprintf("%s/restore_%x", cfg.Name, restored), func(t *testing.T) {
				db, d := testDbAndDomainOfStep(t, cfg, 16, log.New())
				tx, err := db.BeginRw(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				view := d.beginForTests()
				defer view.Close()
				key, ancestor := []byte("key"), []byte{1}
				write := func(txNum uint64, value, previous []byte) []kv.DomainEntryDiff {
					writer := view.NewWriter(db)
					defer writer.Close()
					writer.SetDiff(&kv.DomainDiff{})
					require.NoError(t, writer.PutWithPrev(key, value, txNum, previous))
					diffs := writer.Diff().GetDiffSet()
					require.NoError(t, writer.Flush(t.Context(), tx))
					return diffs
				}
				write(2, ancestor, nil)
				first := write(34, []byte{2}, restored)
				require.NoError(t, view.unwind(t.Context(), tx, 34, first, nil))
				value, _, _, err := view.GetLatest(key, tx)
				require.NoError(t, err)
				require.Equal(t, len(restored), len(value))
				if len(restored) > 0 {
					require.Equal(t, restored, value)
				}

				// A restored version has no original write in the next branch's diffsets.
				second := write(18, []byte{3}, ancestor)
				require.NoError(t, view.unwind(t.Context(), tx, 10, second, nil))
				value, _, _, err = view.GetLatest(key, tx)
				require.NoError(t, err)
				require.Equal(t, ancestor, value)
			})
		}
	}
}
