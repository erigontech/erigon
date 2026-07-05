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

package storage

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func writeKV(t *testing.T, ctx context.Context, dir, filename string, entries [][2][]byte) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	c, err := seg.NewCompressor(ctx, "fixture-kv", path, dir, seg.DefaultCfg, log.LvlError, log.New())
	require.NoError(t, err)
	w := seg.NewWriter(c, seg.CompressNone)
	for _, kv := range entries {
		_, err := w.Write(kv[0])
		require.NoError(t, err)
		_, err = w.Write(kv[1])
		require.NoError(t, err)
	}
	require.NoError(t, c.Compress())
	c.Close()
	return path
}

func readKV(t *testing.T, path string) [][2][]byte {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	r := seg.NewReader(d.MakeGetter(), seg.CompressNone)
	var out [][2][]byte
	var k, v []byte
	for r.HasNext() {
		k, _ = r.Next(k[:0])
		require.True(t, r.HasNext(), "trailing key without value in %s", path)
		v, _ = r.Next(v[:0])
		kCopy := append([]byte(nil), k...)
		vCopy := append([]byte(nil), v...)
		out = append(out, [2][]byte{kCopy, vCopy})
	}
	return out
}

func TestRegenerateBoundaryStepFile_NonCommitment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	keyA := []byte("aa-alice")
	keyB := []byte("bb-bob")
	keyC := []byte("cc-carol-created-post-anchor")
	in := [][2][]byte{
		{keyA, []byte("alice-stale-100ETH")},
		{keyB, []byte("bb-stale-200ETH")},
		{keyC, []byte("carol-50ETH")},
	}
	oldPath := writeKV(t, ctx, dir, "v1.0-accounts.264-266.kv", in)

	lookup := func(d kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
		require.Equal(t, kv.AccountsDomain, d)
		require.EqualValues(t, 103_848_485, ts)
		switch string(k) {
		case string(keyA):
			return []byte("alice-anchored-50ETH"), true, nil
		case string(keyB):
			return []byte("bob-anchored-150ETH"), true, nil
		case string(keyC):
			return nil, false, nil
		}
		t.Fatalf("unexpected key in lookup: %q", k)
		return nil, false, nil
	}

	newPath := oldPath + ".regen"
	err := RegenerateBoundaryStepFile(
		ctx, kv.AccountsDomain, oldPath, "", newPath, lookup, nil, IdentityBranchExpander(),
		103_848_485, seg.CompressNone, nil,
		dir, log.New(),
	)
	require.NoError(t, err)

	got := readKV(t, newPath)
	require.Len(t, got, 3)
	require.Equal(t, [2][]byte{keyA, []byte("alice-anchored-50ETH")}, got[0])
	require.Equal(t, [2][]byte{keyB, []byte("bob-anchored-150ETH")}, got[1])
	require.Equal(t, keyC, got[2][0])
	require.Empty(t, got[2][1], "carol becomes an empty tombstone (was in old file, lookup !found)")
}

// TestRegenerateBoundaryStepFile_CommitmentMergesBaselineWithBranches
// pins the mode-B commitment regen contract: the file's per-key
// content is at-lastTxNum, produced by merge-walking the baseline
// commitment .kv against the recompute's PutBranch collector.
//
// Predicted per-key outcomes:
//   - K in baseline only (unchanged in (baseline, lastTxNum]):
//     baseline's V survives (its stored sub-tree hash still matches
//     the trie at lastTxNum).
//   - K in both baseline and branches: branches' V wins (recompute
//     refolded the sub-tree because a state key under it changed).
//   - K in branches only (new branch created in range): branches' V
//     is written.
//   - KeyCommitmentState: replaced by the supplied anchor blob
//     regardless of what baseline held.
//
// The shared-key K uses the real 34-byte branch prefix (nibble prefix
// 1c86… reaching the ERC-20 address 0x4d38bd670764c49cce1e59eeaebd05974760acbd)
// from the 2026-07-04 hoodi file diff so a future refactor breaks the
// test against the same divergence pattern that originally forced the fix.
func TestRegenerateBoundaryStepFile_CommitmentMergesBaselineWithBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	keyBaselineOnly := []byte("branch-only-at-baseline")
	keyShared, err := hex.DecodeString("1c864e62c56da10bb137131400d7f3fde59b3ab352933a76e8d58711762f15d0b890")
	require.NoError(t, err)
	keyBranchOnly := []byte("branch-created-in-range")

	// Baseline fixture MUST be sorted by K — production commitment
	// .kv files are, and the merge-walk relies on that invariant.
	// Byte order: 0x1c86… < 0x62 "branch-only-…" < 0x73 "state".
	baselineIn := [][2][]byte{
		{keyShared, []byte("baseline-hash-STALE-at-lastTxNum")},
		{keyBaselineOnly, []byte("baseline-only-hash")},
		{commitmentdb.KeyCommitmentState, []byte("baseline-state")},
	}
	baselinePath := writeKV(t, ctx, dir, "v2.1-commitment.272-280.kv", baselineIn)

	// Straddler exists on disk but isn't consumed by the commitment
	// regen path; only its filename matters (for the truncated-rename).
	straddlerPath := writeKV(t, ctx, dir, "v2.1-commitment.280-286.kv", [][2][]byte{
		{keyShared, []byte("straddler-hash-post-lastTxNum")},
		{commitmentdb.KeyCommitmentState, []byte("straddler-state")},
	})

	// Branches sorted: keyShared (0x1c) < keyBranchOnly (0x62 "branch-created-…").
	branches := &SortedBranchPairs{
		Keys: [][]byte{keyShared, keyBranchOnly},
		Vals: [][]byte{[]byte("recomputed-hash-CORRECT-at-lastTxNum"), []byte("branch-created-hash")},
	}

	anchor := []byte("anchor-at-lastTxNum")
	newPath := straddlerPath + ".regen"
	require.NoError(t, RegenerateBoundaryStepFile(
		ctx, kv.CommitmentDomain, straddlerPath, baselinePath, newPath, nil, branches, IdentityBranchExpander(),
		108_584_330, seg.CompressNone, anchor,
		dir, log.New(),
	))

	got := readKV(t, newPath)
	// Predicted merge-walk output (sorted by K):
	//   1. 0x1c86… keyShared — both streams; branches' V wins.
	//   2. 0x62 "branch-created-in-range" keyBranchOnly — branches only.
	//   3. 0x62 "branch-only-at-baseline" keyBaselineOnly — baseline only.
	//      ("branch-created…" < "branch-only…" at byte 7: 'c' 0x63 < 'o' 0x6f)
	//   4. 0x73 "state" KeyCommitmentState — baseline; anchor replaces its V.
	require.Equal(t, [][2][]byte{
		{keyShared, []byte("recomputed-hash-CORRECT-at-lastTxNum")},
		{keyBranchOnly, []byte("branch-created-hash")},
		{keyBaselineOnly, []byte("baseline-only-hash")},
		{commitmentdb.KeyCommitmentState, anchor},
	}, got, "commitment regen merges baseline + branches; shared keys use branches' at-lastTxNum V, baseline-only keys pass through, anchor replaces KeyCommitmentState")
}

func TestRegenerateBoundaryStepFile_CommitmentRequiresAnchor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	oldPath := writeKV(t, ctx, dir, "v2.0-commitment.264-266.kv", nil)

	err := RegenerateBoundaryStepFile(
		ctx, kv.CommitmentDomain, oldPath, oldPath, oldPath+".regen",
		nil, nil, IdentityBranchExpander(), 0, seg.CompressNone, nil /* anchor */, dir, log.New(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commitmentAnchor required")
}

func TestRegenerateBoundaryStepFile_CommitmentRequiresBaseline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	oldPath := writeKV(t, ctx, dir, "v2.0-commitment.264-266.kv", nil)

	err := RegenerateBoundaryStepFile(
		ctx, kv.CommitmentDomain, oldPath, "" /* baseline */, oldPath+".regen",
		nil, nil, IdentityBranchExpander(), 0, seg.CompressNone, []byte("anchor"), dir, log.New(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "baselineKVPath required")
}

func TestRegenerateBoundaryStepFile_NonCommitmentRejectsAnchor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	oldPath := writeKV(t, ctx, dir, "v1.0-accounts.264-266.kv", nil)

	err := RegenerateBoundaryStepFile(
		ctx, kv.AccountsDomain, oldPath, "", oldPath+".regen",
		func(kv.Domain, []byte, uint64) ([]byte, bool, error) { return nil, false, nil }, nil, IdentityBranchExpander(),
		0, seg.CompressNone, []byte("anchor"), dir, log.New(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be nil for non-commitment")
}

func TestRegenerateBoundaryStepFile_CommitmentWithoutKeyCommitmentStateErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	// Baseline .kv that's missing KeyCommitmentState entirely —
	// shouldn't happen in production but defensible to detect.
	baselinePath := writeKV(t, ctx, dir, "v2.0-commitment.264-266.kv", [][2][]byte{
		{[]byte("branch-only-no-anchor"), []byte("value")},
	})

	err := RegenerateBoundaryStepFile(
		ctx, kv.CommitmentDomain, baselinePath, baselinePath, baselinePath+".regen",
		nil, nil, IdentityBranchExpander(), 103_848_485, seg.CompressNone, []byte("anchor"), dir, log.New(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no KeyCommitmentState")
}

func TestRenameStepRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                       string
		basename                   string
		fromStep, oldToStep, newTo uint64
		want                       string
	}{
		{"accounts truncate 280→278", "v1.1-accounts.272-280.kv", 272, 280, 278, "v1.1-accounts.272-278.kv"},
		{"accounts truncate to 1-step", "v1.1-accounts.272-280.kv", 272, 280, 273, "v1.1-accounts.272-273.kv"},
		{"commitment v2.1 truncate", "v2.1-commitment.272-280.kv", 272, 280, 278, "v2.1-commitment.272-278.kv"},
		{"aligned no-op (oldTo==newTo)", "v1.1-accounts.272-280.kv", 272, 280, 280, "v1.1-accounts.272-280.kv"},
		{"no matching segment", "v1.1-accounts.999-1000.kv", 272, 280, 278, "v1.1-accounts.999-1000.kv"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renameStepRange(tc.basename, tc.fromStep, tc.oldToStep, tc.newTo)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRegenerateBoundaryStepFile_ContentReflectsLastTxNum(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	keyKept := []byte("kept")
	keyDrop := []byte("drop")
	keySame := []byte("same")

	stalePre := [][2][]byte{
		{keyKept, []byte("kept-stale-post-anchor")},
		{keyDrop, []byte("drop-stale-post-anchor")},
		{keySame, []byte("same-value-stable")},
	}
	oldPath := writeKV(t, ctx, dir, "v1.1-accounts.272-280.kv", stalePre)

	const anchor uint64 = 108_584_330
	lookup := func(d kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
		require.Equal(t, kv.AccountsDomain, d)
		require.Equal(t, anchor, ts, "lookup must be called with lastTxNum")
		switch string(k) {
		case string(keyKept):
			return []byte("kept-as-of-anchor"), true, nil
		case string(keyDrop):
			return nil, false, nil
		case string(keySame):
			return []byte("same-value-stable"), true, nil
		}
		t.Fatalf("unexpected key: %q", k)
		return nil, false, nil
	}

	newPath := oldPath + ".regen"
	require.NoError(t, RegenerateBoundaryStepFile(
		ctx, kv.AccountsDomain, oldPath, "", newPath, lookup, nil, IdentityBranchExpander(),
		anchor, seg.CompressNone, nil,
		dir, log.New(),
	))

	got := readKV(t, newPath)
	require.Len(t, got, 3)
	require.Equal(t, [2][]byte{keyKept, []byte("kept-as-of-anchor")}, got[0])
	require.Equal(t, keyDrop, got[1][0])
	require.Empty(t, got[1][1], "drop-signalled key becomes empty tombstone, not omitted (see tombstone-preservation test)")
	require.Equal(t, [2][]byte{keySame, []byte("same-value-stable")}, got[2])
}

func TestRegenerateBoundaryStepFile_TruncatedNameMatchesContent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	keyA := []byte("alice")
	stalePre := [][2][]byte{
		{keyA, []byte("alice-stale-50ETH-post-anchor")},
	}
	oldPath := writeKV(t, ctx, dir, "v1.1-accounts.272-280.kv", stalePre)

	newKVPath := filepath.Join(dir, "v1.1-accounts.272-278.kv.regen")

	lookup := func(d kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
		return []byte("alice-anchored-30ETH"), true, nil
	}

	require.NoError(t, RegenerateBoundaryStepFile(
		ctx, kv.AccountsDomain, oldPath, "", newKVPath, lookup, nil, IdentityBranchExpander(),
		108_584_330, seg.CompressNone, nil,
		dir, log.New(),
	))

	_, err := os.Stat(newKVPath)
	require.NoError(t, err)

	got := readKV(t, newKVPath)
	require.Equal(t, [][2][]byte{{keyA, []byte("alice-anchored-30ETH")}}, got,
		"truncated file's content must reflect as-of-anchor values, NOT pre-anchor stale ones")

	require.Contains(t, newKVPath, "272-278", "truncated file name carries the narrower step range")
	require.NotContains(t, newKVPath, "272-280", "truncated file name must not retain the original wider range")
}

func TestRegenerateBoundaryStepFile_TombstonePreservedForKeysInOldFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	keyTomb := []byte("slot-tombstoned")
	keyLive := []byte("slot-live")
	in := [][2][]byte{
		{keyTomb, []byte("pre-tombstone-value")},
		{keyLive, []byte("live-value-stale")},
	}
	oldPath := writeKV(t, ctx, dir, "v1.0-storage.272-280.kv", in)

	lookup := func(d kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
		switch string(k) {
		case string(keyTomb):
			return nil, false, nil
		case string(keyLive):
			return []byte("live-value-at-anchor"), true, nil
		}
		t.Fatalf("unexpected key: %q", k)
		return nil, false, nil
	}

	newPath := oldPath + ".regen"
	require.NoError(t, RegenerateBoundaryStepFile(
		ctx, kv.StorageDomain, oldPath, "", newPath, lookup, nil, IdentityBranchExpander(),
		108_584_330, seg.CompressNone, nil,
		dir, log.New(),
	))

	got := readKV(t, newPath)
	require.Len(t, got, 2, "tombstoned key must be preserved (not dropped)")
	require.Equal(t, keyTomb, got[0][0])
	require.Empty(t, got[0][1], "tombstoned key's value must be empty")
	require.Equal(t, [2][]byte{keyLive, []byte("live-value-at-anchor")}, got[1])
}

func TestRegenerateBoundaryStepFile_WritesToNewKVPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	keyA := []byte("alice")
	oldPath := writeKV(t, ctx, dir, "v1.1-accounts.272-280.kv", [][2][]byte{{keyA, []byte("v")}})

	newKVPath := filepath.Join(dir, "v1.1-accounts.272-278.kv.regen")

	lookup := func(d kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
		return []byte("regen-v"), true, nil
	}

	require.NoError(t, RegenerateBoundaryStepFile(
		ctx, kv.AccountsDomain, oldPath, "", newKVPath, lookup, nil, IdentityBranchExpander(),
		108_584_330, seg.CompressNone, nil,
		dir, log.New(),
	))

	got := readKV(t, newKVPath)
	require.Equal(t, [][2][]byte{{keyA, []byte("regen-v")}}, got)

	_, err := os.Stat(oldPath + ".regen")
	require.True(t, os.IsNotExist(err), "legacy oldPath+.regen must not exist when caller supplies newKVPath")
}
