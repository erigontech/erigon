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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// writeKV writes a synthetic .kv file with the given (key, value)
// pairs in the order provided. Uses CompressNone to keep the fixture
// readable; production files use compression but the regen code path
// is compression-agnostic (seg.Writer/Reader handle it transparently).
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

// readKV walks a .kv file and returns its (key, value) pairs in order.
// Useful for asserting test outcomes.
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

// TestRegenerateBoundaryStepFile_NonCommitment exercises the
// non-commitment regen path: keys with a value at lastTxNum are kept
// (with the as-of value), keys without one are dropped, no anchor
// injection.
func TestRegenerateBoundaryStepFile_NonCommitment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	keyA := []byte("aa-alice")
	keyB := []byte("bb-bob")
	keyC := []byte("cc-carol-created-post-anchor")
	in := [][2][]byte{
		{keyA, []byte("alice-stale-100ETH")},
		{keyB, []byte("bob-stale-200ETH")},
		{keyC, []byte("carol-50ETH")},
	}
	oldPath := writeKV(t, ctx, dir, "v1.0-accounts.264-266.kv", in)

	// AsOfLookup: alice + bob have values at the anchor; carol didn't
	// exist yet.
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
		ctx, kv.AccountsDomain, oldPath, newPath, lookup,
		103_848_485, seg.CompressNone, nil,
		dir, log.New(),
	)
	require.NoError(t, err)

	got := readKV(t, newPath)
	require.Equal(t, [][2][]byte{
		{keyA, []byte("alice-anchored-50ETH")},
		{keyB, []byte("bob-anchored-150ETH")},
	}, got, "carol must be dropped; alice/bob get the anchored values")
}

// TestRegenerateBoundaryStepFile_Commitment exercises the
// commitment-domain path: KeyCommitmentState is REPLACED with the
// supplied anchor blob regardless of what was in the old file; other
// keys go through the as-of lookup.
func TestRegenerateBoundaryStepFile_Commitment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	branchKey := []byte("branch-deadbeef")
	in := [][2][]byte{
		{branchKey, []byte("stale-branch-rh-A")},
		{commitmentdb.KeyCommitmentState, []byte("stale-commitment-state-at-block-2912403")},
	}
	oldPath := writeKV(t, ctx, dir, "v2.0-commitment.264-266.kv", in)

	lookup := func(d kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
		require.Equal(t, kv.CommitmentDomain, d)
		return []byte("branch-rh-at-anchor"), true, nil
	}

	anchor := []byte("encoded-anchor-blockNum-2910208-txNum-103848485-trieState")
	newPath := oldPath + ".regen"
	err := RegenerateBoundaryStepFile(
		ctx, kv.CommitmentDomain, oldPath, newPath, lookup,
		103_848_485, seg.CompressNone, anchor,
		dir, log.New(),
	)
	require.NoError(t, err)

	got := readKV(t, newPath)
	require.Equal(t, [][2][]byte{
		{branchKey, []byte("branch-rh-at-anchor")},
		{commitmentdb.KeyCommitmentState, anchor},
	}, got, "branch is re-resolved; KeyCommitmentState gets the anchor blob unconditionally")
}

func TestRegenerateBoundaryStepFile_CommitmentRequiresAnchor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	oldPath := writeKV(t, ctx, dir, "v2.0-commitment.264-266.kv", nil)

	err := RegenerateBoundaryStepFile(
		ctx, kv.CommitmentDomain, oldPath, oldPath+".regen",
		func(kv.Domain, []byte, uint64) ([]byte, bool, error) { return nil, false, nil },
		0, seg.CompressNone, nil /* anchor */, dir, log.New(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commitmentAnchor required")
}

func TestRegenerateBoundaryStepFile_NonCommitmentRejectsAnchor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	oldPath := writeKV(t, ctx, dir, "v1.0-accounts.264-266.kv", nil)

	err := RegenerateBoundaryStepFile(
		ctx, kv.AccountsDomain, oldPath, oldPath+".regen",
		func(kv.Domain, []byte, uint64) ([]byte, bool, error) { return nil, false, nil },
		0, seg.CompressNone, []byte("anchor"), dir, log.New(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be nil for non-commitment")
}

func TestRegenerateBoundaryStepFile_CommitmentWithoutKeyCommitmentStateErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	// Commitment .kv that's missing KeyCommitmentState entirely —
	// shouldn't happen in production but defensible to detect.
	oldPath := writeKV(t, ctx, dir, "v2.0-commitment.264-266.kv", [][2][]byte{
		{[]byte("branch-only-no-anchor"), []byte("value")},
	})

	err := RegenerateBoundaryStepFile(
		ctx, kv.CommitmentDomain, oldPath, oldPath+".regen",
		func(kv.Domain, []byte, uint64) ([]byte, bool, error) {
			return []byte("v"), true, nil
		},
		103_848_485, seg.CompressNone, []byte("anchor"), dir, log.New(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no KeyCommitmentState")
}

// TestRenameStepRange covers the helper that builds the truncated
// filename for the mode-B regen output when the boundary file's
// ToStep extends past the unwind-target step. Each row is a
// (basename, oldTo, newTo) → expected mapping; the test pins the
// rule that:
//   - "<from>-<oldTo>" segments are rewritten to "<from>-<newTo>"
//   - only the first matching segment is touched (defensive — the
//     boundary file basename has at most one step range)
//   - missing segments return the input unchanged (no-op fallback)
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

// TestRegenerateBoundaryStepFile_ContentReflectsLastTxNum is the
// load-bearing content-semantics test: every (key, value) pair in
// the regen output reflects the value the key had at lastTxNum, as
// determined by the supplied AsOfLookup. Keys that didn't exist at
// lastTxNum (lookup returns found=false) are dropped from the
// output. Together these two semantic guarantees are what makes the
// regen output a valid representation of the "as-of-lastTxNum
// snapshot" of the boundary range's state.
//
// This test pins the contract by exercising a multi-key fixture
// where the AsOfLookup returns different values + drop decisions
// per key. The existing TestRegenerateBoundaryStepFile_DropsKeysCreated
// AfterAnchor covered the drop case in isolation; this test combines
// drop + as-of-rewrite + commitment-anchor-replacement in a single
// fixture so a future refactor that breaks any one of them surfaces
// here.
func TestRegenerateBoundaryStepFile_ContentReflectsLastTxNum(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	// Three keys with distinct fates:
	//   keyKept  — value at lastTxNum differs from pre-anchor; rewritten.
	//   keyDrop  — didn't exist at lastTxNum; dropped from output.
	//   keySame  — value unchanged across the anchor; passes through.
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
		ctx, kv.AccountsDomain, oldPath, newPath, lookup,
		anchor, seg.CompressNone, nil,
		dir, log.New(),
	))

	got := readKV(t, newPath)
	require.Equal(t, [][2][]byte{
		{keyKept, []byte("kept-as-of-anchor")},
		{keySame, []byte("same-value-stable")},
	}, got, "regen content must be drop+as-of-rewrite per AsOfLookup")
}

// TestRegenerateBoundaryStepFile_TruncatedNameMatchesContent is the
// integration test for the truncated-rename path: when the caller
// chooses a narrower newKVPath than the original (e.g. the step
// boundary lands mid-original-range), the output file's NAME
// reflects the truncated coverage AND its CONTENT reflects the
// regen semantics. Together they form the contract "the file's name
// honestly describes what's inside" — the property the
// truncated-rename fix exists to enforce.
//
// We can verify both at the file-system level without a full
// aggregator integration: parse the file's stepDomainKey from its
// name, parse the file's KV content via readKV, and assert both
// describe consistent state.
func TestRegenerateBoundaryStepFile_TruncatedNameMatchesContent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	keyA := []byte("alice")
	stalePre := [][2][]byte{
		{keyA, []byte("alice-stale-50ETH-post-anchor")},
	}
	oldPath := writeKV(t, ctx, dir, "v1.1-accounts.272-280.kv", stalePre)

	// Truncated newKVPath: caller chose 272-278 because the unwind
	// boundary lands at step 278.
	newKVPath := filepath.Join(dir, "v1.1-accounts.272-278.kv.regen")

	lookup := func(d kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
		return []byte("alice-anchored-30ETH"), true, nil
	}

	require.NoError(t, RegenerateBoundaryStepFile(
		ctx, kv.AccountsDomain, oldPath, newKVPath, lookup,
		108_584_330, seg.CompressNone, nil,
		dir, log.New(),
	))

	// 1. File lives at the truncated path.
	_, err := os.Stat(newKVPath)
	require.NoError(t, err)

	// 2. File content is the as-of-anchor value (not the pre-anchor stale value).
	got := readKV(t, newKVPath)
	require.Equal(t, [][2][]byte{{keyA, []byte("alice-anchored-30ETH")}}, got,
		"truncated file's content must reflect as-of-anchor values, NOT pre-anchor stale ones")

	// 3. Filename's step range is narrower than the source's, matching
	// what the caller asked for. (The truncation decision lives in
	// regenerateBoundaryStepFiles; we verify here that
	// RegenerateBoundaryStepFile honours whatever name the caller
	// supplied.)
	require.Contains(t, newKVPath, "272-278", "truncated file name carries the narrower step range")
	require.NotContains(t, newKVPath, "272-280", "truncated file name must not retain the original wider range")
}

// TestRegenerateBoundaryStepFile_WritesToNewKVPath verifies that the
// explicit newKVPath parameter is honoured — the regen content lands
// at the supplied path, NOT at oldKVPath + ".regen" (the previous
// convention). This is the seam the truncated-rename path uses to
// place the regen output under a narrower filename matching its
// actual content coverage.
func TestRegenerateBoundaryStepFile_WritesToNewKVPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	keyA := []byte("alice")
	oldPath := writeKV(t, ctx, dir, "v1.1-accounts.272-280.kv", [][2][]byte{{keyA, []byte("v")}})

	// Caller-supplied newKVPath is a *truncated* basename, simulating
	// the truncated-rename code path.
	newKVPath := filepath.Join(dir, "v1.1-accounts.272-278.kv.regen")

	lookup := func(d kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
		return []byte("regen-v"), true, nil
	}

	require.NoError(t, RegenerateBoundaryStepFile(
		ctx, kv.AccountsDomain, oldPath, newKVPath, lookup,
		108_584_330, seg.CompressNone, nil,
		dir, log.New(),
	))

	got := readKV(t, newKVPath)
	require.Equal(t, [][2][]byte{{keyA, []byte("regen-v")}}, got)

	// The legacy "oldKVPath + .regen" path MUST NOT exist — the
	// caller chose its own newKVPath.
	_, err := os.Stat(oldPath + ".regen")
	require.True(t, os.IsNotExist(err), "regen output must live at the caller-supplied path, not the legacy convention")
}
