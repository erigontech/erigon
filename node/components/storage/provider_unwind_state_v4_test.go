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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

// keyWalkerFromSlice returns a StateKeyWalker that yields each key in
// order from the slice, exactly once per key. Callers pass an
// already-deduped list.
func keyWalkerFromSlice(keys [][]byte) StateKeyWalker {
	return func(yield func(key []byte) bool) error {
		for _, k := range keys {
			if !yield(k) {
				return nil
			}
		}
		return nil
	}
}

// fakeAccessorBuilder records BuildKVAccessors calls without doing any
// real accessor build. Used by unit tests that verify the emit invokes
// the builder correctly. Production wires *state.Aggregator, which
// actually builds .bt/.kvei/.kvi from the .kv content.
type fakeAccessorBuilder struct {
	calls []fakeAccessorCall
	err   error // returned by every call when non-nil
}

type fakeAccessorCall struct {
	domain    kv.Domain
	dataPath  string
	finalPath string
}

func (f *fakeAccessorBuilder) BuildKVAccessors(_ context.Context, domain kv.Domain, dataPath, finalPath string) error {
	f.calls = append(f.calls, fakeAccessorCall{domain: domain, dataPath: dataPath, finalPath: finalPath})
	return f.err
}

// TestWriteStateBoundaryFileV4_IncludesKeysMissingFromOldFile pins the
// load-bearing correctness property: keys first-written IN the boundary
// window (which weren't in any pre-unwind boundary .kv) MUST appear in
// the v4 output. This is the exact bug the leg P v7 iter 4 mode_b
// failure surfaced — the old iterate-OLD-file regen missed such keys,
// and forward-exec of the very-next block SLOADed them as empty and
// mis-priced SSTOREs by 220k gas total.
func TestWriteStateBoundaryFileV4_IncludesKeysMissingFromOldFile(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()

	// Three keys touched in the boundary window (fromTxN, lastTxN+1]:
	//   K1 — existed pre-window, updated in window: AsOf value differs from pre-window
	//   K2 — new key first-written in window
	//   K3 — existed pre-window, no touch in window (STILL yielded by the walker if
	//        any prior baseline had it, but our walker takes only in-window touches)
	//        We include K3 in the walker output to represent the case where the
	//        history walk yields a key that was touched by an unrelated read
	//        pattern; the lookup returns its current value regardless.
	k1 := []byte("k1")
	k2 := []byte("k2")
	k3 := []byte("k3")
	lastTxN := uint64(1000)

	// Synthetic AsOf lookup — pins the value the regen should emit per key.
	lookup := func(_ kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
		require.Equal(t, lastTxN, ts, "regen must query at lastTxN exactly")
		switch string(key) {
		case "k1":
			return []byte("v1-at-lastTxN"), true, nil
		case "k2":
			return []byte("v2-first-write-in-window"), true, nil
		case "k3":
			return []byte("v3-carried-forward"), true, nil
		}
		t.Fatalf("lookup called for unexpected key %q", key)
		return nil, false, nil
	}

	walker := keyWalkerFromSlice([][]byte{k1, k2, k3})
	newPath := filepath.Join(dir, "v4.0-accounts.0-1001.kv")

	err := WriteStateBoundaryFileV4(
		ctx,
		kv.AccountsDomain,
		walker,
		lookup,
		lastTxN,
		newPath,
		dir,
		seg.CompressNone,
		&fakeAccessorBuilder{},
		log.New(),
	)
	require.NoError(t, err)

	got := readKV(t, newPath)
	require.Equal(t, [][2][]byte{
		{[]byte("k1"), []byte("v1-at-lastTxN")},
		{[]byte("k2"), []byte("v2-first-write-in-window")},
		{[]byte("k3"), []byte("v3-carried-forward")},
	}, got, "v4 file must contain every walker-yielded key with its AsOf(lastTxN) value")
}

// TestWriteStateBoundaryFileV4_TombstoneOnNotFound: a key the walker
// yields but the lookup reports as not-found (deleted before lastTxN)
// must land in the v4 file as an empty-value tombstone. Without this,
// older baseline files at earlier steps would leak the pre-tombstone
// value on a GetLatest fall-through.
func TestWriteStateBoundaryFileV4_TombstoneOnNotFound(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()

	lookup := func(_ kv.Domain, key []byte, _ uint64) ([]byte, bool, error) {
		switch string(key) {
		case "alive":
			return []byte("v-alive"), true, nil
		case "deleted":
			return nil, false, nil
		}
		return nil, false, nil
	}

	walker := keyWalkerFromSlice([][]byte{[]byte("alive"), []byte("deleted")})
	newPath := filepath.Join(dir, "v4.0-storage.0-100.kv")

	err := WriteStateBoundaryFileV4(
		ctx, kv.StorageDomain, walker, lookup, 100, newPath, dir, seg.CompressNone, &fakeAccessorBuilder{}, log.New(),
	)
	require.NoError(t, err)

	got := readKV(t, newPath)
	require.Len(t, got, 2)
	require.Equal(t, []byte("alive"), got[0][0])
	require.Equal(t, []byte("v-alive"), got[0][1])
	require.Equal(t, []byte("deleted"), got[1][0])
	require.Empty(t, got[1][1], "tombstoned value must round-trip as empty (nil-or-empty)")
}

// TestWriteStateBoundaryFileV4_EmptyWindow: a walker that yields no
// keys emits a valid empty .kv file (no keys). Represents the boundary
// case where the entire window contained no state writes for the domain.
func TestWriteStateBoundaryFileV4_EmptyWindow(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()

	lookup := func(_ kv.Domain, _ []byte, _ uint64) ([]byte, bool, error) {
		t.Fatalf("lookup must not be called when the walker yields nothing")
		return nil, false, nil
	}
	walker := keyWalkerFromSlice(nil)
	newPath := filepath.Join(dir, "v4.0-code.0-100.kv")

	err := WriteStateBoundaryFileV4(
		ctx, kv.CodeDomain, walker, lookup, 100, newPath, dir, seg.CompressNone, &fakeAccessorBuilder{}, log.New(),
	)
	require.NoError(t, err)
	require.Empty(t, readKV(t, newPath))
}

// TestWriteStateBoundaryFileV4_RejectsCommitmentDomain: commitment has
// its own emit path (WriteCommitmentBoundaryFileV4) that uses the
// compute's captured branches. Routing commitment through the state
// path would double-emit KeyCommitmentState and miss the anchor plant;
// the guard makes that misuse a build/test-time error rather than a
// silent producer bug.
func TestWriteStateBoundaryFileV4_RejectsCommitmentDomain(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	walker := keyWalkerFromSlice([][]byte{[]byte("k")})
	lookup := func(_ kv.Domain, _ []byte, _ uint64) ([]byte, bool, error) { return nil, false, nil }
	err := WriteStateBoundaryFileV4(
		t.Context(), kv.CommitmentDomain, walker, lookup, 100,
		filepath.Join(dir, "should-not-write.kv"), dir, seg.CompressNone, &fakeAccessorBuilder{}, log.New(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commitment domain")
}

// TestWriteStateBoundaryFileV4_LookupErrorAborts: an error from the
// lookup during the walk must propagate — the emitted file is
// discarded via defer comp.Close and the caller retries. Without this
// a transient DB error would silently drop keys from the v4 file.
func TestWriteStateBoundaryFileV4_LookupErrorAborts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sentinel := context.DeadlineExceeded
	lookup := func(_ kv.Domain, _ []byte, _ uint64) ([]byte, bool, error) {
		return nil, false, sentinel
	}
	walker := keyWalkerFromSlice([][]byte{[]byte("k")})
	err := WriteStateBoundaryFileV4(
		t.Context(), kv.AccountsDomain, walker, lookup, 100,
		filepath.Join(dir, "aborted.kv"), dir, seg.CompressNone, &fakeAccessorBuilder{}, log.New(),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, sentinel)
}

// TestWriteStateBoundaryFileV4_InvokesAccessorBuilder pins the fix for
// the 2026-08-06 leg-M v1 iter 1 mode_b gas mismatch: the v4 emit must
// build its .bt/.kvei/.kvi sidecars inline. Prior to the fix the emit
// shipped .kv-only, checkForVisibility rejected the file for missing
// bindex/existence, forward-exec state reads bypassed the invisible v4
// → fell through to older files → returned pre-window state → SSTOREs
// mispriced as RESET-instead-of-SET → −135,654 gas / block invalidated.
//
// Verifies the emit invokes the AccessorBuilder with:
//   - the correct domain,
//   - the actual write path as dataPath,
//   - the .regen-stripped final name as finalPath (so accessor filenames
//     land at the paired name FinalizeUnwind's rename produces).
func TestWriteStateBoundaryFileV4_InvokesAccessorBuilder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	lookup := func(_ kv.Domain, _ []byte, _ uint64) ([]byte, bool, error) {
		return []byte("v"), true, nil
	}
	walker := keyWalkerFromSlice([][]byte{[]byte("k")})
	final := filepath.Join(dir, "v4.0-storage.0-1001.kv")
	regen := final + ".regen"

	fab := &fakeAccessorBuilder{}
	err := WriteStateBoundaryFileV4(
		t.Context(), kv.StorageDomain, walker, lookup, 1000,
		regen, dir, seg.CompressNone, fab, log.New(),
	)
	require.NoError(t, err)
	require.Len(t, fab.calls, 1, "accessor builder must be invoked exactly once per emit")
	require.Equal(t, kv.StorageDomain, fab.calls[0].domain)
	require.Equal(t, regen, fab.calls[0].dataPath, "dataPath must be the actual write location (.regen)")
	require.Equal(t, final, fab.calls[0].finalPath, "finalPath must be the .regen-stripped eventual name")
}

// TestWriteStateBoundaryFileV4_AccessorBuilderErrorPropagates: any
// error from the accessor build fails the emit — the caller MUST NOT
// proceed with a rename that would put a .kv on disk without paired
// accessors (same invisibility failure mode).
func TestWriteStateBoundaryFileV4_AccessorBuilderErrorPropagates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sentinel := errAccessorBuild
	lookup := func(_ kv.Domain, _ []byte, _ uint64) ([]byte, bool, error) {
		return []byte("v"), true, nil
	}
	walker := keyWalkerFromSlice([][]byte{[]byte("k")})
	err := WriteStateBoundaryFileV4(
		t.Context(), kv.StorageDomain, walker, lookup, 1000,
		filepath.Join(dir, "v4.0-storage.0-1001.kv.regen"), dir, seg.CompressNone,
		&fakeAccessorBuilder{err: sentinel}, log.New(),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, sentinel)
}

// TestWriteStateBoundaryFileV4_NilAccessorBuilderRejected: the emit
// refuses to run without a builder rather than silently shipping a
// .kv without accessors. Defensive — the wire code always passes
// p.Aggregator, but a future caller that forgets is a soundness bug.
func TestWriteStateBoundaryFileV4_NilAccessorBuilderRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	lookup := func(_ kv.Domain, _ []byte, _ uint64) ([]byte, bool, error) {
		return nil, false, nil
	}
	walker := keyWalkerFromSlice([][]byte{[]byte("k")})
	err := WriteStateBoundaryFileV4(
		t.Context(), kv.StorageDomain, walker, lookup, 100,
		filepath.Join(dir, "v4.0-storage.0-101.kv.regen"), dir, seg.CompressNone, nil, log.New(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "accessors builder is required")
}

// errAccessorBuild is a sentinel used by the accessor-error test.
var errAccessorBuild = errSentinel("accessor build failed")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
