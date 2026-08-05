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
		ctx, kv.StorageDomain, walker, lookup, 100, newPath, dir, seg.CompressNone, log.New(),
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
		ctx, kv.CodeDomain, walker, lookup, 100, newPath, dir, seg.CompressNone, log.New(),
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
		filepath.Join(dir, "should-not-write.kv"), dir, seg.CompressNone, log.New(),
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
		filepath.Join(dir, "aborted.kv"), dir, seg.CompressNone, log.New(),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, sentinel)
}
