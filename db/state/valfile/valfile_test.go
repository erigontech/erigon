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

package valfile

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteSyncReadRoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "v1.0-commitment.0-1.cvl")
	vals := [][]byte{
		[]byte("first-branch"),
		nil,                              // empty value must round-trip
		bytes.Repeat([]byte{0xab}, 4096), // KB-sized branch
		[]byte("last"),
	}
	w, err := NewWriter(path)
	require.NoError(t, err)
	w.DisableFsync()
	handles := make([]Handle, len(vals))
	for i, v := range vals {
		handles[i], err = w.Append(v)
		require.NoError(t, err)
	}
	require.EqualValues(t, len(vals), w.Count())
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// handles must address strictly increasing offsets past the header
	require.GreaterOrEqual(t, handles[0].Offset, headerLen)
	for i := 1; i < len(handles); i++ {
		require.GreaterOrEqual(t, handles[i].Offset, handles[i-1].Offset)
	}

	r, err := OpenReader(path)
	require.NoError(t, err)
	defer r.Close()
	// read out of order to prove offset addressing is position-independent
	for _, i := range []int{2, 0, 3, 1} {
		got, err := r.Get(handles[i], nil)
		require.NoError(t, err)
		require.Equal(t, vals[i], got)
	}
}

// Reopen-for-append after a "crash": the prefix written before the restart is
// still referenced and must remain readable; new values append after it.
func TestReopenForAppend(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "v1.0-commitment.0-1.cvl")
	w, err := NewWriter(path)
	require.NoError(t, err)
	w.DisableFsync()
	hA, err := w.Append([]byte("committed-A"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	w2, err := OpenWriter(path)
	require.NoError(t, err)
	w2.DisableFsync()
	hB, err := w2.Append([]byte("appended-B"))
	require.NoError(t, err)
	require.Greater(t, hB.Offset, hA.Offset)
	require.NoError(t, w2.Sync())
	require.NoError(t, w2.Close())

	r, err := OpenReader(path)
	require.NoError(t, err)
	defer r.Close()
	gotA, err := r.Get(hA, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("committed-A"), gotA)
	gotB, err := r.Get(hB, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("appended-B"), gotB)
}

// A crash can leave bytes past the last committed handle. They are never
// referenced and must not corrupt reads of valid handles; re-execution appends
// fresh values after the garbage.
func TestGarbageTailIsHarmless(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "v1.0-commitment.0-1.cvl")
	w, err := NewWriter(path)
	require.NoError(t, err)
	w.DisableFsync()
	hA, err := w.Append([]byte("referenced-A"))
	require.NoError(t, err)
	require.NoError(t, w.Sync()) // A is durable and (conceptually) committed in MDBX
	// uncommitted tail: appended+flushed but its handle never lands in MDBX
	_, err = w.Append(bytes.Repeat([]byte{0xff}, 500))
	require.NoError(t, err)
	require.NoError(t, w.Close()) // flushes the garbage to disk

	// restart: resume and append the re-computed value after the garbage
	w2, err := OpenWriter(path)
	require.NoError(t, err)
	w2.DisableFsync()
	hC, err := w2.Append([]byte("recomputed-C"))
	require.NoError(t, err)
	require.NoError(t, w2.Sync())
	require.NoError(t, w2.Close())

	r, err := OpenReader(path)
	require.NoError(t, err)
	defer r.Close()
	gotA, err := r.Get(hA, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("referenced-A"), gotA)
	gotC, err := r.Get(hC, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("recomputed-C"), gotC)
	require.Greater(t, hC.Offset, hA.Offset+uint64(hA.Len)) // C lands past the garbage
}

func TestOpenRejectsBadMagic(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "v1.0-commitment.0-1.cvl")
	require.NoError(t, os.WriteFile(path, []byte("not-a-valfile"), 0o644))
	_, err := OpenReader(path)
	require.Error(t, err)
	_, err = OpenWriter(path)
	require.Error(t, err)
}

func TestGetOutOfBoundsErrorsNotPanics(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "v1.0-commitment.0-1.cvl")
	w, err := NewWriter(path)
	require.NoError(t, err)
	w.DisableFsync()
	h, err := w.Append([]byte("only"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	r, err := OpenReader(path)
	require.NoError(t, err)
	defer r.Close()

	_, err = r.Get(h, nil) // valid
	require.NoError(t, err)
	_, err = r.Get(Handle{Offset: h.Offset, Len: h.Len + 1000}, nil) // overruns EOF
	require.Error(t, err)
}
