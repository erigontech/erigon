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

package blob_storage

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

type removeAllFailingFs struct {
	afero.Fs
	failOn map[string]error
}

func newRemoveAllFailingFs(fs afero.Fs) *removeAllFailingFs {
	return &removeAllFailingFs{Fs: fs, failOn: map[string]error{}}
}

func (r *removeAllFailingFs) RemoveAll(path string) error {
	if err, ok := r.failOn[path]; ok {
		return err
	}
	return r.Fs.RemoveAll(path)
}

func newBucketStore(t *testing.T, fs afero.Fs) *bucketStore {
	t.Helper()
	b := &bucketStore{}
	b.init(fs)
	return b
}

func makeBuckets(t *testing.T, fs afero.Fs, names ...string) {
	t.Helper()
	for _, name := range names {
		require.NoError(t, fs.MkdirAll(name, 0o755))
	}
}

func rootEntryNames(t *testing.T, fs afero.Fs) []string {
	t.Helper()
	entries, err := afero.ReadDir(fs, ".")
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	slices.Sort(names)
	return names
}

func TestBucketStorePath(t *testing.T) {
	b := newBucketStore(t, afero.NewMemMapFs())
	root := common.HexToHash("0x1234567890abcdef")
	const rootHex = "0x0000000000000000000000000000000000000000000000001234567890abcdef"

	for _, tc := range []struct {
		name     string
		slot     uint64
		idx      uint64
		wantDir  string
		wantFile string
	}{
		{name: "first bucket", slot: 1000, idx: 2, wantDir: "0", wantFile: "0/" + rootHex + "_2"},
		{name: "bucket boundary", slot: subdivisionSlot, idx: 0, wantDir: "1", wantFile: "1/" + rootHex + "_0"},
		{name: "last slot of bucket", slot: 2*subdivisionSlot - 1, idx: 127, wantDir: "1", wantFile: "1/" + rootHex + "_127"},
		{name: "deep bucket", slot: 12_345_678, idx: 5, wantDir: "1234", wantFile: "1234/" + rootHex + "_5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, file := b.path(tc.slot, root, tc.idx)
			require.Equal(t, tc.wantDir, dir)
			require.Equal(t, tc.wantFile, file)
		})
	}
}

func TestBucketStorePruneBelowRemovesOnlyExpiredBuckets(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0", "1", "2", "3", "4")

	require.NoError(t, b.pruneBelow(3*subdivisionSlot))

	require.Equal(t, []string{"3", "4"}, rootEntryNames(t, fs))
}

func TestBucketStorePruneBelowKeepsTheBucketHoldingTheFloor(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0", "1", "2")

	require.NoError(t, b.pruneBelow(2*subdivisionSlot+4321))

	require.Equal(t, []string{"2"}, rootEntryNames(t, fs))
}

func TestBucketStorePruneBelowIsIdempotent(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0", "1", "2")
	require.NoError(t, afero.WriteFile(fs, "2/kept", []byte("payload"), 0o644))

	require.NoError(t, b.pruneBelow(2*subdivisionSlot))
	require.NoError(t, b.pruneBelow(2*subdivisionSlot))

	require.Equal(t, []string{"2"}, rootEntryNames(t, fs))
	content, err := afero.ReadFile(fs, "2/kept")
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), content)
}

// The blob index database lives in the store root as a sibling of the buckets, so a
// pruner that treats any directory as a bucket deletes it on the first tick.
func TestBucketStorePruneBelowKeepsTheBlobDatabase(t *testing.T) {
	fs := afero.NewBasePathFs(afero.NewOsFs(), t.TempDir())
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0", "1", "chaindata", "0x", "12a", " 7", "007")
	require.NoError(t, afero.WriteFile(fs, "chaindata/mdbx.dat", []byte("database"), 0o644))
	require.NoError(t, afero.WriteFile(fs, "5", []byte("not a bucket"), 0o644))

	require.NoError(t, b.pruneBelow(1000*subdivisionSlot))

	require.Equal(t, []string{" 7", "007", "0x", "12a", "5", "chaindata"}, rootEntryNames(t, fs))
	content, err := afero.ReadFile(fs, "chaindata/mdbx.dat")
	require.NoError(t, err)
	require.Equal(t, []byte("database"), content)
}

func TestBucketStorePruneBelowZeroRemovesNothing(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0", "1", "2")

	require.NoError(t, b.pruneBelow(0))

	require.Equal(t, []string{"0", "1", "2"}, rootEntryNames(t, fs))
	require.Zero(t, fs.count(opRemoveAll))
}

func TestBucketStorePruneBelowBelowOneBucketRemovesNothing(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0")

	require.NoError(t, b.pruneBelow(subdivisionSlot-1))

	require.Equal(t, []string{"0"}, rootEntryNames(t, fs))
}

func TestBucketStorePruneBelowPropagatesReaddirError(t *testing.T) {
	fs := afero.NewBasePathFs(afero.NewOsFs(), filepath.Join(t.TempDir(), "missing-root"))
	b := newBucketStore(t, fs)

	require.Error(t, b.pruneBelow(3*subdivisionSlot))
}

func TestBucketStorePruneBelowContinuesPastRemoveAllError(t *testing.T) {
	fs := newRemoveAllFailingFs(afero.NewMemMapFs())
	fs.failOn["1"] = errInducedFailure
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0", "1", "2", "3")

	require.ErrorIs(t, b.pruneBelow(3*subdivisionSlot), errInducedFailure)

	require.Equal(t, []string{"1", "3"}, rootEntryNames(t, fs))
}

func TestBucketStorePruneBelowRemovesEachExpiredBucketOnce(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0", "1", "2", "7")

	require.NoError(t, b.pruneBelow(5*subdivisionSlot))

	removed := fs.paths(opRemoveAll)
	slices.Sort(removed)
	require.Equal(t, []string{"0", "1", "2"}, removed)

	fs.reset()
	require.NoError(t, b.pruneBelow(5*subdivisionSlot))
	require.Zero(t, fs.count(opRemoveAll))
}

func TestSlotLocksStripesBySlot(t *testing.T) {
	var s slotLocks
	s.init()

	require.Same(t, s.forSlot(7), s.forSlot(7))
	require.Same(t, s.forSlot(7), s.forSlot(7+rwLocksCount))
	require.NotSame(t, s.forSlot(7), s.forSlot(8))
}

func TestSlotLocksDoNotBlockOtherStripes(t *testing.T) {
	var s slotLocks
	s.init()

	s.forSlot(7).Lock()
	defer s.forSlot(7).Unlock()

	require.False(t, s.forSlot(7+rwLocksCount).TryLock())
	require.True(t, s.forSlot(8).TryLock())
	s.forSlot(8).Unlock()
}
