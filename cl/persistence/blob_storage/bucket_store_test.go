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
	"bytes"
	"io"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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

	require.ErrorIs(t, b.pruneBelow(3*subdivisionSlot), ErrPruneNotStarted)
	require.Zero(t, b.pruneFloor)
}

func TestBucketStorePruneBelowContinuesPastRemoveAllError(t *testing.T) {
	fs := newRemoveAllFailingFs(afero.NewMemMapFs())
	fs.failOn["1"] = errInducedFailure
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0", "1", "2", "3")

	require.ErrorIs(t, b.pruneBelow(3*subdivisionSlot), errInducedFailure)

	require.Equal(t, []string{"1", "3"}, rootEntryNames(t, fs))
}

func TestBucketStoreLowerPruneRetriesTheEstablishedFloor(t *testing.T) {
	fs := newRemoveAllFailingFs(afero.NewMemMapFs())
	fs.failOn["1"] = errInducedFailure
	b := newBucketStore(t, fs)
	makeBuckets(t, fs, "0", "1", "2")
	require.ErrorIs(t, b.pruneBelow(2*subdivisionSlot), errInducedFailure)
	require.Equal(t, []string{"1", "2"}, rootEntryNames(t, fs))

	delete(fs.failOn, "1")
	require.NoError(t, b.pruneBelow(subdivisionSlot))
	require.Equal(t, []string{"2"}, rootEntryNames(t, fs))
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
	s.initLocks()

	require.Same(t, s.forSlot(7), s.forSlot(7))
	require.Same(t, s.forSlot(7), s.forSlot(7+rwLocksCount))
	require.NotSame(t, s.forSlot(7), s.forSlot(8))
}

func TestSlotLocksDoNotBlockOtherStripes(t *testing.T) {
	var s slotLocks
	s.initLocks()

	s.forSlot(7).Lock()
	defer s.forSlot(7).Unlock()

	require.False(t, s.forSlot(7+rwLocksCount).TryLock())
	require.True(t, s.forSlot(8).TryLock())
	s.forSlot(8).Unlock()
}

func testSidecar(slot, idx uint64, marker byte) *cltypes.BlobSidecar {
	blob := &cltypes.Blob{}
	blob[0] = marker
	return cltypes.NewBlobSidecar(
		idx,
		blob,
		common.Bytes48{marker},
		common.Bytes48{marker, marker},
		&cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: slot}},
		solid.NewHashVector(cltypes.CommitmentBranchSize),
	)
}

func TestBucketStoreWriteReadRoundTrip(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	root := common.Hash{7}
	want := testSidecar(12_345, 2, 9)

	created, err := b.write(12_345, root, 2, want)
	require.NoError(t, err)
	require.True(t, created)

	got := &cltypes.BlobSidecar{}
	found, err := b.read(12_345, root, 2, got, clparams.DenebVersion)
	require.NoError(t, err)
	require.True(t, found)

	require.Equal(t, want.Index, got.Index)
	require.Equal(t, want.Blob, got.Blob)
	require.Equal(t, want.KzgCommitment, got.KzgCommitment)
	require.Equal(t, want.KzgProof, got.KzgProof)
	require.Equal(t, want.SignedBlockHeader, got.SignedBlockHeader)
}

func TestBucketStoreWriteLandsAtThePathItDerives(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	root := common.Hash{7}

	_, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)

	_, file := b.path(12_345, root, 2)
	exists, err := afero.Exists(fs, file)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestBucketStoreExistsFollowsTheFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	root := common.Hash{7}

	exists, err := b.exists(12_345, root, 2)
	require.NoError(t, err)
	require.False(t, exists)

	_, err = b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)

	exists, err = b.exists(12_345, root, 2)
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = b.exists(12_345, root, 3)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestBucketStoreStreamReproducesTheStoredBytes(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	root := common.Hash{7}

	_, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)

	_, file := b.path(12_345, root, 2)
	want, err := afero.ReadFile(fs, file)
	require.NoError(t, err)
	require.NotEmpty(t, want)

	var got bytes.Buffer
	require.NoError(t, b.stream(&got, 12_345, root, 2))
	require.Equal(t, want, got.Bytes())
}

func TestBucketStoreStreamOfMissingFileErrors(t *testing.T) {
	b := newBucketStore(t, afero.NewMemMapFs())

	var got bytes.Buffer
	require.Error(t, b.stream(&got, 12_345, common.Hash{7}, 2))
}

func TestBucketStoreRemoveThenExistsIsFalse(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	root := common.Hash{7}

	_, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)
	require.NoError(t, b.remove(12_345, root, 2))

	exists, err := b.exists(12_345, root, 2)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestBucketStoreRemoveOfMissingFileIsNil(t *testing.T) {
	b := newBucketStore(t, afero.NewMemMapFs())

	require.NoError(t, b.remove(12_345, common.Hash{7}, 2))
	require.NoError(t, b.remove(12_345, common.Hash{7}, 2))
}

func TestBucketStoreReadOfMissingFileIsNotFound(t *testing.T) {
	b := newBucketStore(t, afero.NewMemMapFs())

	got := &cltypes.BlobSidecar{}
	found, err := b.read(12_345, common.Hash{7}, 2, got, clparams.DenebVersion)
	require.NoError(t, err)
	require.False(t, found)
}

func TestBucketStoreReadOfATruncatedFileIsNotFound(t *testing.T) {
	fs := afero.NewMemMapFs()
	b := newBucketStore(t, fs)
	root := common.Hash{7}

	_, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)

	_, file := b.path(12_345, root, 2)
	original, err := afero.ReadFile(fs, file)
	require.NoError(t, err)
	require.NoError(t, afero.WriteFile(fs, file, original[:len(original)/2], 0o644))

	got := &cltypes.BlobSidecar{}
	found, err := b.read(12_345, root, 2, got, clparams.DenebVersion)
	require.NoError(t, err)
	require.False(t, found)

	created, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 11))
	require.NoError(t, err)
	require.False(t, created)

	got2 := &cltypes.BlobSidecar{}
	found2, err := b.read(12_345, root, 2, got2, clparams.DenebVersion)
	require.NoError(t, err)
	require.True(t, found2)
	require.Equal(t, byte(11), got2.Blob[0])
}

func TestBucketStoreWriteReportsCreatedOnlyOnce(t *testing.T) {
	b := newBucketStore(t, afero.NewMemMapFs())
	root := common.Hash{7}

	created, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)
	require.True(t, created)

	created, err = b.write(12_345, root, 2, testSidecar(12_345, 2, 11))
	require.NoError(t, err)
	require.False(t, created)

	created, err = b.write(12_345, root, 3, testSidecar(12_345, 3, 9))
	require.NoError(t, err)
	require.True(t, created)
}

func TestBucketStoreWriteOverwritesTheStoredValue(t *testing.T) {
	b := newBucketStore(t, afero.NewMemMapFs())
	root := common.Hash{7}

	_, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)
	_, err = b.write(12_345, root, 2, testSidecar(12_345, 2, 11))
	require.NoError(t, err)

	got := &cltypes.BlobSidecar{}
	found, err := b.read(12_345, root, 2, got, clparams.DenebVersion)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, byte(11), got.Blob[0])
}

func TestBucketStoreFailedWriteLeavesNothingBehind(t *testing.T) {
	for _, tc := range []struct {
		name   string
		budget int
	}{
		{name: "no byte written", budget: 0},
		{name: "truncated", budget: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFailingFs(afero.NewMemMapFs())
			b := newBucketStore(t, fs)
			root := common.Hash{7}
			_, file := b.path(12_345, root, 2)
			fs.failWritesAfter(file+tmpSuffix, tc.budget, errInducedFailure)

			created, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
			require.ErrorIs(t, err, errInducedFailure)
			require.False(t, created)

			exists, err := b.exists(12_345, root, 2)
			require.NoError(t, err)
			require.False(t, exists)

			tmpExists, err := afero.Exists(fs, file+tmpSuffix)
			require.NoError(t, err)
			require.False(t, tmpExists)
		})
	}
}

func TestBucketStoreFailedWriteKeepsThePreviousValue(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	b := newBucketStore(t, fs)
	root := common.Hash{7}

	_, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)

	_, file := b.path(12_345, root, 2)
	fs.failWritesAfter(file+tmpSuffix, 4, errInducedFailure)
	_, err = b.write(12_345, root, 2, testSidecar(12_345, 2, 11))
	require.ErrorIs(t, err, errInducedFailure)

	fs.clearFailures()
	got := &cltypes.BlobSidecar{}
	found, err := b.read(12_345, root, 2, got, clparams.DenebVersion)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, byte(9), got.Blob[0])

	tmpExists, err := afero.Exists(fs, file+tmpSuffix)
	require.NoError(t, err)
	require.False(t, tmpExists)
}

func TestBucketStoreFailedSyncLeavesNothingBehind(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	b := newBucketStore(t, fs)
	root := common.Hash{7}
	_, file := b.path(12_345, root, 2)
	fs.failSyncAt(file+tmpSuffix, errInducedFailure)

	_, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.ErrorIs(t, err, errInducedFailure)

	exists, err := b.exists(12_345, root, 2)
	require.NoError(t, err)
	require.False(t, exists)

	tmpExists, err := afero.Exists(fs, file+tmpSuffix)
	require.NoError(t, err)
	require.False(t, tmpExists)
}

func TestFacadePruneRejectsAWriteIntoTheBucketItRemoves(t *testing.T) {
	fs := newPruneRaceFs(afero.NewMemMapFs())
	fs.removePath = "0"
	storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())
	require.NoError(t, fs.MkdirAll("0", 0o755))

	const liveSlot = 5*subdivisionSlot + 1
	liveRoot := common.Hash{1}
	require.NoError(t, storage.WriteColumnSidecars(t.Context(), liveRoot, 0, createTestDataColumnSidecar(liveSlot, 0)))

	pruneDone := make(chan error, 1)
	go func() { pruneDone <- storage.PruneBelow(subdivisionSlot) }()
	<-fs.removeEntered

	var streamed bytes.Buffer
	require.NoError(t, storage.WriteStream(&streamed, liveSlot, liveRoot, 0))

	require.NoError(t, storage.WriteColumnSidecars(t.Context(), common.Hash{2}, 0, createTestDataColumnSidecar(liveSlot+1, 0)))

	require.NoError(t, storage.WriteColumnSidecars(t.Context(), common.Hash{3}, 0, createTestDataColumnSidecar(1, 0)))
	prunedWriteExists, err := storage.ColumnSidecarExists(t.Context(), 1, common.Hash{3}, 0)
	require.NoError(t, err)
	require.False(t, prunedWriteExists)

	close(fs.removeRelease)
	require.NoError(t, <-pruneDone)
}

func TestFacadeWriteRacingPruneLeavesNoPartialFile(t *testing.T) {
	for _, tc := range []struct {
		name              string
		releaseWriteFirst bool
	}{
		{name: "write released first", releaseWriteFirst: true},
		{name: "both released together", releaseWriteFirst: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newPruneRaceFs(afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()))
			storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())
			const slot = 1000
			root := common.Hash{3}
			_, file := (&bucketStore{fs: fs}).path(slot, root, 0)
			fs.removePath = "0"
			fs.createPath = file + tmpSuffix
			require.NoError(t, fs.MkdirAll(fs.removePath, 0o755))

			writeDone := make(chan error, 1)
			go func() {
				writeDone <- storage.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(slot, 0))
			}()
			<-fs.createEntered

			pruneDone := make(chan error, 1)
			go func() { pruneDone <- storage.PruneBelow(subdivisionSlot) }()

			if tc.releaseWriteFirst {
				close(fs.createRelease)
				require.NoError(t, <-writeDone)
				stored, err := storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 0)
				require.NoError(t, err)
				require.Equal(t, uint64(0), stored.Index)
				<-fs.removeEntered
				close(fs.removeRelease)
				require.NoError(t, <-pruneDone)
			} else {
				// The prune lock serializes write and prune regardless of release order, so
				// releasing both gates up front still resolves one at a time and both sides
				// still must succeed.
				close(fs.removeRelease)
				close(fs.createRelease)
				require.NoError(t, <-writeDone)
				require.NoError(t, <-pruneDone)
			}

			tmpExists, err := afero.Exists(fs, file+tmpSuffix)
			require.NoError(t, err)
			require.False(t, tmpExists)
			exists, err := storage.ColumnSidecarExists(t.Context(), slot, root, 0)
			require.NoError(t, err)
			if exists {
				stored, err := storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 0)
				require.NoError(t, err)
				require.Equal(t, uint64(0), stored.Index)
			}
		})
	}
}

func TestFacadeConcurrentWritesUseOneTempFileAtATime(t *testing.T) {
	fs := newSerializedCreateFs(afero.NewMemMapFs())
	storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())
	const slot = 1000
	root := common.Hash{4}
	_, file := (&bucketStore{fs: fs}).path(slot, root, 0)
	fs.tempPath = file + tmpSuffix

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- storage.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(slot, 0))
	}()
	<-fs.firstWriteEntered

	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondDone <- storage.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(slot, 0))
	}()
	<-secondStarted

	interleaved := false
	select {
	case <-fs.secondCreateEntered:
		interleaved = true
	case <-time.After(100 * time.Millisecond):
	}

	close(fs.releaseFirstWrite)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.False(t, interleaved)

	stored, err := storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(0), stored.Index)
	tmpExists, err := afero.Exists(fs, file+tmpSuffix)
	require.NoError(t, err)
	require.False(t, tmpExists)
}

type pruneRaceFs struct {
	afero.Fs
	removePath    string
	createPath    string
	removeEntered chan struct{}
	removeRelease chan struct{}
	createEntered chan struct{}
	createRelease chan struct{}
	removeOnce    sync.Once
	createOnce    sync.Once
}

func newPruneRaceFs(fs afero.Fs) *pruneRaceFs {
	return &pruneRaceFs{
		Fs:            fs,
		removeEntered: make(chan struct{}),
		removeRelease: make(chan struct{}),
		createEntered: make(chan struct{}),
		createRelease: make(chan struct{}),
	}
}

func (f *pruneRaceFs) RemoveAll(path string) error {
	if path == f.removePath {
		f.removeOnce.Do(func() { close(f.removeEntered) })
		<-f.removeRelease
	}
	return f.Fs.RemoveAll(path)
}

func (f *pruneRaceFs) Create(name string) (afero.File, error) {
	fh, err := f.Fs.Create(name)
	if err != nil {
		return nil, err
	}
	if name == f.createPath {
		f.createOnce.Do(func() { close(f.createEntered) })
		<-f.createRelease
	}
	return fh, nil
}

type serializedCreateFs struct {
	afero.Fs
	tempPath            string
	firstWriteEntered   chan struct{}
	releaseFirstWrite   chan struct{}
	secondCreateEntered chan struct{}
	createMu            sync.Mutex
	createCount         int
}

func newSerializedCreateFs(fs afero.Fs) *serializedCreateFs {
	return &serializedCreateFs{
		Fs:                  fs,
		firstWriteEntered:   make(chan struct{}),
		releaseFirstWrite:   make(chan struct{}),
		secondCreateEntered: make(chan struct{}),
	}
}

func (f *serializedCreateFs) Create(name string) (afero.File, error) {
	fh, err := f.Fs.Create(name)
	if err != nil || name != f.tempPath {
		return fh, err
	}

	f.createMu.Lock()
	f.createCount++
	createCount := f.createCount
	f.createMu.Unlock()
	if createCount == 1 {
		return &serializedWriteFile{
			File:    fh,
			entered: f.firstWriteEntered,
			release: f.releaseFirstWrite,
		}, nil
	}
	if createCount == 2 {
		close(f.secondCreateEntered)
	}
	return fh, nil
}

type serializedWriteFile struct {
	afero.File
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *serializedWriteFile) Write(p []byte) (int, error) {
	f.once.Do(func() {
		close(f.entered)
		<-f.release
	})
	return f.File.Write(p)
}

func TestBucketStoreWriteReportsRenameFailureWhenTheTargetIsGone(t *testing.T) {
	fs := newRenameFailingFs(afero.NewMemMapFs(), errInducedFailure)
	b := newBucketStore(t, fs)
	root := common.Hash{7}

	_, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.ErrorIs(t, err, errInducedFailure)

	fs.err = nil
	_, err = b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)

	// A rename failure that is not a sharing violation always propagates, target present
	// or not.
	fs.err = errInducedFailure
	created, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 11))
	require.ErrorIs(t, err, errInducedFailure)
	require.False(t, created)

	fs.dropDestination = true
	_, err = b.write(12_345, root, 2, testSidecar(12_345, 2, 11))
	require.ErrorIs(t, err, errInducedFailure)
}

func TestBucketStoreWriteRenameFailurePropagatesWhenNotASharingViolation(t *testing.T) {
	fs := newRenameFailingFs(afero.NewMemMapFs(), nil)
	b := newBucketStore(t, fs)
	root := common.Hash{7}

	_, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.NoError(t, err)

	fs.err = syscall.ENOSPC
	created, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 11))
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.False(t, created)
}

func TestBucketStoreWriteSurfacesAShortWriteFromTheFinalFlush(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	b := newBucketStore(t, fs)
	root := common.Hash{7}
	_, file := b.path(12_345, root, 2)
	fs.failShortWrite(file + tmpSuffix)

	created, err := b.write(12_345, root, 2, testSidecar(12_345, 2, 9))
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.False(t, created)

	exists, err := afero.Exists(fs, file)
	require.NoError(t, err)
	require.False(t, exists)
}
