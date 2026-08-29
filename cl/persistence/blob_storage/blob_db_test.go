// Copyright 2024 The Erigon Authors
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
	"context"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestVerifyBlobSidecarsGloasDoesNotRequireInclusionProof(t *testing.T) {
	blob := goethkzg.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof(&blob, commitment, 0)
	require.NoError(t, err)
	sidecar := cltypes.NewBlobSidecar(
		0,
		(*cltypes.Blob)(&blob),
		common.Bytes48(commitment),
		common.Bytes48(proof),
		&cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{}},
		solid.NewHashVector(cltypes.CommitmentBranchSize),
	)

	require.NoError(t, VerifyBlobSidecars([]*cltypes.BlobSidecar{sidecar}, clparams.GloasVersion, nil))
	require.Error(t, VerifyBlobSidecars([]*cltypes.BlobSidecar{sidecar}, clparams.FuluVersion, nil))
}

func setupTestDB(t *testing.T) kv.RwDB {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	return db
}

func TestBlobDB(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	s1 := cltypes.NewBlobSidecar(0, &cltypes.Blob{1}, common.Bytes48{2}, common.Bytes48{3}, &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 1}}, solid.NewHashVector(cltypes.CommitmentBranchSize))
	s2 := cltypes.NewBlobSidecar(1, &cltypes.Blob{3}, common.Bytes48{5}, common.Bytes48{9}, &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 1}}, solid.NewHashVector(cltypes.CommitmentBranchSize))

	//
	bs := NewBlobStore(db, afero.NewMemMapFs())
	blockRoot := common.Hash{1}
	err := bs.WriteBlobSidecars(context.Background(), blockRoot, []*cltypes.BlobSidecar{s1, s2})
	require.NoError(t, err)

	sidecars, found, err := bs.ReadBlobSidecars(context.Background(), 1, blockRoot)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, sidecars, 2)

	require.Equal(t, s1.Blob, sidecars[0].Blob)
	require.Equal(t, s2.Blob, sidecars[1].Blob)
	require.Equal(t, s1.Index, sidecars[0].Index)
	require.Equal(t, s2.Index, sidecars[1].Index)
	require.Equal(t, s1.CommitmentInclusionProof, sidecars[0].CommitmentInclusionProof)
	require.Equal(t, s2.CommitmentInclusionProof, sidecars[1].CommitmentInclusionProof)
	require.Equal(t, s1.KzgCommitment, sidecars[0].KzgCommitment)
	require.Equal(t, s2.KzgCommitment, sidecars[1].KzgCommitment)
	require.Equal(t, s1.KzgProof, sidecars[0].KzgProof)
	require.Equal(t, s2.KzgProof, sidecars[1].KzgProof)
	require.Equal(t, s1.SignedBlockHeader, sidecars[0].SignedBlockHeader)
	require.Equal(t, s2.SignedBlockHeader, sidecars[1].SignedBlockHeader)
}

func TestBlobStorePruneFloorRejectsLateWritesAndAllowsCleanup(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	bs := NewBlobStore(db, afero.NewMemMapFs())
	existingRoot := common.Hash{1}
	require.NoError(t, bs.WriteBlobSidecars(t.Context(), existingRoot, []*cltypes.BlobSidecar{testSidecar(100, 0, 1)}))

	require.NoError(t, bs.PruneBelow(500))
	require.NoError(t, bs.RemoveBlobSidecars(t.Context(), 100, existingRoot))
	_, found, err := bs.ReadBlobSidecars(t.Context(), 100, existingRoot)
	require.NoError(t, err)
	require.False(t, found, "cleanup removes must still clear the stale commitment row below the write floor")

	lateRoot := common.Hash{2}
	require.NoError(t, bs.WriteBlobSidecars(t.Context(), lateRoot, []*cltypes.BlobSidecar{testSidecar(499, 0, 2)}))
	count, err := bs.KzgCommitmentsCount(t.Context(), lateRoot)
	require.NoError(t, err)
	require.Zero(t, count, "a rejected write must not publish a count without files")
	_, found, err = bs.ReadBlobSidecars(t.Context(), 499, lateRoot)
	require.NoError(t, err)
	require.False(t, found)

	boundaryRoot := common.Hash{3}
	require.NoError(t, bs.WriteBlobSidecars(t.Context(), boundaryRoot, []*cltypes.BlobSidecar{testSidecar(500, 0, 3)}))
	_, found, err = bs.ReadBlobSidecars(t.Context(), 500, boundaryRoot)
	require.NoError(t, err)
	require.True(t, found, "the floor itself remains writable")
}

type blockingBlobRemoveFs struct {
	afero.Fs
	path    string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingBlobRemoveFs) Remove(path string) error {
	if path == f.path {
		f.once.Do(func() { close(f.entered) })
		<-f.release
	}
	return f.Fs.Remove(path)
}

func TestBlobStoreWriterWaitingForSlotDoesNotBlockPrune(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	db := setupTestDB(t)
	defer db.Close()
	fs := &blockingBlobRemoveFs{
		Fs:      afero.NewMemMapFs(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	storage := NewBlobStore(db, fs)
	bs := storage.(*BlobStore)
	const slot = 2*subdivisionSlot + 1
	oldRoot, lateRoot := common.Hash{1}, common.Hash{2}
	releaseRemove := sync.OnceFunc(func() { close(fs.release) })
	defer releaseRemove()

	require.NoError(t, storage.WriteBlobSidecars(t.Context(), oldRoot, []*cltypes.BlobSidecar{testSidecar(slot, 0, 1)}))
	require.NoError(t, fs.MkdirAll("0", 0o755))
	_, fs.path = bs.path(slot, oldRoot, 0)

	removeDone := make(chan error, 1)
	go func() { removeDone <- storage.RemoveBlobSidecars(context.Background(), slot, oldRoot) }()
	<-fs.entered

	writerCtx, cancelWriter := context.WithCancel(t.Context())
	t.Cleanup(cancelWriter)
	writerStarted := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		close(writerStarted)
		writerDone <- storage.WriteBlobSidecars(writerCtx, lateRoot, []*cltypes.BlobSidecar{testSidecar(slot, 0, 2)})
	}()
	<-writerStarted
	runtime.Gosched()
	select {
	case err := <-writerDone:
		t.Fatalf("writer completed instead of waiting for the remove-held slot lock: %v", err)
	default:
	}
	cancelWriter()

	pruneDone := make(chan error, 1)
	go func() { pruneDone <- storage.PruneBelow(slot + 1) }()
	select {
	case err := <-pruneDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("a writer waiting for a slot lock blocked prune-floor advancement")
	}
	select {
	case err := <-removeDone:
		t.Fatalf("remove completed before its filesystem gate was released: %v", err)
	default:
	}

	releaseRemove()
	require.NoError(t, <-removeDone)
	require.NoError(t, <-writerDone)
	count, err := storage.KzgCommitmentsCount(t.Context(), lateRoot)
	require.NoError(t, err)
	require.Zero(t, count)
	exists, err := storage.BlobSidecarExists(t.Context(), slot, lateRoot, 0)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestBlobStoreRejectsMixedSlotBatchAcrossPruneFloor(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	bs := NewBlobStore(db, afero.NewMemMapFs())
	require.NoError(t, bs.PruneBelow(500))
	root := common.Hash{1}

	err := bs.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{
		testSidecar(500, 0, 1),
		testSidecar(499, 1, 2),
	})
	require.Error(t, err)
	count, err := bs.KzgCommitmentsCount(t.Context(), root)
	require.NoError(t, err)
	require.Zero(t, count)
	for _, slot := range []uint64{499, 500} {
		for index := range uint64(2) {
			exists, err := bs.BlobSidecarExists(t.Context(), slot, root, index)
			require.NoError(t, err)
			require.False(t, exists)
		}
	}
}

func TestBlobStoreRejectsMissingHeadersBeforeMutation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	bs := NewBlobStore(db, afero.NewMemMapFs())
	root := common.Hash{1}

	for _, sidecar := range []*cltypes.BlobSidecar{
		nil,
		{},
		{SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{}},
	} {
		require.Error(t, bs.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{sidecar}))
	}
	count, err := bs.KzgCommitmentsCount(t.Context(), root)
	require.NoError(t, err)
	require.Zero(t, count)
}

type createOrderFs struct {
	afero.Fs
	mu      sync.Mutex
	creates []string
	hook    func(name string)
}

func newCreateOrderFs(fs afero.Fs) *createOrderFs {
	return &createOrderFs{Fs: fs}
}

func (c *createOrderFs) Create(name string) (afero.File, error) {
	c.mu.Lock()
	c.creates = append(c.creates, name)
	hook := c.hook
	c.mu.Unlock()
	if hook != nil {
		hook(name)
	}
	return c.Fs.Create(name)
}

func (c *createOrderFs) order() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.creates)
}

func TestBlobStoreRemoveSucceedsWhenAFileIsAlreadyGone(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs := afero.NewMemMapFs()
	bs := NewBlobStore(db, fs)
	root := common.Hash{4}
	const slot = 12_345

	require.NoError(t, bs.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{
		testSidecar(slot, 0, 1), testSidecar(slot, 1, 2),
	}))

	b := newBucketStore(t, fs)
	_, first := b.path(slot, root, 0)
	require.NoError(t, fs.Remove(first))

	require.NoError(t, bs.RemoveBlobSidecars(t.Context(), slot, root))

	_, second := b.path(slot, root, 1)
	stillThere, err := afero.Exists(fs, second)
	require.NoError(t, err)
	require.False(t, stillThere)

	_, found, err := bs.ReadBlobSidecars(t.Context(), slot, root)
	require.NoError(t, err)
	require.False(t, found)
}

func TestBlobStoreRemoveDropsTheCountRowDespiteAFailureOnAMiddleIndex(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs := newRemoveFailingFs(afero.NewMemMapFs())
	bs := NewBlobStore(db, fs)
	root := common.Hash{6}
	const slot = 12_345

	require.NoError(t, bs.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{
		testSidecar(slot, 0, 1), testSidecar(slot, 1, 2), testSidecar(slot, 2, 3),
	}))

	b := newBucketStore(t, fs)
	_, failPath := b.path(slot, root, 1)
	fs.failOn[failPath] = errInducedFailure

	err := bs.RemoveBlobSidecars(t.Context(), slot, root)
	require.ErrorIs(t, err, errInducedFailure)

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	val, err := tx.GetOne(kv.BlockRootToKzgCommitments, root[:])
	require.NoError(t, err)
	require.Empty(t, val, "the count row should be dropped so the block reads as unknown, not permanently unavailable")
}

func TestBlobStoreBatchWriteDoesNotInterleaveWithAConcurrentWrite(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs := newCreateOrderFs(afero.NewMemMapFs())
	bs := NewBlobStore(db, fs)

	const slot = 100
	batchRoot, otherRoot := common.Hash{1}, common.Hash{2}

	entered := make(chan struct{})
	var once sync.Once
	fs.hook = func(name string) {
		if !strings.Contains(name, batchRoot.String()) {
			return
		}
		once.Do(func() {
			close(entered)
			time.Sleep(200 * time.Millisecond)
		})
	}

	done := make(chan error, 1)
	go func() {
		done <- bs.WriteBlobSidecars(context.Background(), batchRoot, []*cltypes.BlobSidecar{
			testSidecar(slot, 0, 1), testSidecar(slot, 1, 2), testSidecar(slot, 2, 3),
		})
	}()

	<-entered
	require.NoError(t, bs.WriteBlobSidecars(t.Context(), otherRoot, []*cltypes.BlobSidecar{testSidecar(slot, 0, 4)}))
	require.NoError(t, <-done)

	order := fs.order()
	lastOfBatch := -1
	for i, name := range order {
		if strings.Contains(name, batchRoot.String()) {
			lastOfBatch = i
		}
	}
	other := slices.IndexFunc(order, func(name string) bool { return strings.Contains(name, otherRoot.String()) })
	require.Positive(t, other)
	require.Greater(t, other, lastOfBatch, "concurrent write interleaved with the batch: %v", order)
}

func TestBlobStoreReadReturnsACompletedWrite(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bs := NewBlobStore(db, afero.NewMemMapFs())
	root := common.Hash{9}
	const slot = 54_321

	want := []*cltypes.BlobSidecar{testSidecar(slot, 0, 5), testSidecar(slot, 1, 6)}
	require.NoError(t, bs.WriteBlobSidecars(t.Context(), root, want))

	got, found, err := bs.ReadBlobSidecars(t.Context(), slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].Blob, got[i].Blob)
	}
}

func TestBlobStoreEmptyBatchRecordsItsZeroRowWithoutLocking(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bs := NewBlobStore(db, afero.NewMemMapFs()).(*BlobStore)
	root := common.Hash{3}

	for i := range bs.locks {
		bs.locks[i].Lock()
	}
	done := make(chan error, 1)
	go func() { done <- bs.WriteBlobSidecars(context.Background(), root, nil) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("an empty batch took a slot lock")
	}
	for i := range bs.locks {
		bs.locks[i].Unlock()
	}

	sidecars, found, err := bs.ReadBlobSidecars(t.Context(), 0, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, sidecars)
}

func TestKzgCommitmentsCountHonorsCanceledContext(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bs := NewBlobStore(db, afero.NewMemMapFs())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := bs.KzgCommitmentsCount(ctx, common.Hash{})
	require.ErrorIs(t, err, context.Canceled)
}
