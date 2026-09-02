package blob_storage

import (
	"bytes"
	"context"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	globalBeaconConfig *clparams.BeaconChainConfig
	globalCaplinConfig *clparams.CaplinConfig
)

func init() {
	// Initialize global config once for all tests
	// Set GloasForkEpoch high to ensure tests run in Fulu mode
	// (tests create Fulu-style sidecars with SignedBlockHeader)
	cfg := clparams.MainnetBeaconConfig
	cfg.NumberOfColumns = 4
	cfg.SlotsPerEpoch = 32
	cfg.MaxBlobCommittmentsPerBlock = 6
	cfg.GloasForkEpoch = cfg.FarFutureEpoch
	globalBeaconConfig = &cfg
	globalCaplinConfig = &clparams.CaplinConfig{}
	clparams.InitGlobalStaticConfig(globalBeaconConfig, globalCaplinConfig)
}

func setupTestDataColumnStorage(t *testing.T) (DataColumnStorage, afero.Fs, *clparams.BeaconChainConfig) {
	fs := afero.NewBasePathFs(afero.NewOsFs(), t.TempDir())

	emitters := beaconevents.NewEventEmitter()
	storage := NewDataColumnStore(fs, globalBeaconConfig, emitters)
	return storage, fs, globalBeaconConfig
}

func createTestDataColumnSidecar(slot uint64, columnIndex int64) *cltypes.DataColumnSidecar {
	sidecar := cltypes.NewDataColumnSidecar()
	sidecar.Index = uint64(columnIndex)
	sidecar.SignedBlockHeader = &cltypes.SignedBeaconBlockHeader{
		Header: &cltypes.BeaconBlockHeader{
			Slot: slot,
		},
	}
	return sidecar
}

type blockingColumnFs struct {
	afero.Fs
	blockStatPath   string
	blockRemovePath string
	createPath      string
	statEntered     chan struct{}
	statRelease     chan struct{}
	removeEntered   chan struct{}
	removeRelease   chan struct{}
	createCalled    chan struct{}
	statOnce        sync.Once
	removeOnce      sync.Once
	createOnce      sync.Once
}

func (f *blockingColumnFs) Stat(name string) (os.FileInfo, error) {
	if name == f.blockStatPath {
		f.statOnce.Do(func() { close(f.statEntered) })
		<-f.statRelease
	}
	return f.Fs.Stat(name)
}

func (f *blockingColumnFs) Remove(name string) error {
	if name == f.blockRemovePath {
		f.removeOnce.Do(func() { close(f.removeEntered) })
		<-f.removeRelease
	}
	return f.Fs.Remove(name)
}

func (f *blockingColumnFs) Create(name string) (afero.File, error) {
	if name == f.createPath {
		f.createOnce.Do(func() { close(f.createCalled) })
	}
	return f.Fs.Create(name)
}

func TestDataColumnStorageDoesNotReportTruncatedSidecar(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())
	impl := storage.(*dataColumnStorageImpl)
	root := common.HexToHash("0x1234567890abcdef")
	const slot = 1000
	const index = 1
	_, filepath := impl.path(slot, root, index)
	fs.failWritesAfter(filepath, 1, errInducedFailure)
	fs.failWritesAfter(filepath+tmpSuffix, 1, errInducedFailure)

	err := storage.WriteColumnSidecars(t.Context(), root, index, createTestDataColumnSidecar(slot, index))
	require.ErrorIs(t, err, errInducedFailure)

	exists, err := storage.ColumnSidecarExists(t.Context(), slot, root, index)
	require.NoError(t, err)
	require.False(t, exists)

	saved, err := storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Empty(t, saved)
}

func TestDuplicateColumnWriteDoesNotEmitAnotherEvent(t *testing.T) {
	emitters := beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 2)
	subscription := emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	storage := NewDataColumnStore(afero.NewMemMapFs(), globalBeaconConfig, emitters)
	root := common.HexToHash("0x1234567890abcdef")
	sidecar := createTestDataColumnSidecar(1000, 1)

	require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, 1, sidecar))
	event := <-events
	require.Equal(t, beaconevents.OpDataColumnSidecar, event.Event)

	require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, 1, sidecar))
	require.Len(t, events, 0)
}

func TestDataColumnWholeSlotOperationsDoNotInterleaveWithWrite(t *testing.T) {
	t.Run("saved index scan", func(t *testing.T) {
		fs := &blockingColumnFs{Fs: afero.NewMemMapFs()}
		storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())
		root := common.HexToHash("0x1234567890abcdef")
		const slot = 1000
		require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(slot, 0)))
		impl := storage.(*dataColumnStorageImpl)
		_, statPath := impl.path(slot, root, 0)
		_, createPath := impl.path(slot, root, 1)
		fs.blockStatPath = statPath
		fs.statEntered = make(chan struct{})
		fs.statRelease = make(chan struct{})
		fs.createPath = createPath + tmpSuffix
		fs.createCalled = make(chan struct{})

		scanDone := make(chan []uint64, 1)
		scanErr := make(chan error, 1)
		go func() {
			indices, err := storage.GetSavedColumnIndex(t.Context(), slot, root)
			scanDone <- indices
			scanErr <- err
		}()
		<-fs.statEntered

		writeStarted := make(chan struct{})
		writeDone := make(chan error, 1)
		go func() {
			close(writeStarted)
			writeDone <- storage.WriteColumnSidecars(t.Context(), root, 1, createTestDataColumnSidecar(slot, 1))
		}()
		<-writeStarted
		select {
		case <-fs.createCalled:
			t.Fatal("write interleaved with the saved-column scan")
		case <-time.After(100 * time.Millisecond):
		}
		close(fs.statRelease)

		require.NoError(t, <-scanErr)
		require.Equal(t, []uint64{0}, <-scanDone)
		require.NoError(t, <-writeDone)
	})

	t.Run("remove loop", func(t *testing.T) {
		fs := &blockingColumnFs{Fs: afero.NewMemMapFs()}
		storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())
		root := common.HexToHash("0x1234567890abcdef")
		const slot = 1000
		for index := range int64(2) {
			require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, index, createTestDataColumnSidecar(slot, index)))
		}
		impl := storage.(*dataColumnStorageImpl)
		_, removePath := impl.path(slot, root, 0)
		_, createPath := impl.path(slot, root, 2)
		fs.blockRemovePath = removePath
		fs.removeEntered = make(chan struct{})
		fs.removeRelease = make(chan struct{})
		fs.createPath = createPath + tmpSuffix
		fs.createCalled = make(chan struct{})

		removeDone := make(chan error, 1)
		go func() { removeDone <- storage.RemoveColumnSidecars(t.Context(), slot, root, 0, 1) }()
		<-fs.removeEntered

		writeStarted := make(chan struct{})
		writeDone := make(chan error, 1)
		go func() {
			close(writeStarted)
			writeDone <- storage.WriteColumnSidecars(t.Context(), root, 2, createTestDataColumnSidecar(slot, 2))
		}()
		<-writeStarted
		select {
		case <-fs.createCalled:
			t.Fatal("write interleaved with the column-removal loop")
		case <-time.After(100 * time.Millisecond):
		}
		close(fs.removeRelease)

		require.NoError(t, <-removeDone)
		require.NoError(t, <-writeDone)
	})
}

func TestNewDataColumnStore(t *testing.T) {
	fs := afero.NewMemMapFs()
	beaconConfig := &clparams.BeaconChainConfig{}

	storage := NewDataColumnStore(fs, beaconConfig, beaconevents.NewEventEmitter())

	assert.NotNil(t, storage)

	impl, ok := storage.(*dataColumnStorageImpl)
	assert.True(t, ok)
	assert.Equal(t, fs, impl.fs)
	assert.Equal(t, beaconConfig, impl.beaconChainConfig)
}

func TestWriteColumnSidecars(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)
	sidecar := createTestDataColumnSidecar(1000, columnIndex)

	// Test successful write
	err := storage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	require.NoError(t, err)

	// Verify file was created
	exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, columnIndex)
	require.NoError(t, err)
	require.True(t, exists)

	// Test writing to same location again (should not error)
	err = storage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	require.NoError(t, err)
}

func TestReadColumnSidecarByColumnIndex(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)
	sidecar := createTestDataColumnSidecar(1000, columnIndex)

	// Write first
	err := storage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	require.NoError(t, err)

	// Read back
	readSidecar, err := storage.ReadColumnSidecarByColumnIndex(ctx, 1000, blockRoot, columnIndex)
	require.NoError(t, err)
	assert.NotNil(t, readSidecar)
	assert.Equal(t, sidecar.SignedBlockHeader.Header.Slot, readSidecar.SignedBlockHeader.Header.Slot)
	assert.Equal(t, sidecar.Index, readSidecar.Index)

	// Test reading non-existent file
	_, err = storage.ReadColumnSidecarByColumnIndex(ctx, 1000, blockRoot, 999)
	assert.Error(t, err)
}

func TestColumnSidecarExists(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)
	sidecar := createTestDataColumnSidecar(1000, columnIndex)

	// Initially should not exist
	exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, columnIndex)
	require.NoError(t, err)
	assert.False(t, exists)

	// Write the sidecar
	err = storage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	require.NoError(t, err)

	// Now should exist
	exists, err = storage.ColumnSidecarExists(ctx, 1000, blockRoot, columnIndex)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestColumnSidecarExistsWithInvalidParameters(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.Hash{} // Empty hash
	columnIndex := int64(1)

	// Test with empty block root
	exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, columnIndex)
	require.NoError(t, err)
	assert.False(t, exists)

	// Test with negative column index
	blockRoot = common.HexToHash("0x1234567890abcdef")
	exists, err = storage.ColumnSidecarExists(ctx, 1000, blockRoot, -1)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestColumnSidecarExistsWithDirectoryError(t *testing.T) {
	storage, fs, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)

	// Create a directory with the same name as the expected file to cause a stat error
	impl := storage.(*dataColumnStorageImpl)
	_, filepath := impl.path(1000, blockRoot, uint64(columnIndex))
	dir := filepath[:len(filepath)-2] // Remove the "_1" part
	err := fs.MkdirAll(dir, 0o755)
	require.NoError(t, err)

	// This should still work correctly
	exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, columnIndex)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestRemoveColumnSidecars(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")

	// Write multiple sidecars
	for i := range int64(3) {
		sidecar := createTestDataColumnSidecar(1000, i)
		err := storage.WriteColumnSidecars(ctx, blockRoot, i, sidecar)
		require.NoError(t, err)
	}

	// Verify they exist
	for i := range int64(3) {
		exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, i)
		require.NoError(t, err)
		assert.True(t, exists)
	}

	// Remove specific sidecars
	err := storage.RemoveColumnSidecars(ctx, 1000, blockRoot, 0, 2)
	require.NoError(t, err)

	// Verify removal
	exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, 0)
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = storage.ColumnSidecarExists(ctx, 1000, blockRoot, 1)
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = storage.ColumnSidecarExists(ctx, 1000, blockRoot, 2)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestRemoveColumnSidecarsContinuesPastAFailureOnAMiddleIndex(t *testing.T) {
	fs := newRemoveFailingFs(afero.NewMemMapFs())
	storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())
	ctx := context.Background()
	blockRoot := common.HexToHash("0x1234567890abcdef")
	const slot = 1000

	for i := range int64(3) {
		require.NoError(t, storage.WriteColumnSidecars(ctx, blockRoot, i, createTestDataColumnSidecar(slot, i)))
	}

	impl := storage.(*dataColumnStorageImpl)
	_, failPath := impl.path(slot, blockRoot, 1)
	fs.failOn[failPath] = errInducedFailure

	err := storage.RemoveColumnSidecars(ctx, slot, blockRoot, 0, 1, 2)
	require.ErrorIs(t, err, errInducedFailure)

	exists0, err := storage.ColumnSidecarExists(ctx, slot, blockRoot, 0)
	require.NoError(t, err)
	require.False(t, exists0, "index before the failing one should still be removed")

	exists2, err := storage.ColumnSidecarExists(ctx, slot, blockRoot, 2)
	require.NoError(t, err)
	require.False(t, exists2, "index after the failing one should still be removed")
}

func TestWriteStream(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)
	sidecar := createTestDataColumnSidecar(1000, columnIndex)

	// Write the sidecar
	err := storage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	require.NoError(t, err)

	// Test WriteStream
	var buf bytes.Buffer
	err = storage.WriteStream(&buf, 1000, blockRoot, uint64(columnIndex))
	require.NoError(t, err)

	// Verify the streamed data can be decoded
	streamedData := &cltypes.DataColumnSidecar{}
	version := storage.(*dataColumnStorageImpl).beaconChainConfig.GetCurrentStateVersion(1000 / 32)
	err = ssz_snappy.DecodeAndReadNoForkDigest(&buf, streamedData, version)
	require.NoError(t, err)
	assert.Equal(t, sidecar.SignedBlockHeader.Header.Slot, streamedData.SignedBlockHeader.Header.Slot)
}

func TestGetSavedColumnIndex(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")

	// Write sidecars at specific indices
	indices := []int64{0, 2, 3}
	for _, idx := range indices {
		sidecar := createTestDataColumnSidecar(1000, idx)
		err := storage.WriteColumnSidecars(ctx, blockRoot, idx, sidecar)
		require.NoError(t, err)
	}

	// Get saved indices
	savedIndices, err := storage.GetSavedColumnIndex(ctx, 1000, blockRoot)
	require.NoError(t, err)

	// Should contain the written indices
	assert.Len(t, savedIndices, len(indices))
	for _, expectedIdx := range indices {
		found := slices.Contains(savedIndices, uint64(expectedIdx))
		assert.True(t, found, "Expected index %d not found", expectedIdx)
	}
}

func TestDataColumnStorePruneBelowRemovesBucketsUnderTheFloor(t *testing.T) {
	storage, fs, _ := setupTestDataColumnStorage(t)
	for _, bucket := range []string{"0", "1", "2", "3", "4"} {
		require.NoError(t, fs.MkdirAll(bucket, 0o755))
	}

	require.NoError(t, storage.PruneBelow(40000))

	for _, gone := range []string{"0", "1", "2", "3"} {
		exists, err := afero.DirExists(fs, gone)
		require.NoError(t, err)
		require.False(t, exists, "bucket %s is below the floor", gone)
	}
	exists, err := afero.DirExists(fs, "4")
	require.NoError(t, err)
	require.True(t, exists, "the floor's own bucket survives")
}

func TestDataColumnStorePruneBelowZeroRemovesNothing(t *testing.T) {
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("0", 0o755))
	storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())

	require.NoError(t, storage.PruneBelow(0))
	_, err := fs.Stat("0")
	require.NoError(t, err)
}

func TestDataColumnStorePruneBelowFloorAboveEveryBucketEmptiesTheStore(t *testing.T) {
	storage, fs, _ := setupTestDataColumnStorage(t)
	for _, bucket := range []string{"0", "1"} {
		require.NoError(t, fs.MkdirAll(bucket, 0o755))
	}

	require.NoError(t, storage.PruneBelow(50000))

	for _, gone := range []string{"0", "1"} {
		exists, err := afero.DirExists(fs, gone)
		require.NoError(t, err)
		require.False(t, exists)
	}
}

func TestDataColumnStorePruneFloorRejectsLateWritesAndAllowsCleanup(t *testing.T) {
	emitters := beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 4)
	subscription := emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	storage := NewDataColumnStore(afero.NewMemMapFs(), globalBeaconConfig, emitters)
	oldRoot := common.Hash{1}
	require.NoError(t, storage.WriteColumnSidecars(t.Context(), oldRoot, 0, createTestDataColumnSidecar(100, 0)))
	<-events

	require.NoError(t, storage.PruneBelow(500))
	require.NoError(t, storage.RemoveColumnSidecars(t.Context(), 100, oldRoot, 0))
	exists, err := storage.ColumnSidecarExists(t.Context(), 100, oldRoot, 0)
	require.NoError(t, err)
	require.False(t, exists, "cleanup removes must remain available below the write floor")

	lateRoot := common.Hash{2}
	require.NoError(t, storage.WriteColumnSidecars(t.Context(), lateRoot, 0, createTestDataColumnSidecar(499, 0)))
	exists, err = storage.ColumnSidecarExists(t.Context(), 499, lateRoot, 0)
	require.NoError(t, err)
	require.False(t, exists)
	require.Empty(t, events, "a rejected write must not emit a sidecar event")

	boundaryRoot := common.Hash{3}
	require.NoError(t, storage.WriteColumnSidecars(t.Context(), boundaryRoot, 0, createTestDataColumnSidecar(500, 0)))
	exists, err = storage.ColumnSidecarExists(t.Context(), 500, boundaryRoot, 0)
	require.NoError(t, err)
	require.True(t, exists, "the floor itself remains writable")
	require.Equal(t, beaconevents.OpDataColumnSidecar, (<-events).Event)

	require.NoError(t, storage.PruneBelow(250))
	lowerRoot := common.Hash{4}
	require.NoError(t, storage.WriteColumnSidecars(t.Context(), lowerRoot, 0, createTestDataColumnSidecar(499, 0)))
	exists, err = storage.ColumnSidecarExists(t.Context(), 499, lowerRoot, 0)
	require.NoError(t, err)
	require.False(t, exists)
	require.Empty(t, events, "a lower later prune must not lower the write floor")
}

func TestDataColumnStorePartialPruneStillRejectsLateWrites(t *testing.T) {
	fs := newRemoveAllFailingFs(afero.NewMemMapFs())
	fs.failOn["0"] = errInducedFailure
	require.NoError(t, fs.MkdirAll("0", 0o755))
	storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())

	require.ErrorIs(t, storage.PruneBelow(subdivisionSlot), errInducedFailure)
	root := common.Hash{1}
	require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(subdivisionSlot-1, 0)))
	exists, err := storage.ColumnSidecarExists(t.Context(), subdivisionSlot-1, root, 0)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestDataColumnStoreConcurrentPrunesKeepTheHighestFloor(t *testing.T) {
	storage := NewDataColumnStore(afero.NewMemMapFs(), globalBeaconConfig, beaconevents.NewEventEmitter())
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, floor := range []uint64{500, 1_000} {
		go func() {
			<-start
			errs <- storage.PruneBelow(floor)
		}()
	}
	close(start)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)

	root := common.Hash{1}
	require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(999, 0)))
	exists, err := storage.ColumnSidecarExists(t.Context(), 999, root, 0)
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(1_000, 0)))
	exists, err = storage.ColumnSidecarExists(t.Context(), 1_000, root, 0)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestBucketStorePruneFloorsAreIndependent(t *testing.T) {
	first := NewDataColumnStore(afero.NewMemMapFs(), globalBeaconConfig, beaconevents.NewEventEmitter())
	second := NewDataColumnStore(afero.NewMemMapFs(), globalBeaconConfig, beaconevents.NewEventEmitter())
	require.NoError(t, first.PruneBelow(500))
	root := common.Hash{1}
	sidecar := createTestDataColumnSidecar(499, 0)

	require.NoError(t, first.WriteColumnSidecars(t.Context(), root, 0, sidecar))
	require.NoError(t, second.WriteColumnSidecars(t.Context(), root, 0, sidecar))
	firstExists, err := first.ColumnSidecarExists(t.Context(), 499, root, 0)
	require.NoError(t, err)
	secondExists, err := second.ColumnSidecarExists(t.Context(), 499, root, 0)
	require.NoError(t, err)
	require.False(t, firstExists)
	require.True(t, secondExists)
}

func TestWriteColumnSidecarsErrorHandling(t *testing.T) {
	// Create a filesystem that will fail on directory creation
	fs := afero.NewMemMapFs()

	storage := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter())

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)
	sidecar := createTestDataColumnSidecar(1000, columnIndex)

	// This should succeed with normal filesystem
	err := storage.WriteColumnSidecars(context.Background(), blockRoot, columnIndex, sidecar)
	require.NoError(t, err)
}

func TestReadColumnSidecarByColumnIndexErrorHandling(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)

	// Try to read non-existent sidecar
	_, err := storage.ReadColumnSidecarByColumnIndex(context.Background(), 1000, blockRoot, columnIndex)
	assert.Error(t, err)
}

func TestRemoveColumnSidecarsNonExistent(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)

	blockRoot := common.HexToHash("0x1234567890abcdef")

	// Try to remove non-existent sidecars
	err := storage.RemoveColumnSidecars(context.Background(), 1000, blockRoot, 999, 998)
	require.NoError(t, err) // Should not error when removing non-existent files
}

func TestWriteStreamErrorHandling(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)

	// Try to write stream for non-existent sidecar
	var buf bytes.Buffer
	err := storage.WriteStream(&buf, 1000, blockRoot, uint64(columnIndex))
	assert.Error(t, err)
}

func TestConcurrentAccess(t *testing.T) {
	storage, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")

	// Test concurrent writes
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := range numGoroutines {
		go func(idx int) {
			sidecar := createTestDataColumnSidecar(1000, int64(idx))
			err := storage.WriteColumnSidecars(ctx, blockRoot, int64(idx), sidecar)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for range numGoroutines {
		<-done
	}

	// Verify all sidecars were written
	for i := range numGoroutines {
		exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, int64(i))
		require.NoError(t, err)
		assert.True(t, exists)
	}
}
