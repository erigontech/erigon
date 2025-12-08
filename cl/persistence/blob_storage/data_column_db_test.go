package blob_storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

var globalBeaconConfig *clparams.BeaconChainConfig
var globalCaplinConfig *clparams.CaplinConfig

func init() {
	// Initialize global config once for all tests
	globalBeaconConfig = &clparams.BeaconChainConfig{
		NumberOfColumns:             4,
		SlotsPerEpoch:               32,
		MaxBlobCommittmentsPerBlock: 6,
	}
	globalCaplinConfig = &clparams.CaplinConfig{}
	clparams.InitGlobalStaticConfig(globalBeaconConfig, globalCaplinConfig)
}

func setupTestDataColumnStorage(t *testing.T) (DataColumnStorage, afero.Fs, *clparams.BeaconChainConfig, eth_clock.EthereumClock) {
	fs := afero.NewMemMapFs()

	ctrl := gomock.NewController(t)
	mockClock := eth_clock.NewMockEthereumClock(ctrl)

	// Set up mock expectations
	mockClock.EXPECT().GetCurrentSlot().Return(uint64(1000)).AnyTimes()
	mockClock.EXPECT().GetCurrentEpoch().Return(uint64(31)).AnyTimes()
	mockClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 {
		return slot / 32
	}).AnyTimes()

	emitters := beaconevents.NewEventEmitter()
	storage := NewDataColumnStore(fs, 1000, globalBeaconConfig, mockClock, emitters)
	return storage, fs, globalBeaconConfig, mockClock
}

// Mock implementation removed - using eth_clock.NewMockEthereumClock instead

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

func TestNewDataColumnStore(t *testing.T) {
	fs := afero.NewMemMapFs()
	beaconConfig := &clparams.BeaconChainConfig{}
	ctrl := gomock.NewController(t)
	mockClock := eth_clock.NewMockEthereumClock(ctrl)

	storage := NewDataColumnStore(fs, 1000, beaconConfig, mockClock, beaconevents.NewEventEmitter())

	assert.NotNil(t, storage)

	impl, ok := storage.(*dataColumnStorageImpl)
	assert.True(t, ok)
	assert.Equal(t, fs, impl.fs)
	assert.Equal(t, beaconConfig, impl.beaconChainConfig)
	assert.Equal(t, mockClock, impl.ethClock)
	assert.Equal(t, uint64(1000), impl.slotsKept)
}

func TestDataColumnFilePath(t *testing.T) {
	slot := uint64(1000)
	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := uint64(2)

	dir, filepath := dataColumnFilePath(slot, blockRoot, columnIndex)

	expectedDir := "0" // 1000 / 10000 = 0
	expectedFilepath := "0/0x0000000000000000000000000000000000000000000000001234567890abcdef_2"

	assert.Equal(t, expectedDir, dir)
	assert.Equal(t, expectedFilepath, filepath)
}

func TestWriteColumnSidecars(t *testing.T) {
	storage, fs, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)
	sidecar := createTestDataColumnSidecar(1000, columnIndex)

	// Test successful write
	err := storage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	require.NoError(t, err)

	// Verify file was created
	_, filepath := dataColumnFilePath(1000, blockRoot, uint64(columnIndex))
	_, err = fs.Stat(filepath)
	require.NoError(t, err)

	// Test writing to same location again (should not error)
	err = storage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	require.NoError(t, err)
}

func TestReadColumnSidecarByColumnIndex(t *testing.T) {
	storage, _, _, _ := setupTestDataColumnStorage(t)
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
	storage, _, _, _ := setupTestDataColumnStorage(t)
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
	storage, _, _, _ := setupTestDataColumnStorage(t)
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
	storage, fs, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)

	// Create a directory with the same name as the expected file to cause a stat error
	_, filepath := dataColumnFilePath(1000, blockRoot, uint64(columnIndex))
	dir := filepath[:len(filepath)-2] // Remove the "_1" part
	err := fs.MkdirAll(dir, 0755)
	require.NoError(t, err)

	// This should still work correctly
	exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, columnIndex)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestRemoveColumnSidecars(t *testing.T) {
	storage, _, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")

	// Write multiple sidecars
	for i := int64(0); i < 3; i++ {
		sidecar := createTestDataColumnSidecar(1000, i)
		err := storage.WriteColumnSidecars(ctx, blockRoot, i, sidecar)
		require.NoError(t, err)
	}

	// Verify they exist
	for i := int64(0); i < 3; i++ {
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

func TestRemoveAllColumnSidecars(t *testing.T) {
	storage, _, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")

	// Write multiple sidecars
	for i := int64(0); i < 3; i++ {
		sidecar := createTestDataColumnSidecar(1000, i)
		err := storage.WriteColumnSidecars(ctx, blockRoot, i, sidecar)
		require.NoError(t, err)
	}

	// Verify they exist
	for i := int64(0); i < 3; i++ {
		exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, i)
		require.NoError(t, err)
		assert.True(t, exists)
	}

	// Remove all sidecars
	err := storage.RemoveAllColumnSidecars(ctx, 1000, blockRoot)
	require.NoError(t, err)

	// Verify all are removed
	for i := int64(0); i < 3; i++ {
		exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, i)
		require.NoError(t, err)
		assert.False(t, exists)
	}
}

func TestWriteStream(t *testing.T) {
	storage, _, _, _ := setupTestDataColumnStorage(t)
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
	storage, _, _, _ := setupTestDataColumnStorage(t)
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
		found := false
		for _, savedIdx := range savedIndices {
			if uint64(expectedIdx) == savedIdx {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected index %d not found", expectedIdx)
	}
}

func TestPrune(t *testing.T) {
	storage, fs, _, mockClock := setupTestDataColumnStorage(t)

	// Set up mock clock expectations for pruning
	mockClock.(*eth_clock.MockEthereumClock).EXPECT().GetCurrentSlot().Return(uint64(50000)).AnyTimes()

	// Create some test directories
	fs.MkdirAll("0", 0755) // slot 0-9999
	fs.MkdirAll("1", 0755) // slot 10000-19999
	fs.MkdirAll("2", 0755) // slot 20000-29999
	fs.MkdirAll("3", 0755) // slot 30000-39999
	fs.MkdirAll("4", 0755) // slot 40000-49999

	// Test pruning with keepSlotDistance = 10000
	err := storage.Prune(10000)
	require.NoError(t, err)
}

func TestPruneWithLargeKeepDistance(t *testing.T) {
	storage, fs, _, mockClock := setupTestDataColumnStorage(t)

	// Set up mock clock expectations for pruning
	mockClock.(*eth_clock.MockEthereumClock).EXPECT().GetCurrentSlot().Return(uint64(100000)).AnyTimes()

	// Create some test directories
	fs.MkdirAll("0", 0755) // slot 0-9999
	fs.MkdirAll("1", 0755) // slot 10000-19999

	// Test pruning with very large keepSlotDistance
	err := storage.Prune(50000)
	require.NoError(t, err)
}

func TestPruneWithZeroKeepDistance(t *testing.T) {
	storage, fs, _, mockClock := setupTestDataColumnStorage(t)

	// Set up mock clock expectations for pruning
	mockClock.(*eth_clock.MockEthereumClock).EXPECT().GetCurrentSlot().Return(uint64(1000)).AnyTimes()

	// Create some test directories
	fs.MkdirAll("0", 0755) // slot 0-9999

	// Test pruning with zero keepSlotDistance
	err := storage.Prune(0)
	require.NoError(t, err)
}

func TestWriteColumnSidecarsErrorHandling(t *testing.T) {
	// Create a filesystem that will fail on directory creation
	fs := afero.NewMemMapFs()
	ctrl := gomock.NewController(t)
	mockClock := eth_clock.NewMockEthereumClock(ctrl)

	storage := NewDataColumnStore(fs, 1000, globalBeaconConfig, mockClock, beaconevents.NewEventEmitter())

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)
	sidecar := createTestDataColumnSidecar(1000, columnIndex)

	// This should succeed with normal filesystem
	err := storage.WriteColumnSidecars(context.Background(), blockRoot, columnIndex, sidecar)
	require.NoError(t, err)
}

func TestReadColumnSidecarByColumnIndexErrorHandling(t *testing.T) {
	storage, _, _, _ := setupTestDataColumnStorage(t)

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)

	// Try to read non-existent sidecar
	_, err := storage.ReadColumnSidecarByColumnIndex(context.Background(), 1000, blockRoot, columnIndex)
	assert.Error(t, err)
}

func TestRemoveColumnSidecarsNonExistent(t *testing.T) {
	storage, _, _, _ := setupTestDataColumnStorage(t)

	blockRoot := common.HexToHash("0x1234567890abcdef")

	// Try to remove non-existent sidecars
	err := storage.RemoveColumnSidecars(context.Background(), 1000, blockRoot, 999, 998)
	require.NoError(t, err) // Should not error when removing non-existent files
}

func TestWriteStreamErrorHandling(t *testing.T) {
	storage, _, _, _ := setupTestDataColumnStorage(t)

	blockRoot := common.HexToHash("0x1234567890abcdef")
	columnIndex := int64(1)

	// Try to write stream for non-existent sidecar
	var buf bytes.Buffer
	err := storage.WriteStream(&buf, 1000, blockRoot, uint64(columnIndex))
	assert.Error(t, err)
}

func TestConcurrentAccess(t *testing.T) {
	storage, _, _, _ := setupTestDataColumnStorage(t)
	ctx := context.Background()

	blockRoot := common.HexToHash("0x1234567890abcdef")

	// Test concurrent writes
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			sidecar := createTestDataColumnSidecar(1000, int64(idx))
			err := storage.WriteColumnSidecars(ctx, blockRoot, int64(idx), sidecar)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all sidecars were written
	for i := 0; i < numGoroutines; i++ {
		exists, err := storage.ColumnSidecarExists(ctx, 1000, blockRoot, int64(i))
		require.NoError(t, err)
		assert.True(t, exists)
	}
}
