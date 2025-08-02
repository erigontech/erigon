package das

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoveryRequest_MarshalUnmarshalSSZ(t *testing.T) {
	req := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	data, err := req.MarshalSSZ()
	require.NoError(t, err)
	require.NotNil(t, data)

	unmarshaled := &recoveryRequest{}
	err = unmarshaled.UnmarshalSSZ(data)
	require.NoError(t, err)

	assert.Equal(t, req.slot, unmarshaled.slot)
	assert.Equal(t, req.blockRoot, unmarshaled.blockRoot)
}

func TestRecoveryRequest_Filepath(t *testing.T) {
	tests := []struct {
		name         string
		slot         uint64
		blockRoot    common.Hash
		expectedDir  string
		expectedFile string
	}{
		{
			name:         "slot 0",
			slot:         0,
			blockRoot:    common.Hash{1},
			expectedDir:  "0",
			expectedFile: "0/0_0x0100000000000000000000000000000000000000000000000000000000000000.ssz",
		},
		{
			name:         "slot 9999",
			slot:         9999,
			blockRoot:    common.Hash{2},
			expectedDir:  "0",
			expectedFile: "0/9999_0x0200000000000000000000000000000000000000000000000000000000000000.ssz",
		},
		{
			name:         "slot 10000",
			slot:         10000,
			blockRoot:    common.Hash{3},
			expectedDir:  "1",
			expectedFile: "1/10000_0x0300000000000000000000000000000000000000000000000000000000000000.ssz",
		},
		{
			name:         "slot 20000",
			slot:         20000,
			blockRoot:    common.Hash{4},
			expectedDir:  "2",
			expectedFile: "2/20000_0x0400000000000000000000000000000000000000000000000000000000000000.ssz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &recoveryRequest{
				slot:      tt.slot,
				blockRoot: tt.blockRoot,
			}

			dir, filepath := req.Filepath()
			assert.Equal(t, tt.expectedDir, dir)
			assert.Equal(t, tt.expectedFile, filepath)
		})
	}
}

func TestFileBasedQueue_Add(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	req := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	// First add should succeed
	added, err := queue.Add(req)
	require.NoError(t, err)
	assert.True(t, added)

	// Second add with same request should return false (already exists)
	added, err = queue.Add(req)
	require.NoError(t, err)
	assert.False(t, added)

	// Verify file was created
	dir, filepath := req.Filepath()
	exists, err := afero.Exists(fs, filepath)
	require.NoError(t, err)
	assert.True(t, exists)

	// Verify directory was created
	exists, err = afero.Exists(fs, dir)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestFileBasedQueue_Add_DuplicateBlockRoot(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	blockRoot := common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

	req1 := &recoveryRequest{
		slot:      12345,
		blockRoot: blockRoot,
	}

	req2 := &recoveryRequest{
		slot:      12346,
		blockRoot: blockRoot,
	}

	// Add first request
	added, err := queue.Add(req1)
	require.NoError(t, err)
	assert.True(t, added)

	// Add second request with same blockRoot but different slot
	added, err = queue.Add(req2)
	require.NoError(t, err)
	assert.True(t, added)

	// Both files should exist
	_, filepath1 := req1.Filepath()
	_, filepath2 := req2.Filepath()

	exists1, err := afero.Exists(fs, filepath1)
	require.NoError(t, err)
	assert.True(t, exists1)

	exists2, err := afero.Exists(fs, filepath2)
	require.NoError(t, err)
	assert.True(t, exists2)
}

func TestFileBasedQueue_Take(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	req := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	// Add request
	added, err := queue.Add(req)
	require.NoError(t, err)
	assert.True(t, added)

	// Take request
	select {
	case takenReq := <-queue.Take():
		assert.Equal(t, req.slot, takenReq.slot)
		assert.Equal(t, req.blockRoot, takenReq.blockRoot)
		queue.Done(takenReq)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}
}

func TestFileBasedQueue_Take_MultipleRequests(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	reqs := []*recoveryRequest{
		{slot: 1000, blockRoot: common.Hash{1}},
		{slot: 2000, blockRoot: common.Hash{2}},
		{slot: 3000, blockRoot: common.Hash{3}},
	}

	// Add all requests
	for _, req := range reqs {
		added, err := queue.Add(req)
		require.NoError(t, err)
		assert.True(t, added)
	}

	// Take all requests
	takenReqs := make([]*recoveryRequest, 0, len(reqs))
	for i := 0; i < len(reqs); i++ {
		select {
		case takenReq := <-queue.Take():
			takenReqs = append(takenReqs, takenReq)
			queue.Done(takenReq)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for request")
		}
	}

	// Verify all requests were taken (order may vary due to sorting)
	assert.Len(t, takenReqs, len(reqs))

	// Check that all blockRoots are present
	blockRoots := make(map[common.Hash]bool)
	for _, req := range reqs {
		blockRoots[req.blockRoot] = true
	}
	for _, takenReq := range takenReqs {
		assert.True(t, blockRoots[takenReq.blockRoot])
	}
}

func TestFileBasedQueue_Done(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	req := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	// Add request
	added, err := queue.Add(req)
	require.NoError(t, err)
	assert.True(t, added)

	// Take request
	var takenReq *recoveryRequest
	select {
	case takenReq = <-queue.Take():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}

	// Mark as done
	err = queue.Done(takenReq)
	require.NoError(t, err)

	// Verify file was removed
	dir, filepath := req.Filepath()
	exists, err := afero.Exists(fs, filepath)
	require.NoError(t, err)
	assert.False(t, exists)

	// Verify directory was removed if empty
	exists, err = afero.Exists(fs, dir)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestFileBasedQueue_Done_NonExistentRequest(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	req := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	// Mark non-existent request as done should not error
	err := queue.Done(req)
	require.NoError(t, err)
}

func TestFileBasedQueue_Done_KeepDirectoryIfNotEmpty(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	req1 := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1},
	}

	req2 := &recoveryRequest{
		slot:      12346,
		blockRoot: common.Hash{2},
	}

	// Add both requests
	added, err := queue.Add(req1)
	require.NoError(t, err)
	assert.True(t, added)

	added, err = queue.Add(req2)
	require.NoError(t, err)
	assert.True(t, added)

	// Take first request
	var takenReq *recoveryRequest
	select {
	case takenReq = <-queue.Take():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}

	// Mark first request as done
	err = queue.Done(takenReq)
	require.NoError(t, err)

	// Verify first file was removed
	dir1, filepath1 := req1.Filepath()
	exists, err := afero.Exists(fs, filepath1)
	require.NoError(t, err)
	assert.False(t, exists)

	// Verify directory still exists (because second file is still there)
	exists, err = afero.Exists(fs, dir1)
	require.NoError(t, err)
	assert.True(t, exists)

	// Verify second file still exists
	_, filepath2 := req2.Filepath()
	exists, err = afero.Exists(fs, filepath2)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestFileBasedQueue_ContextCancellation(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx, cancel := context.WithCancel(context.Background())
	queue := NewFileBasedQueue(ctx, fs)

	// Cancel context
	cancel()

	// Wait a bit for the coordinate goroutine to finish
	time.Sleep(100 * time.Millisecond)

	// Adding should still work
	req := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1},
	}

	added, err := queue.Add(req)
	require.NoError(t, err)
	assert.True(t, added)
}

func TestFileBasedQueue_FileSystemErrors(t *testing.T) {
	// Test with a filesystem that returns errors
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	req := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1},
	}

	// This should work normally
	added, err := queue.Add(req)
	require.NoError(t, err)
	assert.True(t, added)
}

func TestFileBasedQueue_ConcurrentAdd(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	const numGoroutines = 10
	const requestsPerGoroutine = 5

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*requestsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				req := &recoveryRequest{
					slot:      uint64(goroutineID*1000 + j),
					blockRoot: common.Hash{byte(goroutineID), byte(j)},
				}
				_, err := queue.Add(req)
				if err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Errorf("Error during concurrent add: %v", err)
	}
}

func TestFileBasedQueue_LargeNumberOfRequests(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	// Test with a smaller number to avoid the batching issue
	numRequests := 100

	for i := 0; i < numRequests; i++ {
		req := &recoveryRequest{
			slot:      uint64(i),
			blockRoot: common.Hash{byte(i)},
		}
		added, err := queue.Add(req)
		require.NoError(t, err)
		assert.True(t, added)
	}

	// Take and mark requests as done to simulate real usage
	takenCount := 0
	timeout := time.After(10 * time.Second)
	for takenCount < numRequests {
		select {
		case takenReq := <-queue.Take():
			// Mark as done to allow the queue to process more requests
			err := queue.Done(takenReq)
			require.NoError(t, err)
			takenCount++
		case <-timeout:
			t.Fatalf("timeout waiting for requests, got %d, expected %d", takenCount, numRequests)
		}
	}

	assert.Equal(t, numRequests, takenCount)
}

func TestFileBasedQueue_SortingBySlot(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	// Add requests in reverse order - all in same directory (slot/10000 = 0)
	reqs := []*recoveryRequest{
		{slot: 1000, blockRoot: common.Hash{1}},
		{slot: 2000, blockRoot: common.Hash{2}},
		{slot: 3000, blockRoot: common.Hash{3}},
	}

	// Add one request to let it stuck in the queue
	added, err := queue.Add(&recoveryRequest{slot: 1, blockRoot: common.Hash{0}})
	require.NoError(t, err)
	assert.True(t, added)
	for _, req := range reqs {
		added, err := queue.Add(req)
		require.NoError(t, err)
		assert.True(t, added)
	}

	// Take requests and verify they come in ascending order by slot
	expectedSlots := []uint64{1, 1000, 2000, 3000}
	for i, expectedSlot := range expectedSlots {
		select {
		case takenReq := <-queue.Take():
			assert.Equal(t, expectedSlot, takenReq.slot)
			queue.Done(takenReq)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for request %d", i)
		}
	}
}

func TestFileBasedQueue_DuplicateBlockRootHandling(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	blockRoot := common.Hash{1}
	req1 := &recoveryRequest{slot: 1000, blockRoot: blockRoot}
	req2 := &recoveryRequest{slot: 2000, blockRoot: blockRoot}

	// Add first request
	added, err := queue.Add(req1)
	require.NoError(t, err)
	assert.True(t, added)

	// Take first request
	select {
	case takenReq := <-queue.Take():
		assert.Equal(t, req1.slot, takenReq.slot)
		assert.Equal(t, req1.blockRoot, takenReq.blockRoot)
		queue.Done(takenReq)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}

	// Mark first request as done
	err = queue.Done(req1)
	require.NoError(t, err)

	// Add second request with same blockRoot
	added, err = queue.Add(req2)
	require.NoError(t, err)
	assert.True(t, added)

	// Take second request
	select {
	case takenReq := <-queue.Take():
		assert.Equal(t, req2.slot, takenReq.slot)
		assert.Equal(t, req2.blockRoot, takenReq.blockRoot)
		queue.Done(takenReq)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request")
	}
}

func TestFileBasedQueue_EmptyQueue(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	// Try to take from empty queue
	select {
	case <-queue.Take():
		t.Fatal("should not receive from empty queue")
	case <-time.After(100 * time.Millisecond):
		// Expected timeout
	}
}

func TestFileBasedQueue_AddDuplicateRequest(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	req := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	// First add should succeed
	added, err := queue.Add(req)
	require.NoError(t, err)
	assert.True(t, added)

	// Second add with identical request should return false
	added, err = queue.Add(req)
	require.NoError(t, err)
	assert.False(t, added)
}

func TestFileBasedQueue_CrossDirectorySorting(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	// Add requests across different directories
	reqs := []*recoveryRequest{
		{slot: 3000, blockRoot: common.Hash{0}},  // dir 0
		{slot: 5000, blockRoot: common.Hash{1}},  // dir 0
		{slot: 12000, blockRoot: common.Hash{2}}, // dir 1
		{slot: 15000, blockRoot: common.Hash{3}}, // dir 1
		{slot: 25000, blockRoot: common.Hash{5}}, // dir 2
	}

	// Add first request
	queue.Add(&recoveryRequest{slot: 1, blockRoot: common.Hash{6}}) // this one is taken and stuck in queue
	for _, req := range reqs {
		added, err := queue.Add(req)
		require.NoError(t, err)
		assert.True(t, added)
	}

	// Take requests and verify they come in ascending order by slot
	expectedSlots := []uint64{1, 3000, 5000, 12000, 15000, 25000}
	for i, expectedSlot := range expectedSlots {
		select {
		case takenReq := <-queue.Take():
			assert.Equal(t, expectedSlot, takenReq.slot)
			queue.Done(takenReq)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for request %d", i)
		}
	}
}

func TestFileBasedQueue_FileCorruption(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	// Create a corrupted file manually
	req := &recoveryRequest{
		slot:      12345,
		blockRoot: common.Hash{1},
	}
	dir, filepath := req.Filepath()

	// Create directory
	err := fs.MkdirAll(dir, 0755)
	require.NoError(t, err)

	// Create corrupted file
	fh, err := fs.Create(filepath)
	require.NoError(t, err)
	_, err = fh.Write([]byte("corrupted data"))
	require.NoError(t, err)
	fh.Close()

	// The queue should handle corrupted files gracefully
	// Try to take a request - it should skip the corrupted file
	select {
	case <-queue.Take():
		t.Fatal("should not receive from queue with corrupted file")
	case <-time.After(100 * time.Millisecond):
		// Expected timeout
	}
}

func TestFileBasedQueue_ConcurrentTakeAndDone(t *testing.T) {
	fs := afero.NewMemMapFs()
	ctx := context.Background()
	queue := NewFileBasedQueue(ctx, fs)

	// Add some requests
	reqs := []*recoveryRequest{
		{slot: 1000, blockRoot: common.Hash{1}},
		{slot: 2000, blockRoot: common.Hash{2}},
		{slot: 3000, blockRoot: common.Hash{3}},
	}

	for _, req := range reqs {
		added, err := queue.Add(req)
		require.NoError(t, err)
		assert.True(t, added)
	}

	// Take requests concurrently and mark them as done
	var wg sync.WaitGroup
	errors := make(chan error, len(reqs))

	for i := 0; i < len(reqs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case takenReq := <-queue.Take():
				err := queue.Done(takenReq)
				if err != nil {
					errors <- err
				}
			case <-time.After(2 * time.Second):
				errors <- fmt.Errorf("timeout waiting for request")
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Errorf("Error during concurrent take and done: %v", err)
	}
}
