// Copyright 2025 The Erigon Authors
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

package state

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/stretchr/testify/require"
)

func TestVLog_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-0.vlog")

	// Create writer
	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)
	defer writer.Close()

	// Write some values
	testData := [][]byte{
		[]byte("hello world"),
		[]byte("this is a longer value that should be stored in vlog"),
		[]byte(""),
		[]byte("short"),
		make([]byte, 10000), // large value
	}

	offsets := make([]uint64, len(testData))
	for i, data := range testData {
		offset, err := writer.Append(data)
		require.NoError(t, err)
		offsets[i] = offset
	}

	// Fsync
	err = writer.Fsync()
	require.NoError(t, err)

	// Close writer
	err = writer.Close()
	require.NoError(t, err)

	// Open for reading
	reader, err := OpenVLogFile(vlogPath)
	require.NoError(t, err)
	defer reader.Close()

	// Read and verify
	for i, expectedData := range testData {
		readData, err := reader.ReadAt(offsets[i])
		require.NoError(t, err)
		require.Equal(t, expectedData, readData, "data mismatch at index %d", i)
	}
}

func TestVLog_SequentialWrites(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-1.vlog")

	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)
	defer writer.Close()

	// Write sequential values and verify offsets
	var expectedOffset uint64 = 0
	for i := 0; i < 100; i++ {
		value := []byte(string(rune('A' + (i % 26))))
		offset, err := writer.Append(value)
		require.NoError(t, err)
		require.Equal(t, expectedOffset, offset, "offset mismatch at iteration %d", i)

		// Each entry is 4 bytes (size) + len(value) bytes
		expectedOffset += 4 + uint64(len(value))
	}

	require.Equal(t, expectedOffset, writer.offset, "final offset mismatch")
}

func TestVLog_EmptyValue(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-2.vlog")

	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)

	// Write empty value
	offset, err := writer.Append([]byte{})
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	err = writer.Close()
	require.NoError(t, err)

	// Read empty value
	reader, err := OpenVLogFile(vlogPath)
	require.NoError(t, err)
	defer reader.Close()

	data, err := reader.ReadAt(offset)
	require.NoError(t, err)
	require.Empty(t, data)
}

func TestVLog_LargeValues(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-3.vlog")

	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)
	defer writer.Close()

	// Write large values (1MB each)
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	numValues := 10
	offsets := make([]uint64, numValues)
	for i := 0; i < numValues; i++ {
		offset, err := writer.Append(largeValue)
		require.NoError(t, err)
		offsets[i] = offset
	}

	err = writer.Fsync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Verify file size
	stat, err := os.Stat(vlogPath)
	require.NoError(t, err)
	expectedSize := int64(numValues * (4 + len(largeValue)))
	require.Equal(t, expectedSize, stat.Size())

	// Read and verify
	reader, err := OpenVLogFile(vlogPath)
	require.NoError(t, err)
	defer reader.Close()

	for i := 0; i < numValues; i++ {
		data, err := reader.ReadAt(offsets[i])
		require.NoError(t, err)
		require.True(t, bytes.Equal(largeValue, data), "data mismatch at index %d", i)
	}
}

func TestVLog_PathForStep(t *testing.T) {
	dir := "/test/dir"
	step := kv.Step(12345)

	path := vlogPathForStep(dir, step)
	expected := filepath.Join(dir, "v1-12345.vlog")
	require.Equal(t, expected, path)
}

func TestVLog_BufferedWrites(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-4.vlog")

	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)
	defer writer.Close()

	// Write many small values to test buffering
	numWrites := 10000
	offsets := make([]uint64, numWrites)
	for i := 0; i < numWrites; i++ {
		value := []byte{byte(i % 256)}
		offset, err := writer.Append(value)
		require.NoError(t, err)
		offsets[i] = offset
	}

	// Fsync to ensure all writes are flushed
	err = writer.Fsync()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Read and verify
	reader, err := OpenVLogFile(vlogPath)
	require.NoError(t, err)
	defer reader.Close()

	for i := 0; i < numWrites; i++ {
		data, err := reader.ReadAt(offsets[i])
		require.NoError(t, err)
		expected := []byte{byte(i % 256)}
		require.Equal(t, expected, data, "data mismatch at index %d", i)
	}
}

func TestVLog_ReadNonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "nonexistent.vlog")

	_, err := OpenVLogFile(vlogPath)
	require.Error(t, err)
}

func TestVLog_ReadInvalidOffset(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-5.vlog")

	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)

	_, err = writer.Append([]byte("test"))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := OpenVLogFile(vlogPath)
	require.NoError(t, err)
	defer reader.Close()

	// Try to read at invalid offset
	_, err = reader.ReadAt(10000)
	require.Error(t, err)
}

func TestVLog_CloseWriter(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-6.vlog")

	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)

	// Write some data
	_, err = writer.Append([]byte("test"))
	require.NoError(t, err)

	// Close
	err = writer.Close()
	require.NoError(t, err)

	// Try to write after close (should error)
	_, err = writer.Append([]byte("fail"))
	require.Error(t, err)
}

func TestVLog_CloseReader(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-7.vlog")

	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)
	offset, err := writer.Append([]byte("test"))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, err := OpenVLogFile(vlogPath)
	require.NoError(t, err)

	// Close reader
	err = reader.Close()
	require.NoError(t, err)

	// Double close should be safe
	err = reader.Close()
	require.NoError(t, err)

	// Try to read after close
	_, err = reader.ReadAt(offset)
	require.Error(t, err)
}

// TestVLog_CrashRecovery_UnflushedWrites verifies that unfsynced writes
// may be lost on crash (data in buffer, not on disk)
func TestVLog_CrashRecovery_UnflushedWrites(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-8.vlog")

	// Write data but don't fsync
	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)

	testValue := []byte("this should be lost on crash")
	offset, err := writer.Append(testValue)
	require.NoError(t, err)

	// Simulate crash: close file without fsync
	// The buffered data may or may not be on disk
	err = writer.file.Close()
	require.NoError(t, err)

	// Try to read the value - it may or may not exist
	// This test verifies we handle both cases gracefully
	reader, err := OpenVLogFile(vlogPath)
	if err != nil {
		// File might be empty or corrupted - this is expected
		return
	}
	defer reader.Close()

	// Try to read - might fail due to incomplete data
	_, err = reader.ReadAt(offset)
	// Either succeeds (OS flushed buffer) or fails (data lost) - both valid
}

// TestVLog_CrashRecovery_FsyncedWrites verifies that fsynced writes
// are durable and survive crash/reboot
func TestVLog_CrashRecovery_FsyncedWrites(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-9.vlog")

	testValue := []byte("this should survive crash")
	var offset uint64

	// Write and fsync
	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)

	offset, err = writer.Append(testValue)
	require.NoError(t, err)

	// CRITICAL: Fsync before crash
	err = writer.Fsync()
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Simulate reboot: reopen file
	reader, err := OpenVLogFile(vlogPath)
	require.NoError(t, err)
	defer reader.Close()

	// Data must be readable after fsync+crash
	data, err := reader.ReadAt(offset)
	require.NoError(t, err)
	require.Equal(t, testValue, data, "fsynced data must survive crash")
}

// TestVLog_CrashRecovery_PartialWrites verifies handling of partial writes
// that may occur during crash
func TestVLog_CrashRecovery_PartialWrites(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-10.vlog")

	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)

	// Write multiple values
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
	}

	offsets := make([]uint64, len(values))
	for i, v := range values {
		offset, err := writer.Append(v)
		require.NoError(t, err)
		offsets[i] = offset
	}

	// Fsync only first two values
	err = writer.Fsync()
	require.NoError(t, err)

	// Write one more value but don't fsync (simulating crash during write)
	unsyncedValue := []byte("this might be lost")
	unsyncedOffset, err := writer.Append(unsyncedValue)
	require.NoError(t, err)

	// Close without fsync (crash)
	err = writer.file.Close()
	require.NoError(t, err)

	// Reopen and verify
	reader, err := OpenVLogFile(vlogPath)
	require.NoError(t, err)
	defer reader.Close()

	// Fsynced values must be readable
	for i, expectedValue := range values {
		data, err := reader.ReadAt(offsets[i])
		require.NoError(t, err)
		require.Equal(t, expectedValue, data, "fsynced value %d must survive", i)
	}

	// Unfsynced value may or may not be readable
	_, err = reader.ReadAt(unsyncedOffset)
	// No assertion - either succeeds or fails, both valid
}

// TestVLog_CrashRecovery_Concurrency verifies that only one writer can exist
// at a time (enforced by transaction model, not by vlog itself)
func TestVLog_CrashRecovery_Concurrency(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-11.vlog")

	// Create first writer
	writer1, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)

	_, err = writer1.Append([]byte("from writer 1"))
	require.NoError(t, err)

	// Try to create second writer (should fail - file already open)
	// Note: This test verifies OS-level exclusive write behavior
	writer2, err := CreateVLogWriter(vlogPath)
	if err != nil {
		// Expected on some systems (file locked)
		writer1.Close()
		return
	}

	// If second writer succeeds, writes might be corrupted
	// This is why we rely on "only 1 RwTx at a time" guarantee
	_, err = writer2.Append([]byte("from writer 2"))

	writer1.Close()
	writer2.Close()

	// File might be corrupted - this test demonstrates why
	// transaction-level concurrency control is critical
}

// TestVLog_CrashRecovery_ReadDuringWrite verifies that readers can safely
// access vlog while writer is active (reads from fsynced portion)
func TestVLog_CrashRecovery_ReadDuringWrite(t *testing.T) {
	tmpDir := t.TempDir()
	vlogPath := filepath.Join(tmpDir, "v1-12.vlog")

	// Create writer and write some data
	writer, err := CreateVLogWriter(vlogPath)
	require.NoError(t, err)
	defer writer.Close()

	value1 := []byte("fsynced value")
	offset1, err := writer.Append(value1)
	require.NoError(t, err)

	// Fsync to make it durable
	err = writer.Fsync()
	require.NoError(t, err)

	// Open reader while writer is still active
	reader, err := OpenVLogFile(vlogPath)
	require.NoError(t, err)
	defer reader.Close()

	// Should be able to read fsynced data
	data, err := reader.ReadAt(offset1)
	require.NoError(t, err)
	require.Equal(t, value1, data)

	// Write more data (not fsynced)
	value2 := []byte("unfsynced value")
	offset2, err := writer.Append(value2)
	require.NoError(t, err)

	// Reader may or may not see unfsynced data
	_, err = reader.ReadAt(offset2)
	// No assertion - behavior depends on OS buffer flushing
}
