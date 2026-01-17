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
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/kv"
)

// VLog (values log) stores large values separately from database tables.
// File format: sequential entries of [size: 4 bytes] [value: size bytes]
// Each entry's offset is stored in the database as a reference.

const (
	vlogVersion    = 1
	vlogBufferSize = 4 * 1024 * 1024 // 4MB write buffer
)

// VLogFile represents a read-only vlog file using mmap
type VLogFile struct {
	file        *os.File
	path        string
	mmapHandle1 []byte
	mmapHandle2 *[mmap.MaxMapSize]byte
	size        int64
}

// VLogWriter handles sequential append operations to a vlog file
// Uses buffering for performance. Only 1 RwTx at a time guarantees no concurrency issues.
type VLogWriter struct {
	file   *os.File
	writer *bufio.Writer
	path   string
	offset uint64 // current write offset
}

// vlogPathForStep returns the vlog file path for a given step
func vlogPathForStep(dir string, step kv.Step) string {
	return filepath.Join(dir, fmt.Sprintf("v%d-%d.vlog", vlogVersion, step))
}

// OpenVLogFile opens an existing vlog file for reading using mmap
func OpenVLogFile(path string) (*VLogFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := stat.Size()

	// Handle empty files
	if size == 0 {
		return &VLogFile{
			file: f,
			path: path,
			size: 0,
		}, nil
	}

	// Mmap the file
	mmapHandle1, mmapHandle2, err := mmap.Mmap(f, int(size))
	if err != nil {
		f.Close()
		return nil, err
	}

	// Advise random access pattern
	if err := mmap.MadviseRandom(mmapHandle1); err != nil {
		mmap.Munmap(mmapHandle1, mmapHandle2)
		f.Close()
		return nil, err
	}

	return &VLogFile{
		file:        f,
		path:        path,
		mmapHandle1: mmapHandle1,
		mmapHandle2: mmapHandle2,
		size:        size,
	}, nil
}

// ReadAt reads a value from the vlog at the given offset using mmap
// Format: [size: 4 bytes] [value: size bytes]
func (v *VLogFile) ReadAt(offset uint64) ([]byte, error) {
	if v.mmapHandle2 == nil {
		return nil, fmt.Errorf("vlog file is closed or empty")
	}

	// Check bounds for size field
	if offset+4 > uint64(v.size) {
		return nil, fmt.Errorf("offset %d out of bounds (file size %d)", offset, v.size)
	}

	// Read size (4 bytes) directly from mmap
	size := binary.BigEndian.Uint32(v.mmapHandle2[offset : offset+4])

	// Check bounds for value
	valueEnd := offset + 4 + uint64(size)
	if valueEnd > uint64(v.size) {
		return nil, fmt.Errorf("value at offset %d extends beyond file (size %d, file size %d)", offset, size, v.size)
	}

	// Read value directly from mmap
	value := make([]byte, size)
	copy(value, v.mmapHandle2[offset+4:valueEnd])

	return value, nil
}

// Close closes the vlog file and unmaps the memory
func (v *VLogFile) Close() error {
	if v.file == nil {
		return nil
	}

	var err error
	// Unmap if mapped
	if v.mmapHandle1 != nil {
		err = mmap.Munmap(v.mmapHandle1, v.mmapHandle2)
		v.mmapHandle1 = nil
		v.mmapHandle2 = nil
	}

	// Close file
	if closeErr := v.file.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	v.file = nil

	return err
}

// Path returns the file path
func (v *VLogFile) Path() string {
	return v.path
}

// CreateVLogWriter creates a new vlog file for writing
func CreateVLogWriter(path string) (*VLogWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return &VLogWriter{
		file:   f,
		writer: bufio.NewWriterSize(f, vlogBufferSize),
		path:   path,
		offset: 0,
	}, nil
}

// Append writes a value to the vlog and returns its offset
// Format: [size: 4 bytes] [value: size bytes]
func (w *VLogWriter) Append(value []byte) (uint64, error) {
	if w.writer == nil {
		return 0, fmt.Errorf("vlog writer is closed")
	}

	offset := w.offset

	// Write size (4 bytes)
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(value)))
	if _, err := w.writer.Write(sizeBuf[:]); err != nil {
		return 0, err
	}

	// Write value
	if _, err := w.writer.Write(value); err != nil {
		return 0, err
	}

	// Update offset
	w.offset += 4 + uint64(len(value))

	return offset, nil
}

// Fsync flushes buffered data and syncs to disk
func (w *VLogWriter) Fsync() error {
	if w.writer == nil {
		return nil
	}

	// Flush buffer
	if err := w.writer.Flush(); err != nil {
		return err
	}

	// Sync to disk
	if err := w.file.Sync(); err != nil {
		return err
	}

	return nil
}

// Close closes the vlog writer (flushes but does not fsync)
func (w *VLogWriter) Close() error {
	if w.writer == nil {
		return nil
	}

	// Flush buffer
	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		return err
	}

	err := w.file.Close()
	w.writer = nil
	w.file = nil
	return err
}

// Path returns the file path
func (w *VLogWriter) Path() string {
	return w.path
}
