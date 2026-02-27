// Copyright 2021 The Erigon Authors
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

package etl

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
)

type dataProvider interface {
	Next(keyBuf, valBuf []byte) ([]byte, []byte, error)
	Dispose()    // Safe for repeated call, doesn't return error - means defer-friendly
	Wait() error // join point for async providers
	String() string
}

type fileDataProvider struct {
	file        *os.File
	mmapReader  *mmapBytesReader       // zero-copy reader over mmap'd data
	mmapData    []byte                 // mmap'd file content
	mmapHandle2 *[mmap.MaxMapSize]byte // pointer handle for cleanup
	wg          *errgroup.Group
}

// mmapBytesReader tracks position for reading from mmap'd data
type mmapBytesReader struct {
	data []byte // mmap'd file content
	pos  int    // current read position
}

// FlushToDiskAsync - `doFsync` is true only for 'critical' collectors (which should not loose).
func FlushToDiskAsync(logPrefix string, b Buffer, tmpdir string, lvl log.Lvl, allocator *Allocator) (dataProvider, error) {
	if b.Len() == 0 {
		if allocator != nil {
			allocator.Put(b)
		}
		return nil, nil
	}

	provider := &fileDataProvider{wg: &errgroup.Group{}}
	provider.wg.Go(func() (err error) {
		defer func() {
			if allocator != nil {
				allocator.Put(b)
			}
		}()
		provider.file, err = sortAndFlush(b, tmpdir)
		if err != nil {
			return err
		}
		_, fName := filepath.Split(provider.file.Name())
		log.Log(lvl, fmt.Sprintf("[%s] Flushed buffer file", logPrefix), "name", fName)
		return nil
	})

	return provider, nil
}

// FlushToDisk - `doFsync` is true only for 'critical' collectors (which should not loose).
func FlushToDisk(logPrefix string, b Buffer, tmpdir string, lvl log.Lvl) (dataProvider, error) {
	if b.Len() == 0 {
		return nil, nil
	}

	var err error
	provider := &fileDataProvider{wg: &errgroup.Group{}}
	provider.file, err = sortAndFlush(b, tmpdir)
	if err != nil {
		return nil, err
	}
	_, fName := filepath.Split(provider.file.Name())
	log.Log(lvl, fmt.Sprintf("[%s] Flushed buffer file", logPrefix), "name", fName)
	return provider, nil
}

func sortAndFlush(b Buffer, tmpdir string) (*os.File, error) {
	b.Sort()

	// if we are going to create files in the system temp dir, we don't need any
	// subfolders.
	if tmpdir != "" {
		if err := os.MkdirAll(tmpdir, 0755); err != nil {
			return nil, err
		}
	}

	bufferFile, err := os.CreateTemp(tmpdir, "erigon-sortable-buf-")
	if err != nil {
		return nil, err
	}

	w := bufio.NewWriterSize(bufferFile, BufIOSize)
	defer w.Flush() //nolint:errcheck

	if err = b.Write(w); err != nil {
		return bufferFile, fmt.Errorf("error writing entries to disk: %w", err)
	}
	return bufferFile, nil
}

func (p *fileDataProvider) Next(keyBuf, valBuf []byte) ([]byte, []byte, error) {
	if p.mmapReader == nil {
		// Get file size by seeking to end
		size, err := p.file.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, nil, err
		}
		if size == 0 {
			return nil, nil, io.EOF
		}

		// Memory-map the file
		p.mmapData, p.mmapHandle2, err = mmap.Mmap(p.file, int(size))
		if err != nil {
			return nil, nil, fmt.Errorf("mmap failed: %w", err)
		}

		// Set sequential read pattern for better performance
		if err := mmap.MadviseSequential(p.mmapData); err != nil {
			_ = mmap.Munmap(p.mmapData, p.mmapHandle2)
			return nil, nil, fmt.Errorf("madvise sequential failed: %w", err)
		}

		// Create zero-copy reader over mmap'd data
		p.mmapReader = &mmapBytesReader{data: p.mmapData, pos: 0}
	}
	return readElementFromDiskZeroCopy(p.mmapReader)
}

// ReadVarint decodes a signed varint directly from mmap data
func (m *mmapBytesReader) ReadVarint() (int64, error) {
	v, n := binary.Varint(m.data[m.pos:])
	if n <= 0 {
		if n == 0 {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("varint overflow")
	}
	m.pos += n
	return v, nil
}

// ReadAt returns a slice directly from mmap data (zero-copy) at given length
// The returned slice points directly into the mmap'd memory
func (m *mmapBytesReader) ReadAt(length int) ([]byte, error) {
	if m.pos+length > len(m.data) {
		return nil, io.ErrUnexpectedEOF
	}
	result := m.data[m.pos : m.pos+length]
	m.pos += length
	return result, nil
}

func (p *fileDataProvider) Wait() error { return p.wg.Wait() }
func (p *fileDataProvider) Dispose() {
	if p.file == nil {
		return
	}

	p.Wait()

	filePath := p.file.Name()
	p.file.Close()
	p.file = nil
	_ = dir.RemoveFile(filePath)

	// Note: We intentionally do NOT munmap here. The mmap'd memory remains mapped
	// and valid for zero-copy slices returned to callers. The OS will unmap when
	// the process exits or memory pressure requires it. This is safe for the ETL
	// use case where data is consumed immediately before Close() is called.
}

func (p *fileDataProvider) String() string {
	return fmt.Sprintf("%T(file: %s)", p, p.file.Name())
}

// readElementFromDisk reads key-value pairs from an io.Reader for testing
func readElementFromDisk(r io.Reader, br io.ByteReader, keyBuf, valBuf []byte) ([]byte, []byte, error) {
	n, err := binary.ReadVarint(br)
	if err != nil {
		return nil, nil, err
	}

	var key []byte
	if n >= 0 {
		key = make([]byte, n)
		if _, err = io.ReadFull(r, key); err != nil {
			return nil, nil, err
		}
	}

	n, err = binary.ReadVarint(br)
	if err != nil {
		return nil, nil, err
	}

	var val []byte
	if n >= 0 {
		val = make([]byte, n)
		if _, err = io.ReadFull(r, val); err != nil {
			return nil, nil, err
		}
	}

	return key, val, nil
}

// readElementFromDiskZeroCopy reads key-value pairs directly from mmap'd data
func readElementFromDiskZeroCopy(m *mmapBytesReader) ([]byte, []byte, error) {
	keyLen, err := m.ReadVarint()
	if err != nil {
		return nil, nil, err
	}

	var keyBuf []byte
	if keyLen >= 0 {
		if keyBuf, err = m.ReadAt(int(keyLen)); err != nil {
			return nil, nil, err
		}
	}

	valLen, err := m.ReadVarint()
	if err != nil {
		return nil, nil, err
	}

	var valBuf []byte
	if valLen >= 0 {
		if valBuf, err = m.ReadAt(int(valLen)); err != nil {
			return nil, nil, err
		}
	}

	return keyBuf, valBuf, nil
}

type memoryDataProvider struct {
	buffer       Buffer
	currentIndex int
}

func KeepInRAM(buffer Buffer) dataProvider {
	return &memoryDataProvider{buffer, 0}
}

func (p *memoryDataProvider) Next(keyBuf, valBuf []byte) ([]byte, []byte, error) {
	if p.currentIndex >= p.buffer.Len() {
		return nil, nil, io.EOF
	}
	key, value := p.buffer.Get(p.currentIndex, keyBuf, valBuf)
	p.currentIndex++
	return key, value, nil
}

func (p *memoryDataProvider) Wait() error { return nil }
func (p *memoryDataProvider) Dispose()    {}

func (p *memoryDataProvider) String() string {
	return fmt.Sprintf("%T(buffer.Len: %d)", p, p.buffer.Len())
}
