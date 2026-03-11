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
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
)

type dataProvider interface {
	Next() ([]byte, []byte, error)
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
func FlushToDiskAsync(logPrefix string, b Buffer, tmpdir string, lvl log.Lvl, allocator *Allocator, inProgress *atomic.Bool) (dataProvider, error) {
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
			inProgress.Store(false)
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

func (p *fileDataProvider) Next() ([]byte, []byte, error) {
	if p.mmapReader == nil {
		if err := p.initMmap(); err != nil {
			return nil, nil, err
		}
	}
	key, err := readField(p.mmapReader)
	if err != nil {
		return nil, nil, err
	}
	val, err := readField(p.mmapReader)
	if err != nil {
		return nil, nil, err
	}
	return key, val, nil
}

func (p *fileDataProvider) initMmap() error {
	fi, err := p.file.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		return io.EOF
	}
	p.mmapData, p.mmapHandle2, err = mmap.Mmap(p.file, int(fi.Size()))
	if err != nil {
		return fmt.Errorf("mmap failed: %w", err)
	}
	_ = mmap.MadviseSequential(p.mmapData)
	p.mmapReader = &mmapBytesReader{data: p.mmapData, pos: 0}
	return nil
}

func (m *mmapBytesReader) readVarint() (int, error) {
	v, n := binary.Varint(m.data[m.pos:])
	if n <= 0 {
		if n == 0 {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("varint overflow")
	}
	m.pos += n
	return int(v), nil
}

// readAt returns a zero-copy slice directly from mmap'd memory
func (m *mmapBytesReader) readAt(length int) ([]byte, error) {
	if m.pos+length > len(m.data) {
		return nil, io.ErrUnexpectedEOF
	}
	result := m.data[m.pos : m.pos+length]
	m.pos += length
	return result, nil
}

// readField reads a varint-prefixed byte slice from mmap data (zero-copy).
// Negative length means nil.
func readField(m *mmapBytesReader) ([]byte, error) {
	n, err := m.readVarint()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	return m.readAt(n)
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

type memoryDataProvider struct {
	buffer       Buffer
	currentIndex int
}

func KeepInRAM(buffer Buffer) dataProvider {
	return &memoryDataProvider{buffer, 0}
}

func (p *memoryDataProvider) Next() ([]byte, []byte, error) {
	if p.currentIndex >= p.buffer.Len() {
		return nil, nil, io.EOF
	}
	key, value := p.buffer.Get(p.currentIndex)
	p.currentIndex++
	return key, value, nil
}

func (p *memoryDataProvider) Wait() error { return nil }
func (p *memoryDataProvider) Dispose()    {}

func (p *memoryDataProvider) String() string {
	return fmt.Sprintf("%T(buffer.Len: %d)", p, p.buffer.Len())
}
