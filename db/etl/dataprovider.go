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
	"github.com/erigontech/erigon/db/bufiopool"
)

type dataProvider interface {
	Next() ([]byte, []byte, error)
	Dispose()    // Safe for repeated call, doesn't return error - means defer-friendly
	Wait() error // join point for async providers
	String() string
}

type fileDataProvider struct {
	file       *os.File
	mmapReader *mmapBytesReader // zero-copy reader over mmap'd data
	mmapData   mmap.Ro          // mmap'd file content
	wg         *errgroup.Group
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

	bufferFile, err := os.CreateTemp(tmpdir, "erigon-sortable-buf-")
	if err != nil {
		return nil, err
	}

	w := bufiopool.Writer(bufferFile)
	defer bufiopool.PutWriter(w)

	if err = b.Write(w); err != nil {
		return bufferFile, fmt.Errorf("error writing entries to disk: %w", err)
	}
	if err = w.Flush(); err != nil {
		return bufferFile, fmt.Errorf("error flushing buffer to disk: %w", err)
	}
	return bufferFile, nil
}

func (p *fileDataProvider) Next() ([]byte, []byte, error) {
	if p.mmapReader == nil {
		if err := p.initMmap(); err != nil {
			return nil, nil, err
		}
	}
	key, err := readKeyField(p.mmapReader)
	if err != nil {
		return nil, nil, err
	}
	val, err := readValField(p.mmapReader)
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
	p.mmapData, err = mmap.OpenRo(p.file, int(fi.Size()))
	if err != nil {
		return fmt.Errorf("mmap failed: %w", err)
	}
	_ = mmap.MadviseSequential(p.mmapData)
	p.mmapReader = &mmapBytesReader{data: p.mmapData, pos: 0}
	return nil
}

func (m *mmapBytesReader) readKeyLen() (int, error) {
	if m.pos+keyLenSize > len(m.data) {
		return 0, io.EOF
	}
	n := binary.NativeEndian.Uint16(m.data[m.pos:])
	m.pos += keyLenSize
	if n == nilKeyLen {
		return -1, nil
	}
	return int(n), nil
}

func (m *mmapBytesReader) readValLen() (int, error) {
	if m.pos+valLenSize > len(m.data) {
		return 0, io.EOF
	}
	n := int32(binary.NativeEndian.Uint32(m.data[m.pos:])) //nolint:gosec
	m.pos += valLenSize
	return int(n), nil
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

// A nil field comes back nil. Zero-copy, like readAt.
func readKeyField(m *mmapBytesReader) ([]byte, error) {
	n, err := m.readKeyLen()
	if err != nil || n < 0 {
		return nil, err
	}
	return m.readAt(n)
}

func readValField(m *mmapBytesReader) ([]byte, error) {
	n, err := m.readValLen()
	if err != nil || n < 0 {
		return nil, err
	}
	return m.readAt(n)
}

func (p *fileDataProvider) Wait() error { return p.wg.Wait() }
func (p *fileDataProvider) Dispose() {
	// Wait first: the async flush assigns p.file from its own goroutine, so
	// reading it before joining both races and can leak a file created after.
	_ = p.Wait()
	if p.file == nil {
		return
	}

	if p.mmapData != nil {
		_ = p.mmapData.Unmap()
		p.mmapData = nil
		p.mmapReader = nil
	}

	filePath := p.file.Name()
	p.file.Close()
	p.file = nil
	_ = dir.RemoveFile(filePath)
}

func (p *fileDataProvider) String() string {
	return fmt.Sprintf("%T(file: %s)", p, p.file.Name())
}

type memoryDataProvider struct {
	buffer Buffer
}

func KeepInRAM(buffer Buffer) dataProvider {
	return &memoryDataProvider{buffer}
}

func (p *memoryDataProvider) Next() ([]byte, []byte, error) {
	key, value, ok := p.buffer.Next()
	if !ok {
		return nil, nil, io.EOF
	}
	return key, value, nil
}

func (p *memoryDataProvider) Wait() error { return nil }
func (p *memoryDataProvider) Dispose()    {}

func (p *memoryDataProvider) String() string {
	return fmt.Sprintf("%T(buffer.Len: %d)", p, p.buffer.Len())
}
