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
	file        *os.File
	mmapReader  *mmapBytesReader       // zero-copy reader over mmap'd data
	mmapData    []byte                 // mmap'd file content
	mmapHandle2 *[mmap.MaxMapSize]byte // pointer handle for cleanup
	wg          *errgroup.Group
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
	return p.mmapReader.nextEntry()
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

func (p *fileDataProvider) Wait() error { return p.wg.Wait() }
func (p *fileDataProvider) Dispose() {
	if p.file == nil {
		return
	}

	p.Wait()

	if p.mmapData != nil {
		_ = mmap.Munmap(p.mmapData, p.mmapHandle2)
		p.mmapData = nil
		p.mmapHandle2 = nil
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
