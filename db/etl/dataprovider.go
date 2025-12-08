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
	"github.com/erigontech/erigon-lib/common/dir"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/log/v3"
)

type dataProvider interface {
	Next(keyBuf, valBuf []byte) ([]byte, []byte, error)
	Dispose()    // Safe for repeated call, doesn't return error - means defer-friendly
	Wait() error // join point for async providers
	String() string
}

type fileDataProvider struct {
	file       *os.File
	reader     io.Reader
	byteReader io.ByteReader // Different interface to the same object as reader
	wg         *errgroup.Group
}

// FlushToDiskAsync - `doFsync` is true only for 'critical' collectors (which should not loose).
func FlushToDiskAsync(logPrefix string, b Buffer, tmpdir string, lvl log.Lvl, allocator *Allocator) (dataProvider, error) {
	if b.Len() == 0 {
		if allocator != nil {
			allocator.Put(b)
		}
		return nil, nil
	}

	provider := &fileDataProvider{reader: nil, wg: &errgroup.Group{}}
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
	provider := &fileDataProvider{reader: nil, wg: &errgroup.Group{}}
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
	if p.reader == nil {
		_, err := p.file.Seek(0, 0)
		if err != nil {
			return nil, nil, err
		}
		r := bufio.NewReaderSize(p.file, BufIOSize)
		p.reader = r
		p.byteReader = r

	}
	return readElementFromDisk(p.reader, p.byteReader, keyBuf, valBuf)
}

func (p *fileDataProvider) Wait() error { return p.wg.Wait() }
func (p *fileDataProvider) Dispose() {
	if p.file != nil { //invariant: safe to call multiple time
		p.Wait()
		file := p.file
		p.file = nil

		go func() {
			filePath := file.Name()
			file.Close()
			_ = dir.RemoveFile(filePath)
		}()
	}
}

func (p *fileDataProvider) String() string {
	return fmt.Sprintf("%T(file: %s)", p, p.file.Name())
}

func readElementFromDisk(r io.Reader, br io.ByteReader, keyBuf, valBuf []byte) ([]byte, []byte, error) {
	n, err := binary.ReadVarint(br)
	if err != nil {
		return nil, nil, err
	}
	if n >= 0 {
		// Reallocate the slice or extend it if there is enough capacity
		if keyBuf == nil || len(keyBuf)+int(n) > cap(keyBuf) {
			newKeyBuf := make([]byte, len(keyBuf)+int(n))
			copy(newKeyBuf, keyBuf)
			keyBuf = newKeyBuf
		} else {
			keyBuf = keyBuf[:len(keyBuf)+int(n)]
		}
		if _, err = io.ReadFull(r, keyBuf[len(keyBuf)-int(n):]); err != nil {
			return nil, nil, err
		}
	} else {
		keyBuf = nil
	}
	if n, err = binary.ReadVarint(br); err != nil {
		return nil, nil, err
	}
	if n >= 0 {
		// Reallocate the slice or extend it if there is enough capacity
		if valBuf == nil || len(valBuf)+int(n) > cap(valBuf) {
			newValBuf := make([]byte, len(valBuf)+int(n))
			copy(newValBuf, valBuf)
			valBuf = newValBuf
		} else {
			valBuf = valBuf[:len(valBuf)+int(n)]
		}
		if _, err = io.ReadFull(r, valBuf[len(valBuf)-int(n):]); err != nil {
			return nil, nil, err
		}
	} else {
		valBuf = nil
	}
	return keyBuf, valBuf, err
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
