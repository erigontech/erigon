/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package etl

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

type dataProvider interface {
	Next(keyBuf, valBuf []byte) ([]byte, []byte, error)
	Dispose()    // Safe for repeated call, doesn't return error - means defer-friendly
	Wait() error // join point for async providers
}

type fileDataProvider struct {
	file       *os.File
	reader     io.Reader
	byteReader io.ByteReader // Different interface to the same object as reader
	wg         *errgroup.Group
}

// FlushToDisk - `doFsync` is true only for 'critical' collectors (which should not loose).
func FlushToDisk(logPrefix string, b Buffer, tmpdir string, doFsync bool, lvl log.Lvl) (dataProvider, error) {
	if b.Len() == 0 {
		return nil, nil
	}

	provider := &fileDataProvider{reader: nil, wg: &errgroup.Group{}}
	provider.wg.Go(func() error {
		b.Sort()

		// if we are going to create files in the system temp dir, we don't need any
		// subfolders.
		if tmpdir != "" {
			if err := os.MkdirAll(tmpdir, 0755); err != nil {
				return err
			}
		}

		bufferFile, err := os.CreateTemp(tmpdir, "erigon-sortable-buf-")
		if err != nil {
			return err
		}
		provider.file = bufferFile

		if doFsync {
			defer bufferFile.Sync() //nolint:errcheck
		}

		w := bufio.NewWriterSize(bufferFile, BufIOSize)
		defer w.Flush() //nolint:errcheck

		_, fName := filepath.Split(bufferFile.Name())
		if err = b.Write(w); err != nil {
			return fmt.Errorf("error writing entries to disk: %w", err)
		}
		log.Log(lvl, fmt.Sprintf("[%s] Flushed buffer file", logPrefix), "name", fName)
		return nil
	})

	return provider, nil
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
		_ = p.file.Close()
		_ = os.Remove(p.file.Name())
		p.file = nil
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
