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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

type dataProvider interface {
	Next(decoder Decoder) ([]byte, []byte, error)
	Dispose() uint64 // Safe for repeated call, doesn't return error - means defer-friendly
}

type fileDataProvider struct {
	file   *os.File
	reader io.Reader
}

type Encoder interface {
	Encode(toWrite interface{}) error
	Reset(writer io.Writer)
}

func FlushToDisk(encoder Encoder, logPrefix string, b Buffer, tmpdir string) (dataProvider, error) {
	if b.Len() == 0 {
		return nil, nil
	}
	// if we are going to create files in the system temp dir, we don't need any
	// subfolders.
	if tmpdir != "" {
		if err := os.MkdirAll(tmpdir, 0755); err != nil {
			return nil, err
		}
	}

	bufferFile, err := ioutil.TempFile(tmpdir, "erigon-sortable-buf-")
	if err != nil {
		return nil, err
	}
	defer bufferFile.Sync() //nolint:errcheck

	w := bufio.NewWriterSize(bufferFile, BufIOSize)
	defer w.Flush() //nolint:errcheck

	defer func() {
		b.Reset() // run it after buf.flush and file.sync
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Info(
			"Flushed buffer file",
			"name", bufferFile.Name(),
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	}()

	encoder.Reset(w)
	for _, entry := range b.GetEntries() {
		err = writeToDisk(encoder, entry.key, entry.value)
		if err != nil {
			return nil, fmt.Errorf("error writing entries to disk: %w", err)
		}
	}

	return &fileDataProvider{bufferFile, nil}, nil
}

func (p *fileDataProvider) Next(decoder Decoder) ([]byte, []byte, error) {
	if p.reader == nil {
		_, err := p.file.Seek(0, 0)
		if err != nil {
			return nil, nil, err
		}
		p.reader = bufio.NewReaderSize(p.file, BufIOSize)
	}
	decoder.Reset(p.reader)
	return readElementFromDisk(decoder)
}

func (p *fileDataProvider) Dispose() uint64 {
	info, _ := os.Stat(p.file.Name())
	_ = p.file.Close()
	_ = os.Remove(p.file.Name())
	if info == nil {
		return 0
	}
	return uint64(info.Size())
}

func (p *fileDataProvider) String() string {
	return fmt.Sprintf("%T(file: %s)", p, p.file.Name())
}

func writeToDisk(encoder Encoder, key []byte, value []byte) error {
	toWrite := [][]byte{key, value}
	return encoder.Encode(toWrite)
}

func readElementFromDisk(decoder Decoder) ([]byte, []byte, error) {
	result := make([][]byte, 2)
	err := decoder.Decode(&result)
	return result[0], result[1], err
}

type memoryDataProvider struct {
	buffer       Buffer
	currentIndex int
}

func KeepInRAM(buffer Buffer) dataProvider {
	return &memoryDataProvider{buffer, 0}
}

func (p *memoryDataProvider) Next(decoder Decoder) ([]byte, []byte, error) {
	if p.currentIndex >= p.buffer.Len() {
		return nil, nil, io.EOF
	}
	entry := p.buffer.Get(p.currentIndex)
	p.currentIndex++
	return entry.key, entry.value, nil
}

func (p *memoryDataProvider) Dispose() uint64 {
	return 0 /* doesn't take space on disk */
}

func (p *memoryDataProvider) String() string {
	return fmt.Sprintf("%T(buffer.Len: %d)", p, p.buffer.Len())
}
