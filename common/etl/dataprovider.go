package etl

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
)

type dataProvider interface {
	Next(decoder Decoder) ([]byte, []byte, error)
	Dispose() (uint64, error)
}

type fileDataProvider struct {
	file   *os.File
	reader io.Reader
}

type Encoder interface {
	Encode(toWrite interface{}) error
	Reset(writer io.Writer)
}

func FlushToDisk(encoder Encoder, currentKey []byte, b Buffer, tmpdir string) (dataProvider, error) {
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

	bufferFile, err := ioutil.TempFile(tmpdir, "tg-sync-sortable-buf")
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
			"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
	}()

	encoder.Reset(w)
	for _, entry := range b.GetEntries() {
		err = writeToDisk(encoder, entry.key, entry.value)
		if err != nil {
			return nil, fmt.Errorf("error writing entries to disk: %v", err)
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

func (p *fileDataProvider) Dispose() (uint64, error) {
	info, errStat := os.Stat(p.file.Name())
	errClose := p.file.Close()
	errRemove := os.Remove(p.file.Name())
	if errClose != nil {
		return 0, errClose
	}
	if errRemove != nil {
		return 0, errRemove
	}
	if errStat != nil {
		return 0, errStat
	}
	return uint64(info.Size()), nil
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

func (p *memoryDataProvider) Dispose() (uint64, error) {
	return 0 /* doesn't take space on disk */, nil
}

func (p *memoryDataProvider) String() string {
	return fmt.Sprintf("%T(buffer.Len: %d)", p, p.buffer.Len())
}
