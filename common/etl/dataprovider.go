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
	Dispose() error
}

type fileDataProvider struct {
	file   *os.File
	reader io.Reader
}

type Encoder interface {
	Encode(toWrite interface{}) error
	Reset(writer io.Writer)
}


func FlushToDisk(encoder Encoder, currentKey []byte, b Buffer, datadir string) (dataProvider, error) {
	if b.Len() == 0 {
		return nil, nil
	}
	bufferFile, err := ioutil.TempFile(datadir, "tg-sync-sortable-buf")
	if err != nil {
		return nil, err
	}

	w := bufio.NewWriter(bufferFile)
	defer w.Flush() //nolint:errcheck
	defer func() {
		bufferFile.Sync() //nolint:errcheck
	}()
	encoder.Reset(w)
	for _, entry := range b.GetEntries() {
		err = writeToDisk(encoder, entry.key, entry.value)
		if err != nil {
			return nil, fmt.Errorf("error writing entries to disk: %v", err)
		}
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var currentKeyStr string
	if currentKey == nil {
		currentKeyStr = "final"
	} else if len(currentKey) < 4 {
		currentKeyStr = fmt.Sprintf("%x", currentKey)
	} else {
		currentKeyStr = fmt.Sprintf("%x...", currentKey[:4])
	}
	log.Info(
		"Flushed buffer file",
		"current key", currentKeyStr,
		"name", bufferFile.Name(),
		"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
	b.Reset()

	return &fileDataProvider{bufferFile, nil}, nil
}

func (p *fileDataProvider) Next(decoder Decoder) ([]byte, []byte, error) {
	if p.reader == nil {
		_, err := p.file.Seek(0, 0)
		if err != nil {
			return nil, nil, err
		}
		p.reader = bufio.NewReader(p.file)
	}
	decoder.Reset(p.reader)
	return readElementFromDisk(decoder)
}

func (p *fileDataProvider) Dispose() error {
	errClose := p.file.Close()
	errRemove := os.Remove(p.file.Name())
	if errClose != nil {
		return errClose
	}
	if errRemove != nil {
		return errRemove
	}
	return nil
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

func (p *memoryDataProvider) Dispose() error {
	return nil
}

func (p *memoryDataProvider) String() string {
	return fmt.Sprintf("%T(buffer.Len: %d)", p, p.buffer.Len())
}
