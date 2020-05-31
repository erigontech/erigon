package etl

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/ugorji/go/codec"
)

type dataProvider interface {
	Next(*codec.Decoder) ([]byte, []byte, error)
	Dispose() error
}

type fileDataProvider struct {
	file   *os.File
	reader io.Reader
}

func FlushToDisk(b *sortableBuffer, datadir string) (dataProvider, error) {
	if len(b.entries) == 0 {
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

	b.encoder.Reset(w)

	for i := range b.entries {
		err = writeToDisk(b.encoder, b.entries[i].key, b.entries[i].value)
		if err != nil {
			return nil, fmt.Errorf("error writing entries to disk: %v", err)
		}
	}

	b.entries = b.entries[:0] // keep the capacity
	b.size = 0

	return &fileDataProvider{bufferFile, nil}, nil
}

func (p *fileDataProvider) Next(decoder *codec.Decoder) ([]byte, []byte, error) {
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

func writeToDisk(encoder *codec.Encoder, key []byte, value []byte) error {
	toWrite := [][]byte{key, value}
	return encoder.Encode(toWrite)
}

func readElementFromDisk(decoder Decoder) ([]byte, []byte, error) {
	result := make([][]byte, 2)
	err := decoder.Decode(&result)
	return result[0], result[1], err
}

type memoryDataProvider struct {
	buffer       *sortableBuffer
	currentIndex int
}

func KeepInRAM(buffer *sortableBuffer) dataProvider {
	return &memoryDataProvider{buffer, 0}
}

func (p *memoryDataProvider) Next(decoder *codec.Decoder) ([]byte, []byte, error) {
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
