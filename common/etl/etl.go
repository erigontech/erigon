package etl

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ugorji/go/codec"
)

var cbor codec.CborHandle

type Decoder interface {
	Decode(interface{}) error
}

type State interface {
	Get([]byte) ([]byte, error)
}

type ExtractNextFunc func(k []byte, v interface{}) error
type ExtractFunc func(k []byte, v []byte, next ExtractNextFunc) error

type LoadNextFunc func(k []byte, v []byte) error
type LoadFunc func(k []byte, valueDecoder Decoder, state State, next LoadNextFunc) error

func Transform(
	db ethdb.Database,
	fromBucket []byte,
	toBucket []byte,
	datadir string,
	startkey []byte,
	extractFunc ExtractFunc,
	loadFunc LoadFunc,
) error {

	filenames, err := extractBucketIntoFiles(db, fromBucket, startkey, datadir, extractFunc)

	defer func() {
		deleteFiles(filenames)
	}()

	if err != nil {
		return err
	}

	return loadFilesIntoBucket(db, toBucket, filenames, loadFunc)
}

func extractBucketIntoFiles(
	db ethdb.Database,
	bucket []byte,
	startkey []byte,
	datadir string,
	extractFunc ExtractFunc,
) ([]string, error) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	encoder := codec.NewEncoder(nil, &cbor)
	filenames := make([]string, 0)

	sortableBuffer := newSortableBuffer()

	flushBuffer := func() error {
		filename, err := sortableBuffer.FlushToDisk(datadir)
		if err != nil {
			return err
		}
		if len(filename) > 0 {
			filenames = append(filenames, filename)
		}
		return nil
	}

	extractNextFunc := func(k []byte, v interface{}) error {
		buffer.Reset()
		encoder.Reset(buffer)
		err := encoder.Encode(v)
		if err != nil {
			return err
		}
		encodedValue := buffer.Bytes()
		sortableBuffer.Put(common.CopyBytes(k), common.CopyBytes(encodedValue))
		if sortableBuffer.Size() >= sortableBuffer.OptimalSize {
			err = flushBuffer()
			if err != nil {
				return err
			}
		}
		return nil
	}

	err := db.Walk(bucket, startkey, len(startkey)*8, func(k, v []byte) (bool, error) {
		err := extractFunc(k, v, extractNextFunc)
		return true, err
	})
	if err != nil {
		return nil, err
	}

	err = flushBuffer()
	if err != nil {
		return nil, err
	}
	return filenames, nil
}

func loadFilesIntoBucket(db ethdb.Database, bucket []byte, files []string, loadFunc LoadFunc) error {
	decoder := codec.NewDecoder(nil, &cbor)
	var m runtime.MemStats
	h := &Heap{}
	heap.Init(h)
	readers := make([]io.Reader, len(files))
	for i, filename := range files {
		if f, err := os.Open(filename); err == nil {
			readers[i] = bufio.NewReader(f)
			defer f.Close() //nolint:errcheck
		} else {
			return err
		}
		decoder.Reset(readers[i])
		if key, value, err := readElementFromDisk(decoder); err == nil {
			he := HeapElem{key, i, value}
			heap.Push(h, he)
		} else /* we must have at least one entry per file */ {
			return fmt.Errorf("error reading first readers: n=%d current=%d filename=%s err=%v",
				len(files), i, filename, err)
		}
	}
	batch := db.NewBatch()
	state := &bucketState{batch, bucket}

	loadNextFunc := func(k, v []byte) error {
		if err := batch.Put(bucket, k, v); err != nil {
			return err
		}
		batchSize := batch.BatchSize()
		if batchSize > batch.IdealBatchSize() {
			if _, err := batch.Commit(); err != nil {
				return err
			}
			runtime.ReadMemStats(&m)
			log.Info(
				"Commited hashed state",
				"bucket", string(bucket),
				"size", common.StorageSize(batchSize),
				"hashedKey", fmt.Sprintf("%x...", k[:4]),
				"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
		}
		return nil
	}

	for h.Len() > 0 {
		element := (heap.Pop(h)).(HeapElem)
		reader := readers[element.TimeIdx]
		decoder.ResetBytes(element.Value)
		err := loadFunc(element.Key, decoder, state, loadNextFunc)
		if err != nil {
			return err
		}
		decoder.Reset(reader)
		if element.Key, element.Value, err = readElementFromDisk(decoder); err == nil {
			heap.Push(h, element)
		} else if err != io.EOF {
			return fmt.Errorf("error while reading next element from disk: %v", err)
		}
	}
	_, err := batch.Commit()
	return err
}

func deleteFiles(files []string) {
	for _, filename := range files {
		err := os.Remove(filename)
		if err != nil {
			log.Warn("promoting hashed state, error while removing temp file", "file", filename, "err", err)
		}
	}
}

type sortableBufferEntry struct {
	key   []byte
	value []byte
}

type sortableBuffer struct {
	entries     []sortableBufferEntry
	size        int
	OptimalSize int
	encoder     *codec.Encoder
}

func (b *sortableBuffer) Put(k, v []byte) {
	b.size += len(k)
	b.size += len(v)
	b.entries = append(b.entries, sortableBufferEntry{k, v})
}

func (b *sortableBuffer) Size() int {
	return b.size
}

func (b *sortableBuffer) Len() int {
	return len(b.entries)
}

func (b *sortableBuffer) Less(i, j int) bool {
	return bytes.Compare(b.entries[i].key, b.entries[j].key) < 0
}

func (b *sortableBuffer) Swap(i, j int) {
	b.entries[i], b.entries[j] = b.entries[j], b.entries[i]
}

func (b *sortableBuffer) FlushToDisk(datadir string) (string, error) {
	if len(b.entries) == 0 {
		return "", nil
	}
	bufferFile, err := ioutil.TempFile(datadir, "tg-sync-sortable-buf")
	if err != nil {
		return "", err
	}
	defer bufferFile.Close() //nolint:errcheck

	filename := bufferFile.Name()
	w := bufio.NewWriter(bufferFile)
	defer w.Flush() //nolint:errcheck
	b.encoder.Reset(w)

	for i := range b.entries {
		err = writeToDisk(b.encoder, b.entries[i].key, b.entries[i].value)
		if err != nil {
			return "", fmt.Errorf("error writing entries to disk: %v", err)
		}
	}

	b.entries = b.entries[:0] // keep the capacity
	b.size = 0
	return filename, nil
}

func newSortableBuffer() *sortableBuffer {
	return &sortableBuffer{
		entries:     make([]sortableBufferEntry, 0),
		size:        0,
		OptimalSize: 256 * 1024 * 1024, /* 256 mb */
		encoder:     codec.NewEncoder(nil, &cbor),
	}
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

type bucketState struct {
	getter ethdb.Getter
	bucket []byte
}

func (s *bucketState) Get(key []byte) ([]byte, error) {
	return s.getter.Get(s.bucket, key)
}
