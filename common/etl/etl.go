package etl

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"runtime"

	"github.com/ugorji/go/codec"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

var (
	cbor              codec.CborHandle
	bufferOptimalSize = 256 * 1024 * 1024 /* 256 mb | var because we want to sometimes change it from tests */
)

type Decoder interface {
	Decode(interface{}) error
}

type State interface {
	Get([]byte) ([]byte, error)
	Stopped() error
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
	quit chan struct{},
) error {
	dataProviders, err := extractBucketIntoFiles(db, fromBucket, startkey, datadir, extractFunc, quit)

	defer func() {
		disposeProviders(dataProviders)
	}()

	if err != nil {
		return err
	}

	return loadFilesIntoBucket(db, toBucket, dataProviders, loadFunc, quit)
}

func extractBucketIntoFiles(
	db ethdb.Database,
	bucket []byte,
	startkey []byte,
	datadir string,
	extractFunc ExtractFunc,
	quit chan struct{},
) ([]dataProvider, error) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	encoder := codec.NewEncoder(nil, &cbor)
	providers := make([]dataProvider, 0)

	sortableBuffer := newSortableBuffer()

	flushBuffer := func(canStoreInRam bool) error {
		if sortableBuffer.Len() == 0 {
			return nil
		}
		var provider dataProvider
		var err error
		if canStoreInRam && len(providers) == 0 {
			provider = KeepInRAM(sortableBuffer)
		} else {
			provider, err = FlushToDisk(sortableBuffer, datadir)
		}
		if err != nil {
			return err
		}
		if provider != nil {
			providers = append(providers, provider)
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
			err = flushBuffer(false)
			if err != nil {
				return err
			}
		}
		return nil
	}

	err := db.Walk(bucket, startkey, len(startkey), func(k, v []byte) (bool, error) {
		if err := common.Stopped(quit); err != nil {
			return false, err
		}
		err := extractFunc(k, v, extractNextFunc)
		return true, err
	})
	if err != nil {
		return nil, err
	}

	err = flushBuffer(true)
	if err != nil {
		return nil, err
	}
	return providers, nil
}

func loadFilesIntoBucket(db ethdb.Database, bucket []byte, providers []dataProvider, loadFunc LoadFunc, quit chan struct{}) error {
	decoder := codec.NewDecoder(nil, &cbor)
	var m runtime.MemStats
	h := &Heap{}
	heap.Init(h)
	for i, provider := range providers {
		if key, value, err := provider.Next(decoder); err == nil {
			he := HeapElem{key, i, value}
			heap.Push(h, he)
		} else /* we must have at least one entry per file */ {
			eee := fmt.Errorf("error reading first readers: n=%d current=%d provider=%s err=%v",
				len(providers), i, provider, err)
			panic(eee)
		}
	}
	batch := db.NewBatch()
	state := &bucketState{batch, bucket, quit}

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
		if err := common.Stopped(quit); err != nil {
			return err
		}

		element := (heap.Pop(h)).(HeapElem)
		provider := providers[element.TimeIdx]
		decoder.ResetBytes(element.Value)
		err := loadFunc(element.Key, decoder, state, loadNextFunc)
		if err != nil {
			return err
		}
		if element.Key, element.Value, err = provider.Next(decoder); err == nil {
			heap.Push(h, element)
		} else if err != io.EOF {
			return fmt.Errorf("error while reading next element from disk: %v", err)
		}
	}
	_, err := batch.Commit()
	return err
}

func disposeProviders(providers []dataProvider) {
	for _, p := range providers {
		err := p.Dispose()
		if err != nil {
			log.Warn("promoting hashed state, error while disposing provider", "provier", p, "err", err)
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

func (b *sortableBuffer) Get(i int) sortableBufferEntry {
	return b.entries[i]
}

func newSortableBuffer() *sortableBuffer {
	return &sortableBuffer{
		entries:     make([]sortableBufferEntry, 0),
		size:        0,
		OptimalSize: bufferOptimalSize,
		encoder:     codec.NewEncoder(nil, &cbor),
	}
}

type bucketState struct {
	getter ethdb.Getter
	bucket []byte
	quit   chan struct{}
}

func (s *bucketState) Get(key []byte) ([]byte, error) {
	return s.getter.Get(s.bucket, key)
}

func (s *bucketState) Stopped() error {
	return common.Stopped(s.quit)
}
