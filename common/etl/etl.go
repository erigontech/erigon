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

// Collector performs the job of ETL Transform, but can also be used without "E" (Extract) part
// as a Collect Transform Load
type Collector struct {
	extractNextFunc ExtractNextFunc
	flushBuffer     func([]byte, bool) error
	dataProviders   []dataProvider
	allFlushed      bool
}

func NewCollector(datadir string) *Collector {
	c := &Collector{}
	buffer := bytes.NewBuffer(make([]byte, 0))
	encoder := codec.NewEncoder(nil, &cbor)

	sortableBuffer := newSortableBuffer()

	c.flushBuffer = func(currentKey []byte, canStoreInRam bool) error {
		if sortableBuffer.Len() == 0 {
			return nil
		}
		var provider dataProvider
		var err error
		if canStoreInRam && len(c.dataProviders) == 0 {
			provider = KeepInRAM(sortableBuffer)
			c.allFlushed = true
		} else {
			provider, err = FlushToDisk(currentKey, sortableBuffer, datadir)
		}
		if err != nil {
			return err
		}
		if provider != nil {
			c.dataProviders = append(c.dataProviders, provider)
		}
		return nil
	}

	c.extractNextFunc = func(k []byte, v interface{}) error {
		buffer.Reset()
		encoder.Reset(buffer)
		if err := encoder.Encode(v); err != nil {
			return err
		}
		encodedValue := buffer.Bytes()
		sortableBuffer.Put(common.CopyBytes(k), common.CopyBytes(encodedValue))
		if sortableBuffer.Size() >= sortableBuffer.OptimalSize {
			if err := c.flushBuffer(k, false); err != nil {
				return err
			}
		}
		return nil
	}
	return c
}

func (c *Collector) Collect(k, v []byte) error {
	return c.extractNextFunc(k, v)
}

func (c *Collector) Load(db ethdb.Database, toBucket []byte, loadFunc LoadFunc, quitCh chan struct{}) error {
	defer func() {
		disposeProviders(c.dataProviders)
	}()
	if !c.allFlushed {
		if err := c.flushBuffer(nil, true); err != nil {
			return err
		}
	}
	return loadFilesIntoBucket(db, toBucket, c.dataProviders, loadFunc, quitCh)
}

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
	collector := NewCollector(datadir)
	if err := extractBucketIntoFiles(db, fromBucket, startkey, collector, extractFunc, quit); err != nil {
		disposeProviders(collector.dataProviders)
		return err
	}
	return collector.Load(db, toBucket, loadFunc, quit)
}

func extractBucketIntoFiles(
	db ethdb.Database,
	bucket []byte,
	startkey []byte,
	collector *Collector,
	extractFunc ExtractFunc,
	quit chan struct{},
) error {
	if err := db.Walk(bucket, startkey, len(startkey), func(k, v []byte) (bool, error) {
		if err := common.Stopped(quit); err != nil {
			return false, err
		}
		if err := extractFunc(k, v, collector.extractNextFunc); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}
	return collector.flushBuffer(nil, true)
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
			var currentKeyStr string
			if len(k) < 4 {
				currentKeyStr = fmt.Sprintf("%x", k)
			} else {
				currentKeyStr = fmt.Sprintf("%x...", k[:4])
			}
			runtime.ReadMemStats(&m)
			log.Info(
				"Commited batch",
				"bucket", string(bucket),
				"size", common.StorageSize(batchSize),
				"current key", currentKeyStr,
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

// IdentityLoadFunc loads entries as they are, without transformation
func IdentityLoadFunc(k []byte, valueDecoder Decoder, _ State, next LoadNextFunc) error {
	var v []byte
	err := valueDecoder.Decode(&v)
	if err != nil {
		return err
	}
	return next(k, v)
}
