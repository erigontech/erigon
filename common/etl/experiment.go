package etl

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ugorji/go/codec"
	"golang.org/x/sync/errgroup"
	"io"
	"runtime"
	"sort"
	"time"
)


type getter interface {
	Get(i int) sortableBufferEntry
	Len() int
	Reset()
	EncoderReset(writer io.Writer)
	GetEnt() []sortableBufferEntry
	GetEncoder() Encoder
}
type Encoder interface {
	Encode(toWrite interface{}) error
}


type TransformArgs2 struct {
	ExtractStartKeys [][]byte
	ExtractEndKeys [][]byte
	LoadStartKey    []byte
	Quit            chan struct{}
	OnLoadCommit    LoadCommitHandler
	loadBatchSize   int // used in testing
}

func Transform2(
	db ethdb.Database,
	fromBucket []byte,
	toBucket []byte,
	datadir string,
	extractFunc ExtractFunc2,
	loadFunc LoadFunc2,
	args TransformArgs2,
) error {
	collector := NewCollector2(datadir)
	collectors:=make([]*Collector2, len(args.ExtractStartKeys))
	t:=time.Now()
	errg,_:=errgroup.WithContext(context.TODO())
	if len(args.ExtractStartKeys) >0 {
		for i:=range args.ExtractStartKeys {
			i:=i
			collectors[i] = NewCollector2(datadir)
			extractStartKey :=args.ExtractStartKeys[i]
			endKey :=args.ExtractEndKeys[i]
			errg.Go(func() error {
				fmt.Println(i)
				if err := extractBucketIntoFiles2(db, fromBucket, extractStartKey, endKey, 0, collectors[i], extractFunc, args.Quit); err != nil {
					disposeProviders(collectors[i].dataProviders)
					return err
				}
				return nil
			})
		}
		err:=errg.Wait()
		if err!=nil {
			return err
		}
		for i:=range collectors {
			collector.dataProviders = append(collector.dataProviders, collectors[i].dataProviders...)
		}
	} else {
		if err := extractBucketIntoFiles2(db, fromBucket, nil, nil, 0, collector, extractFunc, args.Quit); err != nil {
			disposeProviders(collector.dataProviders)
			return err
		}
	}
	fmt.Println("loadTime", time.Since(t))


	return collector.Load(db, toBucket, loadFunc, args)
}

func extractBucketIntoFiles2(
	db ethdb.Database,
	bucket []byte,
	startKey []byte,
	endKey []byte,
	fixedBits int,
	collector *Collector2,
	extractFunc ExtractFunc2,
	quit chan struct{},
) error {
	if err := db.Walk(bucket, startKey, fixedBits, func(k, v []byte) (bool, error) {
		if endKey!=nil && bytes.Compare(k,endKey) >= 0 {
			return false, nil
		}
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



type ExtractNextFunc2 func(originalK, k []byte, v []byte) error
type ExtractFunc2 func(k []byte, v []byte, next ExtractNextFunc2) error

type Collector2 struct {
	extractNextFunc ExtractNextFunc2
	flushBuffer     func([]byte, bool) error
	dataProviders   []dataProvider
	allFlushed      bool
}


func NewCollector2(datadir string) *Collector2 {
	c := &Collector2{}
	sortableBuffer := NewAppendBuffer()

	c.flushBuffer = func(currentKey []byte, canStoreInRam bool) error {
		if sortableBuffer.Len() == 0 {
			return nil
		}
		var provider dataProvider
		var err error
		sortableBuffer.MakeSlice()
		sort.Sort(sortableBuffer)
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

	c.extractNextFunc = func(originalK, k []byte, v []byte) error {
		sortableBuffer.Put(common.CopyBytes(k), common.CopyBytes(v))
		if sortableBuffer.Size() >= sortableBuffer.OptimalSize {
			if err := c.flushBuffer(originalK, false); err != nil {
				return err
			}
		}
		return nil
	}
	return c
}

func (c *Collector2) Collect(k, v []byte) error {
	return c.extractNextFunc(k, k, v)
}

func (c *Collector2) Load(db ethdb.Database, toBucket []byte, loadFunc LoadFunc2, args TransformArgs2) error {
	defer func() {
		disposeProviders(c.dataProviders)
	}()
	if !c.allFlushed {
		if err := c.flushBuffer(nil, true); err != nil {
			return err
		}
	}
	return loadFilesIntoBucket2(db, toBucket, c.dataProviders, loadFunc, args)
}

type simpleDecoder struct {
	source io.Reader
}

func loadFilesIntoBucket2(db ethdb.Database, bucket []byte, providers []dataProvider, loadFunc LoadFunc2, args TransformArgs2) error {
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
	state := &bucketState{batch, bucket, args.Quit}

	loadNextFunc := func(k, v []byte) error {
		// we ignore everything that is before this key
		if bytes.Compare(k, args.LoadStartKey) < 0 {
			return nil
		}
		if err := batch.Put(bucket, k, v); err != nil {
			return err
		}
		batchSize := batch.BatchSize()
		if batchSize > batch.IdealBatchSize() || args.loadBatchSize > 0 && batchSize > args.loadBatchSize {
			if _, err := batch.Commit(); err != nil {
				return err
			}
			if args.OnLoadCommit != nil {
				args.OnLoadCommit(k, false)
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
		if err := common.Stopped(args.Quit); err != nil {
			return err
		}

		element := (heap.Pop(h)).(HeapElem)
		provider := providers[element.TimeIdx]
		err := loadFunc(element.Key, element.Value, state, loadNextFunc)
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
	if args.OnLoadCommit != nil {
		args.OnLoadCommit([]byte{}, true)
	}
	return err
}

func NewAppendBuffer() *appendSortableBuffer  {
	return &appendSortableBuffer{
		entries:     make(map[string][]byte),
		size:        0,
		OptimalSize: bufferOptimalSize,
		encoder:     codec.NewEncoder(nil, &cbor),

	}
}

type appendSortableBuffer struct {
	entries     map[string][]byte
	size        int
	OptimalSize int
	sortedBuf	[]sortableBufferEntry
	encoder     *codec.Encoder
}

func (b *appendSortableBuffer) Put(k, v []byte) {
	stored,ok:=b.entries[string(k)]
	if !ok {
		b.size += len(k)
	}
	b.size += len(v)
	stored=append(stored, v...)
	b.entries[string(k)] = stored
}

func (b *appendSortableBuffer) Size() int {
	return b.size
}

func (b *appendSortableBuffer) Len() int {
	return len(b.entries)
}
func (b *appendSortableBuffer) MakeSlice()  {
	for i:=range b.entries {
		b.sortedBuf = append(b.sortedBuf, sortableBufferEntry{key: []byte(i), value: b.entries[i]})
	}
}


func (b *appendSortableBuffer) Less(i, j int) bool {
	return bytes.Compare(b.sortedBuf[i].key, b.sortedBuf[j].key) < 0
}

func (b *appendSortableBuffer) Swap(i, j int) {
	b.sortedBuf[i], b.sortedBuf[j] = b.sortedBuf[j], b.sortedBuf[i]
}

func (b *appendSortableBuffer) Get(i int) sortableBufferEntry {
	return b.sortedBuf[i]
}
func (b *appendSortableBuffer) Reset() {
	b.sortedBuf = b.sortedBuf[:0]
	b.entries=make(map[string][]byte, 0)
	b.size=0
}
func (b *appendSortableBuffer) EncoderReset(writer io.Writer) {
	b.encoder.Reset(writer)
}

func (b *appendSortableBuffer) GetEncoder() Encoder {
	return b.encoder
}
func (b *appendSortableBuffer) GetEnt() []sortableBufferEntry {
	return b.sortedBuf
}



type LoadNextFunc2 func(k []byte, v []byte) error
type LoadFunc2 func(k []byte, value []byte, state State, next LoadNextFunc) error

//func (b *appendSortableBuffer) Get(i int) sortableBufferEntry {
//	return b.entries[i]
//}








/*
=== RUN   TestGenerateTxLookup3
INFO [06-06|11:16:53.406] Flushed buffer file                      current key=3dda6b00... name=/tmp/tg-sync-sortable-buf928716209 alloc=838.62MiB sys=2.11GiB numGC=145
INFO [06-06|11:23:17.316] Flushed buffer file                      current key=final       name=/tmp/tg-sync-sortable-buf764726786 alloc=997.21MiB sys=2.11GiB numGC=282
INFO [06-06|11:23:22.823] Commited batch                           bucket=ltest2 size=50.00MiB current key=040d5a72... alloc=1.22GiB   sys=2.11GiB numGC=283
INFO [06-06|11:27:34.946] Commited batch                           bucket=ltest2 size=50.00MiB current key=ff98a007... alloc=1.44GiB   sys=2.56GiB numGC=342
--- PASS: TestGenerateTxLookup3 (694.40s)
 */