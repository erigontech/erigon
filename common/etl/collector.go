package etl

import (
	"bytes"
	"container/heap"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ugorji/go/codec"
	"io"
	"runtime"
)

type LoadNextFunc func(originalK, k, v []byte) error
type LoadFunc func(k []byte, value []byte, state State, next LoadNextFunc) error

// Collector performs the job of ETL Transform, but can also be used without "E" (Extract) part
// as a Collect Transform Load
type Collector struct {
	extractNextFunc ExtractNextFunc
	flushBuffer     func([]byte, bool) error
	dataProviders   []dataProvider
	allFlushed      bool
}

func NewCollector(datadir string, sortableBuffer Buffer) *Collector {
	c := &Collector{}
	encoder := codec.NewEncoder(nil, &cbor)

	c.flushBuffer = func(currentKey []byte, canStoreInRam bool) error {
		if sortableBuffer.Len() == 0 {
			return nil
		}
		var provider dataProvider
		var err error
		sortableBuffer.Sort()
		if canStoreInRam && len(c.dataProviders) == 0 {
			provider = KeepInRAM(sortableBuffer)
			c.allFlushed = true
		} else {
			provider, err = FlushToDisk(encoder, currentKey, sortableBuffer, datadir)
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
		if sortableBuffer.CheckFlushSize() {
			if err := c.flushBuffer(originalK, false); err != nil {
				return err
			}
		}
		return nil
	}
	return c
}

func (c *Collector) Collect(k, v []byte) error {
	return c.extractNextFunc(k, k, v)
}

func (c *Collector) Load(db ethdb.Database, toBucket []byte, loadFunc LoadFunc, args TransformArgs) error {
	defer func() {
		disposeProviders(c.dataProviders)
	}()
	if !c.allFlushed {
		if err := c.flushBuffer(nil, true); err != nil {
			return err
		}
	}
	return loadFilesIntoBucket(db, toBucket, c.dataProviders, loadFunc, args)
}

func loadFilesIntoBucket(db ethdb.Database, bucket []byte, providers []dataProvider, loadFunc LoadFunc, args TransformArgs) error {
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

	loadNextFunc := func(originalK, k, v []byte) error {
		// we ignore everything that is before this key
		if bytes.Compare(k, args.LoadStartKey) < 0 {
			return nil
		}
		if len(v) == 0 {
			if err := batch.Delete(bucket, k); err != nil {
				return err
			}
		} else {
			if err := batch.Put(bucket, k, v); err != nil {
				return err
			}
		}
		batchSize := batch.BatchSize()
		if batchSize > batch.IdealBatchSize() || args.loadBatchSize > 0 && batchSize > args.loadBatchSize {
			if args.OnLoadCommit != nil {
				if err := args.OnLoadCommit(batch, k, false); err != nil {
					return err
				}
			}
			batchSize := batch.BatchSize()
			if _, err := batch.Commit(); err != nil {
				return err
			}
			runtime.ReadMemStats(&m)
			log.Info(
				"Committed batch",
				"bucket", string(bucket),
				"size", common.StorageSize(batchSize),
				"current key", makeCurrentKeyStr(originalK),
				"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
		}
		return nil
	}
	// Main loading loop
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
	// Final commit
	if args.OnLoadCommit != nil {
		if err := args.OnLoadCommit(batch, []byte{}, true); err != nil {
			return err
		}
	}
	batchSize := batch.BatchSize()
	if _, err := batch.Commit(); err != nil {
		return err
	}
	runtime.ReadMemStats(&m)
	log.Info(
		"Committed batch",
		"bucket", string(bucket),
		"size", common.StorageSize(batchSize),
		"current key", makeCurrentKeyStr(nil),
		"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))

	return nil
}

func makeCurrentKeyStr(k []byte) string {
	var currentKeyStr string
	if k == nil {
		currentKeyStr = "final"
	} else if len(k) < 4 {
		currentKeyStr = fmt.Sprintf("%x", k)
	} else {
		currentKeyStr = fmt.Sprintf("%x...", k[:4])
	}
	return currentKeyStr
}
