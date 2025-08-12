// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package etl

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

type LoadNextFunc func(originalK, k, v []byte) error
type LoadFunc func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error
type simpleLoadFunc func(k, v []byte) error

type Allocator struct {
	p *sync.Pool
}

func NewAllocator(p *sync.Pool) *Allocator { return &Allocator{p: p} }
func (a *Allocator) Put(b Buffer) {
	if b == nil {
		return
	}
	//if cast, ok := b.(*sortableBuffer); ok {
	//	log.Warn("[dbg] return buf", "cap(cast.data)", cap(cast.data), "cap(cast.lens)", cap(cast.lens))
	//}
	a.p.Put(b)
}
func (a *Allocator) Get() Buffer {
	b := a.p.Get().(Buffer)
	b.Reset()
	return b
}

// Collector performs the job of ETL Transform, but can also be used without "E" (Extract) part
// as a Collect Transform Load
type Collector struct {
	buf           Buffer
	logPrefix     string
	tmpdir        string
	dataProviders []dataProvider
	logLvl        log.Lvl
	bufType       int
	allFlushed    bool
	logger        log.Logger

	// sortAndFlushInBackground increase insert performance, but make RAM use less-predictable:
	//   - if disk is over-loaded - app may have much background threads which waiting for flush - and each thread whill hold own `buf` (can't free RAM until flush is done)
	//   - enable it only when writing to `etl` is a bottleneck and unlikely to have many parallel collectors (to not overload CPU/Disk)
	sortAndFlushInBackground bool
	allocator                *Allocator
}

func NewCollectorWithAllocator(logPrefix, tmpdir string, allocator *Allocator, logger log.Logger) *Collector {
	c := NewCollector(logPrefix, tmpdir, allocator.Get(), logger)
	c.Allocator(allocator)
	return c
}
func NewCollector(logPrefix, tmpdir string, sortableBuffer Buffer, logger log.Logger) *Collector {
	return &Collector{bufType: getTypeByBuffer(sortableBuffer), buf: sortableBuffer, logPrefix: logPrefix, tmpdir: tmpdir, logLvl: log.LvlInfo, logger: logger}
}

func (c *Collector) SortAndFlushInBackground(v bool) { c.sortAndFlushInBackground = v }

func (c *Collector) extractNextFunc(originalK, k []byte, v []byte) error {
	if c.buf == nil && c.allocator != nil {
		c.buf = c.allocator.Get()
	}
	c.buf.Put(k, v)
	if !c.buf.CheckFlushSize() {
		return nil
	}
	return c.flushBuffer(false)
}

// Collect does copy `k` and `v`
func (c *Collector) Collect(k, v []byte) error {
	return c.extractNextFunc(k, k, v)
}

func (c *Collector) LogLvl(v log.Lvl) *Collector {
	c.logLvl = v
	return c
}
func (c *Collector) Allocator(a *Allocator) *Collector {
	c.allocator = a
	return c
}

func (c *Collector) flushBuffer(canStoreInRam bool) error {
	if c.buf.Len() == 0 {
		return nil
	}

	var provider dataProvider
	if canStoreInRam && len(c.dataProviders) == 0 {
		c.buf.Sort()
		provider = KeepInRAM(c.buf)
		c.allFlushed = true
	} else {
		var err error

		if c.sortAndFlushInBackground {
			fullBuf := c.buf // can't `.Reset()` because this `buf` will move to another goroutine
			if c.allocator != nil {
				c.buf = c.allocator.Get()
			} else {
				prevLen, prevSize := fullBuf.Len(), fullBuf.SizeLimit()
				c.buf = getBufferByType(c.bufType, datasize.ByteSize(c.buf.SizeLimit()))
				c.buf.Prealloc(prevLen/8, prevSize/8)
			}
			provider, err = FlushToDiskAsync(c.logPrefix, fullBuf, c.tmpdir, c.logLvl, c.allocator)
			if err != nil {
				return err
			}
		} else {
			provider, err = FlushToDisk(c.logPrefix, c.buf, c.tmpdir, c.logLvl)
			if err != nil {
				return err
			}
			c.buf.Reset()
		}
	}
	if provider != nil {
		c.dataProviders = append(c.dataProviders, provider)
	}
	return nil
}

// Flush - an optional method (usually user don't need to call it) - forcing sort+flush current buffer.
// it does trigger background sort and flush, reducing RAM-holding, etc...
// it's useful when working with many collectors: to trigger background sort for all of them
func (c *Collector) Flush() error {
	if !c.allFlushed {
		if e := c.flushBuffer(false); e != nil {
			return e
		}
	}
	return nil
}

func (c *Collector) Load(db kv.RwTx, toBucket string, loadFunc LoadFunc, args TransformArgs) error {
	if c.buf == nil && c.allocator != nil {
		c.buf = c.allocator.Get()
	}
	defer c.Close()
	args.BufferType = c.bufType

	if !c.allFlushed {
		if e := c.flushBuffer(true); e != nil {
			return e
		}
	}

	bucket := toBucket

	var cursor kv.RwCursor
	haveSortingGuaranties := isIdentityLoadFunc(loadFunc) // user-defined loadFunc may change ordering
	var lastKey []byte
	if bucket != "" { // passing empty bucket name is valid case for etl when DB modification is not expected
		var err error
		cursor, err = db.RwCursor(bucket)
		if err != nil {
			return err
		}
		defer cursor.Close()
		var errLast error
		lastKey, _, errLast = cursor.Last()
		if errLast != nil {
			return errLast
		}
	}

	var canUseAppend bool
	isDupSort := kv.ChaindataTablesCfg[bucket].Flags&kv.DupSort != 0 && !kv.ChaindataTablesCfg[bucket].AutoDupSortKeysConversion

	i := 0
	loadNextFunc := func(_, k, v []byte) error {
		if i == 0 {
			isEndOfBucket := lastKey == nil || bytes.Compare(lastKey, k) == -1
			canUseAppend = haveSortingGuaranties && isEndOfBucket
		}
		i++

		isNil := (c.bufType == SortableSliceBuffer && v == nil) ||
			(c.bufType == SortableAppendBuffer && len(v) == 0) || //backward compatibility
			(c.bufType == SortableOldestAppearedBuffer && len(v) == 0)
		if isNil && !args.EmptyVals {
			if canUseAppend {
				return nil // nothing to delete after end of bucket
			}
			if err := cursor.Delete(k); err != nil {
				return err
			}
			return nil
		}
		if len(v) == 0 && args.EmptyVals {
			v = []byte{} // Append empty value
		}
		if canUseAppend {
			if isDupSort {
				if err := cursor.(kv.RwCursorDupSort).AppendDup(k, v); err != nil {
					return fmt.Errorf("%s: bucket: %s, appendDup: k=%x, %w", c.logPrefix, bucket, k, err)
				}
			} else {
				if err := cursor.Append(k, v); err != nil {
					return fmt.Errorf("%s: bucket: %s, append: k=%x, v=%x, %w", c.logPrefix, bucket, k, v, err)
				}
			}

			return nil
		}
		if err := cursor.Put(k, v); err != nil {
			return fmt.Errorf("%s: put: k=%x, %w", c.logPrefix, k, err)
		}
		return nil
	}

	if bucket == "" {
		loadNextFunc = func(_, k, v []byte) error { return nil }
	}

	currentTable := &currentTableReader{db, bucket}
	simpleLoad := func(k, v []byte) error {
		return loadFunc(k, v, currentTable, loadNextFunc)
	}
	if err := mergeSortFiles(c.logPrefix, c.dataProviders, simpleLoad, args, c.buf); err != nil {
		return fmt.Errorf("loadIntoTable %s: %w", toBucket, err)
	}
	//logger.Trace(fmt.Sprintf("[%s] ETL Load done", c.logPrefix), "bucket", bucket, "records", i)
	return nil
}

func (c *Collector) Close() {
	if c.buf != nil { //idempotency
		if c.allocator != nil {
			c.allocator.Put(c.buf)
			c.buf = nil
		} else {
			c.buf.Reset()
		}
	}
	if c.dataProviders != nil { //idempotency
		for _, p := range c.dataProviders {
			p.Dispose()
		}
		c.dataProviders = nil
	}
	c.allFlushed = false
}

// mergeSortFiles uses merge-sort to order the elements stored within the slice of providers,
// regardless of ordering within the files the elements will be processed in order.
// The first pass reads the first element from each of the providers and populates a heap with the key/value/provider index.
// Later, the heap is popped to get the first element, the record is processed using the LoadFunc, and the provider is asked
// for the next item, which is then added back to the heap.
// The subsequent iterations pop the heap again and load up the provider associated with it to get the next element after processing LoadFunc.
// this continues until all providers have reached their EOF.
func mergeSortFiles(logPrefix string, providers []dataProvider, loadFunc simpleLoadFunc, args TransformArgs, buf Buffer) (err error) {
	for _, provider := range providers {
		if err := provider.Wait(); err != nil {
			return err
		}
	}

	h := &Heap{}
	heapInit(h)
	for i, provider := range providers {
		if key, value, err := provider.Next(nil, nil); err == nil {
			heapPush(h, &HeapElem{key, value, i})
		} else /* we must have at least one entry per file */ {
			eee := fmt.Errorf("%s: error reading first readers: n=%d current=%d provider=%s err=%w",
				logPrefix, len(providers), i, provider, err)
			panic(eee)
		}
	}

	var prevK, prevV []byte

	var i int
	// Main loading loop
	for h.Len() > 0 {
		i++
		if i%1024 == 0 {
			if err := common.Stopped(args.Quit); err != nil {
				return err
			}
		}

		element := heapPop(h)
		provider := providers[element.TimeIdx]

		// SortableOldestAppearedBuffer must guarantee that only 1 oldest value of key will appear
		// but because size of buffer is limited - each flushed file does guarantee "oldest appeared"
		// property, but files may overlap. files are sorted, just skip repeated keys here
		if args.BufferType == SortableOldestAppearedBuffer {
			if !bytes.Equal(prevK, element.Key) {
				if err = loadFunc(element.Key, element.Value); err != nil {
					return err
				}
				// Need to copy k because the underlying space will be re-used for the next key
				prevK = common.Copy(element.Key)
			}
		} else if args.BufferType == SortableAppendBuffer {
			if !bytes.Equal(prevK, element.Key) {
				if prevK != nil {
					if err = loadFunc(prevK, prevV); err != nil {
						return err
					}
				}
				// Need to copy k because the underlying space will be re-used for the next key
				prevK = common.Copy(element.Key)
				prevV = common.Copy(element.Value)
			} else {
				prevV = append(prevV, element.Value...)
			}
		} else {
			if err = loadFunc(element.Key, element.Value); err != nil {
				return err
			}
		}

		if element.Key, element.Value, err = provider.Next(element.Key[:0], element.Value[:0]); err == nil {
			heapPush(h, element)
		} else if !errors.Is(err, io.EOF) {
			return fmt.Errorf("%s: error while reading next element from disk: %w", logPrefix, err)
		}
	}

	if args.BufferType == SortableAppendBuffer {
		if prevK != nil {
			if err = loadFunc(prevK, prevV); err != nil {
				return err
			}
		}
	}

	return nil
}

func makeCurrentKeyStr(k []byte) string {
	var currentKeyStr string
	if k == nil {
		currentKeyStr = "final"
	} else if len(k) < 4 {
		currentKeyStr = hex.EncodeToString(k)
	} else if k[0] == 0 && k[1] == 0 && k[2] == 0 && k[3] == 0 && len(k) >= 8 { // if key has leading zeroes, show a bit more info
		currentKeyStr = hex.EncodeToString(k)
	} else {
		currentKeyStr = hex.EncodeToString(k[:4])
	}
	return currentKeyStr
}
