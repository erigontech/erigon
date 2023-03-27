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
	"bytes"
	"container/heap"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type LoadNextFunc func(originalK, k, v []byte) error
type LoadFunc func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error
type simpleLoadFunc func(k, v []byte) error

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
	autoClean     bool
}

// NewCollectorFromFiles creates collector from existing files (left over from previous unsuccessful loading)
func NewCollectorFromFiles(logPrefix, tmpdir string) (*Collector, error) {
	if _, err := os.Stat(tmpdir); os.IsNotExist(err) {
		return nil, nil
	}
	dirEntries, err := os.ReadDir(tmpdir)
	if err != nil {
		return nil, fmt.Errorf("collector from files - reading directory %s: %w", tmpdir, err)
	}
	if len(dirEntries) == 0 {
		return nil, nil
	}
	dataProviders := make([]dataProvider, len(dirEntries))
	for i, dirEntry := range dirEntries {
		fileInfo, err := dirEntry.Info()
		if err != nil {
			return nil, fmt.Errorf("collector from files - reading file info %s: %w", dirEntry.Name(), err)
		}
		var dataProvider fileDataProvider
		dataProvider.file, err = os.Open(filepath.Join(tmpdir, fileInfo.Name()))
		if err != nil {
			return nil, fmt.Errorf("collector from files - opening file %s: %w", fileInfo.Name(), err)
		}
		dataProviders[i] = &dataProvider
	}
	return &Collector{dataProviders: dataProviders, allFlushed: true, autoClean: false, logPrefix: logPrefix}, nil
}

// NewCriticalCollector does not clean up temporary files if loading has failed
func NewCriticalCollector(logPrefix, tmpdir string, sortableBuffer Buffer) *Collector {
	c := NewCollector(logPrefix, tmpdir, sortableBuffer)
	c.autoClean = false
	return c
}

func NewCollector(logPrefix, tmpdir string, sortableBuffer Buffer) *Collector {
	return &Collector{autoClean: true, bufType: getTypeByBuffer(sortableBuffer), buf: sortableBuffer, logPrefix: logPrefix, tmpdir: tmpdir, logLvl: log.LvlInfo}
}

func (c *Collector) extractNextFunc(originalK, k []byte, v []byte) error {
	c.buf.Put(k, v)
	if !c.buf.CheckFlushSize() {
		return nil
	}
	return c.flushBuffer(false)
}

func (c *Collector) Collect(k, v []byte) error {
	return c.extractNextFunc(k, k, v)
}

func (c *Collector) LogLvl(v log.Lvl) { c.logLvl = v }

func (c *Collector) flushBuffer(canStoreInRam bool) error {
	if c.buf.Len() == 0 {
		return nil
	}
	var provider dataProvider
	c.buf.Sort()
	if canStoreInRam && len(c.dataProviders) == 0 {
		provider = KeepInRAM(c.buf)
		c.allFlushed = true
	} else {
		doFsync := !c.autoClean /* is critical collector */
		var err error
		provider, err = FlushToDisk(c.logPrefix, c.buf, c.tmpdir, doFsync, c.logLvl)
		if err != nil {
			return err
		}
	}
	if provider != nil {
		c.dataProviders = append(c.dataProviders, provider)
	}
	return nil
}

func (c *Collector) Load(db kv.RwTx, toBucket string, loadFunc LoadFunc, args TransformArgs) error {
	if c.autoClean {
		defer c.Close()
	}

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
		var errLast error
		lastKey, _, errLast = cursor.Last()
		if errLast != nil {
			return errLast
		}
	}

	var canUseAppend bool
	isDupSort := kv.ChaindataTablesCfg[bucket].Flags&kv.DupSort != 0 && !kv.ChaindataTablesCfg[bucket].AutoDupSortKeysConversion

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	i := 0
	var prevK []byte
	loadNextFunc := func(_, k, v []byte) error {
		if i == 0 {
			isEndOfBucket := lastKey == nil || bytes.Compare(lastKey, k) == -1
			canUseAppend = haveSortingGuaranties && isEndOfBucket
		}
		i++

		// SortableOldestAppearedBuffer must guarantee that only 1 oldest value of key will appear
		// but because size of buffer is limited - each flushed file does guarantee "oldest appeared"
		// property, but files may overlap. files are sorted, just skip repeated keys here
		if c.bufType == SortableOldestAppearedBuffer {
			if bytes.Equal(prevK, k) {
				return nil
			} else {
				// Need to copy k because the underlying space will be re-used for the next key
				prevK = common.Copy(k)
			}
		}

		select {
		default:
		case <-logEvery.C:
			logArs := []interface{}{"into", bucket}
			if args.LogDetailsLoad != nil {
				logArs = append(logArs, args.LogDetailsLoad(k, v)...)
			} else {
				logArs = append(logArs, "current_prefix", makeCurrentKeyStr(k))
			}

			log.Log(c.logLvl, fmt.Sprintf("[%s] ETL [2/2] Loading", c.logPrefix), logArs...)
		}

		isNil := (c.bufType == SortableSliceBuffer && v == nil) ||
			(c.bufType == SortableAppendBuffer && len(v) == 0) || //backward compatibility
			(c.bufType == SortableOldestAppearedBuffer && len(v) == 0)
		if isNil {
			if canUseAppend {
				return nil // nothing to delete after end of bucket
			}
			if err := cursor.Delete(k); err != nil {
				return err
			}
			return nil
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

	currentTable := &currentTableReader{db, bucket}
	simpleLoad := func(k, v []byte) error {
		return loadFunc(k, v, currentTable, loadNextFunc)
	}
	if err := mergeSortFiles(c.logPrefix, c.dataProviders, simpleLoad, args); err != nil {
		return fmt.Errorf("loadIntoTable %s: %w", toBucket, err)
	}
	//log.Trace(fmt.Sprintf("[%s] ETL Load done", c.logPrefix), "bucket", bucket, "records", i)
	return nil
}

func (c *Collector) reset() {
	for _, p := range c.dataProviders {
		p.Dispose()
	}
	c.dataProviders = nil
	c.buf.Reset()
	c.allFlushed = false
}

func (c *Collector) Close() {
	c.reset()
}

// mergeSortFiles uses merge-sort to order the elements stored within the slice of providers,
// regardless of ordering within the files the elements will be processed in order.
// The first pass reads the first element from each of the providers and populates a heap with the key/value/provider index.
// Later, the heap is popped to get the first element, the record is processed using the LoadFunc, and the provider is asked
// for the next item, which is then added back to the heap.
// The subsequent iterations pop the heap again and load up the provider associated with it to get the next element after processing LoadFunc.
// this continues until all providers have reached their EOF.
func mergeSortFiles(logPrefix string, providers []dataProvider, loadFunc simpleLoadFunc, args TransformArgs) error {
	h := &Heap{}
	heap.Init(h)
	for i, provider := range providers {
		if key, value, err := provider.Next(nil, nil); err == nil {
			he := HeapElem{key, value, i}
			heap.Push(h, he)
		} else /* we must have at least one entry per file */ {
			eee := fmt.Errorf("%s: error reading first readers: n=%d current=%d provider=%s err=%w",
				logPrefix, len(providers), i, provider, err)
			panic(eee)
		}
	}

	// Main loading loop
	for h.Len() > 0 {
		if err := common.Stopped(args.Quit); err != nil {
			return err
		}

		element := (heap.Pop(h)).(HeapElem)
		provider := providers[element.TimeIdx]
		err := loadFunc(element.Key, element.Value)
		if err != nil {
			return err
		}
		if element.Key, element.Value, err = provider.Next(element.Key[:0], element.Value[:0]); err == nil {
			heap.Push(h, element)
		} else if !errors.Is(err, io.EOF) {
			return fmt.Errorf("%s: error while reading next element from disk: %w", logPrefix, err)
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
