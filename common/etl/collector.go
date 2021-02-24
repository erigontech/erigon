package etl

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ugorji/go/codec"
)

const TmpDirName = "etl-temp"

type LoadNextFunc func(originalK, k, v []byte) error
type LoadFunc func(k []byte, value []byte, table CurrentTableReader, next LoadNextFunc) error

// Collector performs the job of ETL Transform, but can also be used without "E" (Extract) part
// as a Collect Transform Load
type Collector struct {
	extractNextFunc ExtractNextFunc
	flushBuffer     func([]byte, bool) error
	dataProviders   []dataProvider
	allFlushed      bool
	autoClean       bool
}

// NewCollectorFromFiles creates collector from existing files (left over from previous unsuccessful loading)
func NewCollectorFromFiles(tmpdir string) (*Collector, error) {
	if _, err := os.Stat(tmpdir); os.IsNotExist(err) {
		return nil, nil
	}
	fileInfos, err := ioutil.ReadDir(tmpdir)
	if err != nil {
		return nil, fmt.Errorf("collector from files - reading directory %s: %w", tmpdir, err)
	}
	if len(fileInfos) == 0 {
		return nil, nil
	}
	dataProviders := make([]dataProvider, len(fileInfos))
	for i, fileInfo := range fileInfos {
		var dataProvider fileDataProvider
		dataProvider.file, err = os.Open(filepath.Join(tmpdir, fileInfo.Name()))
		if err != nil {
			return nil, fmt.Errorf("collector from files - opening file %s: %w", fileInfo.Name(), err)
		}
		dataProviders[i] = &dataProvider
	}
	return &Collector{dataProviders: dataProviders, allFlushed: true, autoClean: false}, nil
}

// NewCriticalCollector does not clean up temporary files if loading has failed
func NewCriticalCollector(tmpdir string, sortableBuffer Buffer) *Collector {
	c := NewCollector(tmpdir, sortableBuffer)
	c.autoClean = false
	return c
}

func NewCollector(tmpdir string, sortableBuffer Buffer) *Collector {
	c := &Collector{autoClean: true}
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
			provider, err = FlushToDisk(encoder, currentKey, sortableBuffer, tmpdir)
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

func (c *Collector) Load(logPrefix string, db ethdb.Database, toBucket string, loadFunc LoadFunc, args TransformArgs) (err error) {
	defer func() {
		if c.autoClean {
			c.Close(logPrefix)
		}
	}()
	if !c.allFlushed {
		if err := c.flushBuffer(nil, true); err != nil {
			return err
		}
	}
	err = loadFilesIntoBucket(logPrefix, db, toBucket, c.dataProviders, loadFunc, args)
	if err != nil {
		return err
	}
	return nil
}

func (c *Collector) Close(logPrefix string) {
	disposeProviders(logPrefix, c.dataProviders)
}

func loadFilesIntoBucket(logPrefix string, db ethdb.Database, bucket string, providers []dataProvider, loadFunc LoadFunc, args TransformArgs) error {
	decoder := codec.NewDecoder(nil, &cbor)
	var m runtime.MemStats

	h := &Heap{comparator: args.Comparator}
	heap.Init(h)
	for i, provider := range providers {
		if key, value, err := provider.Next(decoder); err == nil {
			he := HeapElem{key, i, value}
			heap.Push(h, he)
		} else /* we must have at least one entry per file */ {
			eee := fmt.Errorf("%s: error reading first readers: n=%d current=%d provider=%s err=%v",
				logPrefix, len(providers), i, provider, err)
			panic(eee)
		}
	}

	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	currentTable := &currentTableReader{tx, bucket}
	haveSortingGuaranties := isIdentityLoadFunc(loadFunc) // user-defined loadFunc may change ordering
	var lastKey []byte
	if bucket != "" { // passing empty bucket name is valid case for etl when DB modification is not expected
		var errLast error
		lastKey, _, errLast = tx.Last(bucket)
		if errLast != nil {
			return errLast
		}
	}
	var canUseAppend bool
	isDupSort := dbutils.BucketsConfigs[bucket].Flags&dbutils.DupSort != 0 && !dbutils.BucketsConfigs[bucket].AutoDupSortKeysConversion

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	var pervK []byte

	i := 0
	loadNextFunc := func(originalK, k, v []byte) error {
		if i == 0 {
			isEndOfBucket := lastKey == nil || bytes.Compare(lastKey, k) == -1
			canUseAppend = haveSortingGuaranties && isEndOfBucket
		}
		i++

		select {
		default:
		case <-logEvery.C:
			logArs := []interface{}{"into", bucket}
			if args.LogDetailsLoad != nil {
				logArs = append(logArs, args.LogDetailsLoad(k, v)...)
			} else {
				logArs = append(logArs, "current key", makeCurrentKeyStr(k))
			}

			runtime.ReadMemStats(&m)
			logArs = append(logArs, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
			log.Info(fmt.Sprintf("[%s] ETL [2/2] Loading", logPrefix), logArs...)
		}

		if canUseAppend && len(v) == 0 {
			return nil // nothing to delete after end of bucket
		}
		if len(v) == 0 {
			if err := tx.Delete(bucket, k, nil); err != nil {
				return err
			}
			return nil
		}
		if canUseAppend {
			if isDupSort {
				if bytes.Equal(k, pervK) {
					if err := tx.AppendDup(bucket, k, v); err != nil {
						return fmt.Errorf("%s: bucket: %s, appendDup: k=%x, pervK=%x %w", logPrefix, bucket, k, pervK, err)
					}
				} else {
					if err := tx.Append(bucket, k, v); err != nil {
						return fmt.Errorf("%s: bucket: %s, append: k=%x, pervK=%x, %w", logPrefix, bucket, k, pervK, err)
					}
				}
				pervK = k
			} else {
				if err := tx.Append(bucket, k, v); err != nil {
					return fmt.Errorf("%s: bucket: %s, append: k=%x, %w", logPrefix, bucket, k, err)
				}
			}

			return nil
		}
		if err := tx.Put(bucket, k, v); err != nil {
			return fmt.Errorf("%s: put: k=%x, %w", logPrefix, k, err)
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
		err := loadFunc(element.Key, element.Value, currentTable, loadNextFunc)
		if err != nil {
			return err
		}
		if element.Key, element.Value, err = provider.Next(decoder); err == nil {
			heap.Push(h, element)
		} else if err != io.EOF {
			return fmt.Errorf("%s: error while reading next element from disk: %v", logPrefix, err)
		}
	}
	// Final commit
	if args.OnLoadCommit != nil {
		if err := args.OnLoadCommit(tx, []byte{}, true); err != nil {
			return err
		}
	}
	commitTimer := time.Now()
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}
	commitTook := time.Since(commitTimer)

	runtime.ReadMemStats(&m)
	log.Debug(
		fmt.Sprintf("[%s] Committed batch", logPrefix),
		"bucket", bucket,
		"commit", commitTook,
		"records", i,
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
	} else if k[0] == 0 && k[1] == 0 && k[2] == 0 && k[3] == 0 && len(k) >= 8 { // if key has leading zeroes, show a bit more info
		currentKeyStr = fmt.Sprintf("%x", k)
	} else {
		currentKeyStr = fmt.Sprintf("%x...", k[:4])
	}
	return currentKeyStr
}
