package downloader

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
)

func fillChangeSetBuffer(db ethdb.Database, bucket []byte, blockNum uint64, changesets []byte, offsets []int, blockNums []uint64) (bool, uint64, []int, []uint64, error) {
	offset := 0
	offsets = offsets[:0]
	blockNums = blockNums[:0]
	startKey := dbutils.EncodeTimestamp(blockNum)
	done := true
	if err := db.Walk(bucket, startKey, 0, func(k, v []byte) (bool, error) {
		blockNum, _ = dbutils.DecodeTimestamp(k)
		if offset+len(v) > len(changesets) { // Adding the current changeset would overflow the buffer
			done = false
			return false, nil
		}
		copy(changesets[offset:], v)
		offset += len(v)
		offsets = append(offsets, offset)
		blockNums = append(blockNums, blockNum)
		return true, nil
	}); err != nil {
		return true, blockNum, offsets, blockNums, fmt.Errorf("walking over account changeset for block %d: %v", blockNum, err)
	}
	return done, blockNum, offsets, blockNums, nil
}

const emptyValBit uint64 = 0x8000000000000000

// writeBufferMapToTempFile creates temp file in the datadir and writes bufferMap into it
// if sucessful, returns the name of the created file. File is closed
func writeBufferMapToTempFile(datadir string, pattern string, bufferMap map[string][]uint64) (string, error) {
	var filename string
	keys := make([]string, len(bufferMap))
	i := 0
	for key := range bufferMap {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	var w *bufio.Writer
	if bufferFile, err := ioutil.TempFile(datadir, pattern); err == nil {
		//nolint:errcheck
		defer bufferFile.Close()
		filename = bufferFile.Name()
		w = bufio.NewWriter(bufferFile)
	} else {
		return filename, fmt.Errorf("creating temp buf file %s: %v", pattern, err)
	}
	var nbytes [8]byte
	for _, key := range keys {
		if _, err := w.Write([]byte(key)); err != nil {
			return filename, err
		}
		list := bufferMap[key]
		binary.BigEndian.PutUint64(nbytes[:], uint64(len(list)))
		if _, err := w.Write(nbytes[:]); err != nil {
			return filename, err
		}
		for _, b := range list {
			binary.BigEndian.PutUint64(nbytes[:], b)
			if _, err := w.Write(nbytes[:]); err != nil {
				return filename, err
			}
		}
	}
	if err := w.Flush(); err != nil {
		return filename, fmt.Errorf("flushing file %s: %v", filename, err)
	}
	return filename, nil
}

type HeapElem struct {
	key     []byte
	timeIdx int
}

type Heap []HeapElem

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].key, h[j].key); c != 0 {
		return c < 0
	}
	return h[i].timeIdx < h[j].timeIdx
}

func (h Heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(HeapElem))
}

func (h *Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func mergeFilesIntoBucket(bufferFileNames []string, db ethdb.Database, bucket []byte, keyLength int) error {
	var m runtime.MemStats
	h := &Heap{}
	heap.Init(h)
	readers := make([]io.Reader, len(bufferFileNames))
	for i, fileName := range bufferFileNames {
		if f, err := os.Open(fileName); err == nil {
			readers[i] = bufio.NewReader(f)
			//nolint:errcheck
			defer f.Close()
		} else {
			return err
		}
		// Read first key
		keyBuf := make([]byte, keyLength)
		if n, err := io.ReadFull(readers[i], keyBuf); err == nil && n == keyLength {
			heap.Push(h, HeapElem{keyBuf, i})
		} else {
			return fmt.Errorf("init reading from account buffer file: %d %x %v", n, keyBuf[:n], err)
		}
	}
	// By now, the heap has one element for each buffer file
	batch := db.NewBatch()
	var nbytes [8]byte
	for h.Len() > 0 {
		element := (heap.Pop(h)).(HeapElem)
		reader := readers[element.timeIdx]
		k := element.key
		// Read number of items for this key
		var count int
		if n, err := io.ReadFull(reader, nbytes[:]); err == nil && n == 8 {
			count = int(binary.BigEndian.Uint64(nbytes[:]))
		} else {
			return fmt.Errorf("reading from account buffer file: %d %v", n, err)
		}
		for i := 0; i < count; i++ {
			var b uint64
			if n, err := io.ReadFull(reader, nbytes[:]); err == nil && n == 8 {
				b = binary.BigEndian.Uint64(nbytes[:])
			} else {
				return fmt.Errorf("reading from account buffer file: %d %v", n, err)
			}
			vzero := (b & emptyValBit) != 0
			blockNr := b &^ emptyValBit
			currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
			indexBytes, err1 := batch.Get(bucket, currentChunkKey)
			if err1 != nil && err1 != ethdb.ErrKeyNotFound {
				return fmt.Errorf("find chunk failed: %w", err1)
			}
			var index dbutils.HistoryIndexBytes
			if len(indexBytes) == 0 {
				index = dbutils.NewHistoryIndex()
			} else if dbutils.CheckNewIndexChunk(indexBytes, blockNr) {
				// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
				index = dbutils.WrapHistoryIndex(indexBytes)
				indexKey, err3 := index.Key(k)
				if err3 != nil {
					return err3
				}
				// Flush the old chunk
				if err4 := batch.Put(bucket, indexKey, index); err4 != nil {
					return err4
				}
				// Start a new chunk
				index = dbutils.NewHistoryIndex()
			} else {
				index = dbutils.WrapHistoryIndex(indexBytes)
			}
			index = index.Append(blockNr, vzero)

			if err := batch.Put(bucket, currentChunkKey, index); err != nil {
				return err
			}
			batchSize := batch.BatchSize()
			if batchSize > batch.IdealBatchSize() {
				if _, err := batch.Commit(); err != nil {
					return err
				}
				runtime.ReadMemStats(&m)
				log.Info("Commited index batch", "bucket", string(bucket), "size", common.StorageSize(batchSize), "current key", fmt.Sprintf("%x...", k[:4]),
					"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
			}
		}
		// Try to read the next key (reuse the element)
		if n, err := io.ReadFull(reader, element.key); err == nil && n == keyLength {
			heap.Push(h, element)
		} else if err != io.EOF {
			// If it is EOF, we simply do not return anything into the heap
			return fmt.Errorf("next reading from account buffer file: %d %x %v", n, element.key[:n], err)
		}
	}
	if _, err := batch.Commit(); err != nil {
		return err
	}
	return nil
}

const changeSetBufSize = 256 * 1024 * 1024

func spawnAccountHistoryIndex(db ethdb.Database, datadir string, plainState bool) error {
	if plainState {
		log.Info("Skipped account index generation for plain state")
		return nil
	}
	var blockNum uint64
	if lastProcessedBlockNumber, err := GetStageProgress(db, AccountHistoryIndex); err == nil {
		blockNum = lastProcessedBlockNumber + 1
	} else {
		return fmt.Errorf("reading account history process: %v", err)
	}
	log.Info("Account history index generation started", "from", blockNum)
	var m runtime.MemStats
	var bufferFileNames []string
	changesets := make([]byte, changeSetBufSize) // 256 Mb buffer
	var offsets []int
	var blockNums []uint64
	var done = false
	// In the first loop, we read all the changesets, create partial history indices, sort them, and
	// write each batch into a file
	for !done {
		if newDone, newBlockNum, newOffsets, newBlockNums, err := fillChangeSetBuffer(db, dbutils.AccountChangeSetBucket, blockNum, changesets, offsets, blockNums); err == nil {
			done = newDone
			blockNum = newBlockNum
			offsets = newOffsets
			blockNums = newBlockNums
		} else {
			return err
		}
		bufferMap := make(map[string][]uint64)
		prevOffset := 0
		for i, offset := range offsets {
			blockNr := blockNums[i]
			if err := changeset.AccountChangeSetBytes(changesets[prevOffset:offset]).Walk(func(k, v []byte) error {
				sKey := string(k)
				list := bufferMap[sKey]
				b := blockNr
				if len(v) == 0 {
					b |= emptyValBit
				}
				list = append(list, b)
				bufferMap[sKey] = list
				return nil
			}); err != nil {
				return err
			}
			prevOffset = offset
		}
		if filename, err := writeBufferMapToTempFile(datadir, "account-history-inx-", bufferMap); err == nil {
			defer func() {
				//nolint:errcheck
				os.Remove(filename)
			}()
			bufferFileNames = append(bufferFileNames, filename)
			runtime.ReadMemStats(&m)
			log.Info("Created a buffer file", "name", filename, "up to block", blockNum,
				"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		} else {
			return err
		}
	}
	if err := mergeFilesIntoBucket(bufferFileNames, db, dbutils.AccountsHistoryBucket, common.HashLength); err != nil {
		return err
	}
	if err := SaveStageProgress(db, AccountHistoryIndex, blockNum); err != nil {
		return err
	}
	return nil
}

func spawnStorageHistoryIndex(db ethdb.Database, datadir string, plainState bool) error {
	if plainState {
		log.Info("Skipped storage index generation for plain state")
		return nil
	}
	var blockNum uint64
	if lastProcessedBlockNumber, err := GetStageProgress(db, StorageHistoryIndex); err == nil {
		blockNum = lastProcessedBlockNumber + 1
	} else {
		return fmt.Errorf("reading storage history process: %v", err)
	}
	log.Info("Storage history index generation started", "from", blockNum)
	var m runtime.MemStats
	var bufferFileNames []string
	changesets := make([]byte, changeSetBufSize) // 256 Mb buffer
	var offsets []int
	var blockNums []uint64
	var done = false
	// In the first loop, we read all the changesets, create partial history indices, sort them, and
	// write each batch into a file
	for !done {
		if newDone, newBlockNum, newOffsets, newBlockNums, err := fillChangeSetBuffer(db, dbutils.StorageChangeSetBucket, blockNum, changesets, offsets, blockNums); err == nil {
			done = newDone
			blockNum = newBlockNum
			offsets = newOffsets
			blockNums = newBlockNums
		} else {
			return err
		}
		bufferMap := make(map[string][]uint64)
		prevOffset := 0
		for i, offset := range offsets {
			blockNr := blockNums[i]
			if err := changeset.StorageChangeSetBytes(changesets[prevOffset:offset]).Walk(func(k, v []byte) error {
				sKey := string(k)
				list := bufferMap[sKey]
				b := blockNr
				if len(v) == 0 {
					b |= emptyValBit
				}
				list = append(list, b)
				bufferMap[sKey] = list
				return nil
			}); err != nil {
				return err
			}
			prevOffset = offset
		}
		if filename, err := writeBufferMapToTempFile(datadir, "storage-history-inx-", bufferMap); err == nil {
			defer func() {
				//nolint:errcheck
				os.Remove(filename)
			}()
			bufferFileNames = append(bufferFileNames, filename)
			runtime.ReadMemStats(&m)
			log.Info("Created a buffer file", "name", filename, "up to block", blockNum,
				"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		} else {
			return err
		}
	}
	if err := mergeFilesIntoBucket(bufferFileNames, db, dbutils.StorageHistoryBucket, 2*common.HashLength+common.IncarnationLength); err != nil {
		return err
	}
	if err := SaveStageProgress(db, StorageHistoryIndex, blockNum); err != nil {
		return err
	}
	return nil
}

func unwindAccountHistoryIndex(unwindPoint uint64, db ethdb.Database, plainState bool) error {
	ig := core.NewIndexGenerator(db)
	return ig.Truncate(unwindPoint, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, walkerFactory(dbutils.AccountChangeSetBucket, plainState))
}

func unwindStorageHistoryIndex(unwindPoint uint64, db ethdb.Database, plainState bool) error {
	ig := core.NewIndexGenerator(db)
	return ig.Truncate(unwindPoint, dbutils.StorageChangeSetBucket, dbutils.StorageHistoryBucket, walkerFactory(dbutils.StorageChangeSetBucket, plainState))
}

func walkerFactory(csBucket []byte, plainState bool) func(bytes []byte) core.ChangesetWalker {
	switch {
	case bytes.Equal(csBucket, dbutils.AccountChangeSetBucket) && !plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.AccountChangeSetBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.AccountChangeSetBucket) && plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.AccountChangeSetPlainBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.StorageChangeSetBucket) && !plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.StorageChangeSetBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.StorageChangeSetBucket) && plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.StorageChangeSetPlainBytes(bytes)
		}
	default:
		log.Error("incorrect bucket", "bucket", string(csBucket), "plainState", plainState)
		panic("incorrect bucket " + string(csBucket))
	}
}
