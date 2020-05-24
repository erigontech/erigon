package downloader

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"bufio"
	"encoding/binary"
	"container/heap"
	"io"
	"os"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"runtime"
	"sort"
	"time"
)

type HeapElem struct {
	key []byte
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

const changeSetBufSize = 128 * 1024 * 1024

func spawnAccountHistoryIndex(db ethdb.Database, datadir string, plainState bool) error {
	lastProcessedBlockNumber, err := GetStageProgress(db, AccountHistoryIndex)
	if err != nil {
		return err
	}
	if plainState {
		log.Info("Skipped account index generation for plain state")
		return nil
	}
	log.Info("Account history index generation started", "from", lastProcessedBlockNumber)
	var m runtime.MemStats
	var bufferFileNames []string
	changesets := make([]byte, changeSetBufSize) // 128 Mb buffer
	var offsets []int
	var blockNums []uint64
	var blockNum = lastProcessedBlockNumber + 1
	var done = false
	// In the first loop, we read all the changesets, create partial history indices, sort them, and 
	// write each batch into a file
	for !done {
		offset := 0
		offsets = offsets[:0]
		blockNums = blockNums[:0]
		startKey := dbutils.EncodeTimestamp(blockNum)
		done = true
		if err := db.Walk(dbutils.AccountChangeSetBucket, startKey, 0, func(k, v []byte) (bool, error) {
			blockNum, _ = dbutils.DecodeTimestamp(k)
			if offset+len(v) > changeSetBufSize { // Adding the current changeset would overflow the buffer
				done = false
				return false, nil
			}
			copy(changesets[offset:], v)
			offset += len(v)
			offsets = append(offsets, offset)
			blockNums = append(blockNums, blockNum)
			return true, nil
		}); err != nil {
			return err
		}
		bufferMap := make(map[string][]uint64)
		prevOffset := 0
		for i, offset := range offsets {
			blockNr := blockNums[i]
			changeset := changeset.AccountChangeSetBytes(changesets[prevOffset:offset])
			if err := changeset.Walk(func(k, v []byte) error {
				sKey := string(k)
				list := bufferMap[sKey]
				b := blockNr
				if len(v) == 0 {
					b |= 0x8000000000000000
				}
				list = append(list, b)
				bufferMap[sKey] = list
				return nil
			}); err != nil {
				return err
			}
			prevOffset = offset
		}
		keys := make([]string, len(bufferMap))
		i := 0
		for key := range bufferMap {
			keys[i] = key
			i++
		}
		sort.Strings(keys)
		bufferFile, err1 := ioutil.TempFile(datadir, "account-history-idx")
		if err1 != nil {
			return err1
		}
		defer func() {
			//nolint:errcheck
			bufferFile.Close()
			//nolint:errcheck
			os.Remove(bufferFile.Name())
		}()
		bufferFileNames = append(bufferFileNames, bufferFile.Name())
		w := bufio.NewWriter(bufferFile)
		var nbytes [8]byte
		for _, key := range keys {
			if _, err2 := w.Write([]byte(key)); err2 != nil {
				return err2
			}
			list := bufferMap[key]
			binary.BigEndian.PutUint64(nbytes[:], uint64(len(list)))
			if _, err2 := w.Write(nbytes[:]); err2 != nil {
				return err2
			}
			for _, b := range list {
				binary.BigEndian.PutUint64(nbytes[:], b)
				if _, err2 := w.Write(nbytes[:]); err != nil {
					return err2
				}
			}
		}
		if err2 := w.Flush(); err2 != nil {
			return err2
		}
		if err2 := bufferFile.Close(); err2 != nil {
			return err2
		}
		runtime.ReadMemStats(&m)
		log.Info("Created a buffer file", "name", bufferFile.Name(), "up to block", blockNum,
			"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
	}
	h := &Heap{}
	heap.Init(h)
	readers := make([]io.Reader, len(bufferFileNames))
	for i, fileName := range bufferFileNames {
		if f, err2 := os.Open(fileName); err2 == nil {
			readers[i] = bufio.NewReader(f)
			//nolint:errcheck
			defer f.Close()
		} else {
			return err2
		}
		// Read first key
		keyBuf := make([]byte, common.HashLength)
		if n, err2 := readers[i].Read(keyBuf); err2 == nil && n == common.HashLength {
			heap.Push(h, HeapElem{keyBuf, i})
		} else {
			return fmt.Errorf("reading from account buffer file: %d %v", n, err2)
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
		if n, err2 := reader.Read(nbytes[:]); err2 == nil && n == 8 {
			count = int(binary.BigEndian.Uint64(nbytes[:]))
		} else {
			return fmt.Errorf("reading from account buffer file: %d %v", n, err2)
		}
		for i := 0; i < count; i++ {
			var b uint64
			if n, err2 := reader.Read(nbytes[:]); err2 == nil && n == 8 {
				b = binary.BigEndian.Uint64(nbytes[:])
			} else {
				return fmt.Errorf("reading from account buffer file: %d %v", n, err2)
			}
			vzero := (b & 0x8000000000000000) != 0
			blockNr := b &^ 0x8000000000000000
			currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
			indexBytes, err2 := batch.Get(dbutils.AccountsHistoryBucket, currentChunkKey)
			if err2 != nil && err2 != ethdb.ErrKeyNotFound {
				return fmt.Errorf("find chunk failed: %w", err2)
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
				if err4 := batch.Put(dbutils.AccountsHistoryBucket, indexKey, index); err4 != nil {
					return err4
				}
				// Start a new chunk
				index = dbutils.NewHistoryIndex()
			} else {
				index = dbutils.WrapHistoryIndex(indexBytes)
			}
			index = index.Append(blockNr, vzero)

			if err4 := batch.Put(dbutils.AccountsHistoryBucket, currentChunkKey, index); err4 != nil {
				return err4
			}
			batchSize := batch.BatchSize()
			start := time.Now()
			if _, err4 := batch.Commit(); err4 != nil {
				return err4
			}
			runtime.ReadMemStats(&m)
			log.Info("Commited account index batch", "in", time.Since(start), "up to block", blockNum, "batch size", common.StorageSize(batchSize),
				"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		}
		// Try to read the next key (reuse the element)
		if n, err2 := readers[element.timeIdx].Read(element.key); err2 == nil && n == common.HashLength {
			heap.Push(h, element)
		} else if err2 != io.EOF {
			// If it is EOF, we simply do not return anything into the heap
			return fmt.Errorf("reading from account buffer file: %d %v", n, err2)
		}
	}
	if err2 := SaveStageProgress(batch, AccountHistoryIndex, blockNum); err2 != nil {
		return err2
	}
	return nil
}

func spawnStorageHistoryIndex(db ethdb.Database, datadir string, plainState bool) error {
	lastProcessedBlockNumber, err := GetStageProgress(db, StorageHistoryIndex)
	if err != nil {
		return err
	}
	if plainState {
		log.Info("Skipped storage index generation for plain state")
		return nil
	}
	log.Info("Storage history index generation started", "from", lastProcessedBlockNumber)
	var m runtime.MemStats
	changesets := make([]byte, changeSetBufSize) // 128 Mb buffer
	var offsets []int
	var blockNums []uint64
	var blockNum = lastProcessedBlockNumber + 1
	var done = false
	batch := db.NewBatch()
	for !done {
		offset := 0
		offsets = offsets[:0]
		blockNums = blockNums[:0]
		startKey := dbutils.EncodeTimestamp(blockNum)
		done = true
		if err := db.Walk(dbutils.StorageChangeSetBucket, startKey, 0, func(k, v []byte) (bool, error) {
			blockNum, _ = dbutils.DecodeTimestamp(k)
			if offset+len(v) > changeSetBufSize { // Adding the current changeset would overflow the buffer
				done = false
				return false, nil
			}
			copy(changesets[offset:], v)
			offset += len(v)
			offsets = append(offsets, offset)
			blockNums = append(blockNums, blockNum)
			return true, nil
		}); err != nil {
			return err
		}
		prevOffset := 0
		for i, offset := range offsets {
			blockNr := blockNums[i]
			changeset := changeset.StorageChangeSetBytes(changesets[prevOffset:offset])
			if err := changeset.Walk(func(k, v []byte) error {
				currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
				indexBytes, err := batch.Get(dbutils.StorageHistoryBucket, currentChunkKey)
				if err != nil && err != ethdb.ErrKeyNotFound {
					return fmt.Errorf("find chunk failed: %w", err)
				}
				var index dbutils.HistoryIndexBytes
				if len(indexBytes) == 0 {
					index = dbutils.NewHistoryIndex()
				} else if dbutils.CheckNewIndexChunk(indexBytes, blockNr) {
					// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
					index = dbutils.WrapHistoryIndex(indexBytes)
					indexKey, err := index.Key(k)
					if err != nil {
						return err
					}
					// Flush the old chunk
					if err := batch.Put(dbutils.StorageHistoryBucket, indexKey, index); err != nil {
						return err
					}
					// Start a new chunk
					index = dbutils.NewHistoryIndex()
				} else {
					index = dbutils.WrapHistoryIndex(indexBytes)
				}
				index = index.Append(blockNr, len(v) == 0)

				if err := batch.Put(dbutils.StorageHistoryBucket, currentChunkKey, index); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			prevOffset = offset
		}
		if err := SaveStageProgress(batch, StorageHistoryIndex, blockNum); err != nil {
			return err
		}
		batchSize := batch.BatchSize()
		start := time.Now()
		if _, err := batch.Commit(); err != nil {
			return err
		}
		runtime.ReadMemStats(&m)
		log.Info("Commited storage index batch", "in", time.Since(start), "up to block", blockNum, "batch size", common.StorageSize(batchSize),
			"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
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
