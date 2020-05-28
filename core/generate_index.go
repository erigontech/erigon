package core

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func NewIndexGenerator(db ethdb.Database) *IndexGenerator {

	return &IndexGenerator{
		db:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          os.TempDir(),
	}
}

type IndexGenerator struct {
	db               ethdb.Database
	ChangeSetBufSize uint64
	TempDir          string
}

var mapper = map[string]struct {
	IndexBucket   []byte
	WalkerAdapter func(v []byte) ChangesetWalker
	KeySize       int
	Template      string
	New           func() *changeset.ChangeSet
	Encode        func(*changeset.ChangeSet) ([]byte, error)
}{
	string(dbutils.AccountChangeSetBucket): {
		IndexBucket: dbutils.AccountsHistoryBucket,
		WalkerAdapter: func(v []byte) ChangesetWalker {
			return changeset.AccountChangeSetBytes(v)
		},
		KeySize: common.HashLength,
		Template: "acc-ind-",
		New:     changeset.NewAccountChangeSet,
		Encode:  changeset.EncodeAccounts,
	},
	string(dbutils.StorageChangeSetBucket): {
		IndexBucket: dbutils.StorageHistoryBucket,
		WalkerAdapter: func(v []byte) ChangesetWalker {
			return changeset.StorageChangeSetBytes(v)
		},
		KeySize: common.HashLength*2 + common.IncarnationLength,
		Template: "st-ind-",
		New:     changeset.NewStorageChangeSet,
		Encode:  changeset.EncodeStorage,
	},
	string(dbutils.PlainAccountChangeSetBucket): {
		IndexBucket: dbutils.AccountsHistoryBucket,
		WalkerAdapter: func(v []byte) ChangesetWalker {
			return changeset.AccountChangeSetPlainBytes(v)
		},
		KeySize: common.AddressLength,
		Template: "acc-ind-",
		New:     changeset.NewAccountChangeSetPlain,
		Encode:  changeset.EncodeAccountsPlain,
	},
	string(dbutils.PlainStorageChangeSetBucket): {
		IndexBucket: dbutils.StorageHistoryBucket,
		WalkerAdapter: func(v []byte) ChangesetWalker {
			return changeset.StorageChangeSetPlainBytes(v)
		},
		KeySize: common.AddressLength + common.IncarnationLength + common.HashLength,
		Template: "st-ind-",
		New:     changeset.NewStorageChangeSetPlain,
		Encode:  changeset.EncodeStoragePlain,
	},
}

func (ig *IndexGenerator) GenerateIndex(blockNum uint64, changeSetBucket []byte) error {
	v, ok := mapper[string(changeSetBucket)]
	if !ok {
		return errors.New("unknown bucket type")
	}
	log.Info("Index generation started", "from", blockNum, "csbucket", string(changeSetBucket))

	var m runtime.MemStats
	var mtx sync.Mutex
	var bufferFileNames []struct{
		name string
		id uint64
	}
	defer func() {
		mtx.Lock()
		for _,filename:=range bufferFileNames {
			//nolint:errcheck
			os.Remove(filename.name)
		}
		mtx.Unlock()
	}()

	// In the first loop, we read all the changesets, create partial history indices, sort them, and
	// write each batch into a file
	var fill, walk, wri int64
	var fillp, walkp, wrip *int64
	fillp = &fill
	walkp = &walk
	wrip = &wri

	wg,_:=errgroup.WithContext(context.Background())
	f:= func(start, end uint64) func() error {
		blockNum := start
		return func() error {
			startTime:=time.Now()
			fmt.Println("start thread ", start, end)
			defer fmt.Println("end thread ", start, end, time.Since(startTime))
			changesets := make([]byte, ig.ChangeSetBufSize) // 256 Mb buffer by default
			var offsets []int
			var blockNums []uint64
			var done = false

			oldBlocknum := start
			for !done {
				cycle := time.Now()
				if newDone, newBlockNum, newOffsets, newBlockNums, err := ig.fillChangeSetBuffer(changeSetBucket, blockNum, end, changesets, offsets, blockNums); err == nil {
					done = newDone
					oldBlocknum = blockNum
					blockNum = newBlockNum
					offsets = newOffsets
					blockNums = newBlockNums
				} else {
					return err
				}
				if len(offsets) == 0 {
					break
				}
				//fmt.Println("fill", time.Since(cycle))
				atomic.AddInt64(fillp,int64(time.Since(cycle)))
				t2 := time.Now()
				bufferMap := make(map[string][]uint64)
				prevOffset := 0
				for i, offset := range offsets {
					blockNr := blockNums[i]
					if err := v.WalkerAdapter(changesets[prevOffset:offset]).Walk(func(k, v []byte) error {
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
				//fmt.Println("walk", time.Since(t2))
				atomic.AddInt64(walkp, int64(time.Since(t2)))
				t3 := time.Now()
				if filename, err := ig.writeBufferMapToTempFile(ig.TempDir, v.Template, bufferMap); err == nil {
					mtx.Lock()
					bufferFileNames = append(bufferFileNames, struct {
						name string
						id   uint64
					}{name: filename, id:oldBlocknum })
					runtime.ReadMemStats(&m)
					log.Info("Created a buffer file", "name", filename, "from block", oldBlocknum, "up to block", blockNum,
						"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
					mtx.Unlock()
				} else {
					return err
				}
				//fmt.Println("save map", time.Since(t3))
				atomic.AddInt64(wrip, int64(time.Since(t3)))
				//fmt.Println("cycle", time.Since(cycle))
			}
			return nil
		}
	}

	var lastBlock uint64
	lastKey,err:=ig.db.(*ethdb.BoltDatabase).GetLastKey(changeSetBucket)
	if err==nil {
		lastBlock,_ = dbutils.DecodeTimestamp(lastKey)
	}
	numOfWorkers:=runtime.NumGoroutine()
	var startBlock, endBlock uint64
	stepSize:=lastBlock/uint64(numOfWorkers)
	for i:=0;i<numOfWorkers; i++ {
		if i+1==numOfWorkers {
			endBlock = math.MaxUint64
		} else {
			endBlock=startBlock+stepSize
		}
		log.Info("start", "worker", i, "from", startBlock, "to", endBlock)
		wg.Go(f(startBlock, endBlock))
		startBlock+=stepSize
	}

	fnc:=time.Now()
	err=wg.Wait()
	if err!=nil {
		return err
	}
	fmt.Println("caltime", time.Since(fnc))
	if len(bufferFileNames)>0 {
		t := time.Now()
		if err := ig.mergeFilesIntoBucket(bufferFileNames, v.IndexBucket, v.KeySize); err != nil {
			return err
		}
		fmt.Println("merge", time.Since(t))
	}

	fmt.Println("fill", fill)
	fmt.Println("walk", walk)
	fmt.Println("wri", wri)

	return nil
}

func (ig *IndexGenerator) Truncate(timestampTo uint64, changeSetBucket []byte) error {
	vv, ok := mapper[string(changeSetBucket)]
	if !ok {
		return errors.New("unknown bucket type")
	}

	currentKey := dbutils.EncodeTimestamp(timestampTo)
	keys := make(map[string]struct{})
	err := ig.db.Walk(changeSetBucket, currentKey, 0, func(k, v []byte) (b bool, e error) {
		currentKey = common.CopyBytes(k)
		err := vv.WalkerAdapter(v).Walk(func(kk []byte, _ []byte) error {
			keys[string(kk)] = struct{}{}
			return nil
		})
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	accountHistoryEffects := make(map[string][]byte)
	var startKey = make([]byte, common.HashLength+8)

	for key := range keys {
		key := common.CopyBytes([]byte(key))
		copy(startKey, key)

		binary.BigEndian.PutUint64(startKey[common.HashLength:], timestampTo)
		if err := ig.db.Walk(vv.IndexBucket, startKey, 8*common.HashLength, func(k, v []byte) (bool, error) {
			timestamp := binary.BigEndian.Uint64(k[common.HashLength:]) // the last timestamp in the chunk
			kStr := string(common.CopyBytes(k))

			if timestamp > timestampTo {
				accountHistoryEffects[kStr] = nil
				// truncate the chunk
				index := dbutils.WrapHistoryIndex(v)
				index = index.TruncateGreater(timestampTo)
				if len(index) > 8 { // If the chunk is empty after truncation, it gets simply deleted
					// Truncated chunk becomes "the last chunk" with the timestamp 0xffff....ffff
					lastK, err := index.Key(key)
					if err != nil {
						return false, err
					}
					accountHistoryEffects[string(lastK)] = common.CopyBytes(index)
				}
			}
			return true, nil
		}); err != nil {
			return err
		}
	}

	for key, value := range accountHistoryEffects {
		if value == nil {
			//fmt.Println("drop", common.Bytes2Hex([]byte(key)), binary.BigEndian.Uint64([]byte(key)[common.HashLength:]))
			if err := ig.db.Delete(vv.IndexBucket, []byte(key)); err != nil {
				return err
			}
		} else {
			//fmt.Println("write", common.Bytes2Hex([]byte(key)), binary.BigEndian.Uint64([]byte(key)[common.HashLength:]))
			if err := ig.db.Put(vv.IndexBucket, []byte(key), value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ig *IndexGenerator) DropIndex(bucket []byte) error {
	//todo add truncate to all db
	if bolt, ok := ig.db.(*ethdb.BoltDatabase); ok {
		log.Warn("Remove bucket", "bucket", string(bucket))
		err := bolt.DeleteBucket(bucket)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("imposible to drop")
}

type ChangesetWalker interface {
	Walk(func([]byte, []byte) error) error
}

func (ig *IndexGenerator) fillChangeSetBuffer(bucket []byte, startBlock uint64,endBlock uint64, changesets []byte, offsets []int, blockNums []uint64) (bool, uint64, []int, []uint64, error) {
	offset := 0
	offsets = offsets[:0]
	blockNums = blockNums[:0]
	startKey := dbutils.EncodeTimestamp(startBlock)
	done := true
	if err := ig.db.Walk(bucket, startKey, 0, func(k, v []byte) (bool, error) {
		startBlock, _ = dbutils.DecodeTimestamp(k)
		if startBlock >=endBlock {
			fmt.Println("endBlock exit", startBlock, endBlock )
			return false, nil
		}

		if offset+len(v) > len(changesets) { // Adding the current changeset would overflow the buffer
			done = false
			return false, nil
		}
		copy(changesets[offset:], v)
		offset += len(v)
		offsets = append(offsets, offset)
		blockNums = append(blockNums, startBlock)
		return true, nil
	}); err != nil {
		return true, startBlock, offsets, blockNums, fmt.Errorf("walking over account changeset for block %d: %v", startBlock, err)
	}
	return done, startBlock, offsets, blockNums, nil
}

const emptyValBit uint64 = 0x8000000000000000

// writeBufferMapToTempFile creates temp file in the datadir and writes bufferMap into it
// if sucessful, returns the name of the created file. File is closed
func (ig *IndexGenerator) writeBufferMapToTempFile(datadir string, pattern string, bufferMap map[string][]uint64) (string, error) {
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

func (ig *IndexGenerator) mergeFilesIntoBucket(bufferFileNames []struct{
	name string
	id uint64
}, bucket []byte, keyLength int) error {
	var m runtime.MemStats
	sort.Slice(bufferFileNames, func(i, j int) bool {
		return bufferFileNames[i].id<bufferFileNames[j].id
	})
	h := &common.Heap{}
	heap.Init(h)
	readers := make([]io.Reader, len(bufferFileNames))
	for i, fileName := range bufferFileNames {
		if f, err := os.Open(fileName.name); err == nil {
			readers[i] = bufio.NewReader(f)
			//nolint:errcheck
			defer f.Close()
		} else {
			return err
		}
		// Read first key
		keyBuf := make([]byte, keyLength)
		if n, err := io.ReadFull(readers[i], keyBuf); err == nil && n == keyLength {
			heap.Push(h, common.HeapElem{keyBuf, i, nil})
		} else {
			return fmt.Errorf("init reading from account buffer file: %d %x %v", n, keyBuf[:n], err)
		}
	}
	// By now, the heap has one element for each buffer file
	batch := ig.db.NewBatch()
	var nbytes [8]byte
	for h.Len() > 0 {
		element := (heap.Pop(h)).(common.HeapElem)
		reader := readers[element.TimeIdx]
		k := element.Key
		// Read number of items for this key
		var count int
		if n, err := io.ReadFull(reader, nbytes[:]); err != nil || n != 8 {
			return fmt.Errorf("reading from account buffer file: %d %v", n, err)
		}

		count = int(binary.BigEndian.Uint64(nbytes[:]))
		for i := 0; i < count; i++ {
			var b uint64
			if n, err := io.ReadFull(reader, nbytes[:]); err != nil || n != 8 {
				return fmt.Errorf("reading from account buffer file: %d %v", n, err)
			}
			b = binary.BigEndian.Uint64(nbytes[:])
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
		if n, err := io.ReadFull(reader, element.Key); err == nil && n == keyLength {
			heap.Push(h, element)
		} else if err != io.EOF {
			// If it is EOF, we simply do not return anything into the heap
			return fmt.Errorf("next reading from account buffer file: %d %x %v", n, element.Key[:n], err)
		}
	}
	if _, err := batch.Commit(); err != nil {
		return err
	}
	return nil
}
