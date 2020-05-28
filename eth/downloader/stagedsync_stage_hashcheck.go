package downloader

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"

	"github.com/pkg/errors"
	"github.com/ugorji/go/codec"
)

var cbor codec.CborHandle

func spawnCheckFinalHashStage(stateDB ethdb.Database, syncHeadNumber uint64, datadir string) error {
	hashProgress, err := GetStageProgress(stateDB, HashCheck)
	if err != nil {
		return err
	}

	if hashProgress == syncHeadNumber {
		// we already did hash check for this block
		// we don't do the obvious `if hashProgress > syncHeadNumber` to support reorgs more naturally
		return nil
	}


	if core.UsePlainStateExecution {
		err = promoteHashedState(stateDB, hashProgress, datadir)
		if err != nil {
			return err
		}
	}

	//REMOVE THE FOLLOWING LINE WHEN PLAIN => HASHED TRANSFORMATION IS READY
	if hashProgress == 0 {
		return nil
	}

	hash := rawdb.ReadCanonicalHash(stateDB, syncHeadNumber)
	syncHeadBlock := rawdb.ReadBlock(stateDB, hash, syncHeadNumber)

	blockNr := syncHeadBlock.Header().Number.Uint64()

	log.Info("Validating root hash", "block", blockNr, "blockRoot", syncHeadBlock.Root().Hex())
	loader := trie.NewSubTrieLoader(blockNr)
	rl := trie.NewRetainList(0)
	subTries, err1 := loader.LoadFromFlatDB(stateDB, rl, [][]byte{nil}, []int{0}, false)
	if err1 != nil {
		return errors.Wrap(err1, "checking root hash failed")
	}
	if len(subTries.Hashes) != 1 {
		return fmt.Errorf("expected 1 hash, got %d", len(subTries.Hashes))
	}
	if subTries.Hashes[0] != syncHeadBlock.Root() {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", subTries.Hashes[0], syncHeadBlock.Root())
	}

	return SaveStageProgress(stateDB, HashCheck, blockNr)
}

func unwindHashCheckStage(unwindPoint uint64, stateDB ethdb.Database) error {
	// Currently it does not require unwinding because it does not create any Intemediate Hash records
	// and recomputes the state root from scratch
	lastProcessedBlockNumber, err := GetStageProgress(stateDB, HashCheck)
	if err != nil {
		return fmt.Errorf("unwind HashCheck: get stage progress: %v", err)
	}
	if unwindPoint >= lastProcessedBlockNumber {
		err = SaveStageUnwind(stateDB, HashCheck, 0)
		if err != nil {
			return fmt.Errorf("unwind HashCheck: reset: %v", err)
		}
		return nil
	}
	mutation := stateDB.NewBatch()
	err = SaveStageUnwind(mutation, HashCheck, 0)
	if err != nil {
		return fmt.Errorf("unwind HashCheck: reset: %v", err)
	}
	_, err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("unwind HashCheck: failed to write db commit: %v", err)
	}
	return nil
}

func promoteHashedState(db ethdb.Database, progress uint64, datadir string) error {
	if progress == 0 {
		return promoteHashedStateCleanly(db, datadir)
	}
	return errors.New("incremental state promotion not implemented")
}

func promoteHashedStateCleanly(db ethdb.Database, datadir string) error {
	err := copyBucket(datadir, db, dbutils.PlainStateBucket, dbutils.CurrentStateBucket, transformPlainStateKey)
	if err != nil {
		return err
	}
	return copyBucket(datadir, db, dbutils.PlainContractCodeBucket, dbutils.ContractCodeBucket, transformContractCodeKey)
}

func copyBucket(
	datadir string,
	db ethdb.Database,
	sourceBucket,
	destBucket []byte,
	transformKeyFunc func([]byte) ([]byte, error)) error {

	var m runtime.MemStats

	buffer := newSortableBuffer()
	files := []string{}

	defer func() {
		deleteFiles(files)
	}()

	err := db.Walk(sourceBucket, nil, 0, func(k, v []byte) (bool, error) {
		newK, err := transformKeyFunc(k)
		if err != nil {
			return false, err
		}
		buffer.Put(newK, v)

		bufferSize := buffer.Size()
		if bufferSize >= buffer.OptimalSize {
			sort.Sort(buffer)
			file, err := buffer.FlushToDisk(datadir)
			if err != nil {
				return false, err
			}
			if len(file) > 0 {
				files = append(files, file)
				runtime.ReadMemStats(&m)
				log.Info("Plain -> Hashed / created a buffer file",
					"bucket", string(sourceBucket),
					"name", file,
					"size", bufferSize,
					"plainKey", fmt.Sprintf("%x...", k[:4]),
					"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
			}

		}
		return true, nil
	})
	if err != nil {
		return err
	}

	sort.Sort(buffer)
	var file string
	bufferSize := buffer.Size()
	file, err = buffer.FlushToDisk(datadir)
	if err != nil {
		return err
	}
	if len(file) > 0 {
		files = append(files, file)

		runtime.ReadMemStats(&m)
		log.Info("Plain -> Hashed / created a buffer file (final)",
			"bucket", string(sourceBucket),
			"name", file,
			"size", bufferSize,
			"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
	}
	return mergeTempFilesIntoBucket(db, files, destBucket)
}

func transformPlainStateKey(key []byte) ([]byte, error) {
	switch len(key) {
	case common.AddressLength:
		// account
		hash, err := common.HashData(key)
		return hash[:], err
	case common.AddressLength + common.IncarnationLength + common.HashLength:
		// storage
		address, incarnation, key := dbutils.PlainParseCompositeStorageKey(key)
		addrHash, err := common.HashData(address[:])
		if err != nil {
			return nil, err
		}
		secKey, err := common.HashData(key[:])
		if err != nil {
			return nil, err
		}
		compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, secKey)
		return compositeKey, nil
	default:
		// no other keys are supported
		return nil, fmt.Errorf("could not convert key from plain to hashed, unexpected len: %d", len(key))
	}
}

func transformContractCodeKey(key []byte) ([]byte, error) {
	if len(key) != common.AddressLength+common.IncarnationLength {
		return nil, fmt.Errorf("could not convert code key from plain to hashed, unexpected len: %d", len(key))
	}
	address, incarnation := dbutils.PlainParseStoragePrefix(key)

	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	compositeKey := dbutils.GenerateStoragePrefix(addrHash[:], incarnation)
	return compositeKey, nil
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

func (b *sortableBuffer) FlushToDisk(datadir string) (string, error) {
	if len(b.entries) == 0 {
		return "", nil
	}
	bufferFile, err := ioutil.TempFile(datadir, "tg-sync-sortable-buf")
	if err != nil {
		return "", err
	}
	defer bufferFile.Close() //nolint:errcheck

	filename := bufferFile.Name()
	w := bufio.NewWriter(bufferFile)
	defer w.Flush() //nolint:errcheck
	b.encoder.Reset(w)

	for i := range b.entries {
		err = writeToDisk(b.encoder, b.entries[i].key, b.entries[i].value)
		if err != nil {
			return "", fmt.Errorf("error writing entries to disk: %v", err)
		}
	}

	b.entries = b.entries[:0] // keep the capacity
	b.size = 0
	return filename, nil
}

func newSortableBuffer() *sortableBuffer {
	return &sortableBuffer{
		entries:     make([]sortableBufferEntry, 0),
		size:        0,
		OptimalSize: 256 * 1024 * 1024, /* 256 mb */
		encoder:     codec.NewEncoder(nil, &cbor),
	}
}

func writeToDisk(encoder *codec.Encoder, key []byte, value []byte) error {
	toWrite := [][]byte{key, value}
	return encoder.Encode(toWrite)
}

func readElementFromDisk(decoder *codec.Decoder) ([]byte, []byte, error) {
	result := make([][]byte, 2)
	err := decoder.Decode(&result)
	return result[0], result[1], err
}

func mergeTempFilesIntoBucket(db ethdb.Database, files []string, bucket []byte) error {
	decoder := codec.NewDecoder(nil, &cbor)
	var m runtime.MemStats
	h := &common.Heap{}
	heap.Init(h)
	readers := make([]io.Reader, len(files))
	for i, filename := range files {
		if f, err := os.Open(filename); err == nil {
			readers[i] = bufio.NewReader(f)
			defer f.Close() //nolint:errcheck
		} else {
			return err
		}
		decoder.Reset(readers[i])
		if key, value, err := readElementFromDisk(decoder); err == nil {
			he := common.HeapElem{key, i, value}
			heap.Push(h, he)
		} else /* we must have at least one entry per file */ {
			return fmt.Errorf("error reading first readers: n=%d current=%d filename=%s err=%v",
				len(files), i, filename, err)
		}
	}
	batch := db.NewBatch()
	for h.Len() > 0 {
		element := (heap.Pop(h)).(common.HeapElem)
		reader := readers[element.TimeIdx]
		if err := batch.Put(bucket, element.Key, element.Value); err != nil {
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
				"hashedKey", fmt.Sprintf("%x...", element.Key[:4]),
				"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
		}
		var err error
		decoder.Reset(reader)
		if element.Key, element.Value, err = readElementFromDisk(decoder); err == nil {
			heap.Push(h, element)
		} else if err != io.EOF {
			return fmt.Errorf("error while reading next element from disk: %v", err)
		}
	}
	_, err := batch.Commit()
	return err
}

func deleteFiles(files []string) {
	for _, filename := range files {
		err := os.Remove(filename)
		if err != nil {
			log.Warn("promoting hashed state, error while removing temp file", "file", filename, "err", err)
		}
	}
}
