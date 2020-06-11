package stagedsync

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
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"

	"github.com/pkg/errors"
	"github.com/ugorji/go/codec"
)

var cbor codec.CborHandle

func SpawnHashStateStage(s *StageState, stateDB ethdb.Database, datadir string, quit chan struct{}) error {
	hashProgress := s.BlockNumber

	syncHeadNumber, err := s.ExecutionAt(stateDB)
	if err != nil {
		return err
	}

	if hashProgress == syncHeadNumber {
		// we already did hash check for this block
		// we don't do the obvious `if hashProgress > syncHeadNumber` to support reorgs more naturally
		s.Done()
		return nil
	}

	if core.UsePlainStateExecution {
		log.Info("Promoting plain state", "from", hashProgress, "to", syncHeadNumber)
		err := promoteHashedState(stateDB, hashProgress, syncHeadNumber, datadir, quit)
		if err != nil {
			return err
		}
	}
	if err := verifyRootHash(stateDB, syncHeadNumber); err != nil {
		return err
	}
	return s.DoneAndUpdate(stateDB, syncHeadNumber)
}

func verifyRootHash(stateDB ethdb.Database, syncHeadNumber uint64) error {
	hash := rawdb.ReadCanonicalHash(stateDB, syncHeadNumber)
	syncHeadHeader := rawdb.ReadHeader(stateDB, hash, syncHeadNumber)
	log.Info("Validating root hash", "block", syncHeadNumber, "blockRoot", syncHeadHeader.Root.Hex())
	loader := trie.NewSubTrieLoader(syncHeadNumber)
	rl := trie.NewRetainList(0)
	subTries, err1 := loader.LoadFromFlatDB(stateDB, rl, nil /*HashCollector*/, [][]byte{nil}, []int{0}, false)
	if err1 != nil {
		return errors.Wrap(err1, "checking root hash failed")
	}
	if len(subTries.Hashes) != 1 {
		return fmt.Errorf("expected 1 hash, got %d", len(subTries.Hashes))
	}
	if subTries.Hashes[0] != syncHeadHeader.Root {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", subTries.Hashes[0], syncHeadHeader.Root)
	}
	return nil
}

func unwindHashStateStage(u *UnwindState, s *StageState, stateDB ethdb.Database, datadir string, quit chan struct{}) error {
	// Currently it does not require unwinding because it does not create any Intemediate Hash records
	// and recomputes the state root from scratch
	prom := NewPromoter(stateDB, quit)
	prom.TempDir = datadir
	if err := prom.Unwind(s.BlockNumber, u.UnwindPoint, dbutils.PlainAccountChangeSetBucket); err != nil {
		return err
	}
	if err := prom.Unwind(s.BlockNumber, u.UnwindPoint, dbutils.PlainStorageChangeSetBucket); err != nil {
		return err
	}
	if err := verifyRootHash(stateDB, u.UnwindPoint); err != nil {
		return err
	}
	if err := u.Done(stateDB); err != nil {
		return fmt.Errorf("unwind HashState: reset: %v", err)
	}
	return nil
}

func promoteHashedState(db ethdb.Database, from, to uint64, datadir string, quit chan struct{}) error {
	if from == 0 {
		return promoteHashedStateCleanly(db, datadir, quit)
	}
	return promoteHashedStateIncrementally(from, to, db, datadir, quit)
}

func promoteHashedStateCleanly(db ethdb.Database, datadir string, quit chan struct{}) error {
	if err := common.Stopped(quit); err != nil {
		return err
	}
	err := etl.Transform(
		db,
		dbutils.PlainStateBucket,
		dbutils.CurrentStateBucket,
		datadir,
		keyTransformExtractFunc(transformPlainStateKey),
		etl.IdentityLoadFunc,
		etl.TransformArgs{Quit: quit},
	)

	if err != nil {
		return err
	}

	return etl.Transform(
		db,
		dbutils.PlainContractCodeBucket,
		dbutils.ContractCodeBucket,
		datadir,
		keyTransformExtractFunc(transformContractCodeKey),
		etl.IdentityLoadFunc,
		etl.TransformArgs{Quit: quit},
	)
}

func keyTransformExtractFunc(transformKey func([]byte) ([]byte, error)) etl.ExtractFunc {
	return func(k, v []byte, next etl.ExtractNextFunc) error {
		newK, err := transformKey(k)
		if err != nil {
			return err
		}
		return next(k, newK, v)
	}
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

func keyTransformLoadFunc(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
	newK, err := transformPlainStateKey(k)
	if err != nil {
		return err
	}
	return next(newK, value)
}

func NewPromoter(db ethdb.Database, quitCh chan struct{}) *Promoter {
	return &Promoter{
		db:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          os.TempDir(),
	}
}

type Promoter struct {
	db               ethdb.Database
	ChangeSetBufSize uint64
	TempDir          string
	quitCh           chan struct{}
}

var promoterMapper = map[string]struct {
	WalkerAdapter func(v []byte) changeset.Walker
	KeySize       int
	Template      string
}{
	string(dbutils.PlainAccountChangeSetBucket): {
		WalkerAdapter: func(v []byte) changeset.Walker {
			return changeset.AccountChangeSetPlainBytes(v)
		},
		KeySize:  common.AddressLength,
		Template: "acc-prom-",
	},
	string(dbutils.PlainStorageChangeSetBucket): {
		WalkerAdapter: func(v []byte) changeset.Walker {
			return changeset.StorageChangeSetPlainBytes(v)
		},
		KeySize:  common.AddressLength + common.IncarnationLength + common.HashLength,
		Template: "st-prom-",
	},
}

func (p *Promoter) fillChangeSetBuffer(bucket []byte, blockNum, to uint64, changesets []byte, offsets []int) (bool, uint64, []int, error) {
	offset := 0
	offsets = offsets[:0]
	startKey := dbutils.EncodeTimestamp(blockNum)
	done := true
	if err := p.db.Walk(bucket, startKey, 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(p.quitCh); err != nil {
			return false, err
		}
		blockNum, _ = dbutils.DecodeTimestamp(k)
		if blockNum > to {
			return false, nil
		}
		if offset+len(v) > len(changesets) { // Adding the current changeset would overflow the buffer
			done = false
			return false, nil
		}
		copy(changesets[offset:], v)
		offset += len(v)
		offsets = append(offsets, offset)
		return true, nil
	}); err != nil {
		return true, blockNum, offsets, fmt.Errorf("walking over account changeset for block %d: %v", blockNum, err)
	}
	return done, blockNum, offsets, nil
}

// writeBufferMapToTempFile creates temp file in the datadir and writes bufferMap into it
// if sucessful, returns the name of the created file. File is closed
func (p *Promoter) writeBufferMapToTempFile(pattern string, bufferMap map[string]struct{}) (string, error) {
	var filename string
	keys := make([]string, len(bufferMap))
	i := 0
	for key := range bufferMap {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	var w *bufio.Writer
	if bufferFile, err := ioutil.TempFile(p.TempDir, pattern); err == nil {
		//nolint:errcheck
		defer bufferFile.Close()
		filename = bufferFile.Name()
		w = bufio.NewWriter(bufferFile)
	} else {
		return filename, fmt.Errorf("creating temp buf file %s: %v", pattern, err)
	}
	for _, key := range keys {
		if _, err := w.Write([]byte(key)); err != nil {
			return filename, err
		}
	}
	if err := w.Flush(); err != nil {
		return filename, fmt.Errorf("flushing file %s: %v", filename, err)
	}
	return filename, nil
}

func (p *Promoter) writeUnwindBufferMapToTempFile(pattern string, bufferMap map[string][]byte) (string, error) {
	var filename string
	keys := make([]string, len(bufferMap))
	i := 0
	for key := range bufferMap {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	var w *bufio.Writer
	if bufferFile, err := ioutil.TempFile(p.TempDir, pattern); err == nil {
		//nolint:errcheck
		defer bufferFile.Close()
		filename = bufferFile.Name()
		w = bufio.NewWriter(bufferFile)
	} else {
		return filename, fmt.Errorf("creating temp buf file %s: %v", pattern, err)
	}
	for _, key := range keys {
		if _, err := w.Write([]byte(key)); err != nil {
			return filename, err
		}
		value := bufferMap[key]
		if err := w.WriteByte(byte(len(value))); err != nil {
			return filename, err
		}
		if _, err := w.Write(value); err != nil {
			return filename, err
		}
	}
	if err := w.Flush(); err != nil {
		return filename, fmt.Errorf("flushing file %s: %v", filename, err)
	}
	return filename, nil
}

func (p *Promoter) mergeFilesAndCollect(bufferFileNames []string, keyLength int, collector *etl.Collector) error {
	h := &etl.Heap{}
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
			heap.Push(h, etl.HeapElem{keyBuf, i, nil})
		} else {
			return fmt.Errorf("init reading from account buffer file: %d %x %v", n, keyBuf[:n], err)
		}
	}
	// By now, the heap has one element for each buffer file
	var prevKey []byte
	for h.Len() > 0 {
		if err := common.Stopped(p.quitCh); err != nil {
			return err
		}
		element := (heap.Pop(h)).(etl.HeapElem)
		if !bytes.Equal(element.Key, prevKey) {
			// Ignore all the repeating keys
			prevKey = common.CopyBytes(element.Key)
			if v, err := p.db.Get(dbutils.PlainStateBucket, element.Key); err == nil || err == ethdb.ErrKeyNotFound {
				if err1 := collector.Collect(element.Key, v); err1 != nil {
					return err1
				}
			} else {
				return err
			}
		}
		reader := readers[element.TimeIdx]
		// Try to read the next key (reuse the element)
		if n, err := io.ReadFull(reader, element.Key); err == nil && n == keyLength {
			heap.Push(h, element)
		} else if err != io.EOF {
			// If it is EOF, we simply do not return anything into the heap
			return fmt.Errorf("next reading from account buffer file: %d %x %v", n, element.Key[:n], err)
		}
	}
	return nil
}

func (p *Promoter) mergeUnwindFilesAndCollect(bufferFileNames []string, keyLength int, collector *etl.Collector) error {
	h := &etl.Heap{}
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
		if n, err := io.ReadFull(readers[i], keyBuf); err != nil || n != keyLength {
			return fmt.Errorf("init reading from account buffer file: %d %x %v", n, keyBuf[:n], err)
		}
		var l [1]byte
		if n, err := io.ReadFull(readers[i], l[:]); err != nil || n != 1 {
			return fmt.Errorf("init reading from account buffer file: %d %v", n, err)
		}
		var valBuf []byte
		valLength := int(l[0])
		if valLength > 0 {
			valBuf = make([]byte, valLength)
			if n, err := io.ReadFull(readers[i], valBuf); err != nil || n != valLength {
				return fmt.Errorf("init reading from account buffer file: %d %v", n, err)
			}
		}
		heap.Push(h, etl.HeapElem{keyBuf, i, valBuf})
	}
	// By now, the heap has one element for each buffer file
	var prevKey []byte
	for h.Len() > 0 {
		if err := common.Stopped(p.quitCh); err != nil {
			return err
		}
		element := (heap.Pop(h)).(etl.HeapElem)
		if !bytes.Equal(element.Key, prevKey) {
			// Ignore all the repeating keys, and take the earlist
			prevKey = common.CopyBytes(element.Key)
			if err := collector.Collect(element.Key, element.Value); err != nil {
				return err
			}
		}
		reader := readers[element.TimeIdx]
		// Try to read the next key (reuse the element)
		if n, err := io.ReadFull(reader, element.Key); err == nil && n == keyLength {
			var l [1]byte
			if n1, err1 := io.ReadFull(reader, l[:]); err1 != nil || n1 != 1 {
				return fmt.Errorf("reading from account buffer file: %d %v", n1, err1)
			}
			var valBuf []byte
			valLength := int(l[0])
			if valLength > 0 {
				valBuf = make([]byte, valLength)
				if n1, err1 := io.ReadFull(reader, valBuf); err1 != nil || n1 != valLength {
					return fmt.Errorf("reading from account buffer file: %d %v", n1, err1)
				}
			}
			element.Value = valBuf
			heap.Push(h, element)
		} else if err != io.EOF {
			// If it is EOF, we simply do not return anything into the heap
			return fmt.Errorf("next reading from account buffer file: %d %x %v", n, element.Key[:n], err)
		}
	}
	return nil
}

func (p *Promoter) Promote(from, to uint64, changeSetBucket []byte) error {
	v, ok := promoterMapper[string(changeSetBucket)]
	if !ok {
		return fmt.Errorf("unknown bucket type: %s", changeSetBucket)
	}
	log.Info("Incremental promotion started", "from", from, "to", to, "csbucket", string(changeSetBucket))
	var m runtime.MemStats
	var bufferFileNames []string
	changesets := make([]byte, p.ChangeSetBufSize) // 256 Mb buffer by default
	var offsets []int
	var done = false
	blockNum := from + 1
	for !done {
		if newDone, newBlockNum, newOffsets, err := p.fillChangeSetBuffer(changeSetBucket, blockNum, to, changesets, offsets); err == nil {
			done = newDone
			blockNum = newBlockNum
			offsets = newOffsets
		} else {
			return err
		}
		if len(offsets) == 0 {
			break
		}

		bufferMap := make(map[string]struct{})
		prevOffset := 0
		for _, offset := range offsets {
			if err := v.WalkerAdapter(changesets[prevOffset:offset]).Walk(func(k, v []byte) error {
				bufferMap[string(k)] = struct{}{}
				return nil
			}); err != nil {
				return err
			}
			prevOffset = offset
		}

		if filename, err := p.writeBufferMapToTempFile(v.Template, bufferMap); err == nil {
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
	if len(offsets) > 0 {
		collector := etl.NewCollector(p.TempDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		if err := p.mergeFilesAndCollect(bufferFileNames, v.KeySize, collector); err != nil {
			return err
		}
		if err := collector.Load(p.db, dbutils.CurrentStateBucket, keyTransformLoadFunc, etl.TransformArgs{Quit: p.quitCh}); err != nil {
			return err
		}
	}
	return nil
}

func (p *Promoter) Unwind(from, to uint64, changeSetBucket []byte) error {
	v, ok := promoterMapper[string(changeSetBucket)]
	if !ok {
		return fmt.Errorf("unknown bucket type: %s", changeSetBucket)
	}
	log.Info("Unwinding started", "from", from, "to", to, "csbucket", string(changeSetBucket))
	var m runtime.MemStats
	var bufferFileNames []string
	changesets := make([]byte, p.ChangeSetBufSize) // 256 Mb buffer by default
	var offsets []int
	var done = false
	blockNum := to + 1
	for !done {
		if newDone, newBlockNum, newOffsets, err := p.fillChangeSetBuffer(changeSetBucket, blockNum, from, changesets, offsets); err == nil {
			done = newDone
			blockNum = newBlockNum
			offsets = newOffsets
		} else {
			return err
		}
		if len(offsets) == 0 {
			break
		}

		bufferMap := make(map[string][]byte)
		prevOffset := 0
		for _, offset := range offsets {
			if err := v.WalkerAdapter(changesets[prevOffset:offset]).Walk(func(k, v []byte) error {
				ks := string(k)
				if _, ok := bufferMap[ks]; !ok {
					// Do not replace the existing values, so we end up with the earlier possible values
					bufferMap[ks] = v
				}
				return nil
			}); err != nil {
				return err
			}
			prevOffset = offset
		}

		if filename, err := p.writeUnwindBufferMapToTempFile(v.Template, bufferMap); err == nil {
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
	if len(offsets) > 0 {
		collector := etl.NewCollector(p.TempDir, etl.NewAppendBuffer(etl.BufferOptimalSize))
		if err := p.mergeUnwindFilesAndCollect(bufferFileNames, v.KeySize, collector); err != nil {
			return err
		}
		if err := collector.Load(p.db, dbutils.CurrentStateBucket, keyTransformLoadFunc, etl.TransformArgs{Quit: p.quitCh}); err != nil {
			return err
		}
	}
	return nil
}

func promoteHashedStateIncrementally(from, to uint64, db ethdb.Database, datadir string, quit chan struct{}) error {
	prom := NewPromoter(db, quit)
	prom.TempDir = datadir
	if err := prom.Promote(from, to, dbutils.PlainAccountChangeSetBucket); err != nil {
		return err
	}
	if err := prom.Promote(from, to, dbutils.PlainStorageChangeSetBucket); err != nil {
		return err
	}
	return nil
}
