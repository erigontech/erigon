package downloader

import (
	"fmt"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"
	"github.com/pkg/errors"
)

func spawnCheckFinalHashStage(stateDB ethdb.Database, syncHeadNumber uint64) error {
	hashProgress, err := GetStageProgress(stateDB, HashCheck)
	if err != nil {
		return err
	}

	if hashProgress == syncHeadNumber {
		// we already did hash check for this block
		// we don't do the obvious `if hashProgress > syncHeadNumber` to support reorgs more naturally
		return nil
	}

	hashedStatePromotion := stateDB.NewBatch()

	if core.UsePlainStateExecution {
		err = promoteHashedState(hashedStatePromotion, hashProgress)
		if err != nil {
			return err
		}
	}

	_, err = hashedStatePromotion.Commit()
	if err != nil {
		return err
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

func promoteHashedState(db ethdb.Database, progress uint64) error {
	if progress == 0 {
		return promoteHashedStateCleanly(db)
	}
	return errors.New("incremental state promotion not implemented")
}

func promoteHashedStateCleanly(db ethdb.Database) error {
	err := copyBucket(db, dbutils.PlainStateBucket, dbutils.CurrentStateBucket, transformPlainStateKey)
	if err != nil {
		return err
	}
	return copyBucket(db, dbutils.PlainContractCodeBucket, dbutils.ContractCodeBucket, transformContractCodeKey)
}

func copyBucket(
	db ethdb.Database,
	sourceBucket,
	destBucket []byte,
	transformKeyFunc func([]byte) ([]byte, error)) error {

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
		if buffer.Size() >= buffer.OptimalSize {
			sort.Sort(buffer)
			file, err := buffer.FlushToDisk()
			if err != nil {
				return false, err
			}
			files = append(files, file)
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	sort.Sort(buffer)
	var file string
	file, err = buffer.FlushToDisk()
	if err != nil {
		return err
	}
	files = append(files, file)
	return mergeTempFilesIntoBucket(files, destBucket)
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
		secretKey, err := common.HashData(key[:])
		if err != nil {
			return nil, err
		}
		compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, secretKey)
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

type sortableBuffer struct {
	OptimalSize int
}

func (b *sortableBuffer) Put(k, v []byte) {
	panic("implement me")
}

func (b *sortableBuffer) Size() int {
	panic("implement me")
}

func (b *sortableBuffer) Len() int {
	panic("implement me")
}

func (b *sortableBuffer) Less(i, j int) bool {
	panic("implement me")
}

func (b *sortableBuffer) Swap(i, j int) {
	panic("implement me")
}

func (b *sortableBuffer) FlushToDisk() (string, error) {
	panic("implement me")
}

func newSortableBuffer() *sortableBuffer {
	return &sortableBuffer{
		OptimalSize: 1024 * 1024,
	}
}

func mergeTempFilesIntoBucket(files []string, bucket []byte) error {
	panic("implement me")
}

func deleteFiles(files []string) {
	panic("implement me")
}
