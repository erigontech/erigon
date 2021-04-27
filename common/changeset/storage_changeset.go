package changeset

import (
	"errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

const (
	DefaultIncarnation = uint64(1)
)

var (
	ErrNotFound  = errors.New("not found")
	ErrFindValue = errors.New("find value error")
)

/* Plain changesets (key is a common.Address) */

func NewStorageChangeSetPlain() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  common.AddressLength + common.HashLength + common.IncarnationLength,
	}
}

func EncodeStoragePlain(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
	return encodeStorage2(blockN, s, common.AddressLength, f)
}

type StorageChangeSetPlain struct{ c ethdb.CursorDupSort }

func (b StorageChangeSetPlain) Find(blockNumber uint64, k []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet2(b.c, blockNumber, common.AddressLength, k[:common.AddressLength], k[common.AddressLength:])
}

func (b StorageChangeSetPlain) FindWithIncarnation(blockNumber uint64, k []byte) ([]byte, error) {
	return findInStorageChangeSet2(b.c, blockNumber, common.AddressLength, k)
}

func (b StorageChangeSetPlain) FindWithoutIncarnation(blockNumber uint64, addressToFind []byte, keyToFind []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet2(b.c, blockNumber, common.AddressLength, addressToFind, keyToFind)
}

// RewindDataPlain generates rewind data for all plain buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindData(db ethdb.Tx, timestampSrc, timestampDst uint64, tmpdir string, quit <-chan struct{}) (*etl.Collector, error) {
	// Collect list of buckets and keys that need to be considered

	changes := etl.NewCollector(tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))

	if err := walkAndCollect(
		changes.Collect,
		db, dbutils.PlainAccountChangeSetBucket,
		timestampDst+1, timestampSrc,
		quit,
	); err != nil {
		return nil, err
	}

	if err := walkAndCollect(
		changes.Collect,
		db, dbutils.PlainStorageChangeSetBucket,
		timestampDst+1, timestampSrc,
		quit,
	); err != nil {
		return nil, err
	}

	return changes, nil
}

func walkAndCollect(collectorFunc func([]byte, []byte) error, db ethdb.Tx, bucket string, timestampDst, timestampSrc uint64, quit <-chan struct{}) error {
	fromDBFormat := FromDBFormat(Mapper[bucket].KeySize)
	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	return ethdb.Walk(c, dbutils.EncodeBlockNumber(timestampDst), 0, func(dbKey, dbValue []byte) (bool, error) {
		if err := common.Stopped(quit); err != nil {
			return false, err
		}
		timestamp, k, v := fromDBFormat(dbKey, dbValue)
		if timestamp > timestampSrc {
			return false, nil
		}
		if innerErr := collectorFunc(common.CopyBytes(k), common.CopyBytes(v)); innerErr != nil {
			return false, innerErr
		}
		return true, nil
	})
}
