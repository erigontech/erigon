package changeset

import (
	"errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

const (
	DefaultIncarnation = uint64(1)
)

var (
	ErrNotFound      = errors.New("not found")
	errIncorrectData = errors.New("empty prepared data")
	ErrFindValue     = errors.New("find value error")
)

/* Hashed changesets (key is a hash of common.Address) */

func NewStorageChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  2*common.HashLength + common.IncarnationLength,
	}
}

func EncodeStorage(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
	return encodeStorage2(blockN, s, common.HashLength, f)
}

type StorageChangeSet struct{ c ethdb.CursorDupSort }

func (b StorageChangeSet) Walk(from, to uint64, f func(blockNum uint64, k, v []byte) error) error {
	return walk(b.c, from, to, common.HashLength, f)
}

func (b StorageChangeSet) Find(blockNumber uint64, k []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet2(b.c, blockNumber, common.HashLength, k[:common.HashLength], k[common.HashLength:])
}
func (b StorageChangeSet) FindWithIncarnation(blockNumber uint64, k []byte) ([]byte, error) {
	return findInStorageChangeSet2(b.c, blockNumber, common.HashLength, k)
}

func (b StorageChangeSet) FindWithoutIncarnation(blockNumber uint64, addrHashToFind []byte, keyHashToFind []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet2(b.c, blockNumber, common.HashLength, addrHashToFind, keyHashToFind)
}

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

func (b StorageChangeSetPlain) Walk(from, to uint64, f func(blockNum uint64, k, v []byte) error) error {
	return walk(b.c, from, to, common.AddressLength, f)
}

func (b StorageChangeSetPlain) Find(blockNumber uint64, k []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet2(b.c, blockNumber, common.AddressLength, k[:common.AddressLength], k[common.AddressLength:])
}

func (b StorageChangeSetPlain) FindWithIncarnation(blockNumber uint64, k []byte) ([]byte, error) {
	return findInStorageChangeSet2(b.c, blockNumber, common.AddressLength, k)
}

func (b StorageChangeSetPlain) FindWithoutIncarnation(blockNumber uint64, addressToFind []byte, keyToFind []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet2(b.c, blockNumber, common.AddressLength, addressToFind, keyToFind)
}

// RewindDataHashed generates rewind data for all buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindDataHashed(db ethdb.Getter, timestampSrc, timestampDst uint64) (map[string][]byte, map[string][]byte, error) {
	// Collect list of buckets and keys that need to be considered
	collector := newRewindDataCollector()

	if err := walkAndCollect(
		collector.AccountWalker,
		db, dbutils.AccountChangeSetBucket,
		timestampDst+1, timestampSrc,
	); err != nil {
		return nil, nil, err
	}

	if err := walkAndCollect(
		collector.StorageWalker,
		db, dbutils.StorageChangeSetBucket,
		timestampDst+1, timestampSrc,
	); err != nil {
		return nil, nil, err
	}

	return collector.AccountData, collector.StorageData, nil
}

// RewindData generates rewind data for all plain buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindData(db ethdb.Getter, timestampSrc, timestampDst uint64) (map[string][]byte, map[string][]byte, error) {
	// Collect list of buckets and keys that need to be considered
	collector := newRewindDataCollector()

	if err := walkAndCollect(
		collector.AccountWalker,
		db, dbutils.PlainAccountChangeSetBucket,
		timestampDst+1, timestampSrc,
	); err != nil {
		return nil, nil, err
	}

	if err := walkAndCollect(
		collector.StorageWalker,
		db, dbutils.PlainStorageChangeSetBucket,
		timestampDst+1, timestampSrc,
	); err != nil {
		return nil, nil, err
	}

	return collector.AccountData, collector.StorageData, nil
}

type rewindDataCollector struct {
	AccountData map[string][]byte
	StorageData map[string][]byte
}

func newRewindDataCollector() *rewindDataCollector {
	return &rewindDataCollector{make(map[string][]byte), make(map[string][]byte)}
}

func (c *rewindDataCollector) AccountWalker(k, v []byte) error {
	if _, ok := c.AccountData[string(k)]; !ok {
		c.AccountData[string(k)] = v
	}
	return nil
}

func (c *rewindDataCollector) StorageWalker(k, v []byte) error {
	if _, ok := c.StorageData[string(k)]; !ok {
		c.StorageData[string(k)] = v
	}
	return nil
}

func walkAndCollect(collectorFunc func([]byte, []byte) error, db ethdb.Getter, bucket string, timestampDst, timestampSrc uint64) error {
	fromDBFormat := FromDBFormat(Mapper[bucket].KeySize)
	return db.Walk(bucket, dbutils.EncodeBlockNumber(timestampDst), 0, func(dbKey, dbValue []byte) (bool, error) {
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
