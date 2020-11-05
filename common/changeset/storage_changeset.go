package changeset

import (
	"encoding/binary"
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

func EncodeStorage(s *ChangeSet) ([]byte, error) {
	return encodeStorage(s, common.HashLength)
}

func DecodeStorage(b []byte) (*ChangeSet, error) {
	cs := NewStorageChangeSet()
	err := decodeStorage(b, common.HashLength, cs)
	if err != nil {
		return nil, err
	}
	return cs, nil
}

type StorageChangeSetBytes []byte

func (b StorageChangeSetBytes) Walk(f func(k, v []byte) error) error {
	return walkStorageChangeSet(b, common.HashLength, f)
}

func (b StorageChangeSetBytes) Find(k []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet(b, common.HashLength, k[:common.HashLength], k[common.HashLength:])
}
func (b StorageChangeSetBytes) FindWithIncarnation(k []byte) ([]byte, error) {
	return findInStorageChangeSet(b, common.HashLength, k)
}

func (b StorageChangeSetBytes) FindWithoutIncarnation(addrHashToFind []byte, keyHashToFind []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet(b, common.HashLength, addrHashToFind, keyHashToFind)
}

type StorageChangeSet struct{ c ethdb.CursorDupSort }

func (b StorageChangeSet) WalkReverse(from, to uint64, f func(kk, k, v []byte) error) error {
	return walkReverse(b.c, from, to, common.HashLength+common.IncarnationLength+common.HashLength, f)
}

func (b StorageChangeSet) Walk(from, to uint64, f func(kk, k, v []byte) error) error {
	return walk(b.c, from, to, common.HashLength+common.IncarnationLength+common.HashLength, f)
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

func EncodeStoragePlain(s *ChangeSet) ([]byte, error) {
	return encodeStorage(s, common.AddressLength)
}

func DecodeStoragePlain(b []byte) (*ChangeSet, error) {
	cs := NewStorageChangeSetPlain()
	err := decodeStorage(b, common.AddressLength, cs)
	if err != nil {
		return nil, err
	}
	return cs, nil
}

type StorageChangeSetPlain struct{ c ethdb.CursorDupSort }

func (b StorageChangeSetPlain) WalkReverse(from, to uint64, f func(kk, k, v []byte) error) error {
	return walkReverse(b.c, from, to, common.AddressLength+common.IncarnationLength+common.HashLength, f)
}

func (b StorageChangeSetPlain) Walk(from, to uint64, f func(kk, k, v []byte) error) error {
	return walk(b.c, from, to, common.AddressLength+common.IncarnationLength+common.HashLength, f)
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

type StorageChangeSetPlainBytes []byte

func (b StorageChangeSetPlainBytes) Walk(f func(k, v []byte) error) error {
	return walkStorageChangeSet(b, common.AddressLength, f)
}

func (b StorageChangeSetPlainBytes) Find(k []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet(b, common.AddressLength, k[:common.AddressLength], k[common.AddressLength:])
}

func (b StorageChangeSetPlainBytes) FindWithIncarnation(k []byte) ([]byte, error) {
	return findInStorageChangeSet(b, common.AddressLength, k)
}

func (b StorageChangeSetPlainBytes) FindWithoutIncarnation(addressToFind []byte, keyToFind []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet(b, common.AddressLength, addressToFind, keyToFind)
}

// RewindData generates rewind data for all buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindData(db ethdb.Getter, timestampSrc, timestampDst uint64) (map[string][]byte, map[string][]byte, error) {
	// Collect list of buckets and keys that need to be considered
	collector := newRewindDataCollector()

	if err := walkAndCollect(
		collector.AccountWalker,
		db, dbutils.AccountChangeSetBucket2,
		timestampDst, timestampSrc,
		Mapper[dbutils.AccountChangeSetBucket2].KeySize,
	); err != nil {
		return nil, nil, err
	}

	if err := walkAndCollect(
		collector.StorageWalker,
		db, dbutils.StorageChangeSetBucket2,
		timestampDst, timestampSrc,
		Mapper[dbutils.StorageChangeSetBucket2].KeySize,
	); err != nil {
		return nil, nil, err
	}

	return collector.AccountData, collector.StorageData, nil
}

// RewindDataPlain generates rewind data for all plain buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindDataPlain(db ethdb.Getter, timestampSrc, timestampDst uint64) (map[string][]byte, map[string][]byte, error) {
	// Collect list of buckets and keys that need to be considered
	collector := newRewindDataCollector()

	if err := walkAndCollect(
		collector.AccountWalker,
		db, dbutils.PlainAccountChangeSetBucket2,
		timestampDst, timestampSrc,
		Mapper[dbutils.PlainAccountChangeSetBucket2].KeySize,
	); err != nil {
		return nil, nil, err
	}

	if err := walkAndCollect(
		collector.StorageWalker,
		db, dbutils.PlainStorageChangeSetBucket2,
		timestampDst, timestampSrc,
		Mapper[dbutils.PlainStorageChangeSetBucket2].KeySize,
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

func walkAndCollect(collectorFunc func([]byte, []byte) error, db ethdb.Getter, bucket string, timestampDst, timestampSrc uint64, keySize int) error {
	return db.Walk(bucket, dbutils.EncodeBlockNumber(timestampDst), 0, func(k, v []byte) (bool, error) {
		timestamp := binary.BigEndian.Uint64(k)
		if timestamp > timestampSrc {
			return false, nil
		}
		v = common.CopyBytes(v) // Making copy because otherwise it will be invalid after the transaction
		if innerErr := collectorFunc(v[:keySize], v[keySize:]); innerErr != nil {
			return false, innerErr
		}
		return true, nil
	})
}
