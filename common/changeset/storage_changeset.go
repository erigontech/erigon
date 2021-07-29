package changeset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/ethdb"
)

const (
	DefaultIncarnation = uint64(1)
)

var (
	ErrNotFound  = errors.New("not found")
	ErrFindValue = errors.New("find value error")
)

func NewStorageChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  common.AddressLength + common.HashLength + common.IncarnationLength,
	}
}

func EncodeStorage(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
	sort.Sort(s)
	keyPart := common.AddressLength + common.IncarnationLength
	for _, cs := range s.Changes {
		newK := make([]byte, common.BlockNumberLength+keyPart)
		binary.BigEndian.PutUint64(newK, blockN)
		copy(newK[8:], cs.Key[:keyPart])
		newV := make([]byte, 0, common.HashLength+len(cs.Value))
		newV = append(append(newV, cs.Key[keyPart:]...), cs.Value...)
		if err := f(newK, newV); err != nil {
			return err
		}
	}
	return nil
}

func DecodeStorage(dbKey, dbValue []byte) (uint64, []byte, []byte) {
	blockN := binary.BigEndian.Uint64(dbKey)
	k := make([]byte, common.AddressLength+common.IncarnationLength+common.HashLength)
	dbKey = dbKey[common.BlockNumberLength:] // remove BlockN bytes
	copy(k, dbKey)
	copy(k[len(dbKey):], dbValue[:common.HashLength])
	v := dbValue[common.HashLength:]
	if len(v) == 0 {
		v = nil
	}

	return blockN, k, v
}

func FindStorage(c kv.CursorDupSort, blockNumber uint64, k []byte) ([]byte, error) {
	addWithInc, loc := k[:common.AddressLength+common.IncarnationLength], k[common.AddressLength+common.IncarnationLength:]
	seek := make([]byte, common.BlockNumberLength+common.AddressLength+common.IncarnationLength)
	binary.BigEndian.PutUint64(seek, blockNumber)
	copy(seek[8:], addWithInc)
	v, err := c.SeekBothRange(seek, loc)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(v, loc) {
		return nil, ErrNotFound
	}
	return v[common.HashLength:], nil
}

// RewindDataPlain generates rewind data for all plain buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindData(db kv.Tx, timestampSrc, timestampDst uint64, changes *etl.Collector, quit <-chan struct{}) error {
	if err := walkAndCollect(
		changes.Collect,
		db, kv.AccountChangeSet,
		timestampDst+1, timestampSrc,
		quit,
	); err != nil {
		return err
	}

	if err := walkAndCollect(
		changes.Collect,
		db, kv.StorageChangeSet,
		timestampDst+1, timestampSrc,
		quit,
	); err != nil {
		return err
	}

	return nil
}

func walkAndCollect(collectorFunc func([]byte, []byte) error, db kv.Tx, bucket string, timestampDst, timestampSrc uint64, quit <-chan struct{}) error {
	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	return ethdb.Walk(c, dbutils.EncodeBlockNumber(timestampDst), 0, func(dbKey, dbValue []byte) (bool, error) {
		if err := common.Stopped(quit); err != nil {
			return false, err
		}
		timestamp, k, v := Mapper[bucket].Decode(dbKey, dbValue)
		if timestamp > timestampSrc {
			return false, nil
		}
		if innerErr := collectorFunc(common.CopyBytes(k), common.CopyBytes(v)); innerErr != nil {
			return false, innerErr
		}
		return true, nil
	})
}
