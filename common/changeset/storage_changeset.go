package changeset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
)

const (
	DefaultIncarnation = uint64(1)
)

var (
	ErrNotFound = errors.New("not found")
)

func NewStorageChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  length.Addr + length.Hash + length.Incarnation,
	}
}

func EncodeStorage(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
	sort.Sort(s)
	keyPart := length.Addr + length.Incarnation
	for _, cs := range s.Changes {
		newK := make([]byte, length.BlockNum+keyPart)
		binary.BigEndian.PutUint64(newK, blockN)
		copy(newK[8:], cs.Key[:keyPart])
		newV := make([]byte, 0, length.Hash+len(cs.Value))
		newV = append(append(newV, cs.Key[keyPart:]...), cs.Value...)
		if err := f(newK, newV); err != nil {
			return err
		}
	}
	return nil
}

func DecodeStorage(dbKey, dbValue []byte) (uint64, []byte, []byte, error) {
	blockN := binary.BigEndian.Uint64(dbKey)
	if len(dbValue) < length.Hash {
		return 0, nil, nil, fmt.Errorf("storage changes purged for block %d", blockN)
	}
	k := make([]byte, length.Addr+length.Incarnation+length.Hash)
	dbKey = dbKey[length.BlockNum:] // remove BlockN bytes
	copy(k, dbKey)
	copy(k[len(dbKey):], dbValue[:length.Hash])
	v := dbValue[length.Hash:]
	if len(v) == 0 {
		v = nil
	}

	return blockN, k, v, nil
}

func FindStorage(c kv.CursorDupSort, blockNumber uint64, k []byte) ([]byte, error) {
	addWithInc, loc := k[:length.Addr+length.Incarnation], k[length.Addr+length.Incarnation:]
	seek := make([]byte, length.BlockNum+length.Addr+length.Incarnation)
	binary.BigEndian.PutUint64(seek, blockNumber)
	copy(seek[8:], addWithInc)
	v, err := c.SeekBothRange(seek, loc)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(v, loc) {
		return nil, ErrNotFound
	}
	return v[length.Hash:], nil
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
	return ForRange(db, bucket, timestampDst, timestampSrc+1, func(bl uint64, k, v []byte) error {
		if err := libcommon.Stopped(quit); err != nil {
			return err
		}
		if innerErr := collectorFunc(libcommon.Copy(k), libcommon.Copy(v)); innerErr != nil {
			return innerErr
		}
		return nil
	})
}
