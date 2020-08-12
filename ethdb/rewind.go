package ethdb

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

// RewindData generates rewind data for all buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindData(db Getter, timestampSrc, timestampDst uint64) (map[string][]byte, map[string][]byte, error) {
	// Collect list of buckets and keys that need to be considered
	collector := newRewindDataCollector()

	suffixDst := dbutils.EncodeTimestamp(timestampDst + 1)

	if err := walkAndCollect(
		collector.AccountWalker,
		db, dbutils.AccountChangeSetBucket,
		suffixDst, timestampSrc,
		bytesToAccountChangeSetWalker,
	); err != nil {
		return nil, nil, err
	}

	if err := walkAndCollect(
		collector.StorageWalker,
		db, dbutils.StorageChangeSetBucket,
		suffixDst, timestampSrc,
		bytesToStorageChangeSetWalker,
	); err != nil {
		return nil, nil, err
	}

	return collector.AccountData, collector.StorageData, nil
}

// RewindDataPlain generates rewind data for all plain buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindDataPlain(db Getter, timestampSrc, timestampDst uint64) (map[string][]byte, map[string][]byte, error) {
	// Collect list of buckets and keys that need to be considered
	collector := newRewindDataCollector()

	suffixDst := dbutils.EncodeTimestamp(timestampDst + 1)

	if err := walkAndCollect(
		collector.AccountWalker,
		db, dbutils.PlainAccountChangeSetBucket,
		suffixDst, timestampSrc,
		bytesToAccountChangeSetWalkerPlain,
	); err != nil {
		return nil, nil, err
	}

	if err := walkAndCollect(
		collector.StorageWalker,
		db, dbutils.PlainStorageChangeSetBucket,
		suffixDst, timestampSrc,
		bytesToStorageChangeSetWalkerPlain,
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

type walker interface {
	Walk(f func(k, v []byte) error) error
}

func bytesToAccountChangeSetWalker(b []byte) walker {
	return changeset.AccountChangeSetBytes(b)
}

func bytesToStorageChangeSetWalker(b []byte) walker {
	return changeset.StorageChangeSetBytes(b)
}

func bytesToAccountChangeSetWalkerPlain(b []byte) walker {
	return changeset.AccountChangeSetPlainBytes(b)
}

func bytesToStorageChangeSetWalkerPlain(b []byte) walker {
	return changeset.StorageChangeSetPlainBytes(b)
}

func walkAndCollect(collectorFunc func([]byte, []byte) error, db Getter, bucket string, suffixDst []byte, timestampSrc uint64, bytesToWalker func([]byte) walker) error {
	return db.Walk(bucket, suffixDst, 0, func(k, v []byte) (bool, error) {
		timestamp, _ := dbutils.DecodeTimestamp(k)
		if timestamp > timestampSrc {
			return false, nil
		}
		if changeset.Len(v) > 0 {
			v = common.CopyBytes(v) // Making copy because otherwise it will be invalid after the transaction
			if innerErr := bytesToWalker(v).Walk(collectorFunc); innerErr != nil {
				return false, innerErr
			}
		}
		return true, nil
	})
}
