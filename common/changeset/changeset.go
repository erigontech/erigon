package changeset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
)

func NewChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
	}
}

type Change struct {
	Key   []byte
	Value []byte
}

// ChangeSet is a map with keys of the same size.
// Both keys and values are byte strings.
type ChangeSet struct {
	// Invariant: all keys are of the same size.
	Changes []Change
	keyLen  int
}

// BEGIN sort.Interface

func (s *ChangeSet) Len() int {
	return len(s.Changes)
}

func (s *ChangeSet) Swap(i, j int) {
	s.Changes[i], s.Changes[j] = s.Changes[j], s.Changes[i]
}

func (s *ChangeSet) Less(i, j int) bool {
	cmp := bytes.Compare(s.Changes[i].Key, s.Changes[j].Key)
	if cmp == 0 {
		cmp = bytes.Compare(s.Changes[i].Value, s.Changes[j].Value)
	}
	return cmp < 0
}

// END sort.Interface
func (s *ChangeSet) KeySize() int {
	if s.keyLen != 0 {
		return s.keyLen
	}
	for _, c := range s.Changes {
		return len(c.Key)
	}
	return 0
}

func (s *ChangeSet) checkKeySize(key []byte) error {
	if (s.Len() == 0 && s.KeySize() == 0) || (len(key) == s.KeySize() && len(key) > 0) {
		return nil
	}

	return fmt.Errorf("wrong key size in AccountChangeSet: expected %d, actual %d", s.KeySize(), len(key))
}

// Add adds a new entry to the AccountChangeSet.
// One must not add an existing key
// and may add keys only of the same size.
func (s *ChangeSet) Add(key []byte, value []byte) error {
	if err := s.checkKeySize(key); err != nil {
		return err
	}

	s.Changes = append(s.Changes, Change{
		Key:   key,
		Value: value,
	})
	return nil
}

func (s *ChangeSet) ChangedKeys() map[string]struct{} {
	m := make(map[string]struct{}, len(s.Changes))
	for i := range s.Changes {
		m[string(s.Changes[i].Key)] = struct{}{}
	}
	return m
}

func (s *ChangeSet) Equals(s2 *ChangeSet) bool {
	return reflect.DeepEqual(s.Changes, s2.Changes)
}

func (s *ChangeSet) String() string {
	str := ""
	for _, v := range s.Changes {
		str += fmt.Sprintf("%v %s : %s\n", len(v.Key), common.Bytes2Hex(v.Key), string(v.Value))
	}
	return str
}

// Encoded Method

func Len(b []byte) int {
	return int(binary.BigEndian.Uint32(b[0:4]))
}

func FromDBFormat(dbKey, dbValue []byte) (uint64, []byte, []byte) {
	if len(dbKey) == 8 {
		return DecodeAccounts(dbKey, dbValue)
	} else {
		return DecodeStorage(dbKey, dbValue)
	}
}

func Walk(db ethdb.Tx, bucket string, startkey []byte, fixedbits int, walker func(blockN uint64, k, v []byte) (bool, error)) error {
	var blockN uint64
	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	return ethdb.Walk(c, startkey, fixedbits, func(k, v []byte) (bool, error) {
		blockN, k, v = FromDBFormat(k, v)
		return walker(blockN, k, v)
	})
}

func Truncate(tx ethdb.RwTx, from uint64) error {
	keyStart := dbutils.EncodeBlockNumber(from)

	{
		c, err := tx.RwCursorDupSort(dbutils.AccountChangeSetBucket)
		if err != nil {
			return err
		}
		defer c.Close()
		for k, _, err := c.Seek(keyStart); k != nil; k, _, err = c.NextNoDup() {
			if err != nil {
				return err
			}
			err = c.DeleteCurrentDuplicates()
			if err != nil {
				return err
			}
		}
	}
	{
		c, err := tx.RwCursorDupSort(dbutils.StorageChangeSetBucket)
		if err != nil {
			return err
		}
		defer c.Close()
		for k, _, err := c.Seek(keyStart); k != nil; k, _, err = c.NextNoDup() {
			if err != nil {
				return err
			}
			err = c.DeleteCurrentDuplicates()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

var Mapper = map[string]struct {
	IndexBucket   string
	IndexChunkKey func([]byte, uint64) []byte
	Find          func(cursor ethdb.CursorDupSort, blockNumber uint64, key []byte) ([]byte, error)
	New           func() *ChangeSet
	Encode        Encoder
	Decode        Decoder
}{
	dbutils.AccountChangeSetBucket: {
		IndexBucket:   dbutils.AccountsHistoryBucket,
		IndexChunkKey: dbutils.AccountIndexChunkKey,
		New:           NewAccountChangeSet,
		Find:          FindAccount,
		Encode:        EncodeAccounts,
		Decode:        DecodeAccounts,
	},
	dbutils.StorageChangeSetBucket: {
		IndexBucket:   dbutils.StorageHistoryBucket,
		IndexChunkKey: dbutils.StorageIndexChunkKey,
		Find:          FindStorage,
		New:           NewStorageChangeSet,
		Encode:        EncodeStorage,
		Decode:        DecodeStorage,
	},
}
