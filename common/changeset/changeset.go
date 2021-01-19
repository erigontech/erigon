package changeset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type Walker interface {
	Walk(from, to uint64, f func(blockNumber uint64, k, v []byte) error) error
	Find(blockNumber uint64, k []byte) ([]byte, error)
}

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

func FromDBFormat(addrSize int) func(dbKey, dbValue []byte) (blockN uint64, k, v []byte) {
	stSz := addrSize + common.IncarnationLength + common.HashLength
	const BlockNSize = 8
	return func(dbKey, dbValue []byte) (blockN uint64, k, v []byte) {
		blockN = binary.BigEndian.Uint64(dbKey)
		if len(dbKey) == 8 {
			k = dbValue[:addrSize]
			v = dbValue[addrSize:]
		} else {
			k = make([]byte, stSz)
			dbKey = dbKey[BlockNSize:] // remove BlockN bytes
			copy(k, dbKey)
			copy(k[len(dbKey):], dbValue[:common.HashLength])
			v = dbValue[common.HashLength:]
		}

		return blockN, k, v
	}
}

func Walk(db ethdb.Database, bucket string, startkey []byte, fixedbits int, walker func(blockN uint64, k, v []byte) (bool, error)) error {
	fromDBFormat := FromDBFormat(Mapper[bucket].KeySize)
	var blockN uint64
	return db.Walk(bucket, startkey, fixedbits, func(k, v []byte) (bool, error) {
		blockN, k, v = fromDBFormat(k, v)
		return walker(blockN, k, v)
	})
}

func Truncate(tx ethdb.Tx, from uint64) error {
	keyStart := dbutils.EncodeBlockNumber(from)

	{
		c := tx.CursorDupSort(dbutils.PlainAccountChangeSetBucket)
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
		c := tx.CursorDupSort(dbutils.PlainStorageChangeSetBucket)
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
	WalkerAdapter func(cursor ethdb.CursorDupSort) Walker
	KeySize       int
	Template      string
	New           func() *ChangeSet
	Encode        Encoder
	Decode        Decoder
}{
	dbutils.PlainAccountChangeSetBucket: {
		IndexBucket: dbutils.AccountsHistoryBucket,
		WalkerAdapter: func(c ethdb.CursorDupSort) Walker {
			return AccountChangeSetPlain{c: c}
		},
		KeySize:  common.AddressLength,
		Template: "acc-ind-",
		New:      NewAccountChangeSetPlain,
		Encode:   EncodeAccountsPlain,
		Decode:   FromDBFormat(common.AddressLength),
	},
	dbutils.PlainStorageChangeSetBucket: {
		IndexBucket: dbutils.StorageHistoryBucket,
		WalkerAdapter: func(c ethdb.CursorDupSort) Walker {
			return StorageChangeSetPlain{c: c}
		},
		KeySize:  common.AddressLength,
		Template: "st-ind-",
		New:      NewStorageChangeSetPlain,
		Encode:   EncodeStoragePlain,
		Decode:   FromDBFormat(common.AddressLength),
	},
}
