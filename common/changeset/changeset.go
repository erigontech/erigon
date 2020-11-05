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
	Walk(func(k, v []byte) error) error
	Find(k []byte) ([]byte, error)
}

type Walker2 interface {
	Walk(from, to uint64, f func(kk, k, v []byte) error) error
	WalkReverse(from, to uint64, f func(kk, k, v []byte) error) error
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

var Mapper = map[string]struct {
	IndexBucket    string
	WalkerAdapter  func(v []byte) Walker
	WalkerAdapter2 func(cursor ethdb.CursorDupSort) Walker2
	KeySize        int
	Template       string
	New            func() *ChangeSet
	Encode         func(*ChangeSet) ([]byte, error)
}{
	dbutils.AccountChangeSetBucket2: {
		IndexBucket: dbutils.AccountsHistoryBucket,
		WalkerAdapter: func(v []byte) Walker {
			return AccountChangeSetBytes(v)
		},
		WalkerAdapter2: func(c ethdb.CursorDupSort) Walker2 {
			return AccountChangeSet{c: c}
		},
		KeySize:  common.HashLength,
		Template: "acc-ind-",
		New:      NewAccountChangeSet,
		Encode:   EncodeAccounts,
	},
	dbutils.StorageChangeSetBucket2: {
		IndexBucket: dbutils.StorageHistoryBucket,
		WalkerAdapter: func(v []byte) Walker {
			return StorageChangeSetBytes(v)
		},
		WalkerAdapter2: func(c ethdb.CursorDupSort) Walker2 {
			return StorageChangeSet{c: c}
		},
		KeySize:  common.HashLength*2 + common.IncarnationLength,
		Template: "st-ind-",
		New:      NewStorageChangeSet,
		Encode:   EncodeStorage,
	},
	dbutils.PlainAccountChangeSetBucket2: {
		IndexBucket: dbutils.AccountsHistoryBucket,
		WalkerAdapter: func(v []byte) Walker {
			return AccountChangeSetPlainBytes(v)
		},
		WalkerAdapter2: func(c ethdb.CursorDupSort) Walker2 {
			return AccountChangeSetPlain{c: c}
		},
		KeySize:  common.AddressLength,
		Template: "acc-ind-",
		New:      NewAccountChangeSetPlain,
		Encode:   EncodeAccountsPlain,
	},
	dbutils.PlainStorageChangeSetBucket2: {
		IndexBucket: dbutils.StorageHistoryBucket,
		WalkerAdapter: func(v []byte) Walker {
			return StorageChangeSetPlainBytes(v)
		},
		WalkerAdapter2: func(c ethdb.CursorDupSort) Walker2 {
			return StorageChangeSetPlain{c: c}
		},
		KeySize:  common.AddressLength + common.IncarnationLength + common.HashLength,
		Template: "st-ind-",
		New:      NewStorageChangeSetPlain,
		Encode:   EncodeStoragePlain,
	},
}
