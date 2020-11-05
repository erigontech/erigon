package changeset

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

/* Hashed changesets (key is a hash of common.Address) */

func NewAccountChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  common.HashLength,
	}
}

func EncodeAccounts(s *ChangeSet) ([]byte, error) {
	return encodeAccounts(s)
}

func DecodeAccounts(b []byte) (*ChangeSet, error) {
	h := NewAccountChangeSet()
	err := decodeAccountsWithKeyLen(b, common.HashLength, h)
	if err != nil {
		return nil, err
	}
	return h, nil
}

type AccountChangeSetBytes []byte

func (b AccountChangeSetBytes) Walk(f func(k, v []byte) error) error {
	return walkAccountChangeSet(b, common.HashLength, f)
}

func (b AccountChangeSetBytes) Find(k []byte) ([]byte, error) {
	return findInAccountChangeSetBytes(b, k, common.HashLength)
}

type AccountChangeSet struct{ c ethdb.CursorDupSort }

func (b AccountChangeSet) WalkReverse(from, to uint64, f func(k, v []byte) error) error {
	return walkReverse(b.c, from, to, common.HashLength, f)
}

func (b AccountChangeSet) Find(blockNumber uint64, k []byte) ([]byte, error) {
	return findInAccountChangeSet(b.c, blockNumber, k, common.HashLength)
}

/* Plain changesets (key is a common.Address) */

func NewAccountChangeSetPlain() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  common.AddressLength,
	}
}

func EncodeAccountsPlain(s *ChangeSet) ([]byte, error) {
	return encodeAccounts(s)
}

func DecodeAccountsPlain(b []byte) (*ChangeSet, error) {
	h := NewAccountChangeSetPlain()
	err := decodeAccountsWithKeyLen(b, common.AddressLength, h)
	if err != nil {
		return nil, err
	}
	return h, nil
}

type AccountChangeSetPlain struct{ c ethdb.CursorDupSort }

func (b AccountChangeSetPlain) WalkReverse(from, to uint64, f func(k, v []byte) error) error {
	return walkReverse(b.c, from, to, common.AddressLength, f)
}

func (b AccountChangeSetPlain) Find(blockNumber uint64, k []byte) ([]byte, error) {
	return findInAccountChangeSet(b.c, blockNumber, k, common.AddressLength)
}

type AccountChangeSetPlainBytes []byte

func (b AccountChangeSetPlainBytes) Walk(f func(k, v []byte) error) error {
	return walkAccountChangeSet(b, common.AddressLength, f)
}

func (b AccountChangeSetPlainBytes) Find(k []byte) ([]byte, error) {
	return findInAccountChangeSetBytes(b, k, common.AddressLength)
}

// GetModifiedAccounts returns a list of addresses that were modified in the block range
func GetModifiedAccounts(tx ethdb.Tx, startNum, endNum uint64) ([]common.Address, error) {

	changedAddrs := make(map[common.Address]struct{})
	startCode := dbutils.EncodeTimestamp(startNum)

	c := tx.Cursor(dbutils.PlainAccountChangeSetBucket)
	defer c.Close()

	for k, v, err := c.Seek(startCode); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, fmt.Errorf("iterating over account changeset for %v: %w", k, err)
		}
		currentNum, _ := dbutils.DecodeTimestamp(k)
		if currentNum > endNum {
			break
		}

		walker := func(addr, _ []byte) error {
			changedAddrs[common.BytesToAddress(addr)] = struct{}{}
			return nil
		}
		if err := AccountChangeSetPlainBytes(v).Walk(walker); err != nil {
			return nil, fmt.Errorf("iterating over account changeset for %v: %w", k, err)
		}
	}

	if len(changedAddrs) == 0 {
		return nil, nil
	}

	idx := 0
	result := make([]common.Address, len(changedAddrs))
	for addr := range changedAddrs {
		copy(result[idx][:], addr[:])
		idx++
	}

	return result, nil
}
