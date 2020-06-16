package changeset

import (
	"github.com/ledgerwatch/turbo-geth/common"
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

type AccountChangeSetPlainBytes []byte

func (b AccountChangeSetPlainBytes) Walk(f func(k, v []byte) error) error {
	return walkAccountChangeSet(b, common.AddressLength, f)
}

func (b AccountChangeSetPlainBytes) Find(k []byte) ([]byte, error) {
	return findInAccountChangeSetBytes(b, k, common.AddressLength)
}
