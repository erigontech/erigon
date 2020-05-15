package changeset

import (
	"errors"

	"github.com/ledgerwatch/turbo-geth/common"
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
	return findInStorageChangeSet(b, common.HashLength, k)
}

func (b StorageChangeSetBytes) FindWithoutIncarnation(addrHashToFind []byte, keyHashToFind []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet(b, common.HashLength, addrHashToFind, keyHashToFind)
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

type StorageChangeSetPlainBytes []byte

func (b StorageChangeSetPlainBytes) Walk(f func(k, v []byte) error) error {
	return walkStorageChangeSet(b, common.AddressLength, f)
}

func (b StorageChangeSetPlainBytes) Find(k []byte) ([]byte, error) {
	return findInStorageChangeSet(b, common.AddressLength, k)
}

func (b StorageChangeSetPlainBytes) FindWithoutIncarnation(addressToFind []byte, keyToFind []byte) ([]byte, error) {
	return findWithoutIncarnationInStorageChangeSet(b, common.AddressLength, addressToFind, keyToFind)
}
