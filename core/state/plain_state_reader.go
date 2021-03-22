package state

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var _ StateReader = (*PlainStateReader)(nil)

// PlainStateReader reads data from so called "plain state".
// Data in the plain state is stored using un-hashed account/storage items
// as opposed to the "normal" state that uses hashes of merkle paths to store items.
type PlainStateReader struct {
	db ethdb.Database
}

func NewPlainStateReader(db ethdb.Database) *PlainStateReader {
	return &PlainStateReader{
		db: db,
	}
}

func (r *PlainStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := r.db.Get(dbutils.PlainStateBucket, address.Bytes())
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err = a.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *PlainStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc, err := r.db.Get(dbutils.PlainStateBucket, compositeKey)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (r *PlainStateReader) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash.Bytes(), emptyCodeHash) {
		return nil, nil
	}
	code, err := r.db.Get(dbutils.CodeBucket, codeHash.Bytes())
	if len(code) == 0 {
		return nil, nil
	}
	return code, err
}

func (r *PlainStateReader) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (r *PlainStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if b, err := r.db.Get(dbutils.IncarnationMapBucket, address.Bytes()); err == nil {
		return binary.BigEndian.Uint64(b), nil
	} else if errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, nil
	} else {
		return 0, err
	}
}
