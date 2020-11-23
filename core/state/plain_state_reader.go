package state

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
)

var _ StateReader = (*PlainStateReader)(nil)

// PlainStateReader reads data from so called "plain state".
// Data in the plain state is stored using un-hashed account/storage items
// as opposed to the "normal" state that uses hashes of merkle paths to store items.
type PlainStateReader struct {
	db    ethdb.Getter
	cache *shards.StateCache
}

func NewPlainStateReader(db ethdb.Getter) *PlainStateReader {
	return &PlainStateReader{
		db: db,
	}
}

func (r *PlainStateReader) SetCache(cache *shards.StateCache) {
	r.cache = cache
}

func (r *PlainStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	if r.cache != nil {
		if a, ok := r.cache.GetAccount(address.Bytes()); ok {
			return a, nil
		}
	}
	enc, err := r.db.Get(dbutils.PlainStateBucket, address.Bytes())
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	if len(enc) == 0 {
		if r.cache != nil {
			r.cache.SetAccountAbsent(address.Bytes())
		}
		return nil, nil
	}
	var a accounts.Account
	if err = a.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	if r.cache != nil {
		r.cache.SetAccountRead(address.Bytes(), &a)
	}
	return &a, nil
}

func (r *PlainStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	if r.cache != nil {
		if s, ok := r.cache.GetStorage(address.Bytes(), incarnation, key.Bytes()); ok {
			return s, nil
		}
	}
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc, err := r.db.Get(dbutils.PlainStateBucket, compositeKey)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	if len(enc) == 0 {
		if r.cache != nil {
			r.cache.SetStorageAbsent(address.Bytes(), incarnation, key.Bytes())
		}
		return nil, nil
	}
	if r.cache != nil {
		r.cache.SetStorageRead(address.Bytes(), incarnation, key.Bytes(), enc)
	}
	return enc, nil
}

func (r *PlainStateReader) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash.Bytes(), emptyCodeHash) {
		return nil, nil
	}
	if r.cache != nil {
		if c, ok := r.cache.GetCode(address.Bytes(), incarnation); ok {
			return c, nil
		}
	}
	code, err := r.db.Get(dbutils.CodeBucket, codeHash.Bytes())
	if len(code) == 0 {
		if r.cache != nil {
			r.cache.SetCodeAbsent(address.Bytes(), incarnation)
		}
		return nil, nil
	}
	if r.cache != nil && len(code) <= 1024 {
		r.cache.SetCodeRead(address.Bytes(), incarnation, code)
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
