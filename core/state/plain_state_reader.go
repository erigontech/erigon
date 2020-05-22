package state

import (
	"bytes"
	lru "github.com/hashicorp/golang-lru"

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
	db                     ethdb.Getter
	uncommitedIncarnations map[common.Address]uint64
	accountCache           *lru.Cache
	storageCache           *lru.Cache
	codeCache              *lru.Cache
	codeSizeCache          *lru.Cache
}

func NewPlainStateReader(db ethdb.Getter, incarnations map[common.Address]uint64) *PlainStateReader {
	return &PlainStateReader{
		db:                     db,
		uncommitedIncarnations: incarnations,
	}
}

func (r *PlainStateReader) SetAccountCache(accountCache *lru.Cache) {
	r.accountCache = accountCache
}

func (r *PlainStateReader) SetStorageCache(storageCache *lru.Cache) {
	r.storageCache = storageCache
}

func (r *PlainStateReader) SetCodeCache(codeCache *lru.Cache) {
	r.codeCache = codeCache
}

func (r *PlainStateReader) SetCodeSizeCache(codeSizeCache *lru.Cache) {
	r.codeSizeCache = codeSizeCache
}

func (r *PlainStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	if r.accountCache != nil {
		if cached, ok := r.accountCache.Get(address); ok {
			if cached == nil {
				return nil, nil
			}
			return cached.(*accounts.Account), nil
		}
	}
	enc, err := r.db.Get(dbutils.PlainStateBucket, address[:])
	if err == nil {
		acc := &accounts.Account{}
		if err = acc.DecodeForStorage(enc); err != nil {
			return nil, err
		}
		if r.accountCache != nil {
			r.accountCache.Add(address, acc)
		}
		return acc, nil
	} else if !entryNotFound(err) {
		return nil, err
	}
	if r.accountCache != nil {
		r.accountCache.Add(address, nil)
	}
	return nil, nil
}

func (r *PlainStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	var storageKeyP *[20 + 32]byte
	if r.storageCache != nil {
		var storageKey [20 + 32]byte
		copy(storageKey[:], address[:])
		copy(storageKey[20:], key[:])
		if cached, ok := r.storageCache.Get(storageKey); ok {
			if cached == nil {
				return nil, nil
			}
			return cached.([]byte), nil
		}
		storageKeyP = &storageKey
	}
	enc, err := r.db.Get(dbutils.PlainStateBucket, dbutils.PlainGenerateCompositeStorageKey(address, incarnation, *key))
	if err == nil {
		if r.storageCache != nil {
			r.storageCache.Add(*storageKeyP, enc)
		}
		return enc, nil
	} else if !entryNotFound(err) {
		return nil, err
	}
	if r.storageCache != nil {
		r.storageCache.Add(*storageKeyP, nil)
	}
	return nil, nil
}

func (r *PlainStateReader) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if r.codeCache != nil {
		if cached, ok := r.codeCache.Get(address); ok {
			return cached.([]byte), nil
		}
	}
	code, err := r.db.Get(dbutils.CodeBucket, codeHash[:])
	if r.codeCache != nil {
		r.codeCache.Add(address, code)
	}
	return code, err
}

func (r *PlainStateReader) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (int, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return 0, nil
	}
	if r.codeSizeCache != nil {
		if cached, ok := r.codeSizeCache.Get(address); ok {
			return cached.(int), nil
		}
	}
	code, err := r.db.Get(dbutils.CodeBucket, codeHash[:])
	if err != nil {
		return 0, err
	}
	if r.codeSizeCache != nil {
		r.codeSizeCache.Add(address, len(code))
	}
	return len(code), nil
}

func (r *PlainStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if inc, ok := r.uncommitedIncarnations[address]; ok {
		return inc, nil
	}

	incarnation, found, err := ethdb.PlainGetCurrentAccountIncarnation(r.db, address)
	if err != nil {
		return 0, err
	}
	if found {
		return incarnation, nil
	}

	return 0, nil
}

func entryNotFound(err error) bool {
	return err == ethdb.ErrKeyNotFound
}
