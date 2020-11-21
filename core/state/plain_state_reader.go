package state

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/VictoriaMetrics/fastcache"
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
	db            ethdb.Getter
	accountCache  *fastcache.Cache
	storageCache  *fastcache.Cache
	codeCache     *fastcache.Cache
	codeSizeCache *fastcache.Cache
}

func NewPlainStateReader(db ethdb.Getter) *PlainStateReader {
	return &PlainStateReader{
		db: db,
	}
}

func (r *PlainStateReader) SetAccountCache(accountCache *fastcache.Cache) {
	r.accountCache = accountCache
}

func (r *PlainStateReader) SetStorageCache(storageCache *fastcache.Cache) {
	r.storageCache = storageCache
}

func (r *PlainStateReader) SetCodeCache(codeCache *fastcache.Cache) {
	r.codeCache = codeCache
}

func (r *PlainStateReader) SetCodeSizeCache(codeSizeCache *fastcache.Cache) {
	r.codeSizeCache = codeSizeCache
}

func (r *PlainStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var enc []byte
	var ok bool
	if r.accountCache != nil {
		enc, ok = r.accountCache.HasGet(nil, address[:])
	}
	if !ok {
		var err error
		enc, err = r.db.Get(dbutils.PlainStateBucket, address[:])
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil, err
		}
	}
	if !ok && r.accountCache != nil {
		r.accountCache.Set(address[:], enc)
	}
	if enc == nil {
		return nil, nil
	}
	acc := &accounts.Account{}
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return acc, nil
}

func (r *PlainStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, *key)
	if r.storageCache != nil {
		if enc, ok := r.storageCache.HasGet(nil, compositeKey); ok {
			return enc, nil
		}
	}
	enc, err := r.db.Get(dbutils.PlainStateBucket, compositeKey)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	if r.storageCache != nil {
		r.storageCache.Set(compositeKey, enc)
	}
	return enc, nil
}

func (r *PlainStateReader) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if r.codeCache != nil {
		if code, ok := r.codeCache.HasGet(nil, address[:]); ok {
			return code, nil
		}
	}
	code, err := r.db.Get(dbutils.CodeBucket, codeHash[:])
	if r.codeCache != nil && len(code) <= 1024 {
		r.codeCache.Set(address[:], code)
	}
	if r.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		r.codeSizeCache.Set(address[:], b[:])
	}
	return code, err
}

func (r *PlainStateReader) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return 0, nil
	}
	if r.codeSizeCache != nil {
		if b, ok := r.codeSizeCache.HasGet(nil, address[:]); ok {
			return int(binary.BigEndian.Uint32(b)), nil
		}
	}
	code, err := r.db.Get(dbutils.CodeBucket, codeHash[:])
	if err != nil {
		return 0, err
	}
	if r.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		r.codeSizeCache.Set(address[:], b[:])
	}
	return len(code), nil
}

func (r *PlainStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if b, err := r.db.Get(dbutils.IncarnationMapBucket, address[:]); err == nil {
		return binary.BigEndian.Uint64(b), nil
	} else if errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, nil
	} else {
		return 0, err
	}
}
