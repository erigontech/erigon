package state

import (
	"context"
	"encoding/binary"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

var _ WriterWithChangeSets = (*CacheStateWriter)(nil)

type CacheStateWriter struct {
	accountCache  *fastcache.Cache
	storageCache  *fastcache.Cache
	codeCache     *fastcache.Cache
	codeSizeCache *fastcache.Cache
}

func NewCacheStateWriter() *CacheStateWriter {
	return &CacheStateWriter{}
}

func (w *CacheStateWriter) SetAccountCache(accountCache *fastcache.Cache) {
	w.accountCache = accountCache
}

func (w *CacheStateWriter) SetStorageCache(storageCache *fastcache.Cache) {
	w.storageCache = storageCache
}

func (w *CacheStateWriter) SetCodeCache(codeCache *fastcache.Cache) {
	w.codeCache = codeCache
}

func (w *CacheStateWriter) SetCodeSizeCache(codeSizeCache *fastcache.Cache) {
	w.codeSizeCache = codeSizeCache
}

func (w *CacheStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	if w.accountCache != nil {
		w.accountCache.Set(address[:], value)
	}
	return nil
}

func (w *CacheStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if w.codeCache != nil {
		if len(code) <= 1024 {
			w.codeCache.Set(address[:], code)
		} else {
			w.codeCache.Del(address[:])
		}
	}
	if w.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		w.codeSizeCache.Set(address[:], b[:])
	}
	return nil
}

func (w *CacheStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if w.accountCache != nil {
		w.accountCache.Set(address[:], nil)
	}
	if w.codeCache != nil {
		w.codeCache.Set(address[:], nil)
	}
	if w.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], 0)
		w.codeSizeCache.Set(address[:], b[:])
	}
	return nil
}

func (w *CacheStateWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}

	if w.storageCache != nil {
		compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, *key)
		w.storageCache.Set(compositeKey, value.Bytes())
	}
	return nil
}

func (w *CacheStateWriter) CreateContract(address common.Address) error {
	return nil
}

func (w *CacheStateWriter) WriteChangeSets() error {
	return nil
}

func (w *CacheStateWriter) WriteHistory() error {
	return nil
}

func (w *CacheStateWriter) ChangeSetWriter() *ChangeSetWriter {
	return nil
}
