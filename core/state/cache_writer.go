package state

import (
	"context"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
)

var _ WriterWithChangeSets = (*CacheStateWriter)(nil)

type CacheStateWriter struct {
	cache *shards.StateCache
}

func NewCacheStateWriter(cache *shards.StateCache) *CacheStateWriter {
	return &CacheStateWriter{cache: cache}
}

func (w *CacheStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	w.cache.SetAccountWrite(address.Bytes(), account)
	return nil
}

func (w *CacheStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	w.cache.SetCodeWrite(address.Bytes(), incarnation, code)
	return nil
}

func (w *CacheStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	w.cache.SetAccountDelete(address.Bytes())
	return nil
}

func (w *CacheStateWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	if value.IsZero() {
		w.cache.SetStorageDelete(address.Bytes(), incarnation, key.Bytes())
	} else {
		w.cache.SetStorageWrite(address.Bytes(), incarnation, key.Bytes(), value.Bytes())
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
