package state

import (
	"context"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
)

// CachedWriter is a wrapper for an instance of type StateWriter
type CachedWriter struct {
	w     WriterWithChangeSets
	cache *shards.StateCache
}

// NewCachedWriter wraps a given state writer into a cached writer
func NewCachedWriter(w WriterWithChangeSets, cache *shards.StateCache) *CachedWriter {
	return &CachedWriter{w: w, cache: cache}
}

func (cw *CachedWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	cw.cache.SetAccountWrite(address.Bytes(), account)
	return nil
}

func (cw *CachedWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	cw.cache.SetCodeWrite(address.Bytes(), incarnation, code)
	return nil
}

func (cw *CachedWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	cw.cache.SetAccountDelete(address.Bytes())
	return nil
}

func (cw *CachedWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	if value.IsZero() {
		cw.cache.SetStorageDelete(address.Bytes(), incarnation, key.Bytes())
	} else {
		cw.cache.SetStorageWrite(address.Bytes(), incarnation, key.Bytes(), value.Bytes())
	}
	return nil
}

func (cw *CachedWriter) CreateContract(address common.Address) error {
	return cw.w.CreateContract(address)
}

func (cw *CachedWriter) WriteChangeSets() error {
	return cw.w.WriteChangeSets()
}

func (cw *CachedWriter) WriteHistory() error {
	return cw.w.WriteHistory()
}
