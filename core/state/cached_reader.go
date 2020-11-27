package state

import (
	"bytes"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
)

// CachedReader is a wrapper for an instance of type StateReader
// This wrapper only makes calls to the underlying reader if the item is not in the cache
type CachedReader struct {
	r     StateReader
	cache *shards.StateCache
}

// NewCachedReader wraps a given state reader into the cached reader
func NewCachedReader(r StateReader, cache *shards.StateCache) *CachedReader {
	return &CachedReader{r: r, cache: cache}
}

// ReadAccountData is called when an account needs to be fetched from the state
func (cr *CachedReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	if a, ok := cr.cache.GetAccount(address.Bytes()); ok {
		return a, nil
	}
	a, err := cr.r.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if a == nil {
		cr.cache.SetAccountAbsent(address.Bytes())
	} else {
		cr.cache.SetAccountRead(address.Bytes(), a)
	}
	return a, nil
}

// ReadAccountStorage is called when a storage item needs to be fetched from the state
func (cr *CachedReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	if s, ok := cr.cache.GetStorage(address.Bytes(), incarnation, key.Bytes()); ok {
		return s, nil
	}
	v, err := cr.r.ReadAccountStorage(address, incarnation, key)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		cr.cache.SetStorageAbsent(address.Bytes(), incarnation, key.Bytes())
	} else {
		cr.cache.SetStorageRead(address.Bytes(), incarnation, key.Bytes(), v)
	}
	return v, nil
}

// ReadAccountCode is called when code of an account needs to be fetched from the state
// Usually, one of (address;incarnation) or codeHash is enough to uniquely identify the code
func (cr *CachedReader) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if c, ok := cr.cache.GetCode(address.Bytes(), incarnation); ok {
		return c, nil
	}
	c, err := cr.r.ReadAccountCode(address, incarnation, codeHash)
	if err != nil {
		return nil, err
	}
	if cr.cache != nil && len(c) <= 1024 {
		cr.cache.SetCodeRead(address.Bytes(), incarnation, c)
	}
	return c, nil
}

func (cr *CachedReader) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	c, err := cr.ReadAccountCode(address, incarnation, codeHash)
	return len(c), err
}

// ReadAccountIncarnation is called when incarnation of the account is required (to create and recreate contract)
func (cr *CachedReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	deleted := cr.cache.GetDeletedAccount(address.Bytes())
	if deleted != nil {
		return deleted.Incarnation, nil
	}
	return cr.r.ReadAccountIncarnation(address)
}
