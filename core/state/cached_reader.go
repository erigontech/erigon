package state

import (
	"bytes"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/shards"
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
func (cr *CachedReader) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	addrBytes := address.Bytes()
	if a, ok := cr.cache.GetAccount(addrBytes); ok {
		return a, nil
	}
	a, err := cr.r.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if a == nil {
		cr.cache.SetAccountAbsent(addrBytes)
	} else {
		cr.cache.SetAccountRead(addrBytes, a)
	}
	return a, nil
}

// ReadAccountStorage is called when a storage item needs to be fetched from the state
func (cr *CachedReader) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	addrBytes := address.Bytes()
	if s, ok := cr.cache.GetStorage(addrBytes, incarnation, key.Bytes()); ok {
		return s, nil
	}
	v, err := cr.r.ReadAccountStorage(address, incarnation, key)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		cr.cache.SetStorageAbsent(addrBytes, incarnation, key.Bytes())
	} else {
		cr.cache.SetStorageRead(addrBytes, incarnation, key.Bytes(), v)
	}
	return v, nil
}

// ReadAccountCode is called when code of an account needs to be fetched from the state
// Usually, one of (address;incarnation) or codeHash is enough to uniquely identify the code
func (cr *CachedReader) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
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

func (cr *CachedReader) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	c, err := cr.ReadAccountCode(address, incarnation, codeHash)
	return len(c), err
}

// ReadAccountIncarnation is called when incarnation of the account is required (to create and recreate contract)
func (cr *CachedReader) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	deleted := cr.cache.GetDeletedAccount(address.Bytes())
	if deleted != nil {
		return deleted.Incarnation, nil
	}
	return cr.r.ReadAccountIncarnation(address)
}
