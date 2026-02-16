// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/shards"
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

func (cr *CachedReader) SetTrace(trace bool, tracePrefix string) {
	cr.r.SetTrace(trace, tracePrefix)
}

func (cr *CachedReader) Trace() bool         { return cr.r.Trace() }
func (cr *CachedReader) TracePrefix() string { return cr.r.TracePrefix() }

// ReadAccountData is called when an account needs to be fetched from the state
func (cr *CachedReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addrValue := address.Value()
	if a, ok := cr.cache.GetAccount(addrValue[:]); ok {
		return a, nil
	}
	a, err := cr.r.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if a == nil {
		cr.cache.SetAccountAbsent(addrValue[:])
	} else {
		cr.cache.SetAccountRead(addrValue[:], a)
	}
	return a, nil
}

// ReadAccountDataForDebug is called when an account needs to be fetched from the state
func (cr *CachedReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return cr.ReadAccountData(address)
}

// ReadAccountStorage is called when a storage item needs to be fetched from the state
func (cr *CachedReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addrValue := address.Value()
	keyValue := key.Value()
	if s, ok := cr.cache.GetStorage(addrValue[:], 1, keyValue[:]); ok {
		var v uint256.Int
		(&v).SetBytes(s)
		return v, true, nil
	}
	v, ok, err := cr.r.ReadAccountStorage(address, key)
	if err != nil {
		return uint256.Int{}, false, err
	}
	if !ok {
		cr.cache.SetStorageAbsent(addrValue[:], 1, keyValue[:])
	} else {
		cr.cache.SetStorageRead(addrValue[:], 1, keyValue[:], v.Bytes())
	}
	return v, ok, nil
}

func (cr *CachedReader) HasStorage(address accounts.Address) (bool, error) {
	// note: theoretically we could try to use the cache here using cr.cache.StorageTree
	// to traverse the cached storage, however that will be only useful in case of a
	// collision (ie creating an account which already has storage as per eip-7610) which
	// in reality is very rare; for all the other most likely situations in which we query
	// if an account has storage (and that account is newly created and doesn't have storage)
	// the cache will say that there is no known storage in which case we will still need to
	// check in the DB to be absolutely sure anyway (this deems such an "optimisation" useless)
	return cr.r.HasStorage(address)
}

// ReadAccountCode is called when code of an account needs to be fetched from the state
// Usually, one of (address;incarnation) or codeHash is enough to uniquely identify the code
func (cr *CachedReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	addrValue := address.Value()
	if c, ok := cr.cache.GetCode(addrValue[:], 1); ok {
		return c, nil
	}
	c, err := cr.r.ReadAccountCode(address)
	if err != nil {
		return nil, err
	}
	if cr.cache != nil && len(c) <= 1024 {
		cr.cache.SetCodeRead(addrValue[:], 1, c)
	}
	return c, nil
}

func (cr *CachedReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	c, err := cr.ReadAccountCode(address)
	return len(c), err
}

// ReadAccountIncarnation is called when incarnation of the account is required (to create and recreate contract)
func (cr *CachedReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	addrValue := address.Value()
	deleted := cr.cache.GetDeletedAccount(addrValue[:])
	if deleted != nil {
		return deleted.Incarnation, nil
	}
	return cr.r.ReadAccountIncarnation(address)
}
