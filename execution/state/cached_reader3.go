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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// CachedReader3 is a wrapper for an instance of type StateReader
// This wrapper only makes calls to the underlying reader if the item is not in the cache
type CachedReader3 struct {
	cache kvcache.CacheView
	db    kv.TemporalTx
	// addr and storageKey are the keys the next read looks up. As fields of the heap-allocated
	// reader their slices reach the cache interface without escaping a local; the cache clones
	// any key it keeps, and one reader serves one request.
	addr       common.Address
	storageKey [length.Addr + length.Hash]byte
}

// NewCachedReader3 wraps a given state reader into the cached reader
func NewCachedReader3(cache kvcache.CacheView, tx kv.TemporalTx) *CachedReader3 {
	return &CachedReader3{cache: cache, db: tx}
}

func (r *CachedReader3) SetTrace(_ bool, _ string) {}
func (r *CachedReader3) Trace() bool               { return false }
func (r *CachedReader3) TracePrefix() string       { return "" }

// ReadAccountData is called when an account needs to be fetched from the state
func (r *CachedReader3) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	r.addr = address.Value()
	enc, err := r.cache.Get(r.addr[:])
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	a := accounts.Account{}
	if err := accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *CachedReader3) HasAccount(address accounts.Address) (bool, error) {
	r.addr = address.Value()
	enc, err := r.cache.Get(r.addr[:])
	return len(enc) > 0, err
}

// ReadAccountDataForDebug - is like ReadAccountData, but without adding key to `readList`.
// Used to get `prev` account balance
func (r *CachedReader3) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (r *CachedReader3) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addressValue := address.Value()
	keyValue := key.Value()
	copy(r.storageKey[:], addressValue[:])
	copy(r.storageKey[length.Addr:], keyValue[:])
	enc, err := r.cache.Get(r.storageKey[:])
	if err != nil {
		return uint256.Int{}, false, err
	}
	if len(enc) == 0 {
		return uint256.Int{}, false, err
	}
	var v uint256.Int
	(&v).SetBytes(enc)
	return v, true, nil
}

func (r *CachedReader3) ReadAccountCode(address accounts.Address) ([]byte, error) {
	r.addr = address.Value()
	code, err := r.cache.GetCode(r.addr[:])
	if err != nil {
		return nil, err
	}
	if len(code) == 0 {
		return nil, nil
	}
	return code, nil
}

func (r *CachedReader3) ReadAccountCodeSize(address accounts.Address) (int, error) {
	code, err := r.ReadAccountCode(address)
	return len(code), err
}

func (r *CachedReader3) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return 0, nil
}
