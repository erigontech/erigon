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
	"bytes"

	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/erigon-lib/kv/kvcache"

	"github.com/erigontech/erigon/core/types/accounts"
)

// CachedReader3 is a wrapper for an instance of type StateReader
// This wrapper only makes calls to the underlying reader if the item is not in the cache
type CachedReader3 struct {
	cache kvcache.CacheView
	db    kv.TemporalTx
}

// NewCachedReader3 wraps a given state reader into the cached reader
func NewCachedReader3(cache kvcache.CacheView, tx kv.TemporalTx) *CachedReader3 {
	return &CachedReader3{cache: cache, db: tx}
}

// ReadAccountData is called when an account needs to be fetched from the state
func (r *CachedReader3) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := r.cache.Get(address[:])
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	a := accounts.Account{}
	if err = accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *CachedReader3) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	compositeKey := append(address[:], key.Bytes()...)
	enc, err := r.cache.Get(compositeKey)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (r *CachedReader3) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash.Bytes(), emptyCodeHash) {
		return nil, nil
	}
	code, err := r.cache.GetCode(address[:])
	if len(code) == 0 {
		return nil, nil
	}
	return code, err
}

func (r *CachedReader3) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (r *CachedReader3) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}
