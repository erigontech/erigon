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

// CachedWriter is a wrapper for an instance of type StateWriter
type CachedWriter struct {
	w     StateWriter
	cache *shards.StateCache
}

// NewCachedWriter wraps a given state writer into a cached writer
func NewCachedWriter(w StateWriter, cache *shards.StateCache) *CachedWriter {
	return &CachedWriter{w: w, cache: cache}
}

func (cw *CachedWriter) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	if err := cw.w.UpdateAccountData(address, original, account); err != nil {
		return err
	}
	addressValue := address.Value()
	cw.cache.SetAccountWrite(addressValue[:], account)
	return nil
}

func (cw *CachedWriter) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	if err := cw.w.UpdateAccountCode(address, 1, codeHash, code); err != nil {
		return err
	}
	addressValue := address.Value()
	cw.cache.SetCodeWrite(addressValue[:], 1, code)
	return nil
}

func (cw *CachedWriter) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	if err := cw.w.DeleteAccount(address, original); err != nil {
		return err
	}
	addressValue := address.Value()
	cw.cache.SetAccountDelete(addressValue[:])
	return nil
}

func (cw *CachedWriter) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	if err := cw.w.WriteAccountStorage(address, 1, key, original, value); err != nil {
		return err
	}
	if original == value {
		return nil
	}
	addressValue := address.Value()
	keyValue := key.Value()
	if value.IsZero() {
		cw.cache.SetStorageDelete(addressValue[:], 1, keyValue[:])
	} else {
		cw.cache.SetStorageWrite(addressValue[:], 1, keyValue[:], value.Bytes())
	}
	return nil
}

func (cw *CachedWriter) CreateContract(address accounts.Address) error {
	return cw.w.CreateContract(address)
}
