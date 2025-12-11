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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/turbo/shards"
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

func (cw *CachedWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if err := cw.w.UpdateAccountData(address, original, account); err != nil {
		return err
	}
	cw.cache.SetAccountWrite(address.Bytes(), account)
	return nil
}

func (cw *CachedWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := cw.w.UpdateAccountCode(address, incarnation, codeHash, code); err != nil {
		return err
	}
	cw.cache.SetCodeWrite(address.Bytes(), incarnation, code)
	return nil
}

func (cw *CachedWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	if err := cw.w.DeleteAccount(address, original); err != nil {
		return err
	}
	cw.cache.SetAccountDelete(address.Bytes())
	return nil
}

func (cw *CachedWriter) WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error {
	if err := cw.w.WriteAccountStorage(address, incarnation, key, original, value); err != nil {
		return err
	}
	if original == value {
		return nil
	}
	if value.IsZero() {
		cw.cache.SetStorageDelete(address.Bytes(), incarnation, key[:])
	} else {
		cw.cache.SetStorageWrite(address.Bytes(), incarnation, key[:], value.Bytes())
	}
	return nil
}

func (cw *CachedWriter) CreateContract(address common.Address) error {
	return cw.w.CreateContract(address)
}
