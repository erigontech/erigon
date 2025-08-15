// Copyright 2021 The Erigon Authors
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

package kvcache

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/db/kv"
)

// DummyCache - doesn't remember anything - can be used when service is not remote
type DummyCache struct {
}

var _ Cache = (*DummyCache)(nil)    // compile-time interface check
var _ CacheView = (*DummyView)(nil) // compile-time interface check

func NewDummy() *DummyCache { return &DummyCache{} }
func (c *DummyCache) View(_ context.Context, tx kv.TemporalTx) (CacheView, error) {
	return &DummyView{cache: c, tx: tx}, nil
}
func (c *DummyCache) OnNewBlock(sc *remote.StateChangeBatch) {}
func (c *DummyCache) Evict() int                             { return 0 }
func (c *DummyCache) Len() int                               { return 0 }
func (c *DummyCache) Get(k []byte, tx kv.TemporalTx, id uint64) ([]byte, error) {
	if len(k) == 20 {
		v, _, err := tx.GetLatest(kv.AccountsDomain, k)
		return v, err
	}
	v, _, err := tx.GetLatest(kv.StorageDomain, k)
	return v, err
}
func (c *DummyCache) GetCode(k []byte, tx kv.TemporalTx, id uint64) ([]byte, error) {
	v, _, err := tx.GetLatest(kv.CodeDomain, k)
	return v, err
}
func (c *DummyCache) ValidateCurrentRoot(_ context.Context, _ kv.Tx) (*CacheValidationResult, error) {
	return &CacheValidationResult{Enabled: false}, nil
}

type DummyView struct {
	cache *DummyCache
	tx    kv.TemporalTx
}

func (c *DummyView) Get(k []byte) ([]byte, error)     { return c.cache.Get(k, c.tx, 0) }
func (c *DummyView) GetCode(k []byte) ([]byte, error) { return c.cache.GetCode(k, c.tx, 0) }
func (c *DummyView) HasStorage(address common.Address) (bool, error) {
	_, _, hasStorage, err := c.tx.HasPrefix(kv.StorageDomain, address[:])
	return hasStorage, err
}
