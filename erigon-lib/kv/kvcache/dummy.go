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

	remote "github.com/erigontech/erigon/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/erigon-lib/kv"
)

// DummyCache - doesn't remember anything - can be used when service is not remote
type DummyCache struct {
	stateV3 bool
}

var _ Cache = (*DummyCache)(nil)    // compile-time interface check
var _ CacheView = (*DummyView)(nil) // compile-time interface check

func NewDummy() *DummyCache { return &DummyCache{stateV3: true} }
func (c *DummyCache) View(_ context.Context, tx kv.Tx) (CacheView, error) {
	return &DummyView{cache: c, tx: tx}, nil
}
func (c *DummyCache) OnNewBlock(sc *remote.StateChangeBatch) {}
func (c *DummyCache) Evict() int                             { return 0 }
func (c *DummyCache) Len() int                               { return 0 }
func (c *DummyCache) Get(k []byte, tx kv.Tx, id uint64) ([]byte, error) {
	if c.stateV3 {
		if len(k) == 20 {
			v, _, err := tx.(kv.TemporalTx).GetLatest(kv.AccountsDomain, k, nil)
			return v, err
		}
		v, _, err := tx.(kv.TemporalTx).GetLatest(kv.StorageDomain, k, nil)
		return v, err
	}
	return tx.GetOne(kv.PlainState, k)
}
func (c *DummyCache) GetCode(k []byte, tx kv.Tx, id uint64) ([]byte, error) {
	if c.stateV3 {
		v, _, err := tx.(kv.TemporalTx).GetLatest(kv.CodeDomain, k, nil)
		return v, err
	}
	return tx.GetOne(kv.Code, k)
}
func (c *DummyCache) ValidateCurrentRoot(_ context.Context, _ kv.Tx) (*CacheValidationResult, error) {
	return &CacheValidationResult{Enabled: false}, nil
}

type DummyView struct {
	cache *DummyCache
	tx    kv.Tx
}

func (c *DummyView) StateV3() bool                    { return c.cache.stateV3 }
func (c *DummyView) Get(k []byte) ([]byte, error)     { return c.cache.Get(k, c.tx, 0) }
func (c *DummyView) GetCode(k []byte) ([]byte, error) { return c.cache.GetCode(k, c.tx, 0) }
