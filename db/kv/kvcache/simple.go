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
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// SimpleCache is a minimal cache for in-process use. It does not track versions
// — it simply remembers account data from the most recent OnNewBlock batch so
// that callers (e.g. the txpool) see the correct nonce/balance even when their
// read-only DB transaction was opened before the data was committed to the
// database. The map is replaced on every OnNewBlock call, so memory stays
// proportional to one batch (~400 addresses per 60 M-gas block).
type SimpleCache struct {
	mu       sync.RWMutex
	accounts map[common.Address][]byte // address → latest serialised account
}

var _ Cache = (*SimpleCache)(nil)    // compile-time interface check
var _ CacheView = (*SimpleView)(nil) // compile-time interface check

func NewSimple() *SimpleCache { return &SimpleCache{} }
func (c *SimpleCache) View(_ context.Context, tx kv.TemporalTx) (CacheView, error) {
	return &SimpleView{cache: c, tx: tx}, nil
}
func (c *SimpleCache) OnNewBlock(sc *remoteproto.StateChangeBatch) {
	if sc == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Replace the map — only the latest batch's data is needed.
	// By the time the next batch arrives, the DB commit for the
	// previous batch should be visible to new RO transactions.
	c.accounts = nil
	for _, change := range sc.ChangeBatch {
		for i := range change.Changes {
			switch change.Changes[i].Action {
			case remoteproto.Action_UPSERT, remoteproto.Action_UPSERT_CODE:
				addr := gointerfaces.ConvertH160toAddress(change.Changes[i].Address)
				if c.accounts == nil {
					c.accounts = make(map[common.Address][]byte)
				}
				c.accounts[addr] = common.Copy(change.Changes[i].Data)
			case remoteproto.Action_REMOVE:
				addr := gointerfaces.ConvertH160toAddress(change.Changes[i].Address)
				delete(c.accounts, addr)
			}
		}
	}
}
func (c *SimpleCache) Evict() int { return 0 }
func (c *SimpleCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.accounts)
}
func (c *SimpleCache) Get(k []byte, tx kv.TemporalTx, id uint64) ([]byte, error) {
	// Check the in-memory account cache first (populated by OnNewBlock).
	if len(k) == 20 {
		c.mu.RLock()
		if v, ok := c.accounts[common.BytesToAddress(k)]; ok {
			c.mu.RUnlock()
			return common.Copy(v), nil
		}
		c.mu.RUnlock()
		v, _, err := tx.GetLatest(kv.AccountsDomain, k)
		return v, err
	}
	v, _, err := tx.GetLatest(kv.StorageDomain, k)
	return v, err
}
func (c *SimpleCache) GetCode(k []byte, tx kv.TemporalTx, id uint64) ([]byte, error) {
	v, _, err := tx.GetLatest(kv.CodeDomain, k)
	return v, err
}
func (c *SimpleCache) ValidateCurrentRoot(_ context.Context, _ kv.TemporalTx) (*CacheValidationResult, error) {
	return &CacheValidationResult{Enabled: false}, nil
}

type SimpleView struct {
	cache *SimpleCache
	tx    kv.TemporalTx
}

func (c *SimpleView) Get(k []byte) ([]byte, error) { return c.cache.Get(k, c.tx, 0) }
func (c *SimpleView) GetAsOf(key []byte, ts uint64) (v []byte, ok bool, err error) {
	return nil, false, nil
}
func (c *SimpleView) GetCode(k []byte) ([]byte, error) { return c.cache.GetCode(k, c.tx, 0) }
func (c *SimpleView) HasStorage(address common.Address) (bool, error) {
	_, _, hasStorage, err := c.tx.HasPrefix(kv.StorageDomain, address[:])
	return hasStorage, err
}
