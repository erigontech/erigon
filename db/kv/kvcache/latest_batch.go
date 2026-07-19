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

// LatestBatchCache serves the most recent announced state-change batch on top
// of the caller's tx — freshness over snapshot-consistency, with staleness
// bounded to one batch by FCU serialization (contrast Coherent, which keys
// entries by state version so readers stay consistent with their own
// snapshot). It does not track versions: the map remembers account data from
// the latest OnNewBlock batch so that callers (e.g. the txpool) see the
// announced nonce/balance even when their read-only DB transaction was opened
// before that data was committed. The map is replaced on every batch, so
// memory stays proportional to one batch (~400 addresses per 60 M-gas block).
type LatestBatchCache struct {
	mu       sync.RWMutex
	accounts map[common.Address][]byte // address → latest serialised account
}

var _ Cache = (*LatestBatchCache)(nil)    // compile-time interface check
var _ CacheView = (*LatestBatchView)(nil) // compile-time interface check

func NewLatestBatchCache() *LatestBatchCache { return &LatestBatchCache{} }
func (c *LatestBatchCache) View(_ context.Context, tx kv.TemporalTx) (CacheView, error) {
	return &LatestBatchView{cache: c, tx: tx}, nil
}
func (c *LatestBatchCache) OnNewBlock(sc *remoteproto.StateChangeBatch) {
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
func (c *LatestBatchCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.accounts)
}
func (c *LatestBatchCache) Get(k []byte, tx kv.TemporalTx, id uint64) ([]byte, error) {
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
func (c *LatestBatchCache) GetCode(k []byte, tx kv.TemporalTx, id uint64) ([]byte, error) {
	v, _, err := tx.GetLatest(kv.CodeDomain, k)
	return v, err
}
func (c *LatestBatchCache) ValidateCurrentRoot(_ context.Context, _ kv.TemporalTx) (*CacheValidationResult, error) {
	return &CacheValidationResult{Enabled: false}, nil
}

type LatestBatchView struct {
	cache *LatestBatchCache
	tx    kv.TemporalTx
}

func (c *LatestBatchView) Get(k []byte) ([]byte, error) { return c.cache.Get(k, c.tx, 0) }

// The cache holds latest-state only, so historical reads always fall through.
func (c *LatestBatchView) GetAsOf(key []byte, ts uint64) (v []byte, ok bool, err error) {
	return nil, false, nil
}
func (c *LatestBatchView) GetCode(k []byte) ([]byte, error) { return c.cache.GetCode(k, c.tx, 0) }
func (c *LatestBatchView) HasStorage(address common.Address) (bool, error) {
	_, _, hasStorage, err := c.tx.HasPrefix(kv.StorageDomain, address[:])
	return hasStorage, err
}
