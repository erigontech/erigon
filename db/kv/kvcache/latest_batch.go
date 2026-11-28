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
	"bytes"
	"context"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
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
// announced nonce, balance and code even when their read-only DB transaction was opened
// before that data was committed. The map is replaced on every batch, so
// memory stays proportional to one batch (~400 addresses per 60 M-gas block).
type LatestBatchCache struct {
	mu       sync.RWMutex
	accounts map[common.Address][]byte // address → latest serialised account
	code     map[common.Address][]byte
}

var (
	_ Cache     = (*LatestBatchCache)(nil) // compile-time interface check
	_ CacheView = (*LatestBatchView)(nil)  // compile-time interface check
)

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
	c.code = nil
	for _, change := range sc.ChangeBatch {
		for _, account := range change.Changes {
			if account.Action == remoteproto.Action_STORAGE {
				continue
			}
			addr := gointerfaces.ConvertH160toAddress(account.Address)
			switch account.Action {
			case remoteproto.Action_UPSERT_CODE, remoteproto.Action_CODE, remoteproto.Action_REMOVE:
				if c.code == nil {
					c.code = make(map[common.Address][]byte)
				}
				var code []byte
				if account.Action != remoteproto.Action_REMOVE {
					code = bytes.Clone(account.Code)
				}
				c.code[addr] = code
			}
			var data []byte
			switch account.Action {
			case remoteproto.Action_UPSERT, remoteproto.Action_UPSERT_CODE:
				data = bytes.Clone(account.Data)
			case remoteproto.Action_REMOVE:
				// Retain a nil entry: the database snapshot may still contain the deleted account.
			default:
				continue
			}
			if c.accounts == nil {
				c.accounts = make(map[common.Address][]byte)
			}
			c.accounts[addr] = data
		}
	}
}

func (c *LatestBatchCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.accounts)
}

func (c *LatestBatchCache) Get(k []byte, tx kv.TemporalTx) ([]byte, error) {
	// Check the in-memory account cache first (populated by OnNewBlock).
	if len(k) == 20 {
		c.mu.RLock()
		if v, ok := c.accounts[common.BytesToAddress(k)]; ok {
			c.mu.RUnlock()
			return bytes.Clone(v), nil
		}
		c.mu.RUnlock()
		v, _, err := tx.GetLatest(kv.AccountsDomain, k, kv.GetLatestOptions{})
		return v, err
	}
	v, _, err := tx.GetLatest(kv.StorageDomain, k, kv.GetLatestOptions{})
	return v, err
}

func (c *LatestBatchCache) GetCode(k []byte, tx kv.TemporalTx) ([]byte, error) {
	if len(k) == length.Addr {
		c.mu.RLock()
		code, ok := c.code[common.Address(k)]
		c.mu.RUnlock()
		if ok {
			return bytes.Clone(code), nil
		}
	}
	v, _, err := tx.GetLatest(kv.CodeDomain, k, kv.GetLatestOptions{})
	return v, err
}

func (c *LatestBatchCache) ValidateCurrentRoot(_ context.Context, _ kv.TemporalTx) (*CacheValidationResult, error) {
	return &CacheValidationResult{Enabled: false}, nil
}

type LatestBatchView struct {
	cache *LatestBatchCache
	tx    kv.TemporalTx
}

func (c *LatestBatchView) Get(k []byte) ([]byte, error) { return c.cache.Get(k, c.tx) }

// The cache holds latest-state only, so historical reads always fall through.
func (c *LatestBatchView) GetAsOf(key []byte, ts uint64) (v []byte, ok bool, err error) {
	return nil, false, nil
}
func (c *LatestBatchView) GetCode(k []byte) ([]byte, error) { return c.cache.GetCode(k, c.tx) }
