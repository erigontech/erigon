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
	"errors"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// LatestBatchCache overlays account changes on their source snapshot.
type LatestBatchCache struct {
	mu               sync.RWMutex
	accounts         map[common.Address][]byte
	previous, target *remoteproto.ExecutionView
	reset            bool
}

var (
	_ Cache     = (*LatestBatchCache)(nil) // compile-time interface check
	_ CacheView = (*LatestBatchView)(nil)  // compile-time interface check
)

func NewLatestBatchCache() *LatestBatchCache { return &LatestBatchCache{} }

var ErrExecutionViewUnavailable = errors.New("execution view is not available")

func MatchesExecutionView(tx kv.Tx, view *remoteproto.ExecutionView) (bool, error) {
	if view == nil || view.BlockHash == nil {
		return false, nil
	}
	version, err := rawdb.GetStateVersion(tx)
	if err != nil {
		return false, err
	}
	return version == view.StateVersionId && rawdb.ReadHeadBlockHash(tx) == common.Hash(gointerfaces.ConvertH256ToHash(view.BlockHash)), nil
}

func (c *LatestBatchCache) View(_ context.Context, tx kv.TemporalTx) (CacheView, error) {
	c.mu.RLock()
	accounts, previous, target, reset := c.accounts, c.previous, c.target, c.reset
	c.mu.RUnlock()
	if target != nil {
		matches, err := MatchesExecutionView(tx, target)
		if err != nil {
			return nil, err
		}
		if !matches && !reset {
			matches, err = MatchesExecutionView(tx, previous)
			if err != nil {
				return nil, err
			}
		}
		if !matches {
			return nil, ErrExecutionViewUnavailable
		}
	}
	return &LatestBatchView{accounts: accounts, tx: tx}, nil
}

func (c *LatestBatchCache) OnNewBlock(sc *remoteproto.StateChangeBatch) {
	if sc == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.previous, c.target, c.reset = sc.PreviousView, sc.TargetView, sc.StateChangesIncomplete
	c.accounts = nil
	if sc.StateChangesIncomplete {
		return
	}
	for _, change := range sc.ChangeBatch {
		for i := range change.Changes {
			switch change.Changes[i].Action {
			case remoteproto.Action_UPSERT, remoteproto.Action_UPSERT_CODE:
				addr := gointerfaces.ConvertH160toAddress(change.Changes[i].Address)
				if c.accounts == nil {
					c.accounts = make(map[common.Address][]byte)
				}
				c.accounts[addr] = bytes.Clone(change.Changes[i].Data)
			case remoteproto.Action_REMOVE:
				addr := gointerfaces.ConvertH160toAddress(change.Changes[i].Address)
				if c.accounts == nil {
					c.accounts = make(map[common.Address][]byte)
				}
				c.accounts[addr] = nil
			}
		}
	}
}

func (c *LatestBatchCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.accounts)
}

func (c *LatestBatchCache) ValidateCurrentRoot(_ context.Context, _ kv.TemporalTx) (*CacheValidationResult, error) {
	return &CacheValidationResult{Enabled: false}, nil
}

type LatestBatchView struct {
	accounts map[common.Address][]byte
	tx       kv.TemporalTx
}

func (c *LatestBatchView) Get(k []byte) ([]byte, error) {
	domain := kv.StorageDomain
	if len(k) == 20 {
		if v, ok := c.accounts[common.BytesToAddress(k)]; ok {
			return bytes.Clone(v), nil
		}
		domain = kv.AccountsDomain
	}
	v, _, err := c.tx.GetLatest(domain, k, kv.GetLatestOptions{})
	return v, err
}

func (c *LatestBatchView) GetCode(k []byte) ([]byte, error) {
	v, _, err := c.tx.GetLatest(kv.CodeDomain, k, kv.GetLatestOptions{})
	return v, err
}

func (c *LatestBatchView) GetMany(keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	var missing [][]byte
	var indices []int
	for i, key := range keys {
		if value, ok := c.accounts[common.BytesToAddress(key)]; ok {
			values[i] = bytes.Clone(value)
		} else {
			missing = append(missing, key)
			indices = append(indices, i)
		}
	}
	for start := 0; start < len(missing); start += 4096 {
		end := min(start+4096, len(missing))
		if batch, ok := c.tx.(interface {
			GetLatestBatch(kv.Domain, [][]byte) ([][]byte, error)
		}); ok {
			found, err := batch.GetLatestBatch(kv.AccountsDomain, missing[start:end])
			if err != nil {
				return nil, err
			}
			for i, value := range found {
				values[indices[start+i]] = value
			}
		} else {
			for i := start; i < end; i++ {
				value, err := c.Get(missing[i])
				if err != nil {
					return nil, err
				}
				values[indices[i]] = value
			}
		}
	}
	return values, nil
}
