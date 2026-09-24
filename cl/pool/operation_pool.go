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

package pool

import (
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
)

const lifeSpan = 30 * time.Minute

type OperationPool[K comparable, T any] struct {
	pool                 *lru.Cache[K, T] // Map the Signature to the underlying object
	recentlySeen         sync.Map         // map from K to time.Time
	persistentIdentities map[K]any
	persistentMu         sync.Mutex
	pruneMu              sync.Mutex
	lastPruned           time.Time
}

func NewOperationPool[K comparable, T any](capacity int, matricName string) *OperationPool[K, T] {
	pool, err := lru.New[K, T](matricName, capacity)
	if err != nil {
		panic(err)
	}
	return &OperationPool[K, T]{
		pool:                 pool,
		recentlySeen:         sync.Map{},
		persistentIdentities: make(map[K]any),
	}
}

func (o *OperationPool[K, T]) Insert(k K, operation T) {
	if _, ok := o.recentlySeen.Load(k); ok {
		return
	}
	o.put(k, operation)
}

func (o *OperationPool[K, T]) put(k K, operation T) {
	o.pool.Add(k, operation)
	o.recordRecentlySeen(k)
}

func (o *OperationPool[K, T]) recordRecentlySeen(k K) {
	now := time.Now()
	o.pruneRecentlySeen(now)
	o.recentlySeen.LoadOrStore(k, now)
}

func (o *OperationPool[K, T]) pruneRecentlySeen(now time.Time) {
	o.pruneMu.Lock()
	defer o.pruneMu.Unlock()
	if now.Sub(o.lastPruned) > lifeSpan {
		o.recentlySeen.Range(func(k, v any) bool {
			if now.Sub(v.(time.Time)) > lifeSpan {
				o.recentlySeen.Delete(k)
			}
			return true
		})
		o.lastPruned = now
	}
}
func (o *OperationPool[K, T]) restoreIfMissingWithPersistentIdentity(k K, operation T, identity any, matches func(any) bool) bool {
	o.persistentMu.Lock()
	defer o.persistentMu.Unlock()
	if first, ok := o.persistentIdentities[k]; ok {
		if !matches(first) {
			return false
		}
	} else {
		o.persistentIdentities[k] = identity
	}
	contained, _ := o.pool.ContainsOrAdd(k, operation)
	if contained {
		return false
	}
	o.recordRecentlySeen(k)
	return true
}

func (o *OperationPool[K, T]) persistentIdentity(k K) (any, bool) {
	o.persistentMu.Lock()
	defer o.persistentMu.Unlock()
	identity, ok := o.persistentIdentities[k]
	return identity, ok
}

func (o *OperationPool[K, T]) hasPersistentIdentities() bool {
	o.persistentMu.Lock()
	defer o.persistentMu.Unlock()
	return len(o.persistentIdentities) != 0
}

func (o *OperationPool[K, T]) prunePersistentIdentities(shouldDelete func(K, any) bool) {
	o.persistentMu.Lock()
	defer o.persistentMu.Unlock()
	for k, identity := range o.persistentIdentities {
		if shouldDelete(k, identity) {
			o.pool.Remove(k)
			delete(o.persistentIdentities, k)
		}
	}
}

func (o *OperationPool[K, T]) DeleteIfExist(k K) (removed bool) {
	return o.pool.Remove(k)
}

func (o *OperationPool[K, T]) Has(k K) (hash bool) {
	return o.pool.Contains(k)
}

func (o *OperationPool[K, T]) Raw() []T {
	return o.pool.Values()
}

func (o *OperationPool[K, T]) Len() int {
	return o.pool.Len()
}

func (o *OperationPool[K, T]) Get(k K) (T, bool) {
	return o.pool.Get(k)
}
