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

var operationsMultiplier = 20 // Cap the amount of cached element to max_operations_per_block * operations_multiplier

type OperationPool[K comparable, T any] struct {
	pool         *lru.Cache[K, T] // Map the Signature to the underlying object
	recentlySeen sync.Map         // map from K to time.Time
	lastPruned   time.Time
}

func NewOperationPool[K comparable, T any](maxOperationsPerBlock int, matricName string) *OperationPool[K, T] {
	pool, err := lru.New[K, T](matricName, maxOperationsPerBlock*operationsMultiplier)
	if err != nil {
		panic(err)
	}
	return &OperationPool[K, T]{
		pool:         pool,
		recentlySeen: sync.Map{},
	}
}

func (o *OperationPool[K, T]) Insert(k K, operation T) {
	if _, ok := o.recentlySeen.Load(k); ok {
		return
	}
	o.pool.Add(k, operation)
	o.recentlySeen.Store(k, time.Now())
	if time.Since(o.lastPruned) > lifeSpan {
		o.recentlySeen.Range(func(k, v interface{}) bool {
			if time.Since(v.(time.Time)) > lifeSpan {
				o.recentlySeen.Delete(k)
			}
			return true
		})
		o.lastPruned = time.Now()
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

func (o *OperationPool[K, T]) Get(k K) (T, bool) {
	return o.pool.Get(k)
}
