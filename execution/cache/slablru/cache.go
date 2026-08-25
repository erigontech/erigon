// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file is a modified copy of github.com/elastic/go-freelru v0.16.0.
// Unmodified except for the package name.

package slablru

import "time"

type Cache[K comparable, V any] interface {
	// SetLifetime sets the default lifetime of LRU elements.
	// Lifetime 0 means "forever".
	SetLifetime(lifetime time.Duration)

	// SetOnEvict sets the OnEvict callback function.
	// The onEvict function is called for each evicted lru entry.
	SetOnEvict(onEvict OnEvictCallback[K, V])

	// Len returns the number of elements stored in the cache.
	Len() int

	// AddWithLifetime adds a key:value to the cache with a lifetime.
	// Returns true, true if key was updated and eviction occurred.
	AddWithLifetime(key K, value V, lifetime time.Duration) (evicted bool)

	// Add adds a key:value to the cache.
	// Returns true, true if key was updated and eviction occurred.
	Add(key K, value V) (evicted bool)

	// Get returns the value associated with the key, setting it as the most
	// recently used item.
	// If the found cache item is already expired, the evict function is called
	// and the return value indicates that the key was not found.
	Get(key K) (V, bool)

	// GetAndRefresh returns the value associated with the key, setting it as the most
	// recently used item.
	// The lifetime of the found cache item is refreshed, even if it was already expired.
	GetAndRefresh(key K, lifetime time.Duration) (V, bool)

	// Peek looks up a key's value from the cache, without changing its recent-ness.
	// If the found entry is already expired, the evict function is called.
	Peek(key K) (V, bool)

	// Contains checks for the existence of a key, without changing its recent-ness.
	// If the found entry is already expired, the evict function is called.
	Contains(key K) bool

	// Remove removes the key from the cache.
	// The return value indicates whether the key existed or not.
	// The evict function is called for the removed entry.
	Remove(key K) bool

	// RemoveOldest removes the oldest entry from the cache.
	// Key, value and an indicator of whether the entry has been removed is returned.
	// The evict function is called for the removed entry.
	RemoveOldest() (key K, value V, removed bool)

	// Keys returns a slice of the keys in the cache, from oldest to newest.
	// Expired entries are not included.
	// The evict function is called for each expired item.
	Keys() []K

	// Purge purges all data (key and value) from the LRU.
	// The evict function is called for each expired item.
	// The LRU metrics are reset.
	Purge()

	// PurgeExpired purges all expired items from the LRU.
	// The evict function is called for each expired item.
	PurgeExpired()

	// Metrics returns the metrics of the cache.
	Metrics() Metrics

	// ResetMetrics resets the metrics of the cache and returns the previous state.
	ResetMetrics() Metrics
}
