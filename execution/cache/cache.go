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

package cache

// Cache is the interface for domain caches.
// Implementations: GenericCache (for Account/Storage), CodeCache (for Code).
type Cache interface {
	// Get retrieves data for the given key.
	Get(key []byte) ([]byte, bool)

	// Put stores data for the given key, stamped with the txNum the value
	// reflects (used for txNum/epoch unwind invalidation).
	Put(key []byte, value []byte, txNum uint64)

	// Delete removes the data for the given key.
	Delete(key []byte)

	// Clear removes all mutable entries from the cache.
	Clear()

	// Unwind invalidates entries that reflect state above unwindToTxNum on a
	// now-dead fork. Diffset-free: GenericCache bumps an epoch and lowers a
	// floor (lazy evict on read); CodeCache clears its small mutable addr
	// layers. Immutable content-addressed code layers are untouched.
	Unwind(unwindToTxNum uint64)

	// Len returns the number of entries in the cache.
	Len() int
}
