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

import "github.com/erigontech/erigon/common"

// Cache is the interface for domain caches.
// Implementations: GenericCache (for Account/Storage), CodeCache (for Code).
type Cache interface {
	// Get retrieves data for the given key.
	Get(key []byte) ([]byte, bool)

	// Put stores data for the given key.
	Put(key []byte, value []byte)

	// Delete removes the data for the given key.
	Delete(key []byte)

	// Clear removes all mutable entries from the cache.
	Clear()

	// GetBlockHash returns the hash of the last block processed.
	GetBlockHash() common.Hash

	// SetBlockHash sets the hash of the current block being processed.
	SetBlockHash(hash common.Hash)

	// ValidateAndPrepare checks if parentHash matches the cache's blockHash.
	// If mismatch, clears mutable caches. Returns true if valid, false if cleared.
	ValidateAndPrepare(parentHash common.Hash, incomingBlockHash common.Hash) bool

	// ClearWithHash clears the cache and sets the block hash.
	ClearWithHash(hash common.Hash)

	// Len returns the number of entries in the cache.
	Len() int
}
