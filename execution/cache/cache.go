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

// Package cache provides the process-global caches of latest committed state.
//
// StateCache holds the newest applied value per key for the accounts, storage
// and code domains, so repeated GetLatest reads skip the file-accessor/MDBX
// stack. It is not a snapshot and gives readers no isolation: a hit can be
// newer than the reader's tx (snapshot-isolated caching is kvcache's job,
// node/shards). In the forward direction its invariant is monotonicity:
// content never regresses behind what has been applied. Unwinds invalidate
// by epoch and floor instead.
//
// StateCache itself has no data methods. A ReadView — bound to one tx's read
// view and not outliving it — serves reads and fills (cache writes made on
// behalf of a database reader after a miss); admission compares the view's
// frontier — the exclusive txNum end of what its tx can see, so a view with
// frontier N sees txNums < N — against the applied end, under the same lock
// applies take. The Applier handle, held by the SharedDomains
// commit/unwind path, performs the authoritative writes: post-commit
// applies, unwinds, clears.
package cache

// Cache is the interface for domain caches.
// Implementations: GenericCache (for Account/Storage), CodeCache (for Code).
type Cache interface {
	// Get retrieves data for the given key.
	Get(key []byte) ([]byte, bool)

	// GetWithTxNum is Get plus the txNum the cached value reflects, so the
	// read path can apply a step bound against an in-flight unwind's maxStep.
	GetWithTxNum(key []byte) ([]byte, uint64, bool)

	// Put stores data for the given key, stamped with the txNum the value
	// reflects (used for txNum/epoch unwind invalidation).
	Put(key []byte, value []byte, txNum uint64)

	// PutIfAbsent is Put except that a live entry for key is left untouched
	// (a stale one is replaced) — for fill writers, whose read view may
	// already be superseded by an authoritative Put.
	PutIfAbsent(key []byte, value []byte, txNum uint64)

	// Delete removes the data for the given key.
	Delete(key []byte)

	// Clear removes all mutable entries from the cache.
	Clear()

	// Unwind invalidates entries that reflect state above unwindToTxNum on a
	// now-dead fork. Diffset-free and lazy: both GenericCache and CodeCache
	// bump an epoch and lower a floor, so stale entries (including code and
	// size) are evicted on the next read rather than walked eagerly.
	Unwind(unwindToTxNum uint64)

	// Close drops the cache's slot in the shared memory envelope. Idempotent.
	Close()

	// Len returns the number of entries in the cache.
	Len() int
}
