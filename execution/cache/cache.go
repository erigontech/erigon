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

// Package cache provides the process-global cache of latest committed state.
//
// StateCache represents one durable PlainStateVersion at a time. Read views
// are bound to that version, and a publication revokes them before changing
// cache contents. Snapshot-isolated caching is handled separately by kvcache.
package cache

import "github.com/erigontech/erigon/db/kv"

// Cache is the interface for domain caches.
// Implementations: DomainCache (for Account/Storage), CodeCache (for Code).
type Cache interface {
	Get(key []byte) (value []byte, ok bool)
	GetWithStep(key []byte) (value []byte, step kv.Step, ok bool)
	Put(key, value []byte, step kv.Step)
	PutIfAbsent(key, value []byte, step kv.Step)

	Delete(key []byte)
	Clear()
	Close()
	Len() int
}
