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
// StateCache represents exactly one Generation at a time: one durable
// PlainStateVersion over one compatible immutable-files view. It does not keep
// old generations; a transaction with another identity receives an inert
// ReadView and reads from its own database snapshot instead.
//
// Publishing canonical state revokes the current generation before changing
// entries and exposes the next generation only after the database commit.
// This keeps concurrent readers on one complete generation even though the
// cache is process-global. Multi-version snapshot caching remains the
// responsibility of kvcache.
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
