// Copyright 2026 The Erigon Authors
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

import "github.com/erigontech/erigon/db/kv"

// ReadView is a cache handle bound to one durable database state and files
// view. It does not pin the cache or delay publication. Each read checks its
// immutable generation token before and after accessing an underlying cache,
// so concurrent publication turns the result into a miss.
//
// Fills check the same token while holding the cache admission lock. A value
// read from an old database snapshot therefore cannot enter a newer cache
// generation. The zero value is inert and makes callers fall back to the
// database.
type ReadView struct {
	c          *StateCache
	generation GenerationView
}

// View returns a live handle only when the cache currently represents the
// requested generation and no publication is in progress. Callers must derive
// it from their own pinned transaction. A mismatch returns an inert view.
func (c *StateCache) View(generation Generation) ReadView {
	if c == nil {
		return ReadView{}
	}
	return ReadView{c: c, generation: c.generation.View(generation)}
}

// Get retrieves data for the given domain and key.
// Returns (value, true) on cache hit — including (nil, true) for cached negatives —
// and (nil, false) on cache miss.
func (v ReadView) Get(domain kv.Domain, key []byte) ([]byte, bool) {
	value, _, ok := v.GetWithStep(domain, key)
	return value, ok
}

// GetWithStep also returns the entry's source step so an in-flight unwind can
// reject a hit above its per-key bound.
func (v ReadView) GetWithStep(domain kv.Domain, key []byte) ([]byte, kv.Step, bool) {
	value, step, ok := ReadCurrentWithStep(v.generation, func() ([]byte, uint64, bool) {
		value, step, ok := v.c.getWithStep(domain, key)
		return value, uint64(step), ok
	})
	return value, kv.Step(step), ok
}

// GetCodeByHash retrieves code bytes by their Ethereum codeHash (keccak256),
// bypassing the addr-keyed CodeDomain lookup. Returns (nil, false) on miss.
func (v ReadView) GetCodeByHash(codeHash []byte) ([]byte, bool) {
	return ReadCurrent(v.generation, func() ([]byte, bool) {
		return v.c.getCodeByHash(codeHash)
	})
}

// GetCodeSizeByHash returns the cached code length for codeHash.
func (v ReadView) GetCodeSizeByHash(codeHash []byte) (int, bool) {
	return ReadCurrent(v.generation, func() (int, bool) {
		return v.c.getCodeSizeByHash(codeHash)
	})
}

// GetAddrCodeHash returns the Ethereum codeHash for addr without an
// account-domain round-trip. The hash is zero when ok is false.
func (v ReadView) GetAddrCodeHash(addr []byte) ([32]byte, bool) {
	return ReadCurrent(v.generation, func() ([32]byte, bool) {
		return v.c.getAddrCodeHash(addr)
	})
}

func (v ReadView) canFill() bool {
	return v.c != nil && !v.c.disableFills && v.generation.Current()
}

// Fill offers a value read from this view. It never overwrites an authoritative
// entry, and final admission rejects a view revoked by concurrent publication.
func (v ReadView) Fill(domain kv.Domain, key, value []byte, step kv.Step) {
	if !v.canFill() {
		return
	}
	if domain == kv.CodeDomain {
		v.c.fillCode(v.generation, key, value, step)
		return
	}
	v.c.fill(v.generation, domain, key, value, step)
}

// SeedAddrCodeHash offers a binding derived from an account read in this view.
func (v ReadView) SeedAddrCodeHash(addr []byte, hash [32]byte) {
	if !v.canFill() {
		return
	}
	v.c.seedAddrCodeHash(v.generation, addr, hash)
}

// FillCodeSize records a derived size only while this generation is current.
func (v ReadView) FillCodeSize(codeHash []byte, size int) {
	if !v.canFill() {
		return
	}
	v.c.fillCodeSize(v.generation, codeHash, size)
}
