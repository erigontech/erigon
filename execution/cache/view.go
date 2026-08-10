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

func (v ReadView) current() bool {
	return v.c != nil && v.generation.Current()
}

func (v ReadView) Get(domain kv.Domain, key []byte) ([]byte, bool) {
	value, _, ok := v.GetWithStep(domain, key)
	return value, ok
}

func (v ReadView) GetWithStep(domain kv.Domain, key []byte) ([]byte, kv.Step, bool) {
	if !v.current() {
		return nil, 0, false
	}
	value, step, ok := v.c.getWithStep(domain, key)
	if !v.current() {
		return nil, 0, false
	}
	return value, step, ok
}

func (v ReadView) GetCodeByHash(codeHash []byte) ([]byte, bool) {
	if !v.current() {
		return nil, false
	}
	value, ok := v.c.getCodeByHash(codeHash)
	if !v.current() {
		return nil, false
	}
	return value, ok
}

func (v ReadView) GetCodeSizeByHash(codeHash []byte) (int, bool) {
	if !v.current() {
		return 0, false
	}
	size, ok := v.c.getCodeSizeByHash(codeHash)
	if !v.current() {
		return 0, false
	}
	return size, ok
}

func (v ReadView) GetAddrCodeHash(addr []byte) ([32]byte, bool) {
	if !v.current() {
		return [32]byte{}, false
	}
	hash, ok := v.c.getAddrCodeHash(addr)
	if !v.current() {
		return [32]byte{}, false
	}
	return hash, ok
}

func (v ReadView) canFill() bool {
	return v.current() && !v.c.disableFills
}

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

func (v ReadView) SeedAddrCodeHash(addr []byte, hash [32]byte) {
	if !v.canFill() {
		return
	}
	v.c.seedAddrCodeHash(v.generation, addr, hash)
}

func (v ReadView) FillCodeSize(codeHash []byte, size int) {
	if !v.canFill() {
		return
	}
	v.c.fillCodeSize(v.generation, codeHash, size)
}
