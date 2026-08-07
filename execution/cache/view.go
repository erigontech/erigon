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

// ReadView is a cache handle bound to one durable state version. Its zero value
// is inert. A publication invalidates the view before changing cache contents.
type ReadView struct {
	c          *StateCache
	generation *cacheGeneration
}

// View returns an inert handle unless stateVersion is the current durable
// version represented by the cache.
func (c *StateCache) View(stateVersion uint64) ReadView {
	if c == nil {
		return ReadView{}
	}
	generation := c.generationFor(stateVersion)
	if generation == nil {
		return ReadView{}
	}
	return ReadView{c: c, generation: generation}
}

func (v ReadView) current() bool {
	return v.c != nil && v.generation != nil && v.c.generation.Load() == v.generation
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
