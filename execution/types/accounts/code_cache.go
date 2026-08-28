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

package accounts

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/c2h5oh/datasize"
)

// Deploying the same bytecode at many addresses is the norm — proxies, token
// clones, factory output — and each deployment otherwise keeps its own copy of
// bytes that Code's invariant already forbids mutating. Interning by the
// interned CodeHash collapses them to one.
const (
	codeCacheMaxBytes = 128 * datasize.MB

	// Above this a single entry would take a large share of the budget, and the
	// deployments that repeat are far below it.
	codeCacheMaxEntryBytes = 64 * datasize.KB
)

// Write-once, read-many: a hash's bytes never change, so readers take no lock.
// The counter is advisory — it trails concurrent stores — which is all a budget
// needs, since going over costs memory rather than correctness.
var (
	codeCache      sync.Map // CodeHash -> []byte
	codeCacheBytes atomic.Int64
)

// internCodeBytes returns the cache's copy of code. The cache clones on insert
// rather than adopting: callers pass slices that alias a domain mmap a
// background merge can unmap, and this outlives any of them. The result must
// not be modified — one backing array serves every caller with the same hash.
func internCodeBytes(hash CodeHash, code []byte) []byte {
	if datasize.ByteSize(len(code)) > codeCacheMaxEntryBytes {
		return code
	}
	if shared, ok := codeCache.Load(hash); ok {
		return shared.([]byte)
	}

	stored := bytes.Clone(code)
	actual, loaded := codeCache.LoadOrStore(hash, stored)
	if loaded {
		// Another caller interned the same code first; its copy is the shared one.
		return actual.([]byte)
	}

	// Dropping every entry at once re-clones whatever is still live, which costs
	// less than tracking ages: a slice already handed out stays valid on its own.
	if codeCacheBytes.Add(int64(len(stored))) > int64(codeCacheMaxBytes) {
		codeCache.Clear()
		codeCacheBytes.Store(0)
	}
	return stored
}
