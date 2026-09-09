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

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
)

// A budget must hold whatever the contract-size distribution turns out to be.
// An entry-count bound derived from an assumed average retained 5.3x the
// configured bytes when every contract was 64 KiB, which OOMed devnet nodes.
func TestCodeCacheStaysWithinByteBudget(t *testing.T) {
	for _, codeLen := range []int{64 * 1024, 24 * 1024, 1024} {
		t.Run(datasize.ByteSize(codeLen).HR(), func(t *testing.T) {
			const budget = 4 * datasize.MB
			cache := closeOnCleanup(t, NewCodeCache(budget, 1*datasize.MB))

			for i := range 4000 {
				code := make([]byte, codeLen)
				binary.BigEndian.PutUint64(code, uint64(i))
				h := crypto.Keccak256(code)
				cache.PutWithCodeHash(nil, code, h, uint64(i))
			}

			require.LessOrEqual(t, cache.CodeSizeBytes(), int64(budget),
				"resident code bytes exceed the configured budget")
			require.LessOrEqual(t, cache.codeHashCodeSize.Load(), int64(budget),
				"resident codeHash-layer bytes exceed the configured budget")
		})
	}
}

// Same bound when the writers are concurrent: charge-then-insert must not let
// in-flight puts accumulate past the budget.
func TestCodeCacheStaysWithinByteBudgetConcurrent(t *testing.T) {
	const budget = 4 * datasize.MB
	cache := closeOnCleanup(t, NewCodeCache(budget, 1*datasize.MB))

	var wg sync.WaitGroup
	for w := range 16 {
		wg.Go(func() {
			for i := range 500 {
				code := make([]byte, 64*1024)
				binary.BigEndian.PutUint64(code, uint64(w*500+i))
				cache.PutWithCodeHash(nil, code, crypto.Keccak256(code), 1)
			}
		})
	}
	wg.Wait()

	require.LessOrEqual(t, cache.CodeSizeBytes(), int64(budget))
	require.LessOrEqual(t, cache.codeHashCodeSize.Load(), int64(budget))
}
