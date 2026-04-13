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

package commitment

import (
	"context"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/db/kv"
)

// BenchmarkCachingPatriciaContext_ReadThrough measures the overhead of
// read-through caching compared to direct access.
func BenchmarkCachingPatriciaContext_ReadThrough(b *testing.B) {
	mock := newMockPatriciaContext()
	const numKeys = 1000
	for i := range numKeys {
		key := fmt.Sprintf("key-%04d", i)
		mock.branches[key] = mockBranch{data: []byte(key), step: kv.Step(i)}
		mock.accounts[key] = &Update{Flags: BalanceUpdate}
		mock.storage[key] = &Update{Flags: StorageUpdate}
	}

	b.Run("direct", func(b *testing.B) {
		// Baseline: read directly from mock (no caching layer).
		keys := make([][]byte, numKeys)
		for i := range numKeys {
			keys[i] = fmt.Appendf(nil, "key-%04d", i)
		}
		b.ResetTimer()
		for b.Loop() {
			for _, key := range keys {
				mock.Branch(key)  //nolint:errcheck
				mock.Account(key) //nolint:errcheck
				mock.Storage(key) //nolint:errcheck
			}
		}
	})

	b.Run("read-through", func(b *testing.B) {
		// Read-through: every call misses the cache and populates it.
		keys := make([][]byte, numKeys)
		for i := range numKeys {
			keys[i] = fmt.Appendf(nil, "key-%04d", i)
		}
		b.ResetTimer()
		for b.Loop() {
			cache := NewCachingPatriciaContext()
			view := cache.Wrap(mock)
			for _, key := range keys {
				view.Branch(key)  //nolint:errcheck
				view.Account(key) //nolint:errcheck
				view.Storage(key) //nolint:errcheck
			}
		}
	})
}

// BenchmarkCachingPatriciaContext_CacheHit measures the latency of
// cache hits (no underlying access).
func BenchmarkCachingPatriciaContext_CacheHit(b *testing.B) {
	mock := newMockPatriciaContext()
	const numKeys = 1000
	keys := make([][]byte, numKeys)
	for i := range numKeys {
		key := fmt.Sprintf("key-%04d", i)
		keys[i] = []byte(key)
		mock.branches[key] = mockBranch{data: []byte(key), step: kv.Step(i)}
		mock.accounts[key] = &Update{Flags: BalanceUpdate}
		mock.storage[key] = &Update{Flags: StorageUpdate}
	}

	// Pre-populate the cache.
	cache := NewCachingPatriciaContext()
	view := cache.Wrap(mock)
	for _, key := range keys {
		view.Branch(key)  //nolint:errcheck
		view.Account(key) //nolint:errcheck
		view.Storage(key) //nolint:errcheck
	}

	b.Run("branch", func(b *testing.B) {
		for b.Loop() {
			for _, key := range keys {
				view.Branch(key) //nolint:errcheck
			}
		}
	})

	b.Run("account", func(b *testing.B) {
		for b.Loop() {
			for _, key := range keys {
				view.Account(key) //nolint:errcheck
			}
		}
	})

	b.Run("storage", func(b *testing.B) {
		for b.Loop() {
			for _, key := range keys {
				view.Storage(key) //nolint:errcheck
			}
		}
	})

	// Report stats at the end.
	stats := cache.Stats()
	b.Logf("cache stats after benchmarks: %s", stats)
}

// BenchmarkWarmuper_TrieReader benchmarks the TrieReader-based warmuper
// against a baseline (no warmup) on a synthetic trie using MockState.
func BenchmarkWarmuper_TrieReader(b *testing.B) {
	const numKeys = 500

	// Build a synthetic state via UpdateBuilder.
	rnd := deterministicRand()
	builder := NewUpdateBuilder()
	for i := range numKeys {
		key := make([]byte, 20) // length.Addr
		rnd.Read(key)
		builder.Balance(fmt.Sprintf("%x", key), rnd.Uint64())
		_ = i
	}
	pk, updates := builder.Build()

	ms := NewMockState(b)
	if err := ms.applyPlainUpdates(pk, updates); err != nil {
		b.Fatal(err)
	}

	b.Run("no-warmup", func(b *testing.B) {
		for b.Loop() {
			hph := NewHexPatriciaHashed(20, ms)
			upds := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, nil, nil)
			WrapKeyUpdatesInto(b, upds, pk, updates)
			_, err := hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
			if err != nil {
				b.Fatal(err)
			}
			upds.Close()
		}
	})

	b.Run("with-warmup", func(b *testing.B) {
		for b.Loop() {
			hph := NewHexPatriciaHashed(20, ms)
			upds := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, nil, nil)
			WrapKeyUpdatesInto(b, upds, pk, updates)

			// Use a factory that creates views of the MockState.
			cfg := WarmupConfig{
				Enabled:    true,
				NumWorkers: 4,
				CtxFactory: func() (PatriciaContext, func()) {
					return ms, func() {}
				},
				LogPrefix: "bench",
			}
			_, err := hph.Process(context.Background(), upds, "", nil, cfg)
			if err != nil {
				b.Fatal(err)
			}
			upds.Close()
		}
	})
}

// deterministicRand returns a deterministic random source for benchmarks.
func deterministicRand() *deterministicRandSource {
	return &deterministicRandSource{state: 0x12345678}
}

type deterministicRandSource struct {
	state uint64
}

func (r *deterministicRandSource) Read(p []byte) (n int, err error) {
	for i := range p {
		r.state = r.state*6364136223846793005 + 1442695040888963407
		p[i] = byte(r.state >> 33)
	}
	return len(p), nil
}

func (r *deterministicRandSource) Uint64() uint64 {
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}
