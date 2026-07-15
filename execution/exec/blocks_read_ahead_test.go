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

package exec

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/cache"
)

// stubTemporalGetter stands in for the committed-state snapshot a warmup
// goroutine reads: every GetLatest returns the same fixed value.
type stubTemporalGetter struct {
	v    []byte
	step kv.Step
}

func (s stubTemporalGetter) GetLatest(kv.Domain, []byte) ([]byte, kv.Step, error) {
	return s.v, s.step, nil
}

func (s stubTemporalGetter) HasPrefix(kv.Domain, []byte) ([]byte, []byte, bool, error) {
	return nil, nil, false, nil
}

func (s stubTemporalGetter) StepsInFiles(...kv.Domain) kv.Step { return 0 }

func newTestStateCache() *cache.StateCache {
	b := 1 * datasize.MB
	return cache.NewStateCache(b, b, b, b)
}

// A warmup read-through must never replace a fresher entry an authoritative
// writer (the FCU flush cache-apply) has already put: the warmup reads a
// pre-flush snapshot, so a laggard Put landing after the flush would pin stale
// state in the cache and corrupt the next block's execution.
func TestCachePopulatingGetterKeepsFresherEntry(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	fresh := []byte("account-record-nonce-5")
	stale := []byte("account-record-nonce-4")
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		sc := newTestStateCache()
		sc.Put(domain, key, fresh, 54)
		cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: stale}, sc: sc, stepSize: 1_562_500, progressBounds: zeroProgressBounds}

		v, _, err := cpg.GetLatest(domain, key)
		require.NoError(t, err)
		require.Equal(t, stale, v, "read-through must still return the snapshot value")

		got, ok := sc.Get(domain, key)
		require.True(t, ok, "domain %s", domain)
		require.Equal(t, fresh, got, "domain %s: warmup must not clobber the fresher entry", domain)
	}
}

// Same invariant for the code addr→code binding, which is rebound when an
// account's code changes and is therefore just as clobber-able as accounts.
func TestCachePopulatingGetterKeepsFresherCodeBinding(t *testing.T) {
	addr := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	freshCode := []byte{0xaa, 0x01, 0x02, 0x03}
	staleCode := []byte{0xbb, 0x04, 0x05, 0x06}
	sc := newTestStateCache()
	sc.PutCodeWithHash(addr, freshCode, crypto.Keccak256(freshCode), 54)
	cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: staleCode}, sc: sc, stepSize: 1_562_500, progressBounds: zeroProgressBounds}

	_, _, err := cpg.GetLatest(kv.CodeDomain, addr)
	require.NoError(t, err)

	got, ok := sc.Get(kv.CodeDomain, addr)
	require.True(t, ok)
	require.Equal(t, freshCode, got, "warmup must not rebind addr to older code")
}

// Cold keys must still be warmed — that is the prefetcher's purpose.
func TestCachePopulatingGetterWarmsColdKeys(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	val := []byte("account-record")
	code := []byte{0xaa, 0x01, 0x02, 0x03}

	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		sc := newTestStateCache()
		cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: val}, sc: sc, stepSize: 1_562_500, progressBounds: zeroProgressBounds}
		_, _, err := cpg.GetLatest(domain, key)
		require.NoError(t, err)
		got, ok := sc.Get(domain, key)
		require.True(t, ok, "domain %s", domain)
		require.Equal(t, val, got, "domain %s", domain)
	}

	sc := newTestStateCache()
	cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: code}, sc: sc, stepSize: 1_562_500, progressBounds: zeroProgressBounds}
	_, _, err := cpg.GetLatest(kv.CodeDomain, key)
	require.NoError(t, err)
	got, ok := sc.Get(kv.CodeDomain, key)
	require.True(t, ok)
	require.Equal(t, code, got)

	// Negative results (missing account, empty slot) are cached as nil hits.
	sc = newTestStateCache()
	cpg = &cachePopulatingGetter{g: stubTemporalGetter{v: nil}, sc: sc, stepSize: 1_562_500, progressBounds: zeroProgressBounds}
	_, _, err = cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	got, ok = sc.Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Empty(t, got)
}

func TestCachePopulatingGetterNegativeDropsOnUnwind(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	sc := newTestStateCache()
	cpg := &cachePopulatingGetter{
		g: stubTemporalGetter{v: nil}, sc: sc, stepSize: 1_562_500,
		progressBounds: func(kv.Domain) (uint64, uint64, bool) { return 10_000_000, 10_000_001, true },
	}
	_, _, err := cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	_, ok := sc.Get(kv.AccountsDomain, key)
	require.True(t, ok)

	sc.Unwind(5_000_000)
	_, ok = sc.Get(kv.AccountsDomain, key)
	require.False(t, ok, "a negative observed at txNum 10M must not survive an unwind to 5M")
}

func TestCachePopulatingGetterNilProgressBoundsNeverFills(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	sc := newTestStateCache()
	cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: nil}, sc: sc, stepSize: 1_562_500}
	require.NotPanics(t, func() {
		_, _, err := cpg.GetLatest(kv.AccountsDomain, key)
		require.NoError(t, err)
	})
	_, ok := sc.Get(kv.AccountsDomain, key)
	require.False(t, ok, "no progress oracle — nothing may be cached")
}

func TestCachePopulatingGetterStaleSnapshotDoesNotFill(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	sc := newTestStateCache()
	sc.Apply(kv.AccountsDomain, key, nil, 20)
	cpg := &cachePopulatingGetter{
		g:              stubTemporalGetter{v: []byte("pre-delete-record")},
		sc:             sc,
		stepSize:       1_562_500,
		progressBounds: func(kv.Domain) (uint64, uint64, bool) { return 10, 11, true },
	}

	_, _, err := cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	_, ok := sc.Get(kv.AccountsDomain, key)
	require.False(t, ok)
}

func zeroProgressBounds(kv.Domain) (uint64, uint64, bool) { return 0, 0, true }
