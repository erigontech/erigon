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
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types"
)

// stubTemporalGetter stands in for the committed-state read view a warmup
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

func TestBlockReadAheaderCarriesBlockAccessList(t *testing.T) {
	bra := NewBlockReadAheader()
	header := &types.Header{Number: *uint256.NewInt(1)}
	body := &types.Body{Transactions: []types.Transaction{types.NewTransaction(0, common.Address{}, new(uint256.Int), 0, new(uint256.Int), nil)}}
	blockHash := header.Hash()
	bal := []byte{0xc0}
	sender := common.Address{1}
	bra.AddHeaderAndBody(context.Background(), nil, header, body)
	bra.AddBlockAccessList(blockHash, bal)
	bra.AddSenders(sender[:], blockHash)
	block, ok := bra.ReadBlockWithSenders(blockHash)
	require.True(t, ok)
	require.Equal(t, bal, block.BlockAccessList())
}

// seedFill places an entry with an exact txNum stamp through the public fill
// API without moving the applied frontier.
func seedFill(sc *cache.StateCache, domain kv.Domain, k, v []byte, txNum uint64) {
	sc.View(cache.FrontierFunc(func(kv.Domain) (uint64, bool) { return txNum + 1, true })).Fill(domain, k, v, txNum)
}

// A warmup read-through must never replace a fresher entry an authoritative
// writer (the FCU flush cache-apply) has already put: the warmup reads a
// pre-flush read view, so a laggard Put landing after the flush would pin
// stale state in the cache and corrupt the next block's execution.
func TestCachePopulatingGetterKeepsFresherEntry(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	fresh := []byte("account-record-nonce-5")
	stale := []byte("account-record-nonce-4")
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		sc := newTestStateCache()
		seedFill(sc, domain, key, fresh, 54)
		cpg := &cachePopulatingGetter{TemporalGetter: stubTemporalGetter{v: stale}, view: sc.View(cache.FrontierFunc(emptyVisibleEnd)), stepSize: 1_562_500}

		v, _, err := cpg.GetLatest(domain, key)
		require.NoError(t, err)
		require.Equal(t, stale, v, "read-through must still return the view's value")

		got, ok := sc.View(nil).Get(domain, key)
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
	seedFill(sc, kv.CodeDomain, addr, freshCode, 54)
	cpg := &cachePopulatingGetter{TemporalGetter: stubTemporalGetter{v: staleCode}, view: sc.View(cache.FrontierFunc(emptyVisibleEnd)), stepSize: 1_562_500}

	_, _, err := cpg.GetLatest(kv.CodeDomain, addr)
	require.NoError(t, err)

	got, ok := sc.View(nil).Get(kv.CodeDomain, addr)
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
		cpg := &cachePopulatingGetter{TemporalGetter: stubTemporalGetter{v: val}, view: sc.View(cache.FrontierFunc(emptyVisibleEnd)), stepSize: 1_562_500}
		_, _, err := cpg.GetLatest(domain, key)
		require.NoError(t, err)
		got, ok := sc.View(nil).Get(domain, key)
		require.True(t, ok, "domain %s", domain)
		require.Equal(t, val, got, "domain %s", domain)
	}

	sc := newTestStateCache()
	cpg := &cachePopulatingGetter{TemporalGetter: stubTemporalGetter{v: code}, view: sc.View(cache.FrontierFunc(emptyVisibleEnd)), stepSize: 1_562_500}
	_, _, err := cpg.GetLatest(kv.CodeDomain, key)
	require.NoError(t, err)
	got, ok := sc.View(nil).Get(kv.CodeDomain, key)
	require.True(t, ok)
	require.Equal(t, code, got)
	got, ok = sc.View(nil).GetCodeByHash(crypto.Keccak256(code))
	require.True(t, ok)
	require.Equal(t, code, got)

	// Negative results (missing account, empty slot) are cached as nil hits.
	sc = newTestStateCache()
	cpg = &cachePopulatingGetter{TemporalGetter: stubTemporalGetter{v: nil}, view: sc.View(cache.FrontierFunc(emptyVisibleEnd)), stepSize: 1_562_500}
	_, _, err = cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	got, ok = sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Empty(t, got)
}

func TestCachePopulatingGetterNegativeUsesLastVisibleTxNum(t *testing.T) {
	const visibleEnd = uint64(10_000_001)
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	sc := newTestStateCache()
	cpg := &cachePopulatingGetter{
		TemporalGetter: stubTemporalGetter{v: nil}, stepSize: 1_562_500,
		view: sc.View(cache.FrontierFunc(func(kv.Domain) (uint64, bool) { return visibleEnd, true })),
	}
	_, _, err := cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok)

	sc.Applier().Unwind(visibleEnd)
	_, ok = sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "a negative observed before the unwind floor must remain cached")

	sc.Applier().Unwind(visibleEnd - 1)
	_, ok = sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "a negative observed at the unwind floor must be invalidated")
}

func TestCachePopulatingGetterUnavailableVisibleEndNeverFills(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	sc := newTestStateCache()
	cpg := &cachePopulatingGetter{
		TemporalGetter: stubTemporalGetter{v: nil}, stepSize: 1_562_500,
		view: sc.View(cache.FrontierFunc(func(kv.Domain) (uint64, bool) { return 0, false })),
	}
	_, _, err := cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "no exact frontier — nothing may be cached")
}

func TestCachePopulatingGetterStaleViewDoesNotFill(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	sc := newTestStateCache()
	sc.Applier().Apply(kv.AccountsDomain, key, nil, 20)
	cpg := &cachePopulatingGetter{
		TemporalGetter: stubTemporalGetter{v: []byte("pre-delete-record")},
		stepSize:       1_562_500,
		view:           sc.View(cache.FrontierFunc(func(kv.Domain) (uint64, bool) { return 11, true })),
	}

	_, _, err := cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok)
}

func emptyVisibleEnd(kv.Domain) (uint64, bool) { return 0, true }
