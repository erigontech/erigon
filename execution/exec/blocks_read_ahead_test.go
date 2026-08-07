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
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// stubTemporalGetter stands in for the committed-state snapshot a warmup
// goroutine reads: every GetLatest returns the same fixed value.
type stubTemporalGetter struct {
	v    []byte
	step kv.Step
}

type sharedCodeTemporalGetter struct {
	account   []byte
	code      []byte
	codeReads int
}

func (s stubTemporalGetter) GetLatest(kv.Domain, []byte) ([]byte, kv.Step, error) {
	return s.v, s.step, nil
}

func (s stubTemporalGetter) HasPrefix(kv.Domain, []byte) ([]byte, []byte, bool, error) {
	return nil, nil, false, nil
}

func (s stubTemporalGetter) StepsInFiles(...kv.Domain) kv.Step { return 0 }

func (s *sharedCodeTemporalGetter) GetLatest(domain kv.Domain, _ []byte) ([]byte, kv.Step, error) {
	if domain == kv.AccountsDomain {
		return s.account, 0, nil
	}
	if domain == kv.CodeDomain {
		s.codeReads++
		return s.code, 0, nil
	}
	return nil, 0, nil
}

func (s *sharedCodeTemporalGetter) HasPrefix(kv.Domain, []byte) ([]byte, []byte, bool, error) {
	return nil, nil, false, nil
}

func (s *sharedCodeTemporalGetter) StepsInFiles(...kv.Domain) kv.Step { return 0 }

func newTestStateCache() *cache.StateCache {
	b := 1 * datasize.MB
	return cache.NewStateCache(b, b, b, b)
}

func TestBlockReadAheaderWaitsForConfiguredWarmup(t *testing.T) {
	oldReadAheadWait := dbg.ReadAheadWait
	dbg.ReadAheadWait = true
	t.Cleanup(func() { dbg.ReadAheadWait = oldReadAheadWait })
	readAheader := NewBlockReadAheader()
	release := make(chan struct{})
	readAheader.warmWg.Go(func() { <-release })
	done := make(chan struct{})
	go func() {
		readAheader.waitForWarmupIfConfigured(t.Context())
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("configured warmup wait returned before warmup completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("configured warmup wait did not return after warmup completed")
	}
}

func TestMakeBALWarmupTasksSplitsStorageHeavyAccount(t *testing.T) {
	bal := types.BlockAccessList{{
		StorageChanges: make([]*types.SlotChanges, 5),
		StorageReads:   make([]accounts.StorageKey, 3),
	}}
	tasks, workers := makeBALWarmupPlan(bal, 4)
	require.Equal(t, 4, workers)
	require.Equal(t, []balWarmupTask{
		{accountIndex: 0, kind: balWarmAccount},
		{accountIndex: 0, kind: balWarmStorageChanges, slotIndex: 0},
		{accountIndex: 0, kind: balWarmStorageChanges, slotIndex: 1},
		{accountIndex: 0, kind: balWarmStorageChanges, slotIndex: 2},
		{accountIndex: 0, kind: balWarmStorageChanges, slotIndex: 3},
		{accountIndex: 0, kind: balWarmStorageChanges, slotIndex: 4},
		{accountIndex: 0, kind: balWarmStorageReads, slotIndex: 0},
		{accountIndex: 0, kind: balWarmStorageReads, slotIndex: 1},
		{accountIndex: 0, kind: balWarmStorageReads, slotIndex: 2},
	}, tasks)
}

func TestBlockReadAheaderWarmsOverlayBlockAccessList(t *testing.T) {
	oldReadAhead := dbg.ReadAhead
	dbg.SetReadAhead(true)
	t.Cleanup(func() { dbg.SetReadAhead(oldReadAhead) })
	ctx := t.Context()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	address := common.Address{19: 0x42}
	account := accounts.Account{
		Nonce:    1,
		Balance:  *uint256.NewInt(1),
		CodeHash: accounts.EmptyCodeHash,
	}
	accountBytes := accounts.SerialiseV3(&account)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, address[:], accountBytes, 1, nil))
	require.NoError(t, domains.Commit(ctx, rwTx))
	domains.Close()
	bal := types.BlockAccessList{{Address: accounts.InternAddress(address)}}
	balBytes, err := types.EncodeBlockAccessListBytes(bal)
	require.NoError(t, err)
	balHash := bal.Hash()
	header := &types.Header{
		Number:              *uint256.NewInt(1),
		BlockAccessListHash: &balHash,
	}
	body := new(types.Body)
	baseTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer baseTx.Rollback()
	overlay, err := membatchwithdb.NewMemoryBatch(baseTx, dirs.Tmp, log.New())
	require.NoError(t, err)
	require.NoError(t, rawdb.WriteBlockAccessListBytes(overlay, header.Hash(), header.Number.Uint64(), balBytes))
	// The regression requires the BAL to be present only in BlockOverlay.
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		stored, err := rawdb.ReadBlockAccessListBytes(tx, header.Hash(), header.Number.Uint64())
		require.NoError(t, err)
		require.Empty(t, stored)
		return nil
	}))
	stateCache := newTestStateCache()
	readAheader := NewBlockReadAheader()
	readAheader.SetStateCache(stateCache)
	readAheader.AddHeaderAndBody(ctx, db, overlay, header, body)
	// AddHeaderAndBody must have copied the sidecar synchronously; its caller may
	// release the overlay before the asynchronous state warming completes.
	overlay.Close()
	baseTx.Rollback()
	readAheader.WaitForWarmup(ctx)
	got, ok := stateCache.Get(kv.AccountsDomain, address[:])
	require.True(t, ok, "overlay-only BAL account was not warmed")
	require.Equal(t, accountBytes, got)
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
		cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: stale}, sc: sc, stepSize: 1_562_500}

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
	cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: staleCode}, sc: sc, stepSize: 1_562_500}

	_, _, err := cpg.GetLatest(kv.CodeDomain, addr)
	require.NoError(t, err)

	got, ok := sc.Get(kv.CodeDomain, addr)
	require.True(t, ok)
	require.Equal(t, freshCode, got, "warmup must not rebind addr to older code")
}

func TestCachePopulatingGetterReusesCodeByHashAcrossGetters(t *testing.T) {
	code := []byte{0xaa, 0x01, 0x02, 0x03}
	account := accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}
	source := &sharedCodeTemporalGetter{account: accounts.SerialiseV3(&account), code: code}
	stateCache := newTestStateCache()
	for _, address := range []accounts.Address{
		accounts.InternAddress(common.Address{19: 1}),
		accounts.InternAddress(common.Address{19: 2}),
	} {
		reader := state.NewReaderV3(&cachePopulatingGetter{g: source, sc: stateCache, stepSize: 16})
		gotAccount, err := reader.ReadAccountData(address)
		require.NoError(t, err)
		require.Equal(t, account.CodeHash, gotAccount.CodeHash)
		gotCode, err := reader.ReadAccountCode(address)
		require.NoError(t, err)
		require.Equal(t, code, gotCode)
	}
	require.Equal(t, 1, source.codeReads, "identical code must be loaded from the address-keyed domain only once")
}

// Cold keys must still be warmed — that is the prefetcher's purpose.
func TestCachePopulatingGetterWarmsColdKeys(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	val := []byte("account-record")
	code := []byte{0xaa, 0x01, 0x02, 0x03}

	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		sc := newTestStateCache()
		cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: val}, sc: sc, stepSize: 1_562_500}
		_, _, err := cpg.GetLatest(domain, key)
		require.NoError(t, err)
		got, ok := sc.Get(domain, key)
		require.True(t, ok, "domain %s", domain)
		require.Equal(t, val, got, "domain %s", domain)
	}

	sc := newTestStateCache()
	cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: code}, sc: sc, stepSize: 1_562_500}
	_, _, err := cpg.GetLatest(kv.CodeDomain, key)
	require.NoError(t, err)
	got, ok := sc.Get(kv.CodeDomain, key)
	require.True(t, ok)
	require.Equal(t, code, got)

	// Negative results (missing account, empty slot) are cached as nil hits.
	sc = newTestStateCache()
	cpg = &cachePopulatingGetter{g: stubTemporalGetter{v: nil}, sc: sc, stepSize: 1_562_500, progress: func(kv.Domain) uint64 { return 100 }}
	_, _, err = cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	got, ok = sc.Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Empty(t, got)
}

// A negative (missing account, empty slot) carries no write step, so a
// step-derived stamp pins it at the start of history where no unwind can drop
// it. It must be stamped with the domain's progress at observation time —
// any unwind at or below that progress then invalidates it (mirroring the SD
// read-fill).
func TestCachePopulatingGetterNegativeDroppedByUnwind(t *testing.T) {
	key := []byte("\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44")
	sc := newTestStateCache()
	cpg := &cachePopulatingGetter{g: stubTemporalGetter{v: nil}, sc: sc, stepSize: 16, progress: func(kv.Domain) uint64 { return 100 }}

	_, _, err := cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	_, ok := sc.Get(kv.AccountsDomain, key)
	require.True(t, ok, "the negative result must be cached")

	sc.Unwind(50)
	_, ok = sc.Get(kv.AccountsDomain, key)
	require.False(t, ok, "a negative must not survive an unwind below the progress at which it was observed")
}
