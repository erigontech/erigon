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

package execctx_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type valSizeTx struct {
	kv.TemporalTx
	size  int
	found bool
}

func (tx valSizeTx) GetLatestValSize(kv.Domain, []byte) (int, bool, error) {
	return tx.size, tx.found, nil
}

type latestOptionsCaptureTx struct {
	kv.TemporalTx
	sawMetrics bool
	opts       kv.GetLatestOptions
}

func (tx *latestOptionsCaptureTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	metrics, _ := opts.Metrics()
	tx.sawMetrics = metrics != nil
	tx.opts = opts
	return tx.TemporalTx.GetLatest(domain, key, opts)
}

func TestPlainGetLatestDoesNotPassNilRequestMetrics(t *testing.T) {
	metricsEnabled := dbg.KVReadLevelledMetrics
	dbg.KVReadLevelledMetrics = true
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = metricsEnabled })
	db := newTestDb(t, 16)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	tx := &latestOptionsCaptureTx{TemporalTx: roTx}
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	tx.sawMetrics = false
	key := make([]byte, 20)
	_, _, err = sd.GetLatest(kv.AccountsDomain, tx, key)
	require.NoError(t, err)
	require.False(t, tx.sawMetrics)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, key, []byte{1}, 1, nil))
	_, _, err = sd.GetLatest(kv.AccountsDomain, tx, key)
	require.NoError(t, err)
}

func TestStateGetterUsesSharedBranchCacheForCommitment(t *testing.T) {
	db := newTestDb(t, 16)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	tx := &latestOptionsCaptureTx{TemporalTx: roTx}
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	branchCache := roTx.AggTx().(commitment.BranchCacheProvider).BranchCache()
	branchCache.Clear()
	getter := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})
	key := []byte{0xaa, 0xbb}
	cached := []byte("cached")
	branchCache.Put(key, cached, 0, 0)
	tx.opts = kv.GetLatestOptions{}
	got, _, err := getter.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, cached, got)
	branchCache.Clear()
	tx.opts = kv.GetLatestOptions{}
	_, _, err = getter.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.True(t, tx.opts.BranchCache())
}

func TestSnapshotGetterHonorsStepBound(t *testing.T) {
	t.Parallel()
	db := newTestDb(t, 16)
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	key, _, committed, _ := twoStepRows(t, db, stateCache)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	tx := &latestOptionsCaptureTx{TemporalTx: roTx}
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	domains.BindStateCache(stateCache)
	newer := encAccount(3)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, key, newer, 40, committed))
	getter := domains.AsSnapshotGetter(tx)

	got, _, err := getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{}.WithMaxStep(1))
	require.NoError(t, err)
	require.Equal(t, committed, got, "an in-memory value above the caller's bound must not be served")
	require.Equal(t, kv.Step(1), tx.opts.MaxStep())
	got, _, err = getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, newer, got, "an unbounded read must still use the selected in-memory value")
}

func TestTemporalTxStateGetterCodePresence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		size      int
		keyFound  bool
		codeFound bool
	}{
		{name: "missing", keyFound: false, codeFound: false},
		{name: "empty record", keyFound: true, codeFound: false},
		{name: "code", size: 3, keyFound: true, codeFound: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			getter := execctx.NewTemporalTxStateGetter(valSizeTx{size: test.size, found: test.keyFound})
			size, found, err := getter.GetCodeSize(nil, 0)
			require.NoError(t, err)
			require.Equal(t, test.size, size)
			require.Equal(t, test.codeFound, found)
		})
	}
}

// codeBufTx stands in for the compressed-file layer: it decodes into the buffer
// the caller lends, the way seg's Getter does.
type codeBufTx struct {
	kv.TemporalTx
	code     map[string][]byte
	lentCaps *[]int
}

func (tx codeBufTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if domain != kv.CodeDomain {
		return tx.TemporalTx.GetLatest(domain, key, opts)
	}
	if tx.lentCaps != nil {
		*tx.lentCaps = append(*tx.lentCaps, cap(opts.Buf()))
	}
	return append(opts.Buf()[:0], tx.code[string(key)]...), 0, nil
}

// Identical code at many addresses is decoded once; any other read into the buffer ends the reuse.
func TestStateGetterCodeSegmentReusesTheLastDecode(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	code := bytes.Repeat([]byte{0x21}, 1024)
	other := bytes.Repeat([]byte{0x22}, 1024)
	addr := func(b byte) []byte { return append(make([]byte, 19), b) }
	byAddr := map[string][]byte{string(addr(1)): code, string(addr(2)): code, string(addr(3)): other}
	for a, c := range byAddr {
		commitAccount(t, db, nil, []byte(a), accounts.SerialiseV3(&accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(c))}), 1)
	}

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	var lentCaps []int
	tx := codeBufTx{TemporalTx: roTx, code: byAddr, lentCaps: &lentCaps}
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)
	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(sc)
	var buf []byte
	getter := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{}.WithCodeBuf(&buf))
	borrower := getter.(interface {
		ReadCodeSegmentTo(addr []byte, txNum, from uint64, dst []byte) (bool, error)
	})
	borrow := func(b byte, want []byte, decodes int) {
		t.Helper()
		got := make([]byte, len(want))
		borrowed, lendErr := borrower.ReadCodeSegmentTo(addr(b), 5, 0, got)
		require.NoError(t, lendErr)
		require.True(t, borrowed)
		require.Equal(t, want, got)
		require.Len(t, lentCaps, decodes)
	}

	borrow(1, code, 1)
	borrow(2, code, 1)
	borrow(3, other, 2)
	borrow(1, code, 3)

	_, ok, err := getter.GetCode(addr(3), 5)
	require.NoError(t, err)
	require.True(t, ok)
	decodes := len(lentCaps)
	borrow(1, code, decodes+1)
}

// A caller-owned buffer outlives each getter, so a later getter decodes into it
// instead of growing its own.
func TestStateGetterCallerCodeBufOutlivesGetter(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	codes := [][]byte{
		bytes.Repeat([]byte{0x17}, 1024),
		bytes.Repeat([]byte{0x28}, 1024),
	}
	addrs := make([][]byte, len(codes))
	var lentCaps []int
	tx := codeBufTx{TemporalTx: roTx, code: map[string][]byte{}, lentCaps: &lentCaps}
	for i := range codes {
		addrs[i] = make([]byte, 20)
		addrs[i][0] = byte(i + 1)
		tx.code[string(addrs[i])] = codes[i]
	}

	sc := newSmallStateCache()
	t.Cleanup(sc.Close)
	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(sc)
	for i, addr := range addrs {
		acc := accounts.SerialiseV3(&accounts.Account{
			Nonce:    1,
			CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(codes[i])),
		})
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, addr, acc, 5, nil))
	}

	var buf []byte
	opts := execctxapi.StateGetterOptions{}.WithCodeBuf(&buf)
	for i, addr := range addrs {
		got, ok, err := sd.AsStateGetter(tx, opts).GetCode(addr, 5)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, codes[i], got)
	}
	require.Len(t, lentCaps, len(codes))
	require.GreaterOrEqual(t, lentCaps[1], len(codes[1]), "the second getter must decode into the buffer the first one grew")
}

func TestStateGetterCodeBufDoesNotAliasEarlierResult(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)

	codes := [][]byte{
		bytes.Repeat([]byte{0xa1}, 1024),
		bytes.Repeat([]byte{0xb2}, 1024),
		bytes.Repeat([]byte{0xc3}, 1024),
	}
	addrs := make([][]byte, len(codes))
	for i := range codes {
		addrs[i] = make([]byte, 20)
		addrs[i][0] = byte(i + 1)
		// Committed, not batched: a codeHash from the batch makes the read
		// clone the bytes itself instead of going through FillCode.
		commitAccount(t, db, nil, addrs[i], accounts.SerialiseV3(&accounts.Account{
			Nonce:    1,
			CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(codes[i])),
		}), 5)
	}

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	var lentCaps []int
	tx := codeBufTx{TemporalTx: roTx, code: map[string][]byte{}, lentCaps: &lentCaps}
	for i, addr := range addrs {
		tx.code[string(addr)] = codes[i]
	}

	sc := newSmallStateCache()
	t.Cleanup(sc.Close)
	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(sc)

	getter := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})
	got := make([][]byte, len(codes))
	for i, addr := range addrs {
		var ok bool
		got[i], ok, err = getter.GetCode(addr, 5)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, codes[i], got[i])
	}
	for i := range codes {
		require.Equal(t, codes[i], got[i], "read %d must not alias the buffer a later read decodes into", i)
	}
	require.Len(t, lentCaps, len(codes))
	for i := 1; i < len(codes); i++ {
		require.Positive(t, lentCaps[i], "read %d must decode into the lent buffer", i)
	}
}

// A read that does not resolve a codeHash skips the cache copy, so it must not
// be handed the shared buffer either.
func TestStateGetterCodeBufNotLentWithoutCodeHash(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	codes := [][]byte{
		bytes.Repeat([]byte{0xd4}, 1024),
		bytes.Repeat([]byte{0xe5}, 1024),
		bytes.Repeat([]byte{0xf6}, 1024),
	}
	addrs := make([][]byte, len(codes))
	tx := codeBufTx{TemporalTx: roTx, code: map[string][]byte{}}
	for i := range codes {
		addrs[i] = make([]byte, 20)
		addrs[i][0] = byte(i + 1)
		tx.code[string(addrs[i])] = codes[i]
	}

	sc := newSmallStateCache()
	t.Cleanup(sc.Close)
	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(sc)

	// No account records, so codeHashForAddr answers nil for every address.
	getter := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})
	got := make([][]byte, len(codes))
	for i, addr := range addrs {
		var ok bool
		got[i], ok, err = getter.GetCode(addr, 5)
		require.NoError(t, err)
		require.True(t, ok)
	}
	for i := range codes {
		require.Equal(t, codes[i], got[i], "read %d must not alias the buffer a later read decodes into", i)
	}
}

type countingLatestTx struct {
	kv.TemporalTx
	reads int
}

func (tx *countingLatestTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	tx.reads++
	return tx.TemporalTx.GetLatest(domain, key, opts)
}

// commitAccount commits one account write; a nil stateCache commits without
// publishing to any cache.
func commitAccount(t *testing.T, db kv.TemporalRwDB, stateCache *cache.StateCache, key, value []byte, txNum uint64) {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	if stateCache != nil {
		sd.BindStateCache(stateCache)
	}
	sd.SetTxNum(txNum)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, key, value, txNum, nil))
	require.NoError(t, sd.Commit(t.Context(), rwTx))
}

// committedCacheState commits one account, so a read tx has a frontier above
// zero, through a SharedDomains bound to the returned cache.
func committedCacheState(t *testing.T) (kv.TemporalRwDB, *cache.StateCache) {
	t.Helper()
	db := newTestDb(t, 16)
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	committed := make([]byte, 20)
	committed[0] = 0xcc
	commitAccount(t, db, stateCache, committed, encAccount(1), 5)
	return db, stateCache
}

func TestCachedTemporalTxStateGetterServesRepeatReadFromCache(t *testing.T) {
	db, stateCache := committedCacheState(t)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	tx := &countingLatestTx{TemporalTx: roTx}
	getter := execctx.NewCachedTemporalTxStateGetter(tx, stateCache)
	key := make([]byte, 20)

	first, _, err := getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, tx.reads)

	second, _, err := getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 1, tx.reads)
}

// A tx reads past the cache when the cache's values are not its own state.
func TestCachedTemporalTxStateGetterForeignCacheStateReadsTx(t *testing.T) {
	for name, tc := range map[string]struct {
		beforeBind bool
		publish    func(a cache.Applier, stateVersion, end uint64, key []byte)
	}{
		// A value published after the getter was bound is newer than its tx.
		"later publication": {publish: func(a cache.Applier, stateVersion, end uint64, key []byte) {
			a.Publish(stateVersion, stateVersion+1, []cache.StateUpdate{{Domain: kv.AccountsDomain, Key: key, Value: []byte{1}, TxNum: end}})
		}},
		// After an unwind, the cache can hold another fork's values at txNums the tx can see.
		"unwind": {publish: func(a cache.Applier, stateVersion, end uint64, key []byte) {
			a.Unwind(0)
			a.Publish(stateVersion, stateVersion+1, []cache.StateUpdate{{Domain: kv.AccountsDomain, Key: key, Value: []byte{2}, TxNum: end - 1}})
		}},
		// The cache holds a newer durable state version's values, which are not this tx's state.
		"stale state version": {beforeBind: true, publish: func(a cache.Applier, stateVersion, end uint64, key []byte) {
			a.PublishUnwind(stateVersion, stateVersion+1, end-1, []cache.StateUpdate{{Domain: kv.AccountsDomain, Key: key, Value: []byte{1}, TxNum: end - 1}})
		}},
	} {
		t.Run(name, func(t *testing.T) {
			db, stateCache := committedCacheState(t)
			roTx, err := db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			defer roTx.Rollback()
			stateVersion, err := rawdb.GetStateVersion(roTx)
			require.NoError(t, err)
			end, ok := roTx.Debug().DomainVisibleEnd(kv.AccountsDomain)
			require.True(t, ok)
			key := make([]byte, 20)

			tx := &countingLatestTx{TemporalTx: roTx}
			if tc.beforeBind {
				tc.publish(stateCache.Applier(), stateVersion, end, key)
			}
			getter := execctx.NewCachedTemporalTxStateGetter(tx, stateCache)
			if !tc.beforeBind {
				tc.publish(stateCache.Applier(), stateVersion, end, key)
			}
			v, _, err := getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
			require.NoError(t, err)
			require.Empty(t, v)
			require.Equal(t, 1, tx.reads)
		})
	}
}

func TestCachedTemporalTxStateGetterRetriesAfterCachePublication(t *testing.T) {
	db, stateCache := committedCacheState(t)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	sourceVersion, err := rawdb.GetStateVersion(roTx)
	require.NoError(t, err)
	roTx.Rollback()

	key, value := bytes.Repeat([]byte{0xdd}, 20), encAccount(2)
	commitAccount(t, db, nil, key, value, 10)
	roTx, err = db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	committedVersion, err := rawdb.GetStateVersion(roTx)
	require.NoError(t, err)
	tx := &countingLatestTx{TemporalTx: roTx}
	getter := execctx.NewCachedTemporalTxStateGetter(tx, stateCache)
	got, _, err := getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, 1, tx.reads, "the cache has not published this transaction's state yet")

	stateCache.Applier().Publish(sourceVersion, committedVersion, []cache.StateUpdate{
		{Domain: kv.AccountsDomain, Key: key, Value: value, TxNum: 10},
	})
	got, _, err = getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, 1, tx.reads, "the existing getter must use the cache once publication catches up")

	missing := make([]byte, 20)
	for range 2 {
		got, _, err = getter.GetLatest(kv.AccountsDomain, missing, kv.GetLatestOptions{})
		require.NoError(t, err)
		require.Empty(t, got)
	}
	require.Equal(t, 2, tx.reads, "the rebound view must also admit fills")

	stateCache.Applier().Publish(committedVersion, committedVersion+1, nil)
	cached, hit := stateCache.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, hit)
	require.Equal(t, value, cached)
	got, _, err = getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, 2, tx.reads, "an unchanged entry must stay usable after another forward publication")
}

func TestCachedTemporalTxStateGetterWithoutCacheReadsEveryTime(t *testing.T) {
	db := newTestDb(t, 16)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	tx := &countingLatestTx{TemporalTx: roTx}
	getter := execctx.NewCachedTemporalTxStateGetter(tx, nil)
	key := make([]byte, 20)
	for i := 1; i <= 2; i++ {
		_, _, err = getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
		require.NoError(t, err)
		require.Equal(t, i, tx.reads)
	}
}

// roWrappingRwTx is a read-only facade over a writable tx: it satisfies only
// kv.TemporalTx, so the writable check has to unwrap to find the hazard.
type roWrappingRwTx struct {
	kv.TemporalTx
	under kv.TemporalTx
}

func (tx roWrappingRwTx) UnderlyingTx() kv.TemporalTx { return tx.under }

func TestCachedTemporalTxStateGetterRefusesWrappedWritableTx(t *testing.T) {
	db, stateCache := committedCacheState(t)
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	counting := &countingLatestTx{TemporalTx: rwTx}
	wrapped := roWrappingRwTx{TemporalTx: counting, under: rwTx}
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	require.NoError(t, domains.InitBlockOverlay(rwTx))
	key := make([]byte, 20)
	for _, tc := range []struct {
		name string
		tx   kv.TemporalTx
	}{
		{"read-only facade", wrapped},
		{"overlay read view", domains.BlockOverlay().NewReadView(wrapped)},
		{"temporal overlay read view", domains.BlockOverlay().NewTemporalReadView(wrapped)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counting.reads = 0
			getter := execctx.NewCachedTemporalTxStateGetter(tc.tx, stateCache)
			for i := 1; i <= 2; i++ {
				_, _, err := getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
				require.NoError(t, err)
				require.Equal(t, i, counting.reads)
			}
			_, hit := stateCache.View(nil).Get(kv.AccountsDomain, key)
			require.False(t, hit, "a writable backing transaction must not fill the shared cache")
		})
	}
}

// Before any SharedDomains binds the cache, commits do not publish to it, so a
// fill from one read tx is not valid state for a later one.
func TestCachedTemporalTxStateGetterUnversionedCacheReadsTx(t *testing.T) {
	db := newTestDb(t, 16)
	stateCache := newSmallStateCache()
	defer stateCache.Close()
	commitAccount(t, db, nil, bytes.Repeat([]byte{0xcc}, 20), encAccount(1), 5)
	key := bytes.Repeat([]byte{0xdd}, 20)

	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	v, _, err := execctx.NewCachedTemporalTxStateGetter(roTx, stateCache).GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Empty(t, v)
	roTx.Rollback()

	commitAccount(t, db, nil, key, encAccount(2), 10)
	roTx, err = db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	v, _, err = execctx.NewCachedTemporalTxStateGetter(roTx, stateCache).GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, encAccount(2), v)
}

func TestCachedTemporalTxStateGetterServesRepeatPositiveReadFromCache(t *testing.T) {
	db, stateCache := committedCacheState(t)
	key := bytes.Repeat([]byte{0xdd}, 20)
	commitAccount(t, db, stateCache, key, encAccount(2), 6)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	tx := &countingLatestTx{TemporalTx: roTx}
	domains, err := execctx.NewSharedDomains(t.Context(), roTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	require.NoError(t, domains.InitBlockOverlay(roTx))
	for _, tc := range []struct {
		name string
		tx   kv.TemporalTx
	}{
		{"plain transaction", tx},
		{"overlay read view", domains.BlockOverlay().NewReadView(tx)},
		{"temporal overlay read view", domains.BlockOverlay().NewTemporalReadView(tx)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stateCache.Applier().Clear()
			tx.reads = 0
			getter := execctx.NewCachedTemporalTxStateGetter(tc.tx, stateCache)
			for range 2 {
				v, _, err := getter.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
				require.NoError(t, err)
				require.Equal(t, encAccount(2), v)
			}
			require.Equal(t, 1, tx.reads, "the second read must hit the shared cache")
		})
	}
}

// indexBlock1 maps block 1 to the txNums the tests commit at.
func indexBlock1(t *testing.T, db kv.TemporalRwDB, execProgress uint64) {
	t.Helper()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := rawdbv3.TxNums.Append(tx, 1, 20); err != nil {
			return err
		}
		return stages.SaveStageProgress(tx, stages.Execution, execProgress)
	}))
}

// readTwice reads key through two getters on one read-only tx and returns the
// value and how many reads reached the tx.
func readTwice(t *testing.T, db kv.TemporalRwDB, stateCache *cache.StateCache, key []byte) ([]byte, int) {
	t.Helper()
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	tx := &countingLatestTx{TemporalTx: roTx}
	var v []byte
	for range 2 {
		v, _, err = execctx.NewCachedTemporalTxStateGetter(tx, stateCache).GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
		require.NoError(t, err)
	}
	return v, tx.reads
}

func TestInitStateCacheVersionLetsReadOnlyTxsUseTheCache(t *testing.T) {
	db := newTestDb(t, 16)
	stateCache := newSmallStateCache()
	defer stateCache.Close()
	key := bytes.Repeat([]byte{0xcc}, 20)
	commitAccount(t, db, nil, key, encAccount(1), 5)
	indexBlock1(t, db, 1)

	_, reads := readTwice(t, db, stateCache, key)
	require.Equal(t, 2, reads, "a cache without a state version serves nothing")

	require.NoError(t, execctx.InitStateCacheVersion(t.Context(), db, stateCache))
	v, reads := readTwice(t, db, stateCache, key)
	require.Equal(t, encAccount(1), v)
	require.Equal(t, 1, reads, "the repeat read is served from the cache")
}

func TestInitStateCacheVersionSkipsANodeWithoutExecutedBlocks(t *testing.T) {
	db := newTestDb(t, 16)
	stateCache := newSmallStateCache()
	defer stateCache.Close()
	key := bytes.Repeat([]byte{0xcc}, 20)
	commitAccount(t, db, nil, key, encAccount(1), 5)
	indexBlock1(t, db, 0)

	require.NoError(t, execctx.InitStateCacheVersion(t.Context(), db, stateCache))
	_, reads := readTwice(t, db, stateCache, key)
	require.Equal(t, 2, reads)
}

// A commit after the startup bind must replace the absent read it cached,
// whether or not the commit is published to the cache.
func TestInitStateCacheVersionThenCommitServesTheNewValue(t *testing.T) {
	for _, tc := range []struct {
		name      string
		published bool
	}{{"published", true}, {"missed", false}} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDb(t, 16)
			stateCache := newSmallStateCache()
			defer stateCache.Close()
			commitAccount(t, db, nil, bytes.Repeat([]byte{0xcc}, 20), encAccount(1), 5)
			indexBlock1(t, db, 1)
			require.NoError(t, execctx.InitStateCacheVersion(t.Context(), db, stateCache))
			key := bytes.Repeat([]byte{0xdd}, 20)
			v, _ := readTwice(t, db, stateCache, key)
			require.Empty(t, v)

			publishTo := stateCache
			if !tc.published {
				publishTo = nil
			}
			commitAccount(t, db, publishTo, key, encAccount(2), 10)
			v, _ = readTwice(t, db, stateCache, key)
			require.Equal(t, encAccount(2), v)
		})
	}
}

type countingAsOfTx struct {
	kv.TemporalTx
	reads int
}

func (tx *countingAsOfTx) GetAsOf(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	tx.reads++
	return tx.TemporalTx.GetAsOf(domain, key, ts)
}

// The as-of read serves a value committed before ts from the state cache, and reads the tx for one written at ts.
func TestAsOfGetterServesCommittedValueFromStateCache(t *testing.T) {
	db, stateCache := committedCacheState(t)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(stateCache)

	tx := &countingAsOfTx{TemporalTx: roTx}
	asOf := sd.AsOfGetter(tx)
	committed := make([]byte, 20)
	committed[0] = 0xcc

	v, ok, err := asOf.GetAsOf(kv.AccountsDomain, committed, 10)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, encAccount(1), v)
	require.Zero(t, tx.reads, "a value committed at txNum 5 is the value as of 10")

	_, _, err = asOf.GetAsOf(kv.AccountsDomain, committed, 5)
	require.NoError(t, err)
	require.Equal(t, 1, tx.reads, "a value committed at txNum 5 is not the value as of 5")
}

func TestStateGetterCodeSegmentLeavesTheCacheEmpty(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	addr := make([]byte, 20)
	addr[0] = 0x17
	code := bytes.Repeat([]byte{0x17}, 1024)
	codeHash := crypto.Keccak256Hash(code)
	commitAccount(t, db, nil, addr, accounts.SerialiseV3(&accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(codeHash)}), 5)
	addr2 := bytes.Clone(addr)
	addr2[0] = 0x18
	code2 := bytes.Repeat([]byte{0x18}, 1024)
	commitAccount(t, db, nil, addr2, accounts.SerialiseV3(&accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code2))}), 5)

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	var lentCaps []int
	tx := codeBufTx{TemporalTx: roTx, code: map[string][]byte{string(addr): code, string(addr2): code2}, lentCaps: &lentCaps}

	sc := newSmallStateCache()
	t.Cleanup(sc.Close)
	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(sc)

	var buf []byte
	getter := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{}.WithCodeBuf(&buf))
	borrower := getter.(interface {
		ReadCodeSegmentTo(addr []byte, txNum, from uint64, dst []byte) (bool, error)
	})

	got := make([]byte, len(code))
	borrowed, err := borrower.ReadCodeSegmentTo(addr, 5, 0, got)
	require.NoError(t, err)
	require.True(t, borrowed)
	require.Equal(t, code, got)
	_, cached := sc.View(nil).GetCodeByHash(codeHash[:])
	require.False(t, cached, "a borrowed read must not fill the code cache")

	got = make([]byte, len(code2))
	borrowed, err = borrower.ReadCodeSegmentTo(addr2, 5, 0, got)
	require.NoError(t, err)
	require.True(t, borrowed)
	require.Equal(t, code2, got)
	require.GreaterOrEqual(t, lentCaps[1], len(code2), "the second borrow must decode into the grown buffer")

	got, ok, err := getter.GetCode(addr, 5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, code, got)
	decodes := len(lentCaps)
	got = make([]byte, len(code))
	borrowed, err = borrower.ReadCodeSegmentTo(addr, 5, 0, got)
	require.NoError(t, err)
	require.True(t, borrowed, "a cached code is copied from the cache")
	require.Equal(t, code, got)
	require.Len(t, lentCaps, decodes, "a cache hit must not decode")
}

// Without a bound state cache the segment copy still works: it decodes into the caller's buffer.
func TestStateGetterCodeSegmentWorksWithoutAStateCache(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	addr := make([]byte, 20)
	addr[0] = 0x1b
	code := bytes.Repeat([]byte{0x1b}, 1024)
	commitAccount(t, db, nil, addr, accounts.SerialiseV3(&accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}), 1)

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	var lentCaps []int
	tx := codeBufTx{TemporalTx: roTx, code: map[string][]byte{string(addr): code}, lentCaps: &lentCaps}
	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	var buf []byte
	borrower := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{}.WithCodeBuf(&buf)).(interface {
		ReadCodeSegmentTo(addr []byte, txNum, from uint64, dst []byte) (bool, error)
	})
	got := make([]byte, len(code))
	borrowed, err := borrower.ReadCodeSegmentTo(addr, 5, 0, got)
	require.NoError(t, err)
	require.True(t, borrowed, "the segment copy must not need a state cache")
	require.Equal(t, code, got)
	require.Len(t, lentCaps, 1, "the decode goes into the caller's buffer")
}

func TestStateGetterCodeSegmentRefusesAnAccountWrittenInTheBatch(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	addr := make([]byte, 20)
	addr[0] = 0x19
	committedCode := bytes.Repeat([]byte{0x19}, 1024)
	batchHash := crypto.Keccak256Hash([]byte{0x1a})

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	tx := codeBufTx{TemporalTx: roTx, code: map[string][]byte{string(addr): committedCode}}

	sc := newSmallStateCache()
	t.Cleanup(sc.Close)
	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(sc)
	account := accounts.SerialiseV3(&accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(batchHash)})
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, addr, account, 5, nil))

	var buf []byte
	borrower := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{}.WithCodeBuf(&buf)).(interface {
		ReadCodeSegmentTo(addr []byte, txNum, from uint64, dst []byte) (bool, error)
	})
	borrowed, err := borrower.ReadCodeSegmentTo(addr, 5, 0, nil)
	require.NoError(t, err)
	require.False(t, borrowed, "the batch's code hash must not be paired with the committed bytes")
}
