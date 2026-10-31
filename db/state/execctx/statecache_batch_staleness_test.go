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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func accWithCode(nonce uint64, code []byte) []byte {
	a := accounts.Account{Nonce: nonce}
	if len(code) == 0 {
		a.CodeHash = accounts.EmptyCodeHash
	} else {
		a.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	}
	return accounts.SerialiseV3(&a)
}

func TestStateCache_UncommittedBatchWinsOnEveryReadPath(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := t.Context()
	db := newTestDb(t, 16)
	sc := newSmallStateCache()

	var addr common.Address
	addr[0] = 0xc1
	slot := append(append([]byte{}, addr[:]...), make([]byte, 32)...)
	slot[len(slot)-1] = 1
	code1, code2 := []byte{0x60, 0x01, 0x00}, []byte{0x60, 0x02, 0x60, 0x03, 0x00}
	acc1, acc2 := accWithCode(1, code1), accWithCode(2, code2)
	st1, st2 := []byte{0x01}, []byte{0x02}

	rw1, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rw1.Rollback()
	sd1, err := execctx.NewSharedDomains(ctx, rw1, log.New())
	require.NoError(t, err)
	defer sd1.Close()
	sd1.BindStateCache(sc)
	sd1.SetTxNum(5)
	require.NoError(t, sd1.DomainPut(kv.AccountsDomain, rw1, addr[:], acc1, 5, nil))
	require.NoError(t, sd1.DomainPut(kv.CodeDomain, rw1, addr[:], code1, 5, nil))
	require.NoError(t, sd1.DomainPut(kv.StorageDomain, rw1, slot, st1, 5, nil))
	require.NoError(t, sd1.Commit(ctx, rw1))

	v, ok := sc.View(nil).Get(kv.AccountsDomain, addr[:])
	require.True(t, ok)
	require.Equal(t, acc1, v)

	rw2, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rw2.Rollback()
	sd2, err := execctx.NewSharedDomains(ctx, rw2, log.New())
	require.NoError(t, err)
	defer sd2.Close()
	sd2.BindStateCache(sc)
	sd2.SetInMemHistoryReads(true)
	sd2.SetTxNum(30)
	require.NoError(t, sd2.DomainPut(kv.AccountsDomain, rw2, addr[:], acc2, 30, acc1))
	require.NoError(t, sd2.DomainPut(kv.CodeDomain, rw2, addr[:], code2, 30, code1))
	require.NoError(t, sd2.DomainPut(kv.StorageDomain, rw2, slot, st2, 30, st1))

	g := sd2.AsStateGetter(rw2, execctxapi.StateGetterOptions{})
	got, _, err := g.GetLatest(kv.AccountsDomain, addr[:], kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, acc2, got, "getter account")
	got, _, err = g.GetLatest(kv.StorageDomain, slot, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, st2, got, "getter storage")
	c, ok, err := g.GetCode(addr[:], 31)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, code2, c, "getter code")
	n, ok, err := g.GetCodeSize(addr[:], 31)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, len(code2), n, "getter code size")

	got, _, err = sd2.GetLatest(kv.StorageDomain, rw2, slot)
	require.NoError(t, err)
	require.Equal(t, st2, got, "sd.GetLatest storage")

	ao := sd2.AsOfGetter(rw2)
	got, ok, err = ao.GetAsOf(kv.AccountsDomain, addr[:], 31)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, acc2, got, "asOf account after write")
	got, ok, err = ao.GetAsOf(kv.StorageDomain, slot, 31)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, st2, got, "asOf storage after write")
	got, ok, err = ao.GetAsOf(kv.StorageDomain, slot, 20)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, st1, got, "asOf storage before write")
}

func TestStateCache_CodeByHashNotPoisonedByHalfAppliedDeploy(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := t.Context()
	db := newTestDb(t, 16)
	sc := newSmallStateCache()

	var addr common.Address
	addr[0] = 0xc2
	code1, code2 := []byte{0x60, 0x01, 0x00}, []byte{0x60, 0x02, 0x60, 0x03, 0x00}
	h2 := crypto.Keccak256(code2)

	rw1, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rw1.Rollback()
	sd1, err := execctx.NewSharedDomains(ctx, rw1, log.New())
	require.NoError(t, err)
	defer sd1.Close()
	sd1.BindStateCache(sc)
	sd1.SetTxNum(5)
	require.NoError(t, sd1.DomainPut(kv.AccountsDomain, rw1, addr[:], accWithCode(1, code1), 5, nil))
	require.NoError(t, sd1.DomainPut(kv.CodeDomain, rw1, addr[:], code1, 5, nil))
	require.NoError(t, sd1.Commit(ctx, rw1))

	rw2, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rw2.Rollback()
	sd2, err := execctx.NewSharedDomains(ctx, rw2, log.New())
	require.NoError(t, err)
	defer sd2.Close()
	sd2.BindStateCache(sc)
	sd2.SetTxNum(30)
	require.NoError(t, sd2.DomainPut(kv.AccountsDomain, rw2, addr[:], accWithCode(2, code2), 30, nil))

	g := sd2.AsStateGetter(rw2, execctxapi.StateGetterOptions{})
	_, _, err = g.GetCode(addr[:], 31)
	require.NoError(t, err)
	_, _, err = g.GetCodeSize(addr[:], 31)
	require.NoError(t, err)

	if cv, ok := sc.View(nil).GetCodeByHash(h2); ok {
		require.Equal(t, code2, cv, "bytes cached under the new codeHash")
	}
	if sz, ok := sc.View(nil).GetCodeSizeByHash(h2); ok {
		require.Equal(t, len(code2), sz, "size cached under the new codeHash")
	}
}

func coldCacheOverCommittedCode(t *testing.T, code1 []byte, addr common.Address) (kv.TemporalRwDB, *cache.StateCache) {
	t.Helper()
	ctx := t.Context()
	db := newTestDb(t, 16)
	seedCache := newSmallStateCache()
	rw, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rw.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rw, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(seedCache)
	sd.SetTxNum(5)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rw, addr[:], accWithCode(1, code1), 5, nil))
	require.NoError(t, sd.DomainPut(kv.CodeDomain, rw, addr[:], code1, 5, nil))
	require.NoError(t, sd.Commit(ctx, rw))

	sc := newSmallStateCache()
	require.NoError(t, execctx.InitStateCacheVersion(ctx, db, sc))
	return db, sc
}

func TestStateCache_ColdCodeFillSkipsHashFromUncommittedAccount(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := t.Context()
	var addr common.Address
	addr[0] = 0xc3
	code1, code2 := []byte{0x60, 0x01, 0x00}, []byte{0x60, 0x02, 0x60, 0x03, 0x00}
	h2 := crypto.Keccak256(code2)
	db, sc := coldCacheOverCommittedCode(t, code1, addr)

	rw, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rw.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rw, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(sc)
	sd.SetTxNum(30)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rw, addr[:], accWithCode(2, code2), 30, nil))

	_, _, err = sd.AsStateGetter(rw, execctxapi.StateGetterOptions{}).GetCode(addr[:], 31)
	require.NoError(t, err)

	cv, ok := sc.View(nil).GetCodeByHash(h2)
	require.False(t, ok, "code bytes cached under the uncommitted codeHash: %x", cv)
}

func TestStateCache_ColdReadStillFillsCodeAndSizeByHash(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := t.Context()
	var addr common.Address
	addr[0] = 0xc4
	code1 := []byte{0x60, 0x01, 0x00}
	h1 := crypto.Keccak256(code1)
	db, sc := coldCacheOverCommittedCode(t, code1, addr)

	rw, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rw.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rw, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(sc)
	sd.SetTxNum(30)

	g := sd.AsStateGetter(rw, execctxapi.StateGetterOptions{})
	n, ok, err := g.GetCodeSize(addr[:], 31)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, len(code1), n)
	sz, ok := sc.View(nil).GetCodeSizeByHash(h1)
	require.True(t, ok, "size not filled by hash on a committed address")
	require.Equal(t, len(code1), sz)

	_, _, err = g.GetCode(addr[:], 31)
	require.NoError(t, err)
	cv, ok := sc.View(nil).GetCodeByHash(h1)
	require.True(t, ok, "code not filled by hash on a committed address")
	require.Equal(t, code1, cv)
}
