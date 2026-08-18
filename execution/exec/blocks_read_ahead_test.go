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
	"errors"
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

// stubTemporalGetter stands in for the committed-state read view a warmup
// goroutine reads: every GetLatest returns the same fixed value.
type stubTemporalGetter struct {
	v    []byte
	step kv.Step
}

type sharedCodeTemporalGetter struct {
	account      []byte
	code         []byte
	accountReads int
	codeReads    int
}

type countingGetter struct {
	kv.Getter
	getOneCalls int
}

type singleTxRoDB struct {
	kv.RoDB
	tx kv.Tx
}

type firstAccountReadErrorTx struct {
	kv.TemporalTx
	accountReads int
}

func (g *countingGetter) GetOne(string, []byte) ([]byte, error) {
	g.getOneCalls++
	return nil, nil
}

func (db *singleTxRoDB) BeginRo(context.Context) (kv.Tx, error) {
	return db.tx, nil
}

func (tx *firstAccountReadErrorTx) GetLatest(domain kv.Domain, _ []byte) ([]byte, kv.Step, error) {
	if domain == kv.AccountsDomain {
		tx.accountReads++
		if tx.accountReads == 1 {
			return nil, 0, errors.New("transient account read failure")
		}
	}
	return nil, 0, nil
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
		s.accountReads++
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

func TestBlockReadAheaderWaitForWarmup(t *testing.T) {
	readAheader := NewBlockReadAheader()
	for range 2 {
		warmupStarted := make(chan struct{})
		finishWarmup := make(chan struct{})
		require.True(t, readAheader.startWarmup(func() {
			close(warmupStarted)
			<-finishWarmup
		}))
		<-warmupStarted
		done := make(chan struct{})
		go func() {
			readAheader.WaitForWarmup(t.Context())
			close(done)
		}()
		select {
		case <-done:
			t.Fatal("warmup wait returned before warmup completed")
		case <-time.After(50 * time.Millisecond):
		}
		close(finishWarmup)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("warmup wait did not return after warmup completed")
		}
	}
}

func TestBlockReadAheaderIgnoresMissingHeaderOrBody(t *testing.T) {
	readAheader := NewBlockReadAheader()
	header := &types.Header{Number: *uint256.NewInt(1)}
	require.NotPanics(t, func() { readAheader.AddHeaderAndBody(t.Context(), nil, nil, nil, new(types.Body)) })
	require.NotPanics(t, func() { readAheader.AddHeaderAndBody(t.Context(), nil, nil, header, nil) })
	require.Zero(t, readAheader.headers.Len())
	require.Zero(t, readAheader.bodies.Len())
}

func TestBlockReadAheaderIgnoresNilGetter(t *testing.T) {
	oldReadAhead := dbg.ReadAhead
	dbg.SetReadAhead(true)
	t.Cleanup(func() { dbg.SetReadAhead(oldReadAhead) })
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	bal := types.BlockAccessList{{Address: accounts.InternAddress(common.Address{19: 1})}}
	balHash := bal.Hash()
	header := &types.Header{Number: *uint256.NewInt(1), BlockAccessListHash: &balHash}
	readAheader := NewBlockReadAheader()
	require.NotPanics(t, func() { readAheader.AddHeaderAndBody(t.Context(), db, nil, header, new(types.Body)) })
}

func TestMakeBALWarmupTasksSplitsStorageHeavyAccount(t *testing.T) {
	bal := types.BlockAccessList{{
		StorageChanges: make([]*types.SlotChanges, 65),
		StorageReads:   make([]accounts.StorageKey, 3),
	}}
	tasks, workers := makeBALWarmupPlan(bal, 4)
	require.Equal(t, 2, workers)
	require.Equal(t, []balWarmupTask{
		{accountIndex: 0, slotFrom: 0, slotTo: 64},
		{accountIndex: 0, slotFrom: 64, slotTo: 68},
	}, tasks)
}

func TestBALCodeWarmupModeForFlags(t *testing.T) {
	tests := []struct {
		name        string
		warmBALCode bool
		warmTxCode  bool
		want        balCodeWarmupMode
	}{
		{name: "all BAL code", warmBALCode: true, want: balCodeWarmupAll},
		{name: "BAL code takes precedence", warmBALCode: true, warmTxCode: true, want: balCodeWarmupAll},
		{name: "transaction destinations", warmTxCode: true, want: balCodeWarmupTxnDestinations},
		{name: "no code", want: balCodeWarmupNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, balCodeWarmupModeForFlags(test.warmBALCode, test.warmTxCode))
		})
	}
}

func TestUniqueTransactionDestinations(t *testing.T) {
	destinationA := common.Address{19: 0xa1}
	destinationB := common.Address{19: 0xb2}
	txns := types.Transactions{
		types.NewTransaction(0, destinationA, nil, 0, nil, nil),
		types.NewTransaction(1, destinationA, nil, 0, nil, []byte{0x01}),
		types.NewContractCreation(2, nil, 0, nil, []byte{0x02}),
		types.NewTransaction(3, destinationB, nil, 0, nil, nil),
	}
	require.Equal(t, map[accounts.Address]struct{}{accounts.InternAddress(destinationA): {}, accounts.InternAddress(destinationB): {}}, uniqueTransactionDestinations(txns))
}

func TestWarmBALStateTaskLoadsSelectedCode(t *testing.T) {
	for _, test := range []struct {
		name          string
		mode          balCodeWarmupMode
		destination   bool
		contract      bool
		codeChanges   bool
		wantCodeReads int
	}{
		{name: "transaction destination contract", mode: balCodeWarmupTxnDestinations, destination: true, contract: true, wantCodeReads: 1},
		{name: "transaction non-destination contract", mode: balCodeWarmupTxnDestinations, contract: true},
		{name: "transaction destination EOA", mode: balCodeWarmupTxnDestinations, destination: true},
		{name: "all contract", mode: balCodeWarmupAll, contract: true, wantCodeReads: 1},
		{name: "all EOA", mode: balCodeWarmupAll},
		{name: "all forced code change", mode: balCodeWarmupAll, codeChanges: true, wantCodeReads: 1},
		{name: "none", destination: true, contract: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			code := []byte{0xaa, 0x01, 0x02, 0x03}
			account := accounts.NewAccount()
			if test.contract {
				account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
			}
			source := &sharedCodeTemporalGetter{account: accounts.SerialiseV3(&account), code: code}
			stateCache := newTestStateCache()
			t.Cleanup(stateCache.Close)
			frontier := cache.FrontierFunc(func(kv.Domain) (uint64, bool) { return 16, true })
			address := accounts.InternAddress(common.Address{19: 1})
			destinations := make(map[accounts.Address]struct{})
			if test.destination {
				destinations[address] = struct{}{}
			}
			accountChanges := &types.AccountChanges{Address: address}
			if test.codeChanges {
				accountChanges.CodeChanges = []*types.CodeChange{{Bytecode: code}}
			}
			getter := &cachePopulatingGetter{TemporalGetter: source, view: stateCache.View(frontier), stepSize: 16}
			reader := state.NewReaderV3(getter)
			require.NoError(t, warmBALStateTask(reader, accountChanges, balWarmupTask{}, test.mode, destinations))
			require.Equal(t, 1, source.accountReads)
			require.Equal(t, test.wantCodeReads, source.codeReads)
		})
	}
}

func TestWarmBALStateTaskDoesNotRepeatCodeForLaterChunks(t *testing.T) {
	code := []byte{0xaa, 0x01, 0x02, 0x03}
	account := accounts.NewAccount()
	account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	source := &sharedCodeTemporalGetter{account: accounts.SerialiseV3(&account), code: code}
	address := accounts.InternAddress(common.Address{19: 1})
	accountChanges := &types.AccountChanges{Address: address, StorageReads: make([]accounts.StorageKey, 65)}
	reader := state.NewReaderV3(source)
	require.NoError(t, warmBALStateTask(reader, accountChanges, balWarmupTask{slotFrom: 64, slotTo: 65}, balCodeWarmupAll, nil))
	require.Zero(t, source.accountReads)
	require.Zero(t, source.codeReads)
}

func TestMakeBALWarmupTasksKeepsSmallAccountsTogether(t *testing.T) {
	bal := types.BlockAccessList{
		{StorageChanges: make([]*types.SlotChanges, 1)},
		{StorageReads: make([]accounts.StorageKey, 1)},
	}
	tasks, workers := makeBALWarmupPlan(bal, 4)
	require.Equal(t, 2, workers)
	require.Equal(t, []balWarmupTask{
		{accountIndex: 0, slotFrom: 0, slotTo: 1},
		{accountIndex: 1, slotFrom: 0, slotTo: 1},
	}, tasks)
}

func TestWarmBALPropagatesWorkerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	bal := types.BlockAccessList{{Address: accounts.InternAddress(common.Address{19: 1})}}
	err := NewBlockReadAheader().warmBAL(ctx, db, bal, nil, balCodeWarmupNone, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestWarmBALContinuesAfterReadError(t *testing.T) {
	ctx := t.Context()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	baseTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer baseTx.Rollback()
	tx := &firstAccountReadErrorTx{TemporalTx: baseTx}
	readDB := &singleTxRoDB{RoDB: db, tx: tx}
	bal := types.BlockAccessList{
		{Address: accounts.InternAddress(common.Address{19: 1})},
		{Address: accounts.InternAddress(common.Address{19: 2})},
	}
	require.NoError(t, NewBlockReadAheader().warmBAL(ctx, readDB, bal, nil, balCodeWarmupNone, 1))
	require.Equal(t, 2, tx.accountReads)
}

func TestWarmTxnsContinuesAfterReadError(t *testing.T) {
	ctx := t.Context()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	baseTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer baseTx.Rollback()
	tx := &firstAccountReadErrorTx{TemporalTx: baseTx}
	readDB := &singleTxRoDB{RoDB: db, tx: tx}
	txns := types.Transactions{
		types.NewTransaction(0, common.Address{19: 1}, nil, 0, nil, nil),
		types.NewTransaction(1, common.Address{19: 2}, nil, 0, nil, nil),
	}
	require.NoError(t, NewBlockReadAheader().warmTxns(ctx, readDB, txns, 1))
	require.Equal(t, 2, tx.accountReads)
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
	t.Cleanup(stateCache.Close)
	readAheader := NewBlockReadAheader()
	readAheader.SetStateCache(stateCache)
	readAheader.AddHeaderAndBody(ctx, db, overlay, header, body)
	// AddHeaderAndBody must have copied the sidecar synchronously; its caller may
	// release the overlay before the asynchronous state warming completes.
	overlay.Close()
	baseTx.Rollback()
	readAheader.WaitForWarmup(ctx)
	got, ok := stateCache.View(nil).Get(kv.AccountsDomain, address[:])
	require.True(t, ok, "overlay-only BAL account was not warmed")
	require.Equal(t, accountBytes, got)
}

func TestBlockReadAheaderCarriesBlockAccessList(t *testing.T) {
	bra := NewBlockReadAheader()
	header := &types.Header{Number: *uint256.NewInt(1)}
	body := &types.Body{Transactions: []types.Transaction{types.NewTransaction(0, common.Address{}, new(uint256.Int), 0, new(uint256.Int), nil)}}
	blockHash := header.Hash()
	bal := types.BlockAccessList{}
	sender := common.Address{1}
	bra.AddHeaderAndBody(context.Background(), nil, nil, header, body)
	bra.AddBlockAccessList(blockHash, bal)
	bra.AddSenders(sender[:], blockHash)
	block, ok := bra.ReadBlockWithSenders(blockHash)
	require.True(t, ok)
	require.Equal(t, bal, block.BlockAccessList())
	require.NotNil(t, block.BlockAccessList())
}

func TestBlockReadAheaderPrefersCachedBlockAccessList(t *testing.T) {
	oldReadAhead := dbg.ReadAhead
	dbg.SetReadAhead(true)
	t.Cleanup(func() { dbg.SetReadAhead(oldReadAhead) })
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	bal := make(types.BlockAccessList, 0)
	balHash := bal.Hash()
	header := &types.Header{Number: *uint256.NewInt(1), BlockAccessListHash: &balHash}
	bra := NewBlockReadAheader()
	bra.AddBlockAccessList(header.Hash(), bal)
	getter := new(countingGetter)
	bra.AddHeaderAndBody(t.Context(), db, getter, header, new(types.Body))
	bra.WaitForWarmup(t.Context())
	require.Zero(t, getter.getOneCalls)
}

func TestBlockReadAheaderSkipsBlockAccessListReadWhenDisabledOrAbsent(t *testing.T) {
	oldReadAhead := dbg.ReadAhead
	dbg.SetReadAhead(false)
	t.Cleanup(func() { dbg.SetReadAhead(oldReadAhead) })
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	getter := new(countingGetter)
	bal := make(types.BlockAccessList, 0)
	balHash := bal.Hash()
	headerWithBAL := &types.Header{Number: *uint256.NewInt(1), BlockAccessListHash: &balHash}
	bra := NewBlockReadAheader()
	bra.AddHeaderAndBody(t.Context(), db, getter, headerWithBAL, new(types.Body))
	require.Zero(t, getter.getOneCalls, "READ_AHEAD=false must skip the BAL lookup")
	dbg.SetReadAhead(true)
	headerWithoutBAL := &types.Header{Number: *uint256.NewInt(2)}
	bra.AddHeaderAndBody(t.Context(), db, getter, headerWithoutBAL, new(types.Body))
	bra.WaitForWarmup(t.Context())
	require.Zero(t, getter.getOneCalls, "pre-Amsterdam blocks must skip the BAL lookup")
	emptyBAL := make(types.BlockAccessList, 0)
	emptyBALHash := emptyBAL.Hash()
	headerWithEmptyBAL := &types.Header{Number: *uint256.NewInt(3), BlockAccessListHash: &emptyBALHash}
	bra.AddHeaderAndBody(t.Context(), db, getter, headerWithEmptyBAL, new(types.Body))
	bra.WaitForWarmup(t.Context())
	require.Zero(t, getter.getOneCalls, "empty BAL commitments must skip the BAL lookup")
}

func TestBlockReadAheaderSuspendWarmupWaitsForActiveWarmup(t *testing.T) {
	bra := NewBlockReadAheader()
	warmupStarted := make(chan struct{})
	finishWarmup := make(chan struct{})
	warmupDone := make(chan struct{})
	require.True(t, bra.startWarmup(func() {
		close(warmupStarted)
		<-finishWarmup
		close(warmupDone)
	}))
	<-warmupStarted

	suspendStarted := make(chan struct{})
	type suspendResult struct {
		resume func()
		err    error
	}
	suspended := make(chan suspendResult)
	go func() {
		close(suspendStarted)
		resume, err := bra.SuspendWarmup(t.Context())
		suspended <- suspendResult{resume: resume, err: err}
	}()
	<-suspendStarted
	select {
	case result := <-suspended:
		require.NoError(t, result.err)
		result.resume()
		close(finishWarmup)
		<-warmupDone
		t.Fatal("SuspendWarmup returned while a warmup was active")
	case <-time.After(50 * time.Millisecond):
	}

	close(finishWarmup)
	result := <-suspended
	require.NoError(t, result.err)
	result.resume()
	<-warmupDone
}

func TestBlockReadAheaderSuspendWarmupSkipsNewWarmup(t *testing.T) {
	bra := NewBlockReadAheader()
	resume, err := bra.SuspendWarmup(t.Context())
	require.NoError(t, err)

	warmupStarted := make(chan struct{})
	require.False(t, bra.startWarmup(func() { close(warmupStarted) }),
		"warmup must be skipped rather than queued behind the suspension")

	resume()
	select {
	case <-warmupStarted:
		t.Fatal("a skipped warmup started after suspension ended")
	default:
	}

	nextWarmupDone := make(chan struct{})
	require.True(t, bra.startWarmup(func() { close(nextWarmupDone) }))
	select {
	case <-nextWarmupDone:
	case <-time.After(time.Second):
		t.Fatal("a new warmup did not start after suspension ended")
	}
	bra.WaitForWarmup(t.Context())
}

func TestBlockReadAheaderSuspendWarmupHonorsContext(t *testing.T) {
	bra := NewBlockReadAheader()
	warmupStarted := make(chan struct{})
	finishWarmup := make(chan struct{})
	warmupDone := make(chan struct{})
	require.True(t, bra.startWarmup(func() {
		close(warmupStarted)
		<-finishWarmup
		close(warmupDone)
	}))
	<-warmupStarted

	ctx, cancel := context.WithCancel(t.Context())
	suspendStarted := make(chan struct{})
	suspendResult := make(chan error)
	go func() {
		close(suspendStarted)
		resume, err := bra.SuspendWarmup(ctx)
		if resume != nil {
			resume()
		}
		suspendResult <- err
	}()
	<-suspendStarted
	cancel()

	select {
	case err := <-suspendResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		close(finishWarmup)
		<-warmupDone
		<-suspendResult
		t.Fatal("SuspendWarmup did not return when its context was cancelled")
	}

	close(finishWarmup)
	<-warmupDone
	bra.WaitForWarmup(t.Context())
	nextWarmupDone := make(chan struct{})
	require.True(t, bra.startWarmup(func() { close(nextWarmupDone) }),
		"a cancelled suspension must not retain the warmup permit")
	<-nextWarmupDone
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

func TestCachePopulatingGetterReusesCodeByHashAcrossGetters(t *testing.T) {
	code := []byte{0xaa, 0x01, 0x02, 0x03}
	account := accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}
	source := &sharedCodeTemporalGetter{account: accounts.SerialiseV3(&account), code: code}
	stateCache := newTestStateCache()
	t.Cleanup(stateCache.Close)
	frontier := cache.FrontierFunc(func(kv.Domain) (uint64, bool) { return 16, true })
	firstAddress := accounts.InternAddress(common.Address{19: 1})
	secondAddress := accounts.InternAddress(common.Address{19: 2})
	firstReader := state.NewReaderV3(&cachePopulatingGetter{TemporalGetter: source, view: stateCache.View(frontier), stepSize: 16})
	gotAccount, err := firstReader.ReadAccountData(firstAddress)
	require.NoError(t, err)
	require.Equal(t, account.CodeHash, gotAccount.CodeHash)
	gotCode, err := firstReader.ReadAccountCode(firstAddress)
	require.NoError(t, err)
	require.Equal(t, code, gotCode)
	accountReader := state.NewReaderV3(&cachePopulatingGetter{TemporalGetter: source, view: stateCache.View(frontier), stepSize: 16})
	gotAccount, err = accountReader.ReadAccountData(secondAddress)
	require.NoError(t, err)
	require.Equal(t, account.CodeHash, gotAccount.CodeHash)
	codeGetter := &cachePopulatingGetter{TemporalGetter: source, view: stateCache.View(frontier), stepSize: 16}
	codeReader := state.NewReaderV3(codeGetter)
	gotCode, err = codeReader.ReadAccountCode(secondAddress)
	require.NoError(t, err)
	require.Equal(t, code, gotCode)
	require.Equal(t, 2, source.accountReads, "the code phase must reuse the account read from the state phase")
	require.Equal(t, 1, source.codeReads, "identical code must be loaded from the address-keyed domain only once")
	secondAddressValue := secondAddress.Value()
	boundCode, ok := stateCache.View(nil).Get(kv.CodeDomain, secondAddressValue[:])
	require.True(t, ok, "the code-hash fast path must bind code to the second address")
	require.Equal(t, code, boundCode)
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
	sc.Applier().Publish(0, 1, []cache.StateUpdate{{Domain: kv.AccountsDomain, Key: key, TxNum: 20}})
	cpg := &cachePopulatingGetter{
		TemporalGetter: stubTemporalGetter{v: []byte("pre-delete-record")},
		stepSize:       1_562_500,
		view: sc.View(cache.FrontierWithStateVersion(
			cache.FrontierFunc(func(kv.Domain) (uint64, bool) { return 11, true }), 1)),
	}

	_, _, err := cpg.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok)
}

func emptyVisibleEnd(kv.Domain) (uint64, bool) { return 0, true }
