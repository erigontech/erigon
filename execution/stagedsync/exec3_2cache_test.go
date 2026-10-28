// Copyright 2025 The Erigon Authors
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

// these tests have cleanup issues for mdbx on windows
package stagedsync

import (
	"context"
	"os"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/shards"
)

// setup2CacheTest creates the minimal mdbx+temporal+domains stack used by the
// 2-cache baseline tests. Cleanup is registered via t.Cleanup.
func setup2CacheTest(t *testing.T) (kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
	lgr := log.New()

	tmpDir, err := os.MkdirTemp("", "erigon-2cache-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.RemoveAll(tmpDir) })

	dirs := datadir.New(tmpDir)
	rawDb := mdbx.New(dbcfg.ChainDB, lgr).InMem(dirs.Chaindata).MustOpen()
	t.Cleanup(rawDb.Close)

	agg, err := dbstate.NewTest(dirs).StepSize(16).Logger(lgr).Open(context.Background())
	require.NoError(t, err)
	t.Cleanup(agg.Close)

	db, err := temporal.New(rawDb, agg, nil)
	require.NoError(t, err)

	tx, err := db.BeginTemporalRw(context.Background()) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	domains, err := execctx.NewSharedDomains(context.Background(), tx, lgr)
	require.NoError(t, err)
	t.Cleanup(domains.Close)

	return tx, domains
}

// TestCrossBlockTimingRace demonstrates that rs.accounts (StateV3Buffered.accounts)
// allows block N+1 workers to read block N's state even when SharedDomains has
// not yet been updated by the async applyResults goroutine.
//
// Current behaviour (Phase 1 baseline):
//   - BufferedWriter.UpdateAccountData writes to rs.accounts synchronously
//     during finalize(), before the applyResults channel is sent.
//   - Block N+1 workers use NewBufferedReader, which checks rs.accounts before
//     falling back to the (potentially stale) domain reader.
//   - This test PASSES with the current code because rs.accounts is populated.
//
// Phase 3 (#19702) will make this test pass via synchronous domain apply
// instead of rs.accounts, then remove rs.accounts entirely.
func TestCrossBlockTimingRace(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)

	addr := accounts.InternAddress(common.HexToAddress("0xF00D"))
	lgr := log.New()

	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, lgr))

	// Simulate block N's finalize(): versionedWriteCollector writes to rs.accounts
	// synchronously, before the async applyResults channel fires.
	// Domains are NOT updated yet.
	collector := state.NewVersionedWriteCollector(rs)
	blockNAccount := &accounts.Account{Balance: *uint256.NewInt(500), Nonce: 7}
	original := &accounts.Account{}
	err := collector.UpdateAccountData(addr, original, blockNAccount)
	require.NoError(t, err)

	// Block N+1 worker reads via NewBufferedReader.
	// The base reader points at domains, which still have zero balance.
	baseReader := state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	bufferedRdr := state.NewBufferedReader(rs, baseReader)
	ibsN1 := state.New(bufferedRdr)
	defer ibsN1.Close()

	// Must read block N's value from rs.accounts, not stale domains.
	gotBal, err := ibsN1.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(500), gotBal.Uint64(),
		"block N+1 worker must read block N's balance from rs.accounts before domain apply")

	gotNonce, err := ibsN1.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(7), gotNonce,
		"block N+1 worker must read block N's nonce from rs.accounts before domain apply")

	// Sanity check: a plain domain reader (no buffering) still sees zero —
	// the timing hole is real without rs.accounts.
	ibsRaw := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer ibsRaw.Close()
	rawBal, err := ibsRaw.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(0), rawBal.Uint64(),
		"plain domain reader must NOT see block N's balance yet (the timing hole)")

	// Simulate ApplyStateWrites completing (domain apply catches up).
	w := state.NewWriter(domains.AsPutDel(tx), nil, 5)
	ibsApply := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer ibsApply.Close()
	ibsApply.SetTxContext(1, 0)
	err = ibsApply.SetBalance(addr, *uint256.NewInt(500), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibsApply.SetNonce(addr, 7, tracing.NonceChangeUnspecified)
	require.NoError(t, err)
	err = ibsApply.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	// After domain apply, plain reader must see the correct value.
	ibsRawAfter := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer ibsRawAfter.Close()
	rawBalAfter, err := ibsRawAfter.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(500), rawBalAfter.Uint64(),
		"after domain apply, plain reader must see block N's balance")
}

// TestNotifyAccumulatorFromVersionedWrites verifies that NotifyAccumulator
// correctly drives ChangeAccount, ChangeCode, and ChangeStorage notifications
// from VersionedWrites — the same data that flows through the apply loop.
//
// This is a regression guard: the accumulator must receive state-diff
// notifications for the txpool regardless of which finalize path produced
// the writes (legacy IBS path or direct finalizeTx path).
func TestNotifyAccumulatorFromVersionedWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	lgr := log.New()

	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, lgr))

	accumulator := shards.NewAccumulator()

	// StartChange must be called before any ChangeAccount/ChangeCode/ChangeStorage
	// calls — the accumulator panics if latestChange is nil.
	var hdr types.Header
	hdr.Number.SetUint64(42)
	accumulator.StartChange(&hdr, nil, false)

	// Produce collector-format writes via versionedWriteCollector.
	collector := state.NewVersionedWriteCollector(rs)

	addr := accounts.InternAddress(common.HexToAddress("0xBEEF"))

	original := &accounts.Account{}
	code := []byte{0x60, 0x00}
	codeHash := accounts.NewCode(code).Hash
	account := &accounts.Account{Balance: *uint256.NewInt(1000), Nonce: 3, Incarnation: 1, CodeHash: codeHash}
	err := collector.UpdateAccountData(addr, original, account)
	require.NoError(t, err)

	err = collector.UpdateAccountCode(addr, 1, codeHash, code)
	require.NoError(t, err)

	storageKey := accounts.InternKey(common.HexToHash("0x01"))
	var storageVal uint256.Int
	storageVal.SetUint64(999)
	err = collector.WriteAccountStorage(addr, 1, storageKey, uint256.Int{}, storageVal)
	require.NoError(t, err)

	// No-op write (original==value) must not produce a write.
	err = collector.WriteAccountStorage(addr, 1, storageKey, storageVal, storageVal)
	require.NoError(t, err)

	writes := collector.Writes()
	require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, 1, writes, nil, &chain.Rules{}))
	require.NoError(t, rs.NotifyAccumulator(accumulator, tx, []state.WriteSetView{writes}))

	changes := accumulator.Changes()
	require.Len(t, changes, 1, "one StateChange batch expected")
	require.Len(t, changes[0].Changes, 1, "one AccountChange expected (all merged for same address)")
	require.NotEmpty(t, changes[0].Changes[0].Data, "ChangeAccount must populate Data field")
	require.Equal(t, uint64(1), changes[0].Changes[0].Incarnation)
	var notified accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&notified, changes[0].Changes[0].Data))
	require.Equal(t, uint64(1000), notified.Balance.Uint64())
	require.Equal(t, uint64(3), notified.Nonce)
	require.Equal(t, uint64(1), notified.Incarnation)
	require.Equal(t, codeHash, notified.CodeHash)
	require.Equal(t, code, changes[0].Changes[0].Code, "ChangeCode must populate Code field")
	require.Len(t, changes[0].Changes[0].StorageChanges, 1, "one StorageChange expected (no-op skipped)")
	require.Equal(t, storageVal.Bytes(), changes[0].Changes[0].StorageChanges[0].Data,
		"ChangeStorage must populate StorageChange.Data")
}

func notifyAccumulatorFromAppliedWrites(t *testing.T, addr accounts.Address, seed accounts.Account, writes ...state.WriteSetView) *shards.Accumulator {
	t.Helper()

	tx, domains := setup2CacheTest(t)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, log.New()))
	addrVal := addr.Value()
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrVal[:], accounts.SerialiseV3(&seed), 0, nil))

	for i, writeSet := range writes {
		require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, uint64(i+1), writeSet, nil, &chain.Rules{}))
	}

	accumulator := shards.NewAccumulator()
	var hdr types.Header
	hdr.Number.SetUint64(1)
	accumulator.StartChange(&hdr, nil, false)
	require.NoError(t, rs.NotifyAccumulator(accumulator, tx, writes))
	return accumulator
}

func TestNotifyAccumulatorUsesFinalAccount(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xacc0"))
	codeHash := accounts.NewCode([]byte{0x60, 0x01}).Hash
	seed := accounts.Account{
		Nonce:       5,
		Balance:     *uint256.NewInt(100),
		CodeHash:    codeHash,
		Incarnation: 1,
	}
	writes := newWS().
		bal(addr, state.Version{}, *uint256.NewInt(200)).
		build()

	accumulator := notifyAccumulatorFromAppliedWrites(t, addr, seed, writes)
	changes := accumulator.Changes()
	require.Len(t, changes, 1)
	require.Len(t, changes[0].Changes, 1)
	change := changes[0].Changes[0]
	require.Equal(t, addr.Value(), change.Address)
	require.Equal(t, uint64(1), change.Incarnation)

	var got accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&got, change.Data))
	require.Equal(t, uint64(5), got.Nonce)
	require.Equal(t, uint64(200), got.Balance.Uint64())
	require.Equal(t, codeHash, got.CodeHash)
	require.Equal(t, uint64(1), got.Incarnation)
}

func TestNotifyAccumulatorMergesBalanceAndStorageAtFinalIncarnation(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xacc1"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	seed := accounts.Account{Balance: *uint256.NewInt(100), Incarnation: 1}
	storage := *uint256.NewInt(7)
	writes := newWS().
		bal(addr, state.Version{}, *uint256.NewInt(200)).
		stor(addr, key, state.Version{}, storage).
		build()

	accumulator := notifyAccumulatorFromAppliedWrites(t, addr, seed, writes)
	changes := accumulator.Changes()
	require.Len(t, changes, 1)
	require.Len(t, changes[0].Changes, 1)
	change := changes[0].Changes[0]
	require.Equal(t, uint64(1), change.Incarnation)
	require.Len(t, change.StorageChanges, 1)
	require.Equal(t, key.Value(), change.StorageChanges[0].Location)
	require.Equal(t, storage.Bytes(), change.StorageChanges[0].Data)

	var got accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&got, change.Data))
	require.Equal(t, uint64(1), got.Incarnation)
}

func TestNotifyAccumulatorKeepsFinalIncarnationAcrossWriteSets(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xacc2"))
	key := accounts.InternKey(common.HexToHash("0x02"))
	seed := accounts.Account{Balance: *uint256.NewInt(100), Incarnation: 1}
	storage := *uint256.NewInt(8)
	balanceWrites := newWS().
		bal(addr, state.Version{}, *uint256.NewInt(200)).
		build()
	storageWrites := newWS().
		stor(addr, key, state.Version{}, storage).
		build()

	accumulator := notifyAccumulatorFromAppliedWrites(t, addr, seed, balanceWrites, storageWrites)
	changes := accumulator.Changes()
	require.Len(t, changes, 1)
	require.Len(t, changes[0].Changes, 1)
	change := changes[0].Changes[0]
	require.Equal(t, uint64(1), change.Incarnation)
	require.Len(t, change.StorageChanges, 1)
	require.Equal(t, key.Value(), change.StorageChanges[0].Location)
	require.Equal(t, storage.Bytes(), change.StorageChanges[0].Data)

	var got accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&got, change.Data))
	require.Equal(t, uint64(1), got.Incarnation)
}

func TestNotifyAccumulatorMergesBalanceAndCodeAtFinalIncarnation(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xacc3"))
	seed := accounts.Account{Balance: *uint256.NewInt(100), Incarnation: 1}
	code := []byte{0x60, 0x02}
	writes := newWS().
		bal(addr, state.Version{}, *uint256.NewInt(200)).
		code(addr, state.Version{}, accounts.NewCode(code)).
		build()

	accumulator := notifyAccumulatorFromAppliedWrites(t, addr, seed, writes)
	changes := accumulator.Changes()
	require.Len(t, changes, 1)
	require.Len(t, changes[0].Changes, 1)
	change := changes[0].Changes[0]
	require.Equal(t, uint64(1), change.Incarnation)
	require.Equal(t, code, change.Code)

	var got accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&got, change.Data))
	require.Equal(t, uint64(1), got.Incarnation)
}
