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

package integrity_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type accountHistoryFaults struct {
	storageKeptAfterDestroy bool
	codeKeptAfterClear      bool
}

// buildAccountHistory writes, in files of 10-tx steps: a contract with two storage slots
// destroyed at txNum 15 and re-created at 17 with one slot, and an EOA whose delegation is
// cleared at txNum 25. One fault wipes the storage at the re-create before the destroy is
// written, which leaves the destroy nothing to wipe; the other skips the code delete at the
// clear.
func buildAccountHistory(t *testing.T, faults accountHistoryFaults) kv.TemporalRwDB {
	t.Helper()
	ctx := t.Context()
	const stepSize, txs = 10, 60
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(stepSize))
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	contract, eoa := common.HexToAddress("0xc0"), common.HexToAddress("0xe0")
	code := []byte{0x60, 0x00, 0xff}
	delegate := common.HexToAddress("0xde")
	delegation := append([]byte{0xef, 0x01, 0x00}, delegate[:]...)
	account := func(nonce uint64, code []byte) []byte {
		acc := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
		if len(code) > 0 {
			acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
		}
		return accounts.SerialiseV3(&acc)
	}
	slot := func(i byte) []byte {
		h := common.BytesToHash([]byte{i})
		return append(contract[:], h[:]...)
	}
	put := func(d kv.Domain, k, v []byte, txNum uint64) {
		require.NoError(t, domains.DomainPut(d, tx, k, v, txNum, nil))
	}
	del := func(d kv.Domain, k []byte, txNum uint64) { require.NoError(t, domains.DomainDel(d, tx, k, txNum, nil)) }

	put(kv.AccountsDomain, contract[:], account(1, code), 1)
	put(kv.CodeDomain, contract[:], code, 1)
	put(kv.StorageDomain, slot(0), []byte{7}, 1)
	put(kv.StorageDomain, slot(1), []byte{9}, 1)
	put(kv.AccountsDomain, eoa[:], account(1, delegation), 2)
	put(kv.CodeDomain, eoa[:], delegation, 2)

	if faults.storageKeptAfterDestroy {
		require.NoError(t, domains.DomainDelPrefix(kv.StorageDomain, tx, contract[:], 17))
	}
	del(kv.AccountsDomain, contract[:], 15)
	put(kv.AccountsDomain, contract[:], account(1, code), 17)
	put(kv.CodeDomain, contract[:], code, 17)
	put(kv.StorageDomain, slot(0), []byte{7}, 17)

	put(kv.AccountsDomain, eoa[:], account(2, nil), 25)
	if !faults.codeKeptAfterClear {
		del(kv.CodeDomain, eoa[:], 25)
	}

	filler := common.HexToAddress("0xf0")
	for txNum := uint64(30); txNum < txs; txNum++ {
		put(kv.AccountsDomain, filler[:], account(txNum, nil), txNum)
	}

	require.NoError(t, domains.Flush(ctx, tx))
	require.NoError(t, tx.Commit())
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	require.NoError(t, agg.BuildFiles(db, txs, collateAll))
	require.GreaterOrEqual(t, agg.EndTxNumMinimax(), uint64(30))
	return db
}

func TestCheckStorageOfAbsentAccount(t *testing.T) {
	sc := integrity.SamplerCfg{SampleRatio: 1}
	t.Run("storage wiped at destroy", func(t *testing.T) {
		db := buildAccountHistory(t, accountHistoryFaults{codeKeptAfterClear: true})
		require.NoError(t, integrity.CheckStorageOfAbsentAccount(t.Context(), sc, db, true, 0, log.New()))
	})
	t.Run("storage kept after destroy", func(t *testing.T) {
		db := buildAccountHistory(t, accountHistoryFaults{storageKeptAfterDestroy: true})
		err := integrity.CheckStorageOfAbsentAccount(t.Context(), sc, db, true, 0, log.New())
		require.ErrorIs(t, err, integrity.ErrIntegrity)
		require.ErrorContains(t, err, "txNum 15")
	})
}

func TestCheckCodeHash(t *testing.T) {
	sc := integrity.SamplerCfg{SampleRatio: 1}
	t.Run("code deleted at clear", func(t *testing.T) {
		db := buildAccountHistory(t, accountHistoryFaults{storageKeptAfterDestroy: true})
		require.NoError(t, integrity.CheckCodeHash(t.Context(), sc, db, true, 0, log.New()))
	})
	t.Run("code kept after clear", func(t *testing.T) {
		db := buildAccountHistory(t, accountHistoryFaults{codeKeptAfterClear: true})
		err := integrity.CheckCodeHash(t.Context(), sc, db, true, 0, log.New())
		require.ErrorIs(t, err, integrity.ErrIntegrity)
		require.ErrorContains(t, err, "txNum 25")
	})
	t.Run("code kept after clear, latest state only", func(t *testing.T) {
		db := buildAccountHistory(t, accountHistoryFaults{codeKeptAfterClear: true})
		err := integrity.CheckCodeHash(t.Context(), sc, db, true, 4, log.New())
		require.ErrorIs(t, err, integrity.ErrIntegrity)
		require.ErrorContains(t, err, "latest")
	})
	t.Run("never wiped, one problem per account", func(t *testing.T) {
		db := buildDestroyedContract(t, 15, 20)
		err := integrity.CheckStorageOfAbsentAccount(t.Context(), sc, db, false, 0, log.New())
		require.ErrorIs(t, err, integrity.ErrIntegrity)
		require.ErrorContains(t, err, "found 1 problem(s)")
	})
}

// buildDestroyedContract writes a contract with one storage slot at txNum 1 and destroys it
// at txNum 15 without re-creating it. wipeAt is where the storage wipe is written (0: never),
// and writeAt is where two slots are written after the destroy (0: never). The contract
// address ends in 0xff, and its NextSubtree neighbour is a live account with storage.
func buildDestroyedContract(t *testing.T, wipeAt, writeAt uint64) kv.TemporalRwDB {
	t.Helper()
	ctx := t.Context()
	const stepSize, txs = 10, 60
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(stepSize))
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	contract, neighbour, filler := common.HexToAddress("0xff"), common.HexToAddress("0x0100"), common.HexToAddress("0xf0")
	h := common.BytesToHash([]byte{1})
	slot := append(contract[:], h[:]...)
	account := func(nonce uint64) []byte {
		return accounts.SerialiseV3(&accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash})
	}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, contract[:], account(1), 1, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, slot, []byte{7}, 1, nil))
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, neighbour[:], account(1), 1, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, append(neighbour[:], h[:]...), []byte{7}, 1, nil))
	if wipeAt > 0 {
		require.NoError(t, domains.DomainDelPrefix(kv.StorageDomain, tx, contract[:], wipeAt))
	}
	require.NoError(t, domains.DomainDel(kv.AccountsDomain, tx, contract[:], 15, nil))
	if writeAt > 0 {
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, slot, []byte{8}, writeAt, nil))
		h2 := common.BytesToHash([]byte{2})
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, append(contract[:], h2[:]...), []byte{9}, writeAt, nil))
	}
	for txNum := uint64(30); txNum < txs; txNum++ {
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, filler[:], account(txNum), txNum, nil))
	}

	require.NoError(t, domains.Flush(ctx, tx))
	require.NoError(t, tx.Commit())
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	require.NoError(t, agg.BuildFiles(db, txs, collateAll))
	return db
}

func TestStorageKeptAfterDestroyNotRecreated(t *testing.T) {
	sc := integrity.SamplerCfg{SampleRatio: 1}
	t.Run("wiped at destroy", func(t *testing.T) {
		db := buildDestroyedContract(t, 15, 0)
		require.NoError(t, integrity.CheckStorageOfAbsentAccount(t.Context(), sc, db, true, 0, log.New()))
	})
	t.Run("wiped late", func(t *testing.T) {
		db := buildDestroyedContract(t, 25, 0)
		err := integrity.CheckStorageOfAbsentAccount(t.Context(), sc, db, true, 0, log.New())
		require.ErrorIs(t, err, integrity.ErrIntegrity)
		require.ErrorContains(t, err, "txNum 15")
	})
	t.Run("wiped late, all problems listed", func(t *testing.T) {
		db := buildDestroyedContract(t, 25, 0)
		err := integrity.CheckStorageOfAbsentAccount(t.Context(), sc, db, false, 0, log.New())
		require.ErrorIs(t, err, integrity.ErrIntegrity)
		require.ErrorContains(t, err, "found 1 problem(s)")
	})
	t.Run("never wiped", func(t *testing.T) {
		db := buildDestroyedContract(t, 15, 20)
		err := integrity.CheckStorageOfAbsentAccount(t.Context(), sc, db, true, 0, log.New())
		require.ErrorIs(t, err, integrity.ErrIntegrity)
		require.ErrorContains(t, err, "latest")
	})
	t.Run("never wiped, one problem per account", func(t *testing.T) {
		db := buildDestroyedContract(t, 15, 20)
		err := integrity.CheckStorageOfAbsentAccount(t.Context(), sc, db, false, 0, log.New())
		require.ErrorIs(t, err, integrity.ErrIntegrity)
		require.ErrorContains(t, err, "found 1 problem(s)")
	})
}
