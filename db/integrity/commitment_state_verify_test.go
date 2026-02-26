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

package integrity_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCheckStateVerify(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()
	stepSize := uint64(100)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, stepSize)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer domains.Close()

	// Generate 500 txs (5 steps with stepSize=100).
	// Compute commitment at EVERY txNum to ensure all accounts/storage entries
	// have their trie branch data in the same step (no boundary effects).
	txs := stepSize * 5
	rnd := rand.New(rand.NewSource(42))

	for txNum := uint64(1); txNum <= txs; txNum++ {
		addr := make([]byte, length.Addr)
		loc := make([]byte, length.Hash)
		rnd.Read(addr)
		rnd.Read(loc)

		acc := accounts.Account{
			Nonce:       txNum,
			Balance:     *uint256.NewInt(txNum * 1000),
			CodeHash:    accounts.EmptyCodeHash,
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf, txNum, nil)
		require.NoError(t, err)

		storageKey := append(common.Copy(addr), loc...)
		err = domains.DomainPut(kv.StorageDomain, tx, storageKey, []byte{addr[0], loc[0]}, txNum, nil)
		require.NoError(t, err)

		// Compute commitment after each write to ensure trie branch data
		// is in the same step as the domain entry.
		blockNum := txNum
		_, err = domains.ComputeCommitment(ctx, tx, true /* saveStateAfter */, blockNum, txNum, "test", nil)
		require.NoError(t, err)
	}

	// Flush data and commit
	err = domains.Flush(ctx, tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Build snapshot files
	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	endTxNum := agg.EndTxNumMinimax()
	t.Logf("BuildFiles produced files up to txNum=%d (step=%d)", endTxNum, endTxNum/stepSize)
	require.Greater(t, endTxNum, uint64(0), "expected BuildFiles to produce snapshot files")

	// Run the state verification check
	err = integrity.CheckStateVerify(ctx, db, dirs.Tmp, true /* failFast */, 0 /* fromStep */, logger)
	require.NoError(t, err)
}

// TestCheckStateVerify_NoopWrite verifies that no-op writes (same value re-written)
// are detected and not flagged as correspondence failures.
// It writes entries in two step ranges, re-writes one entry with the same value in
// the second range, builds snapshots, and checks that verify-state passes.
func TestCheckStateVerify_NoopWrite(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()
	stepSize := uint64(100)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, stepSize)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer domains.Close()

	rnd := rand.New(rand.NewSource(99))

	// --- Step range 1: txNum 1..200 (steps 0-1) ---
	// Write 5 unique entries per txNum.
	var noopAddr, noopStorageKey []byte
	var noopAccBuf []byte
	var noopStorageVal []byte

	for txNum := uint64(1); txNum <= 200; txNum++ {
		addr := make([]byte, length.Addr)
		loc := make([]byte, length.Hash)
		rnd.Read(addr)
		rnd.Read(loc)

		acc := accounts.Account{
			Nonce:    txNum,
			Balance:  *uint256.NewInt(txNum * 1000),
			CodeHash: accounts.EmptyCodeHash,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf, txNum, nil)
		require.NoError(t, err)

		storageKey := append(common.Copy(addr), loc...)
		storageVal := []byte{addr[0], loc[0]}
		err = domains.DomainPut(kv.StorageDomain, tx, storageKey, storageVal, txNum, nil)
		require.NoError(t, err)

		// Save one entry from step 1 (txNum 100-199) for re-writing in step range 2.
		if txNum == 150 {
			noopAddr = common.Copy(addr)
			noopStorageKey = common.Copy(storageKey)
			noopAccBuf = common.Copy(buf)
			noopStorageVal = common.Copy(storageVal)
		}

		blockNum := txNum
		_, err = domains.ComputeCommitment(ctx, tx, true, blockNum, txNum, "test", nil)
		require.NoError(t, err)
	}

	// --- Step range 2: txNum 201..400 (steps 2-3) ---
	// Write unique entries PLUS re-write the saved entry with the same value (no-op).
	for txNum := uint64(201); txNum <= 400; txNum++ {
		addr := make([]byte, length.Addr)
		loc := make([]byte, length.Hash)
		rnd.Read(addr)
		rnd.Read(loc)

		acc := accounts.Account{
			Nonce:    txNum,
			Balance:  *uint256.NewInt(txNum * 1000),
			CodeHash: accounts.EmptyCodeHash,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf, txNum, nil)
		require.NoError(t, err)

		storageKey := append(common.Copy(addr), loc...)
		err = domains.DomainPut(kv.StorageDomain, tx, storageKey, []byte{addr[0], loc[0]}, txNum, nil)
		require.NoError(t, err)

		// At txNum=250, re-write the saved entry with the SAME value (no-op).
		if txNum == 250 {
			err = domains.DomainPut(kv.AccountsDomain, tx, noopAddr, noopAccBuf, txNum, nil)
			require.NoError(t, err)
			err = domains.DomainPut(kv.StorageDomain, tx, noopStorageKey, noopStorageVal, txNum, nil)
			require.NoError(t, err)
		}

		blockNum := txNum
		_, err = domains.ComputeCommitment(ctx, tx, true, blockNum, txNum, "test", nil)
		require.NoError(t, err)
	}

	// Flush and commit
	err = domains.Flush(ctx, tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Build snapshot files for all steps
	err = agg.BuildFiles(400)
	require.NoError(t, err)

	endTxNum := agg.EndTxNumMinimax()
	t.Logf("BuildFiles produced files up to txNum=%d (step=%d)", endTxNum, endTxNum/stepSize)
	require.Greater(t, endTxNum, uint64(0))

	// Run verify-state — should pass, detecting the no-op write.
	err = integrity.CheckStateVerify(ctx, db, dirs.Tmp, true /* failFast */, 0 /* fromStep */, logger)
	require.NoError(t, err)
}

// TestVerifyBranchHashesFromDB writes accounts+storage, computes commitment,
// then reads the branch data directly from the DB and verifies the hash.
// This tests whether hash verification works on DB-level data before snapshot creation.
func TestVerifyBranchHashesFromDB(t *testing.T) {
	t.Parallel()

	logger := log.New()
	ctx := context.Background()
	stepSize := uint64(100)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, stepSize)

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer domains.Close()

	// Write 5 accounts + storage entries, compute commitment after each
	type entry struct {
		addr       []byte
		loc        []byte
		storageKey []byte
		acc        accounts.Account
		storageVal []byte
	}

	var entries []entry
	rnd := rand.New(rand.NewSource(42))

	for txNum := uint64(1); txNum <= 5; txNum++ {
		addr := make([]byte, length.Addr)
		loc := make([]byte, length.Hash)
		rnd.Read(addr)
		rnd.Read(loc)

		acc := accounts.Account{
			Nonce:    txNum,
			Balance:  *uint256.NewInt(txNum * 1000),
			CodeHash: accounts.EmptyCodeHash,
		}
		accBuf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, accBuf, txNum, nil)
		require.NoError(t, err)

		storageKey := append(common.Copy(addr), loc...)
		storageVal := []byte{addr[0], loc[0]}
		err = domains.DomainPut(kv.StorageDomain, tx, storageKey, storageVal, txNum, nil)
		require.NoError(t, err)

		entries = append(entries, entry{addr: addr, loc: loc, storageKey: storageKey, acc: acc, storageVal: storageVal})

		_, err = domains.ComputeCommitment(ctx, tx, true, txNum, txNum, "test", nil)
		require.NoError(t, err)
	}

	// Flush so data is in the DB
	err = domains.Flush(ctx, tx)
	require.NoError(t, err)

	// Build value maps for all entries
	accountValues := make(map[string][]byte)
	storageValues := make(map[string][]byte)
	for _, e := range entries {
		accBuf := accounts.SerialiseV3(&e.acc)
		accountValues[string(e.addr)] = accBuf
		storageValues[string(e.storageKey)] = e.storageVal
	}

	// Read commitment branch entries from DB using RangeAsOf
	it, err := tx.RangeAsOf(kv.CommitmentDomain, nil, nil, 1000, order.Asc, -1)
	require.NoError(t, err)

	var checked, passed, failed int
	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)

		// Skip the state key (keyCommitmentState)
		if string(k) == "state" || (len(k) > 0 && k[0] == 'S') {
			continue
		}

		bd := commitment.BranchData(v)
		t.Logf("Branch key=%x data=%s", k, bd.String())

		err = commitment.VerifyBranchHashes(k, bd, accountValues, storageValues)
		checked++
		if err != nil {
			t.Errorf("VerifyBranchHashes FAILED for key=%x: %v", k, err)
			failed++
		} else {
			t.Logf("VerifyBranchHashes PASSED for key=%x", k)
			passed++
		}
	}
	it.Close()

	t.Logf("Checked %d branches: %d passed, %d failed", checked, passed, failed)
	require.Zero(t, failed, "expected all branch hashes to verify")
}
