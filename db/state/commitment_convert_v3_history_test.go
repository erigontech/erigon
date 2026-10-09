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

package state_test

import (
	"bytes"
	"encoding/binary"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	_ "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func storageSlotsSharingFirstNibble(tb testing.TB) [][]byte {
	tb.Helper()
	byNibble := map[byte][][]byte{}
	for i := uint64(1); ; i++ {
		slot := make([]byte, length.Hash)
		binary.BigEndian.PutUint64(slot[length.Hash-8:], i)
		nibble := crypto.Keccak256(slot)[0] >> 4
		byNibble[nibble] = append(byNibble[nibble], slot)
		if len(byNibble[nibble]) < 2 {
			continue
		}
		for other := range byte(16) {
			if other != nibble && len(byNibble[other]) != 0 {
				return [][]byte{byNibble[nibble][0], byNibble[nibble][1], byNibble[other][0]}
			}
		}
	}
}

func testDbAggregatorWithCommitmentHistory(t *testing.T, stepSize uint64, steps int) (kv.TemporalRwDB, *state.Aggregator, map[uint64][]byte) {
	t.Helper()
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	db, agg := testDbAndAggregatorv3(t, stepSize)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	ctx := t.Context()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New(), execctx.WithParaTrieDB(db))
	require.NoError(t, err)
	defer domains.Close()

	rnd := newRnd(7)
	addrs, _ := generateInputData(t, length.Addr, 1, 40)
	slots := storageSlotsSharingFirstNibble(t)
	storageKey := func(a, j int) []byte { return append(bytes.Clone(addrs[a]), slots[j]...) }
	put, del, _ := domainWriter(t, domains, rwTx)

	roots := map[uint64][]byte{}
	txCount := stepSize * uint64(steps)
	var blockNum uint64
	for txNum := range txCount {
		for range 3 {
			a := rnd.IntN(len(addrs))
			acc := accounts.Account{Nonce: txNum, Balance: *uint256.NewInt(rnd.Uint64()), CodeHash: accounts.EmptyCodeHash}
			put(kv.AccountsDomain, addrs[a], accounts.SerialiseV3(&acc), txNum)
			j := rnd.IntN(len(slots))
			if rnd.IntN(4) == 0 {
				del(kv.StorageDomain, storageKey(a, j), txNum)
			} else {
				put(kv.StorageDomain, storageKey(a, j), []byte{byte(rnd.IntN(255) + 1)}, txNum)
			}
		}
		if rnd.IntN(8) == 0 {
			a := rnd.IntN(len(addrs))
			for j := range slots {
				del(kv.StorageDomain, storageKey(a, j), txNum)
			}
			del(kv.AccountsDomain, addrs[a], txNum)
		}
		if txNum%3 == 2 || (txNum+1)%stepSize == 0 {
			root, rootErr := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, rootErr)
			roots[txNum] = root
			blockNum++
		}
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, txCount, unboundedFinalityCtx))
	return db, agg, roots
}

func storageSlotPairs(tb testing.TB) [2][2][]byte {
	tb.Helper()
	byNibble := map[byte][][]byte{}
	var pairs [2][2][]byte
	found := 0
	for i := uint64(1); found < 2; i++ {
		slot := make([]byte, length.Hash)
		binary.BigEndian.PutUint64(slot[length.Hash-8:], i)
		nibble := crypto.Keccak256(slot)[0] >> 4
		byNibble[nibble] = append(byNibble[nibble], slot)
		if len(byNibble[nibble]) == 2 {
			pairs[found] = [2][]byte{byNibble[nibble][0], byNibble[nibble][1]}
			found++
		}
	}
	return pairs
}

func TestConvertCommitmentFiles_V3HistoryOrphanStorageRoot(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	const stepSize, steps = 4, 2
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	put, del, putAccount := domainWriter(t, domains, rwTx)
	ctx := t.Context()

	addrs, _ := generateInputData(t, length.Addr, 1, 4)
	owner := addrs[0]
	pairs := storageSlotPairs(t)
	slots := [][]byte{pairs[0][0], pairs[0][1], pairs[1][0], pairs[1][1]}
	slot := func(s []byte) []byte { return append(bytes.Clone(owner), s...) }

	roots := map[uint64][]byte{}
	for txNum := range uint64(stepSize * steps) {
		switch txNum {
		case 0:
			for _, a := range addrs {
				putAccount(a, txNum)
			}
			for _, s := range slots {
				put(kv.StorageDomain, slot(s), []byte{1}, txNum)
			}
		case 1:
			for _, s := range slots {
				del(kv.StorageDomain, slot(s), txNum)
			}
			del(kv.AccountsDomain, owner, txNum)
		case 2:
			putAccount(owner, txNum)
			put(kv.StorageDomain, slot(pairs[0][0]), []byte{2}, txNum)
		case 3:
			put(kv.StorageDomain, slot(pairs[1][0]), []byte{3}, txNum)
		default:
			putAccount(addrs[1+int(txNum)%3], txNum)
		}
		root, rootErr := domains.ComputeCommitment(ctx, rwTx, true, txNum, txNum, "", nil)
		require.NoError(t, rootErr)
		roots[txNum] = root
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))

	ownerPath := commitment.KeyToHexNibbleHash(owner)
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	orphan, _, err := roTx.GetAsOf(kv.CommitmentDomain, nibbles.HexToCompact(ownerPath), 3)
	require.NoError(t, err)
	roTx.Rollback()
	require.NotEmpty(t, orphan, "legacy storage root record of the recreated account is expected to survive its deletion")

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	roTx, err = db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	latestState, _, err := roTx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentV3State, kv.GetLatestOptions{})
	require.NoError(t, err)
	roTx.Rollback()
	_, lastFrozenCommit, _, err := commitment.DecodeCommitmentV3State(latestState)
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastFrozenCommit, uint64(3))
	for txNum := range lastFrozenCommit + 1 {
		root, rootErr := state.DebugCommitmentV3RootAsOf(ctx, agg, txNum+1)
		require.NoErrorf(t, rootErr, "records as of txNum %d", txNum+1)
		require.Equalf(t, roots[txNum], root, "root as of txNum %d", txNum+1)
	}
}

func TestConvertCommitmentFiles_V3History(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	db, agg, roots := testDbAggregatorWithCommitmentHistory(t, 10, 32)
	legacyHistory, err := filepath.Glob(filepath.Join(agg.Dirs().SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.NotEmpty(t, legacyHistory)

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	converted, err := filepath.Glob(filepath.Join(agg.Dirs().SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.Len(t, converted, len(legacyHistory))
	for _, p := range converted {
		require.Truef(t, strings.HasPrefix(filepath.Base(p), "v3.0-"), "%s is not a v3.0 history file", filepath.Base(p))
	}
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	latestState, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentV3State, kv.GetLatestOptions{})
	require.NoError(t, err)
	_, lastFrozenCommit, _, err := commitment.DecodeCommitmentV3State(latestState)
	require.NoError(t, err)
	tx.Rollback()
	checked := 0
	for _, txNum := range slices.Sorted(maps.Keys(roots)) {
		if txNum > lastFrozenCommit {
			break
		}
		checked++
		root, rootErr := state.DebugCommitmentV3RootAsOf(t.Context(), agg, txNum+1)
		require.NoErrorf(t, rootErr, "records as of txNum %d", txNum+1)
		require.Equalf(t, roots[txNum], root, "root as of txNum %d", txNum+1)
	}
	require.Greater(t, checked, 90)
}

func TestRestoreCommitmentFiles_V3RestoresHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	db, agg, _ := testDbAggregatorWithCommitmentHistory(t, 10, 32)
	dirs := agg.Dirs()
	legacyHistory, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.NotEmpty(t, legacyHistory)

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
	agg.CloseFilesNoReopen()
	require.NoError(t, state.RestoreCommitmentFiles(t.Context(), dirs, log.New()))

	history, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.ElementsMatch(t, legacyHistory, history)
	domain, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	for _, p := range domain {
		require.Falsef(t, strings.HasPrefix(filepath.Base(p), "v3.0-"), "%s left after restore", filepath.Base(p))
	}
}

func TestConvertCommitmentFiles_V3HistoryStaleStorageBranchOfRecreatedAccount(t *testing.T) {
	testV3HistoryStaleStorageBranch(t, 4)
}

func testV3HistoryStaleStorageBranch(t *testing.T, accounts int) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	const stepSize, steps = 4, 3
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	put, del, putAccount := domainWriter(t, domains, rwTx)
	ctx := t.Context()

	addrs, _ := generateInputData(t, length.Addr, 1, accounts)
	owner := addrs[0]
	next := uint64(1)
	live := [][]byte{
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x2}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x5}, &next),
		storageSlotWithHashPrefix(t, []byte{0x3}, &next),
	}
	old := [][]byte{
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x7, 0x0}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x7, 0x9}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0xe, 0x5}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0xe, 0xa}, &next),
	}
	later := storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x7, 0x3}, &next)
	slot := func(s []byte) []byte { return append(bytes.Clone(owner), s...) }
	for txNum := range uint64(stepSize * steps) {
		switch txNum {
		case 0:
			for _, a := range addrs {
				putAccount(a, txNum)
			}
			for _, s := range append(slices.Clone(live), old...) {
				put(kv.StorageDomain, slot(s), []byte{1}, txNum)
			}
		case 1:
			for _, s := range append(slices.Clone(live), old...) {
				del(kv.StorageDomain, slot(s), txNum)
			}
			del(kv.AccountsDomain, owner, txNum)
		case 2:
			putAccount(owner, txNum)
			for _, s := range live {
				put(kv.StorageDomain, slot(s), []byte{2}, txNum)
			}
		case 4:
			put(kv.StorageDomain, slot(later), []byte{3}, txNum)
		default:
			putAccount(addrs[1], txNum)
		}
		_, err := domains.ComputeCommitment(ctx, rwTx, true, txNum, txNum, "", nil)
		require.NoError(t, err)
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	for txNum := uint64(1); txNum <= stepSize*steps; txNum++ {
		_, err := state.DebugCommitmentV3RootAsOf(ctx, agg, txNum)
		require.NoErrorf(t, err, "records as of txNum %d", txNum)
	}
}

func TestRestoreCommitmentFiles_V3ResumesHistoryAfterDomains(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	db, agg, _ := testDbAggregatorWithCommitmentHistory(t, 10, 32)
	dirs := agg.Dirs()
	legacyHistory, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
	agg.CloseFilesNoReopen()
	converted, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "v3.0-commitment.*"))
	require.NoError(t, err)
	for _, p := range converted {
		require.NoError(t, os.Remove(p))
	}
	domainBackup := filepath.Join(dirs.Snap, "backup", "domains")
	backups, err := os.ReadDir(domainBackup)
	require.NoError(t, err)
	for _, e := range backups {
		require.NoError(t, os.Rename(filepath.Join(domainBackup, e.Name()), filepath.Join(dirs.SnapDomain, e.Name())))
	}
	require.NoError(t, os.Remove(domainBackup))

	require.NoError(t, state.RestoreCommitmentFiles(t.Context(), dirs, log.New()))
	history, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.ElementsMatch(t, legacyHistory, history)
}
