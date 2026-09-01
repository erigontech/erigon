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
	"encoding/hex"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type acceptanceEntry struct {
	domain kv.Domain
	key    []byte
	value  []byte
}

func newAcceptanceDB(t *testing.T, stepSize, frozenSteps uint64) (kv.TemporalRwDB, *state.Aggregator) {
	t.Helper()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	rawDB := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, logger), dirs.Chaindata).
		GrowthStep(32 * 1024 * 1024).
		MapSize(2 * 1024 * 1024 * 1024).
		MustOpen()
	t.Cleanup(rawDB.Close)

	agg := state.NewTest(dirs).
		StepSize(stepSize).
		StepsInFrozenFile(frozenSteps).
		Logger(logger).
		MustOpen(t.Context(), rawDB)
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder())

	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	return db, agg
}

func acceptanceAccount(prefix byte, nonce, balance uint64) acceptanceEntry {
	key := make([]byte, length.Addr)
	key[0] = prefix
	key[len(key)-1] = prefix
	account := accounts.Account{
		Nonce:    nonce,
		Balance:  *uint256.NewInt(balance),
		CodeHash: accounts.EmptyCodeHash,
	}
	return acceptanceEntry{domain: kv.AccountsDomain, key: key, value: accounts.SerialiseV3(&account)}
}

// acceptanceStorage builds a slot under the account acceptanceAccount(addrPrefix, ...) writes, so
// batches carrying several slots per account exercise storage subtrees and the address hoist.
func acceptanceStorage(addrPrefix, slotPrefix byte, value uint64) acceptanceEntry {
	key := make([]byte, length.Addr+length.Hash)
	key[0] = addrPrefix
	key[length.Addr-1] = addrPrefix
	key[length.Addr] = slotPrefix
	key[len(key)-1] = slotPrefix
	return acceptanceEntry{domain: kv.StorageDomain, key: key, value: uint256.NewInt(value).Bytes()}
}

func acceptanceBatches() [][]acceptanceEntry {
	return [][]acceptanceEntry{
		{
			acceptanceAccount(0x11, 1, 101), acceptanceAccount(0x22, 1, 202),
			acceptanceStorage(0x11, 0x01, 11), acceptanceStorage(0x11, 0x02, 12),
		},
		{
			acceptanceAccount(0x11, 2, 303), acceptanceAccount(0x33, 1, 404),
			acceptanceStorage(0x11, 0x02, 22), acceptanceStorage(0x22, 0x01, 21),
		},
		{
			acceptanceAccount(0x22, 2, 505), acceptanceAccount(0x44, 1, 606),
			acceptanceStorage(0x11, 0x03, 13), acceptanceStorage(0x33, 0x01, 31),
		},
	}
}

func applyAcceptanceBatch(t *testing.T, db kv.TemporalRwDB, entries []acceptanceEntry, txNum uint64) []byte {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, rawdbv3.TxNums.Append(tx, txNum, txNum))
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	for i := range entries {
		entry := &entries[i]
		previous, _, err := domains.GetLatest(entry.domain, tx, entry.key)
		require.NoError(t, err)
		require.NoError(t, domains.DomainPut(entry.domain, tx, entry.key, entry.value, txNum, previous))
	}
	root, err := domains.ComputeCommitment(t.Context(), tx, true, txNum, txNum, "acceptance", nil)
	require.NoError(t, err)
	require.NoError(t, domains.Commit(t.Context(), tx))
	domains.Close()
	return root
}

func allAcceptanceRecords(t *testing.T, db kv.TemporalRoDB) map[string][]byte {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	it, err := tx.Debug().RangeLatest(kv.CommitmentDomain, nil, nil, -1)
	require.NoError(t, err)
	defer it.Close()

	records := make(map[string][]byte)
	for it.HasNext() {
		key, value, err := it.Next()
		require.NoError(t, err)
		if bytes.Equal(key, commitment.KeyCommitmentState) || bytes.Equal(key, commitment.LegacyKeyCommitmentState) {
			continue
		}
		if len(value) == 0 {
			continue
		}
		records[string(key)] = bytes.Clone(value)
	}
	return records
}

func nonEmptyAcceptanceRecords(records map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(records))
	for key, value := range records {
		if len(value) > 0 {
			result[key] = value
		}
	}
	return result
}

func cloneAcceptanceRecords(records map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(records))
	for key, value := range records {
		result[key] = bytes.Clone(value)
	}
	return result
}

func recomputeAcceptanceRoot(t *testing.T, db kv.TemporalRwDB) []byte {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		it, err := tx.Debug().RangeLatest(domain, nil, nil, -1)
		require.NoError(t, err)
		for it.HasNext() {
			key, _, err := it.Next()
			require.NoError(t, err)
			domains.GetCommitmentContext().TouchKey(domain, string(key), nil)
		}
		it.Close()
	}
	root, err := domains.ComputeCommitment(t.Context(), tx, false, 0, 0, "", nil)
	require.NoError(t, err)
	return root
}

func acceptanceCommitmentFiles(t *testing.T, agg *state.Aggregator) []kv.VisibleFile {
	t.Helper()
	at := agg.BeginFilesRo()
	defer at.Close()
	return at.Files(kv.CommitmentDomain)
}

// Roots the legacy trie produces for acceptanceBatches, captured before any v3 work touched the
// shared fold. The v2 arm is the oracle for root parity, so it has to be pinned independently:
// without this a change to fold moves both arms together and the comparison goes quiet.
var acceptanceLegacyRoots = [...]string{
	"c7c00ebd56b4ae505a9bb96f94e1d0119f86dfb1c75ac32e6a45e314188fe672",
	"d593f72675e698ba2e5c4650e75d3b596cc8f2ac694643a7eca54b4c62937136",
	"bb68582092fc3e61dbe7f0b7aee9b9a1e29a5d885a5bda81ec390383a4751793",
}

func requireArmRecordFormat(t *testing.T, agg *state.Aggregator, edgeRecords bool) {
	t.Helper()
	files := acceptanceCommitmentFiles(t, agg)
	require.NotEmpty(t, files, "arm produced no commitment files")
	for _, file := range files {
		require.Equalf(t, edgeRecords, statecfg.CommitmentEdgeRecords(file.Version()),
			"commitment file %s has the wrong record format", file.Fullpath())
	}
}

// The v3 arm must agree with the legacy arm on every root it produces. Nothing else in the
// suite compares the two formats over a real update stream, so a divergence introduced by the
// edge-record encoding, the address hoist or the record merge surfaces only here.
func TestCommitmentV3RootsMatchLegacyAcrossBatches(t *testing.T) {
	legacyDB, legacyAgg := newAcceptanceDB(t, 1, 2)
	legacyAgg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, false)
	v3DB, v3Agg := newAcceptanceDB(t, 1, 2)
	v3Agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)
	require.False(t, legacyAgg.Cfg(kv.CommitmentDomain).EdgeRecordsInCommitment)
	require.True(t, v3Agg.Cfg(kv.CommitmentDomain).EdgeRecordsInCommitment)

	for batchNumber, batch := range acceptanceBatches() {
		txNum := uint64(batchNumber + 1)
		wantRoot := applyAcceptanceBatch(t, legacyDB, batch, txNum)
		require.Equalf(t, acceptanceLegacyRoots[batchNumber], hex.EncodeToString(wantRoot),
			"batch %d legacy root moved: the oracle is no longer independent", txNum)
		gotRoot := applyAcceptanceBatch(t, v3DB, batch, txNum)
		require.Equalf(t, wantRoot, gotRoot, "batch %d root", txNum)
	}

	for _, agg := range []*state.Aggregator{legacyAgg, v3Agg} {
		require.NoError(t, agg.BuildFiles(4))
		require.NoError(t, agg.MergeLoop(t.Context()))
	}
	requireArmRecordFormat(t, legacyAgg, false)
	requireArmRecordFormat(t, v3Agg, true)

	require.Equal(t, recomputeAcceptanceRoot(t, legacyDB), recomputeAcceptanceRoot(t, v3DB), "post-merge root")
}

// Merging must carry the newest record per key into the merged file and leave the trie readable.
func TestCommitmentV3MergedFileHoldsLatestRecords(t *testing.T) {
	db, agg := newAcceptanceDB(t, 1, 2)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)

	recordsByTxNum := make(map[uint64]map[string][]byte, len(acceptanceBatches()))
	var lastRoot []byte
	for batchNumber, batch := range acceptanceBatches() {
		txNum := uint64(batchNumber + 1)
		lastRoot = applyAcceptanceBatch(t, db, batch, txNum)
		recordsByTxNum[txNum] = cloneAcceptanceRecords(nonEmptyAcceptanceRecords(allAcceptanceRecords(t, db)))
	}

	require.NoError(t, agg.BuildFiles(4))
	require.NoError(t, agg.MergeLoop(t.Context()))

	var mergedFile kv.VisibleFile
	for _, file := range acceptanceCommitmentFiles(t, agg) {
		if statecfg.CommitmentEdgeRecords(file.Version()) && file.EndRootNum()-file.StartRootNum() > agg.StepSize() {
			mergedFile = file
			break
		}
	}
	require.NotNil(t, mergedFile, "expected a merged v3 commitment file")
	mergedRecords, ok := recordsByTxNum[mergedFile.EndRootNum()-1]
	require.Truef(t, ok, "no batch state recorded for the merged file ending at root %d", mergedFile.EndRootNum())

	keys, values := readKVFile(t, agg, mergedFile.Fullpath())
	checked := 0
	for i, key := range keys {
		if commitment.IsCommitmentStateKey(key) || len(values[i]) == 0 {
			continue
		}
		want, ok := mergedRecords[string(key)]
		require.Truef(t, ok, "merged file contains unexpected record key %x", key)
		require.Equalf(t, want, values[i], "merged record value for key %x", key)
		checked++
	}
	require.Positive(t, checked, "merged file must contain edge records")
	require.Equal(t, lastRoot, recomputeAcceptanceRoot(t, db), "post-merge fresh commitment read")
}

// A datadir keeps one record format for its whole life: v2 and v3 are incompatible, and moving
// between them means converting to a fresh set of files rather than flipping the flag mid-stream.
// So every commitment file a DB produces has to match the format its own version advertises.
func TestCommitmentV3FilesMatchTheirVersion(t *testing.T) {
	db, agg := newAcceptanceDB(t, 1, 2)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)
	batches := acceptanceBatches()

	var v3Root []byte
	for batchNumber, batch := range batches {
		v3Root = applyAcceptanceBatch(t, db, batch, uint64(batchNumber+1))
	}
	require.NoError(t, agg.BuildFiles(4))

	files := acceptanceCommitmentFiles(t, agg)
	require.NotEmpty(t, files, "arm produced no commitment files")
	for _, file := range files {
		require.Truef(t, statecfg.CommitmentEdgeRecords(file.Version()),
			"a v3 arm produced a legacy-stamped file %s", file.Fullpath())
		keys, values := readKVFile(t, agg, file.Fullpath())
		for i, key := range keys {
			if commitment.IsCommitmentStateKey(key) || len(values[i]) == 0 {
				continue
			}
			require.Truef(t, commitment.BranchData(values[i]).IsEdgeRecord(),
				"v3 file %s contains a bundled row for key %x", file.Fullpath(), key)
		}
	}

	controlDB, controlAgg := newAcceptanceDB(t, 1, 2)
	controlAgg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, false)
	var controlRoot []byte
	for batchNumber, batch := range batches {
		controlRoot = applyAcceptanceBatch(t, controlDB, batch, uint64(batchNumber+1))
	}
	require.Equal(t, controlRoot, v3Root, "v3 root must match the legacy-only root")
	require.Equal(t, v3Root, recomputeAcceptanceRoot(t, db), "post-build fresh commitment read")
}
