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
	"errors"
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
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type acceptanceEntry struct {
	domain kv.Domain
	key    []byte
	value  []byte
	update commitment.Update
}

type acceptanceOracle struct {
	plain     map[string][]byte
	records   map[string][]byte
	trieState []byte
	numBuf    [binary.MaxVarintLen64]byte
}

func newAcceptanceOracle() *acceptanceOracle {
	return &acceptanceOracle{
		plain:   make(map[string][]byte),
		records: make(map[string][]byte),
	}
}

func (o *acceptanceOracle) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if bytes.Equal(prefix, commitment.LegacyKeyCommitmentState) {
		return bytes.Clone(o.trieState), 0, nil
	}
	data, step, _, _, err := o.branchWithMask(prefix, 0, false)
	return data, step, err
}

func (o *acceptanceOracle) BranchWithMask(prefix []byte, mask uint16, maskKnown bool) (data []byte, step kv.Step, childMasks [16]uint16, childMasksKnown uint16, err error) {
	return o.branchWithMask(prefix, mask, maskKnown)
}

func (o *acceptanceOracle) branchWithMask(prefix []byte, mask uint16, maskKnown bool) (data []byte, step kv.Step, childMasks [16]uint16, childMasksKnown uint16, err error) {
	if bytes.Equal(prefix, commitment.LegacyKeyCommitmentState) {
		return bytes.Clone(o.trieState), 0, childMasks, 0, nil
	}

	nodeKey := nibbles.EncodeKeyV3(nibbles.CompactToHex(prefix))
	var records [16][]byte
	var recordsPresent uint16
	for nibble := range 16 {
		key := nibbles.ChildKeyV3(nodeKey, byte(nibble))
		if record, ok := o.records[string(key)]; ok {
			records[nibble] = bytes.Clone(record)
			recordsPresent |= 1 << nibble
		}
	}
	read, err := commitment.SynthesizeBranchRow(mask, maskKnown, records, recordsPresent, nil)
	if err != nil {
		return nil, 0, childMasks, 0, err
	}
	return bytes.Clone(read.Data), 0, read.ChildMasks, read.ChildMasksKnown, nil
}

func (o *acceptanceOracle) PutBranch(prefix, data, _ []byte) error {
	if bytes.Equal(prefix, commitment.LegacyKeyCommitmentState) {
		o.trieState = bytes.Clone(data)
		return nil
	}
	if len(data) == 0 {
		delete(o.records, string(prefix))
		return nil
	}
	o.records[string(prefix)] = bytes.Clone(data)
	return nil
}

func (o *acceptanceOracle) Account(key []byte) (*commitment.Update, error) {
	return o.plainUpdate(key)
}

func (o *acceptanceOracle) Storage(key []byte) (*commitment.Update, error) {
	return o.plainUpdate(key)
}

func (o *acceptanceOracle) plainUpdate(key []byte) (*commitment.Update, error) {
	encoded, ok := o.plain[string(key)]
	if !ok {
		return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
	}
	var update commitment.Update
	pos, err := update.Decode(encoded, 0)
	if err != nil {
		return nil, err
	}
	if pos != len(encoded) {
		return nil, errors.New("plain update has trailing bytes")
	}
	return &update, nil
}

func (o *acceptanceOracle) apply(keys [][]byte, updates []commitment.Update) error {
	for i, key := range keys {
		update := updates[i]
		if update.Deleted() {
			delete(o.plain, string(key))
			continue
		}

		var existing commitment.Update
		if encoded, ok := o.plain[string(key)]; ok {
			pos, err := existing.Decode(encoded, 0)
			if err != nil {
				return err
			}
			if pos != len(encoded) {
				return errors.New("plain update has trailing bytes")
			}
		}
		existing.Merge(&update)
		o.plain[string(key)] = existing.Encode(nil, o.numBuf[:])
	}
	return nil
}

func (o *acceptanceOracle) process(t *testing.T, entries []acceptanceEntry) []byte {
	t.Helper()
	keys := make([][]byte, len(entries))
	updates := make([]commitment.Update, len(entries))
	for i := range entries {
		keys[i] = entries[i].key
		updates[i] = entries[i].update
	}
	require.NoError(t, o.apply(keys, updates))

	cfg := commitment.DefaultTrieConfig()
	cfg.EdgeRecords = true
	cfg.DeferBranchUpdates = false
	trie := commitment.NewHexPatriciaHashed(length.Addr, o, cfg)
	defer trie.Release()
	require.NoError(t, trie.SetState(o.trieState))
	wrapped := commitment.NewUpdates(commitment.ModeDirect, t.TempDir(), commitment.KeyToHexNibbleHash)
	defer wrapped.Close()
	for _, key := range keys {
		wrapped.TouchPlainKey(string(key), nil, nil)
	}
	root, err := trie.Process(t.Context(), wrapped, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	o.trieState, err = trie.EncodeCurrentState(nil)
	require.NoError(t, err)
	return root
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
	return acceptanceEntry{
		domain: kv.AccountsDomain,
		key:    key,
		value:  accounts.SerialiseV3(&account),
		update: commitment.Update{
			Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
			Balance:  account.Balance,
			Nonce:    nonce,
			CodeHash: accounts.EmptyCodeHash.Value(),
		},
	}
}

func acceptanceBatches() [][]acceptanceEntry {
	return [][]acceptanceEntry{
		{acceptanceAccount(0x11, 1, 101), acceptanceAccount(0x22, 1, 202)},
		{acceptanceAccount(0x11, 2, 303), acceptanceAccount(0x33, 1, 404)},
		{acceptanceAccount(0x22, 2, 505), acceptanceAccount(0x44, 1, 606)},
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

func latestAcceptanceState(t *testing.T, db kv.TemporalRoDB) []byte {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	value, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(value), 18)
	stateLen := int(binary.BigEndian.Uint16(value[16:18]))
	require.GreaterOrEqual(t, len(value), 18+stateLen)
	return bytes.Clone(value[18 : 18+stateLen])
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

func TestCommitmentV3StoredRecordsMatchSequentialTrieAcrossBatches(t *testing.T) {
	db, agg := newAcceptanceDB(t, 1, 2)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)
	require.True(t, agg.Cfg(kv.CommitmentDomain).EdgeRecordsInCommitment)
	oracle := newAcceptanceOracle()
	var wantRoot []byte
	recordsByTxNum := make(map[uint64]map[string][]byte, len(acceptanceBatches()))

	for batchNumber, batch := range acceptanceBatches() {
		txNum := uint64(batchNumber + 1)
		gotRoot := applyAcceptanceBatch(t, db, batch, txNum)
		wantRoot = oracle.process(t, batch)
		require.Equal(t, nonEmptyAcceptanceRecords(oracle.records), allAcceptanceRecords(t, db), "batch %d stored records", txNum)
		require.Equal(t, oracle.trieState, latestAcceptanceState(t, db), "batch %d serialized trie state", txNum)
		recordsByTxNum[txNum] = cloneAcceptanceRecords(nonEmptyAcceptanceRecords(oracle.records))
		require.Equal(t, wantRoot, gotRoot, "batch %d root", txNum)
	}

	require.NoError(t, agg.BuildFiles(4))
	require.NoError(t, agg.MergeLoop(t.Context()))
	files := acceptanceCommitmentFiles(t, agg)
	var mergedFile kv.VisibleFile
	for _, file := range files {
		if statecfg.CommitmentEdgeRecords(file.Version()) && file.EndRootNum()-file.StartRootNum() > agg.StepSize() {
			mergedFile = file
			break
		}
	}
	require.NotNil(t, mergedFile, "expected a merged v3 commitment file")
	keys, values := readKVFile(t, agg, mergedFile.Fullpath())
	mergedRecords, ok := recordsByTxNum[mergedFile.EndRootNum()-1]
	require.Truef(t, ok, "missing sequential oracle state for merged file ending at root %d", mergedFile.EndRootNum())
	checked := 0
	for i, key := range keys {
		if commitment.IsCommitmentStateKey(key) {
			continue
		}
		want, ok := mergedRecords[string(key)]
		require.Truef(t, ok, "merged file contains unexpected record key %x", key)
		require.Equalf(t, want, values[i], "merged record value for key %x", key)
		checked++
	}
	require.Positive(t, checked, "merged file must contain edge records")
	require.Equal(t, nonEmptyAcceptanceRecords(oracle.records), allAcceptanceRecords(t, db), "post-merge stored records")
	require.Equal(t, wantRoot, recomputeAcceptanceRoot(t, db), "post-merge fresh commitment read")
}

func TestCommitmentV3ReadsMixedLegacyAndV3Files(t *testing.T) {
	db, agg := newAcceptanceDB(t, 1, 2)
	batches := acceptanceBatches()

	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, false)
	applyAcceptanceBatch(t, db, batches[0], 1)
	legacyRoot := applyAcceptanceBatch(t, db, batches[1], 2)
	require.NoError(t, agg.BuildFiles(3))

	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)
	v3Root := applyAcceptanceBatch(t, db, batches[2], 3)
	require.NoError(t, agg.BuildFiles(4))

	files := acceptanceCommitmentFiles(t, agg)
	var legacyFiles, v3Files int
	for _, file := range files {
		edgeRecords := statecfg.CommitmentEdgeRecords(file.Version())
		if edgeRecords {
			v3Files++
		} else {
			legacyFiles++
		}
		keys, values := readKVFile(t, agg, file.Fullpath())
		for i, key := range keys {
			if commitment.IsCommitmentStateKey(key) || len(values[i]) == 0 {
				continue
			}
			record := commitment.BranchData(values[i])
			if edgeRecords {
				require.Truef(t, record.IsEdgeRecord(), "v3 file %s contains a bundled row for key %x", file.Fullpath(), key)
			} else {
				require.Falsef(t, record.IsEdgeRecord(), "legacy file %s contains an edge record for key %x", file.Fullpath(), key)
			}
		}
	}
	require.Positive(t, legacyFiles, "expected a legacy commitment file")
	require.Positive(t, v3Files, "expected a v3 commitment file")
	require.NotEqual(t, legacyRoot, v3Root)
	require.Equal(t, v3Root, recomputeAcceptanceRoot(t, db), "mixed-version fresh commitment read")
}
