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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	shardTombstoneStepSize    = uint64(8)
	shardTombstoneFrozenSteps = 8
	shardTombstoneRange1Steps = 8
	shardTombstoneRange2Steps = 8
	shardTombstoneAccounts    = 8000
	// shardTombstoneSlot is the single slot every account owns. A shared numeric
	// slot with a per-address group hash spreads each account's one leaf to an
	// independent, effectively random tree position — unlike a multi-slot account,
	// where the whole group is one owning shard's subtree and a delete inside it
	// only ever promotes its sibling unread, never triggering the read this bug
	// depends on.
	shardTombstoneSlot     = 1000
	shardTombstoneMaxShard = uint64(4)
)

func shardTombstoneAddr(i int) []byte {
	a := make([]byte, length.Addr)
	a[0] = 0xb1
	binary.BigEndian.PutUint32(a[1:5], uint32(i))
	return a
}

func shardTombstoneSlotKey(addr []byte) []byte {
	k := make([]byte, length.Addr+length.Hash)
	copy(k, addr)
	binary.BigEndian.PutUint32(k[length.Addr:], shardTombstoneSlot)
	return k
}

// shardTombstoneAgg pins StepsInFrozenFile explicitly: rebuildVariantAgg's helper
// never calls ReloadErigonDBSettings, so the erigondb.toml value this test writes
// is never read, and the aggregator would otherwise keep the production default
// (256 steps) — far larger than the handful of steps a fast test can use to force
// two separate, never-merging account/storage file ranges.
func shardTombstoneAgg(t *testing.T, rawDB kv.RwDB, dirs datadir.Dirs) *state.Aggregator {
	t.Helper()
	agg := state.NewTest(dirs).StepSize(shardTombstoneStepSize).StepsInFrozenFile(shardTombstoneFrozenSteps).
		Logger(log.New()).MustOpen(t.Context(), rawDB)
	t.Cleanup(agg.Close)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	require.NoError(t, agg.OpenFolder())
	return agg
}

// rebuildShardTombstoneDatadir builds two commitment ranges: range 1 holds many
// accounts, each with one live, non-zero storage slot. Range 2 refreshes every
// account but tombstones the second half's slot (an SSTORE-to-zero, not a
// selfdestruct) while writing a fresh value to the first half. The plain-key
// split lines up with the shard split, so any tree sibling pairing a first-half
// (live, earlier-shard) leaf with a second-half (dead, later-shard) one has its
// live side fold before the dead side's own shard ever collapses that row —
// the shape that leaves the dead leaf looking present to an earlier shard's fold.
// Two BuildFiles calls separated by a frozen-file boundary keep the ranges as
// two separate, unmerged account/storage files, so a rebuild walks range 2 with
// a commitment file inherited from range 1.
func rebuildShardTombstoneDatadir(t *testing.T) (kv.TemporalRwDB, datadir.Dirs) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE),
		fmt.Appendf(nil, "step_size = %d\nsteps_in_frozen_file = %d\nreferences_in_commitment_branches = false\n",
			shardTombstoneStepSize, shardTombstoneFrozenSteps), 0644))

	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).InMem(t, dirs.Chaindata).
		GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(rawDB.Close)

	agg := shardTombstoneAgg(t, rawDB, dirs)
	tdb, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(tdb.Close)
	var db kv.TemporalRwDB = tdb

	range1TxCount := shardTombstoneRange1Steps * shardTombstoneStepSize
	range2TxCount := shardTombstoneRange2Steps * shardTombstoneStepSize

	writeShardTombstoneRange(t, db, 0, range1TxCount, 1, func(i int) (drop bool, val []byte) {
		return false, []byte{byte(i + 1), byte(i + 2), 0xAA}
	})
	require.NoError(t, agg.BuildFiles(range1TxCount))
	agg, db = reopenShardTombstoneAgg(t, rawDB, dirs)

	writeShardTombstoneRange(t, db, range1TxCount, range2TxCount, 2, func(i int) (drop bool, val []byte) {
		if i >= shardTombstoneAccounts/2 {
			return true, nil
		}
		return false, []byte{byte(i + 1), byte(i + 2), 0xBB}
	})
	require.NoError(t, agg.BuildFiles(range1TxCount+range2TxCount))
	agg, db = reopenShardTombstoneAgg(t, rawDB, dirs)

	// Collation holds a step back until a write in the next one proves it closed
	// (`step+1 records visible`, aggregator.go). Without this, range 2's own last
	// step never seals into a file and the range never forms.
	writeShardTombstoneGuard(t, db, range1TxCount+range2TxCount)
	require.NoError(t, agg.BuildFiles(range1TxCount+range2TxCount+shardTombstoneStepSize))
	_, db = reopenShardTombstoneAgg(t, rawDB, dirs)

	return db, dirs
}

func shardTombstoneGuardAddr() []byte {
	a := make([]byte, length.Addr)
	a[0] = 0xff
	return a
}

// writeShardTombstoneGuard writes one throwaway account at txNum, which itself
// never seals into a file (its own step never gets a next-step proof), purely to
// give the previous step the proof collation needs to seal it.
func writeShardTombstoneGuard(t *testing.T, db kv.TemporalRwDB, txNum uint64) {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New(),
		execctx.WithTrieConfig(rebuildVariantTrieCfg(commitment.VariantHexPatriciaTrie)))
	require.NoError(t, err)
	defer sd.Close()
	sd.DiscardWrites(kv.CommitmentDomain)

	addr := shardTombstoneGuardAddr()
	acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
	prev, _, err := sd.GetLatest(kv.AccountsDomain, rwTx, addr)
	require.NoError(t, err)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), txNum, prev))

	require.NoError(t, sd.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
}

// writeShardTombstoneRange writes every account and its one slot once, at a
// txNum spread across [rangeFrom, rangeFrom+rangeTxCount) proportionally to
// account index, so every step in the range carries real writes rather than
// only its last one.
func writeShardTombstoneRange(t *testing.T, db kv.TemporalRwDB, rangeFrom, rangeTxCount uint64, nonce uint64,
	slotUpdate func(i int) (drop bool, val []byte)) {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New(),
		execctx.WithTrieConfig(rebuildVariantTrieCfg(commitment.VariantHexPatriciaTrie)))
	require.NoError(t, err)
	defer sd.Close()
	sd.DiscardWrites(kv.CommitmentDomain)

	for i := range shardTombstoneAccounts {
		txNum := rangeFrom + uint64(i)*rangeTxCount/shardTombstoneAccounts
		addr := shardTombstoneAddr(i)

		acc := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(uint64(i) + nonce), CodeHash: accounts.EmptyCodeHash}
		prev, _, err := sd.GetLatest(kv.AccountsDomain, rwTx, addr)
		require.NoError(t, err)
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), txNum, prev))

		sk := shardTombstoneSlotKey(addr)
		prevSlot, _, err := sd.GetLatest(kv.StorageDomain, rwTx, sk)
		require.NoError(t, err)

		drop, val := slotUpdate(i)
		if drop {
			require.NoError(t, sd.DomainDel(kv.StorageDomain, rwTx, sk, txNum, prevSlot))
			continue
		}
		require.NoError(t, sd.DomainPut(kv.StorageDomain, rwTx, sk, val, txNum, prevSlot))
	}
	require.NoError(t, sd.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
}

// reopenShardTombstoneAgg drops the commitment domain file BuildFiles seals even
// with commitment writes discarded, and reopens against the trimmed folder: a
// resumed rebuild takes any file covering a range as that range already done.
func reopenShardTombstoneAgg(t *testing.T, rawDB kv.RwDB, dirs datadir.Dirs) (*state.Aggregator, kv.TemporalRwDB) {
	t.Helper()
	paths, err := dir.ListFiles(dirs.SnapDomain)
	require.NoError(t, err)
	for _, p := range paths {
		if strings.Contains(filepath.Base(p), kv.CommitmentDomain.String()) {
			require.NoError(t, dir.RemoveFile(p))
		}
	}

	agg := shardTombstoneAgg(t, rawDB, dirs)
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	return agg, db
}

// Shards slice a range in plain-key order while the trie is ordered by tree key,
// so a slot this range tombstones can belong to a later shard than the one
// re-hashing the inherited branch that still names its leaf. Sharding a range
// must not change what it commits to.
func TestRebuildCommitmentFilesBinTargetShardedRangeAppliesInheritedRemovals(t *testing.T) {
	shardedDB, _ := rebuildShardTombstoneDatadir(t)
	shardedRoot, report, err := state.RebuildCommitmentFiles(t.Context(), shardedDB, &rawdbv3.TxNums, log.New(), false,
		state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, MaxShardSteps: shardTombstoneMaxShard})
	require.NoError(t, err)
	require.NotEmpty(t, shardedRoot)

	var inherited *state.RebuildRangeReport
	for i := range report.Ranges {
		if report.Ranges[i].StepFrom > 0 {
			inherited = &report.Ranges[i]
		}
	}
	require.NotNil(t, inherited, "a range has to inherit a commitment file, or nothing is being proven")
	require.Greaterf(t, len(inherited.Shards), 1,
		"the inheriting range must shard, or the plain-key slicing never splits a removal from its branch")

	wholeDB, _ := rebuildShardTombstoneDatadir(t)
	wholeRoot, _, err := state.RebuildCommitmentFiles(t.Context(), wholeDB, &rawdbv3.TxNums, log.New(), false,
		state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie})
	require.NoError(t, err)
	require.Equal(t, wholeRoot, shardedRoot)
}
