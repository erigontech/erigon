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

// These tests drive the bin commitment trie over a real MDBX datadir with no
// external oracle for the roots. The cross-check is determinism: a forward run
// and a rebuild that has only the account and storage domains to work from must
// agree.
package backtester_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

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
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	pbinM1AStepSize = uint64(8)
	pbinM1AAccounts = 6
	pbinM1ASlots    = 4
)

// Makes PickTrieVariant() resolve to the bin trie. The flag is process-wide, so
// these tests never run in parallel.
func pbinM1ABinVariant(t *testing.T) {
	t.Helper()
	orig := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = orig })
	statecfg.ExperimentalBinCommitment = true
}

func pbinM1ANewAgg(t *testing.T, rawDB kv.RwDB, dirs datadir.Dirs, stepSize uint64) *state.Aggregator {
	t.Helper()
	agg := state.NewTest(dirs).StepSize(stepSize).Logger(log.New()).MustOpen(t.Context(), rawDB)
	t.Cleanup(agg.Close)
	// Referenced branches rewrite bytes at hex cell offsets during merge. Production
	// refuses that combination when resolving settings; NewTest bypasses it.
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	require.NoError(t, agg.OpenFolder())
	return agg
}

func pbinM1ANewDatadir(t *testing.T, stepSize uint64) (kv.TemporalRwDB, *state.Aggregator, datadir.Dirs) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).InMem(t, dirs.Chaindata).
		GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(rawDB.Close)

	agg := pbinM1ANewAgg(t, rawDB, dirs, stepSize)
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	return db, agg, dirs
}

// Reopens the aggregator over the same folder — the file-visibility half of a
// node restart.
func pbinM1AReopen(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, dirs datadir.Dirs, stepSize uint64) (kv.TemporalRwDB, *state.Aggregator) {
	t.Helper()
	agg.Close()
	newAgg := pbinM1ANewAgg(t, db, dirs, stepSize)
	newDB, err := temporal.New(db, newAgg, nil)
	require.NoError(t, err)
	return newDB, newAgg
}

func pbinM1AAddr(i int) []byte {
	a := make([]byte, length.Addr)
	a[0] = 0xa0
	a[1] = byte(i)
	a[length.Addr-1] = byte(i*7 + 1)
	return a
}

func pbinM1ASlotKey(addr []byte, j int) []byte {
	k := make([]byte, length.Addr+length.Hash)
	copy(k, addr)
	k[length.Addr] = byte(j)
	k[len(k)-1] = byte(j*13 + 3)
	return k
}

// Pins that the bin trie is really in play — a hex fallback would make every
// assertion below vacuous.
func pbinM1ABinSharedDomains(t *testing.T, tx kv.TemporalTx) *execctx.SharedDomains {
	t.Helper()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	require.IsType(t, &commitment.PBinPatriciaHashed{}, sd.GetCommitmentCtx().Trie())
	return sd
}

// Writes accounts and storage for txNums [fromTx, toTx), saving the commitment
// state at every step boundary. Returns the root at each boundary keyed by the
// boundary txNum, plus the last root.
func pbinM1AForwardRun(t *testing.T, db kv.TemporalRwDB, stepSize, fromTx, toTx uint64) (map[uint64][]byte, []byte) {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd := pbinM1ABinSharedDomains(t, rwTx)
	defer sd.Close()

	roots := make(map[uint64][]byte)
	var last []byte
	for txNum := fromTx; txNum < toTx; txNum++ {
		for i := range pbinM1AAccounts {
			addr := pbinM1AAddr(i)
			acc := accounts.Account{
				Nonce:    txNum + 1,
				Balance:  *uint256.NewInt(txNum*1_000 + uint64(i)),
				CodeHash: accounts.EmptyCodeHash,
			}
			prev, _, err := sd.GetLatest(kv.AccountsDomain, rwTx, addr)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), txNum, prev))

			for j := range pbinM1ASlots {
				sk := pbinM1ASlotKey(addr, j)
				val := []byte{byte(txNum + 1), byte(i + 1), byte(j + 1)}
				prev, _, err := sd.GetLatest(kv.StorageDomain, rwTx, sk)
				require.NoError(t, err)
				require.NoError(t, sd.DomainPut(kv.StorageDomain, rwTx, sk, val, txNum, prev))
			}
		}
		if (txNum+1)%stepSize == 0 {
			last, err = sd.ComputeCommitment(t.Context(), rwTx, true, 0, txNum, "pbin-m1a", nil)
			require.NoError(t, err)
			require.NotEmpty(t, last)
			roots[txNum] = bytes.Clone(last)
		}
	}
	require.NoError(t, sd.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	return roots, last
}

// Re-folds the whole tree with every leaf touched, so no leaf value comes from
// a branch record.
func pbinM1ARecomputeRoot(t *testing.T, db kv.TemporalRwDB) []byte {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd := pbinM1ABinSharedDomains(t, rwTx)
	defer sd.Close()

	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		it, err := rwTx.Debug().RangeLatest(d, nil, nil, -1)
		require.NoError(t, err)
		for it.HasNext() {
			k, _, err := it.Next()
			require.NoError(t, err)
			sd.GetCommitmentCtx().TouchKey(d, string(k), nil)
		}
		it.Close()
	}
	root, err := sd.ComputeCommitment(t.Context(), rwTx, false, 0, 0, "pbin-m1a-recompute", nil)
	require.NoError(t, err)
	return root
}

// The root a freshly opened SharedDomains restores from the saved commitment
// state, without folding anything.
func pbinM1ARestoredRoot(t *testing.T, db kv.TemporalRwDB) []byte {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd := pbinM1ABinSharedDomains(t, tx)
	defer sd.Close()
	root, err := sd.GetCommitmentCtx().Trie().RootHash()
	require.NoError(t, err)
	return root
}

// The first txNum not yet in the account and storage files. Collation always
// leaves the newest step in the db, so a files-only rebuild reproduces the root
// as of this boundary, not the last one the forward run computed.
func pbinM1ACollatedTxNum(t *testing.T, db kv.TemporalRwDB) uint64 {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := state.AggTx(tx)
	accTxNum := at.TxNumsInFiles(kv.AccountsDomain)
	require.Equal(t, accTxNum, at.TxNumsInFiles(kv.StorageDomain),
		"the rebuild reads both domains at one boundary")
	return accTxNum
}

func pbinM1ABranchRecords(t *testing.T, db kv.TemporalRwDB) map[string][]byte {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	out := make(map[string][]byte)
	it, err := tx.Debug().RangeLatest(kv.CommitmentDomain, nil, nil, -1)
	require.NoError(t, err)
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			continue
		}
		out[string(k)] = bytes.Clone(v)
	}
	return out
}

// Counts branch records gone from the db table, so a latest read of them can
// only come from the collated files.
func pbinM1AFileServedRecords(t *testing.T, db kv.TemporalRwDB, records map[string][]byte) int {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	var fromFiles int
	for k := range records {
		v, err := tx.GetOne(kv.TblCommitmentVals, []byte(k))
		require.NoError(t, err)
		if len(v) == 0 {
			fromFiles++
		}
	}
	return fromFiles
}

// Wipes commitment from the db tables and the snapshot dir, so a rebuild has to
// derive the tree from the account and storage domains alone.
func pbinM1AWipeCommitment(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, dirs datadir.Dirs, stepSize uint64) (kv.TemporalRwDB, *state.Aggregator) {
	t.Helper()
	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	tables, err := rwTx.ListTables()
	require.NoError(t, err)
	commitStr := kv.CommitmentDomain.String()
	for _, b := range tables {
		if strings.Contains(strings.ToLower(b), commitStr) {
			require.NoError(t, rwTx.ClearTable(b))
		}
	}
	require.NoError(t, rwTx.Commit())

	// Windows refuses to remove a still-mapped file, so drop the file handles first.
	agg.Close()
	paths, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	require.NoError(t, err)
	for _, p := range paths {
		if !strings.Contains(p, commitStr) {
			continue
		}
		require.NoError(t, dir.RemoveFile(p))
		base := strings.TrimSuffix(p, ".kv")
		for _, ext := range []string{".kvi", ".kvei", ".bt"} {
			_ = dir.RemoveFile(base + ext) // accessors may not exist
		}
	}

	newAgg := pbinM1ANewAgg(t, db, dirs, stepSize)
	newDB, err := temporal.New(db, newAgg, nil)
	require.NoError(t, err)
	return newDB, newAgg
}

func TestPBinM1AForwardRunMatchesRebuildFromDomains(t *testing.T) {
	pbinM1ABinVariant(t)
	txCount := 4 * pbinM1AStepSize

	db, agg, dirs := pbinM1ANewDatadir(t, pbinM1AStepSize)
	stepRoots, forwardRoot := pbinM1AForwardRun(t, db, pbinM1AStepSize, 0, txCount)
	require.NoError(t, agg.BuildFiles(txCount))

	require.Equal(t, forwardRoot, pbinM1ARecomputeRoot(t, db),
		"a full-touch recompute over the same datadir must reproduce the forward root")

	collatedTxNum := pbinM1ACollatedTxNum(t, db)
	require.Positive(t, collatedTxNum, "collation must produce account and storage files to rebuild from")
	wantRoot := stepRoots[collatedTxNum-1]
	require.NotEmpty(t, wantRoot, "the collated boundary must be one the forward run computed a root at")

	db, agg = pbinM1AWipeCommitment(t, db, agg, dirs, pbinM1AStepSize)
	require.Empty(t, pbinM1ABranchRecords(t, db), "the wipe must leave no commitment records")

	rebuiltRoot, _, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), false, state.RebuildTarget{})
	require.NoError(t, err)
	require.Equal(t, wantRoot, rebuiltRoot, "rebuild-from-domains must reproduce the forward root")

	require.NoError(t, agg.OpenFolder())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))
	require.Equal(t, wantRoot, pbinM1ARestoredRoot(t, db),
		"the rebuilt files must carry a trie state that restores to the rebuilt root")
	require.Equal(t, forwardRoot, pbinM1ARecomputeRoot(t, db),
		"the rebuilt commitment records must fold back to the forward root")
}

// The second half touches only its own keys, so the root can only come out right
// if the saved trie state and the persisted branch records both round-trip.
func TestPBinM1ARestartResumesToSameRoot(t *testing.T) {
	pbinM1ABinVariant(t)
	half := 2 * pbinM1AStepSize

	uninterrupted, _, _ := pbinM1ANewDatadir(t, pbinM1AStepSize)
	_, wantRoot := pbinM1AForwardRun(t, uninterrupted, pbinM1AStepSize, 0, 2*half)

	restarted, agg, dirs := pbinM1ANewDatadir(t, pbinM1AStepSize)
	_, firstRoot := pbinM1AForwardRun(t, restarted, pbinM1AStepSize, 0, half)
	require.NotEqual(t, wantRoot, firstRoot, "the two halves must not write identical state")

	restarted, _ = pbinM1AReopen(t, restarted, agg, dirs, pbinM1AStepSize)
	require.Equal(t, firstRoot, pbinM1ARestoredRoot(t, restarted),
		"a restart must restore the saved root before folding anything")

	_, resumedRoot := pbinM1AForwardRun(t, restarted, pbinM1AStepSize, half, 2*half)
	require.Equal(t, wantRoot, resumedRoot, "a restart mid-run must resume to the uninterrupted root")
}

func TestPBinM1ABranchRecordsSurviveCollationAndMerge(t *testing.T) {
	pbinM1ABinVariant(t)
	txCount := 4 * pbinM1AStepSize

	db, agg, dirs := pbinM1ANewDatadir(t, pbinM1AStepSize)
	pbinM1AForwardRun(t, db, pbinM1AStepSize, 0, txCount)

	inDB := pbinM1ABranchRecords(t, db)
	require.NotEmpty(t, inDB)
	require.Zero(t, pbinM1AFileServedRecords(t, db, inDB), "before collation every record lives in the db")

	require.NoError(t, agg.BuildFiles(txCount))
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	_, err = rwTx.PruneSmallBatches(t.Context(), time.Hour)
	require.NoError(t, err)
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.MergeLoop(t.Context()))
	require.Positive(t, pbinM1AFileServedRecords(t, db, inDB),
		"pruning must move records out of the db, otherwise the reads below never reach the files")

	require.Equal(t, inDB, pbinM1ABranchRecords(t, db),
		"collation and merge must preserve bin branch records byte-for-byte")

	db, _ = pbinM1AReopen(t, db, agg, dirs, pbinM1AStepSize)
	require.Equal(t, inDB, pbinM1ABranchRecords(t, db),
		"the records must read back identically after a folder reopen")
}
