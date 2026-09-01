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
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

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
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Commitment cost comparison between the bundled-row (v2) and per-edge (v3) record formats over a
// synthetic update stream.
//
//	BENCH_BLOCKS=1000000 go test ./db/state/ -run '^$' -bench CommitmentFormatCost -benchtime 1x -timeout 0 -v
//
// BENCH_ARMS selects arms by name and enables the -nocache ones; BENCH_TRACE writes a trie trace to
// the named file, which is the only practical way to find where two arms stop agreeing on a root.

type benchArm struct {
	name        string
	edgeRecords bool
	parallel    bool
	cache       bool
}

type benchResult struct {
	arm             string
	blocks          int
	commitWall      time.Duration
	recordsWritten  uint64
	keysProcessed   uint64
	commitmentBytes int64
	commitmentFiles int
	totalBytes      int64
	root            string
	cacheStats      string
}

func benchEnvInt(key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

// benchWorkload draws touched keys from a Zipf distribution so a minority of accounts and slots are
// rewritten constantly and the rest go cold, which is what makes the record-vs-row difference show.
type benchWorkload struct {
	rnd           *rand.Rand
	acctZipf      *rand.Zipf
	slotZipf      *rand.Zipf
	accountSpace  uint64
	slotSpace     uint64
	acctsPerBlock int
	slotsPerBlock int
	contractShare int
}

func newBenchWorkload(seed int64, accountSpace, slotSpace uint64, acctsPerBlock, slotsPerBlock int) *benchWorkload {
	rnd := rand.New(rand.NewSource(seed))
	return &benchWorkload{
		rnd:           rnd,
		acctZipf:      rand.NewZipf(rnd, 1.1, 1, accountSpace-1),
		slotZipf:      rand.NewZipf(rnd, 1.05, 1, slotSpace-1),
		accountSpace:  accountSpace,
		slotSpace:     slotSpace,
		acctsPerBlock: acctsPerBlock,
		slotsPerBlock: slotsPerBlock,
		contractShare: 8, // one account in eight carries storage
	}
}

func benchAccountKey(i uint64) []byte {
	key := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(key[length.Addr-8:], i)
	key[0] = byte(i)
	return key
}

func benchStorageKey(account, slot uint64) []byte {
	key := make([]byte, length.Addr+length.Hash)
	copy(key, benchAccountKey(account))
	binary.BigEndian.PutUint64(key[len(key)-8:], slot)
	return key
}

// contractAccounts is the id range storage slots are written under.
func (w *benchWorkload) contractAccounts() uint64 {
	return max(1, w.accountSpace/uint64(w.contractShare))
}

func (w *benchWorkload) seedAccounts() []acceptanceEntry {
	entries := make([]acceptanceEntry, 0, w.contractAccounts())
	for id := range w.contractAccounts() {
		account := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(id + 1), CodeHash: accounts.EmptyCodeHash}
		entries = append(entries, acceptanceEntry{
			domain: kv.AccountsDomain,
			key:    benchAccountKey(id),
			value:  accounts.SerialiseV3(&account),
		})
	}
	return entries
}

func (w *benchWorkload) block(blockNum uint64) []acceptanceEntry {
	entries := make([]acceptanceEntry, 0, w.acctsPerBlock+w.slotsPerBlock)
	for range w.acctsPerBlock {
		id := w.acctZipf.Uint64()
		account := accounts.Account{
			Nonce:    blockNum,
			Balance:  *uint256.NewInt(blockNum*1_000 + id),
			CodeHash: accounts.EmptyCodeHash,
		}
		entries = append(entries, acceptanceEntry{
			domain: kv.AccountsDomain,
			key:    benchAccountKey(id),
			value:  accounts.SerialiseV3(&account),
		})
	}
	for range w.slotsPerBlock {
		id := w.acctZipf.Uint64() % w.contractAccounts()
		slot := w.slotZipf.Uint64()
		entries = append(entries, acceptanceEntry{
			domain: kv.StorageDomain,
			key:    benchStorageKey(id, slot),
			value:  uint256.NewInt(blockNum + slot).Bytes(),
		})
	}
	return entries
}

func newBenchDB(t testing.TB, dir string, stepSize uint64, cache bool) (kv.TemporalRwDB, *state.Aggregator) {
	t.Helper()
	logger := log.New()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	dirs := datadir.New(dir)

	rawDB := mdbx.New(dbcfg.ChainDB, logger).
		InMem(dirs.Chaindata).
		AutoRemove(false).
		GrowthStep(32 * datasize.MB).
		DirtySpace(uint64(512 * datasize.MB)).
		MapSize(16 * datasize.GB).
		MustOpen()
	t.Cleanup(rawDB.Close)

	opts := state.NewTest(dirs).StepSize(stepSize).Logger(logger)
	if !cache {
		opts = opts.DisableBranchCache()
	}
	agg, err := opts.Open(t.Context(), rawDB)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder())

	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	return db, agg
}

func dirBytes(t testing.TB, root, match string) (int64, int) {
	t.Helper()
	var total int64
	var count int
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil //nolint:nilerr
		}
		if match != "" && !strings.Contains(filepath.Base(path), match) {
			return nil
		}
		total += info.Size()
		count++
		return nil
	})
	return total, count
}

func runBenchArm(t testing.TB, arm benchArm, blocks, blocksPerTx, buildEvery, progressEvery int, w *benchWorkload, stepSize uint64) benchResult {
	t.Helper()
	dir := filepath.Join(t.TempDir(), arm.name)
	db, agg := newBenchDB(t, dir, stepSize, arm.cache)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, arm.edgeRecords)

	previousParallel := statecfg.ExperimentalParallelCommitment
	statecfg.ExperimentalParallelCommitment = arm.parallel
	defer func() { statecfg.ExperimentalParallelCommitment = previousParallel }()

	recordsBefore := metrics.GetOrCreateCounter("domain_commitment_updates_applied").GetValueUint64()
	keysBefore := metrics.GetOrCreateCounter("domain_commitment_keys").GetValueUint64()

	ctx := t.Context()
	var commitWall time.Duration
	var root []byte

	// One transaction per span of blocks, released before the next one opens.
	runSpan := func(start, end int, entriesFor func(uint64) []acceptanceEntry, timed bool) {
		tx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		if arm.parallel {
			// The parallel variant stays pending until the DB is wired; without this the arm
			// silently runs the sequential trie and reports it as parallel.
			domains.EnableParaTrieDB(db)
		}
		if path := os.Getenv("BENCH_TRACE"); path != "" {
			f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			require.NoError(t, err)
			defer f.Close()
			domains.GetCommitmentContext().SetTraceWriter(f)
		}

		for blockNum := start; blockNum < end; blockNum++ {
			txNum := uint64(blockNum + 1)
			require.NoError(t, rawdbv3.TxNums.Append(tx, txNum, txNum))
			for _, entry := range entriesFor(uint64(blockNum)) {
				previous, _, err := domains.GetLatest(entry.domain, tx, entry.key)
				require.NoError(t, err)
				require.NoError(t, domains.DomainPut(entry.domain, tx, entry.key, entry.value, txNum, previous))
			}
			began := time.Now()
			root, err = domains.ComputeCommitment(ctx, tx, true, txNum, txNum, "bench", nil)
			if timed {
				commitWall += time.Since(began)
			}
			require.NoError(t, err)
		}
		require.NoError(t, domains.Commit(ctx, tx))
	}

	// Seed every account a slot can be written under: storage for an account that does not exist
	// is not a state the trie has to represent, and generating it only produces harness noise.
	runSpan(-1, 0, func(uint64) []acceptanceEntry { return w.seedAccounts() }, false)

	for start := 0; start < blocks; start += blocksPerTx {
		end := min(start+blocksPerTx, blocks)
		runSpan(start, end, w.block, true)

		if buildEvery > 0 && end%buildEvery == 0 {
			require.NoError(t, agg.BuildFiles(uint64(end)))
		}
		if end%progressEvery == 0 {
			t.Logf("%s %d/%d blocks, commit %s", arm.name, end, blocks, commitWall.Round(time.Millisecond))
		}
	}

	require.NoError(t, agg.BuildFiles(uint64(blocks)))
	require.NoError(t, agg.MergeLoop(ctx))

	cacheStats := "disabled"
	if at := agg.BeginFilesRo(); at != nil {
		if bc := at.BranchCache(); bc != nil {
			cacheStats = bc.Stats()
		}
		at.Close()
	}

	commitmentBytes, commitmentFiles := dirBytes(t, dir, "commitment")
	totalBytes, _ := dirBytes(t, dir, "")
	return benchResult{
		arm:             arm.name,
		blocks:          blocks,
		commitWall:      commitWall,
		recordsWritten:  metrics.GetOrCreateCounter("domain_commitment_updates_applied").GetValueUint64() - recordsBefore,
		keysProcessed:   metrics.GetOrCreateCounter("domain_commitment_keys").GetValueUint64() - keysBefore,
		commitmentBytes: commitmentBytes,
		commitmentFiles: commitmentFiles,
		totalBytes:      totalBytes,
		root:            hex.EncodeToString(root),
		cacheStats:      cacheStats,
	}
}

func BenchmarkCommitmentFormatCost(b *testing.B) {
	blocks := benchEnvInt("BENCH_BLOCKS", 10_000)
	blocksPerTx := benchEnvInt("BENCH_BLOCKS_PER_TX", 1_000)
	accountSpace := uint64(benchEnvInt("BENCH_ACCOUNT_SPACE", 200_000))
	slotSpace := uint64(benchEnvInt("BENCH_SLOT_SPACE", 50_000))
	acctsPerBlock := benchEnvInt("BENCH_ACCOUNTS_PER_BLOCK", 20)
	slotsPerBlock := benchEnvInt("BENCH_SLOTS_PER_BLOCK", 40)
	stepSize := uint64(benchEnvInt("BENCH_STEP_SIZE", 8_192))
	buildEvery := benchEnvInt("BENCH_BUILD_EVERY", int(stepSize))
	progressEvery := benchEnvInt("BENCH_PROGRESS_EVERY", 10_000)

	arms := []benchArm{
		{name: "v2-serial", edgeRecords: false, parallel: false, cache: true},
		{name: "v3-serial", edgeRecords: true, parallel: false, cache: true},
		{name: "v2-parallel", edgeRecords: false, parallel: true, cache: true},
		{name: "v3-parallel", edgeRecords: true, parallel: true, cache: true},
		{name: "v2-serial-nocache", edgeRecords: false, parallel: false},
		{name: "v3-serial-nocache", edgeRecords: true, parallel: false},
		{name: "v2-parallel-nocache", edgeRecords: false, parallel: true},
		{name: "v3-parallel-nocache", edgeRecords: true, parallel: true},
	}
	if os.Getenv("BENCH_ARMS") == "" {
		arms = arms[:4]
	}

	if only := os.Getenv("BENCH_ARMS"); only != "" {
		wanted := strings.Split(only, ",")
		filtered := arms[:0:0]
		for _, arm := range arms {
			for _, name := range wanted {
				if strings.TrimSpace(name) == arm.name {
					filtered = append(filtered, arm)
				}
			}
		}
		arms = filtered
	}

	for range b.N {
		results := make([]benchResult, 0, len(arms))
		for _, arm := range arms {
			w := newBenchWorkload(1, accountSpace, slotSpace, acctsPerBlock, slotsPerBlock)
			began := time.Now()
			result := runBenchArm(b, arm, blocks, blocksPerTx, buildEvery, progressEvery, w, stepSize)
			b.Logf("%s done in %s", arm.name, time.Since(began).Round(time.Second))
			results = append(results, result)
		}

		fmt.Printf("\nblocks=%d accounts/blk=%d slots/blk=%d acctSpace=%d slotSpace=%d step=%d\n",
			blocks, acctsPerBlock, slotsPerBlock, accountSpace, slotSpace, stepSize)
		fmt.Printf("%-20s %12s %14s %14s %14s %8s %14s\n",
			"arm", "commit", "records", "keys", "commitment", "files", "datadir")
		for _, r := range results {
			fmt.Printf("%-20s %12s %14d %14d %14s %8d %14s   root=%s\n",
				r.arm, r.commitWall.Round(time.Millisecond), r.recordsWritten, r.keysProcessed,
				datasize.ByteSize(r.commitmentBytes).HR(), r.commitmentFiles,
				datasize.ByteSize(r.totalBytes).HR(), r.root[:16])
		}
		for _, r := range results {
			fmt.Printf("%-20s %s\n", r.arm, r.cacheStats)
		}

		// One workload, one state: an arm that disagrees on the root has not computed the same trie,
		// and none of its numbers describe the same work.
		for _, r := range results[1:] {
			require.Equalf(b, results[0].root, r.root, "%s root differs from %s", r.arm, results[0].arm)
		}
	}
}
