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

// Benchmarks for the "Snapshot vs MDBX read-cost equivalence"
// investigation. See memory: snapshot-vs-mdbx-performance-equivalence.md
//
// Premise: with the OS page cache warm, a key lookup served from
// MDBX vs from a snapshot .kv file should cost roughly the same — the
// disk and page cache are identical. Today they don't. These benches
// quantify the gap (H0) and decompose it (H1-H4).
//
// **Status: SCAFFOLD.** Compiles and runs (skipped) so the structure
// survives. Real-datadir bootstrap and synthetic-fixture bootstrap are
// the two TODOs that turn this into a measurement tool.
//
// Two operating modes are anticipated:
//
//   1. **Synthetic mode** (preferred for CI / reproducibility):
//      use testDbAndAggregatorBench, write K1 keys at low txNums,
//      buildFiles+prune so they land in files, write K2 at high
//      txNums and leave in MDBX. No external datadir required.
//
//   2. **Real-datadir mode** (preferred for fidelity):
//      open an existing perf-devnet-3 / mainnet datadir read-only,
//      pick keys by cursor-iterating the values table (mdbxKeys) and
//      walking the latest .kv decompressor (fileKeys). Matches
//      production file-count, Bloom false-positive rate, etc.
//
// Both fail with b.Skip() in this scaffold; the TODOs below are the
// agenda for turning it into a real measurement.

import (
	"encoding/binary"
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
)

var snapDataDir = flag.String("snapdatadir", "",
	"path to a real datadir (chaindata + snapshots) for snapshot-vs-mdbx benches; "+
		"when empty, real-datadir benches are skipped and synthetic mode runs")

// snapBenchSetup carries everything a sub-bench needs: an open RO tx,
// the AggregatorRoTx for Debug* paths, the domain under test, and the
// pre-picked key sets. Set up once per Benchmark function so warmup
// and timing happen on the same data.
type snapBenchSetup struct {
	tx       kv.TemporalTx
	aggTx    *state.AggregatorRoTx
	domain   kv.Domain
	mdbxKeys [][]byte
	fileKeys [][]byte
}

// openSnapBench builds a snapBenchSetup for the given domain. Picks
// real-datadir mode iff --snapdatadir is set, otherwise synthetic.
// Both paths currently call b.Skip; the TODOs below are the work.
func openSnapBench(b *testing.B, domain kv.Domain) *snapBenchSetup {
	b.Helper()
	if *snapDataDir != "" {
		return openSnapBenchRealDatadir(b, domain, *snapDataDir)
	}
	return openSnapBenchSynthetic(b, domain)
}

// openSnapBenchRealDatadir opens an existing chaindata+snapshots
// datadir read-only and picks keys from production files.
//
// Uses MDBX Accede mode (don't create, must already exist) and
// state.New(...).MustOpen which loads the on-disk schema. ResolveDB
// settings reads stepSize / stepsInFrozenFile from the existing DB.
//
// CAUTION: opening a chaindata that another erigon process is writing
// to is risky. Bench against an idle copy. The script that drives
// these benches typically does prepare-run.sh first to snapshot a
// fresh chaindata copy under /erigon-data/perf-devnet-3-run/ or
// similar. Pass that path via --snapdatadir.
func openSnapBenchRealDatadir(b *testing.B, domain kv.Domain, datadirPath string) *snapBenchSetup {
	b.Helper()
	logger := log.New()
	dirs := datadir.New(datadirPath)

	rawDB := mdbx.New(dbcfg.ChainDB, logger).
		Path(dirs.Chaindata).
		Accede(true). // must exist; do not create
		MustOpen()
	b.Cleanup(rawDB.Close)

	settings, err := state.ResolveErigonDBSettings(dirs, logger, false /*noDownloader*/)
	require.NoError(b, err, "ResolveErigonDBSettings")

	agg := state.New(dirs).
		Logger(logger).
		WithErigonDBSettings(settings).
		MustOpen(b.Context(), rawDB)
	require.NoError(b, agg.OpenFolder(), "agg.OpenFolder")
	b.Cleanup(agg.Close)

	tdb, err := temporal.New(rawDB, agg)
	require.NoError(b, err)
	b.Cleanup(tdb.Close)

	tx, err := tdb.BeginTemporalRo(b.Context())
	require.NoError(b, err)
	b.Cleanup(func() { tx.Rollback() })

	aggTx, ok := tx.AggTx().(*state.AggregatorRoTx)
	require.True(b, ok, "tx.AggTx() must be *state.AggregatorRoTx")

	mdbxKeys, fileKeys := pickRealDatadirKeys(b, aggTx, tx, domain, 10_000)
	require.NotEmpty(b, fileKeys, "no file-resident keys found in %s for domain %v", datadirPath, domain)
	if len(mdbxKeys) == 0 {
		// Heavily pruned production datadir — every MDBX row is
		// step-shadowed by a file. The MDBX-side sub-benches will skip
		// (runH0 short-circuits on empty key set). Compare File_path
		// against the synthetic MDBX_path baseline; clearly note the
		// cross-mode caveat in the issue write-up.
		b.Logf("real-datadir: no MDBX-resident keys (datadir is fully pruned). " +
			"File_path numbers are real; cross-reference MDBX_path against the synthetic bench.")
	}
	b.Logf("real-datadir keys: mdbx=%d file=%d (domain=%v, path=%s)",
		len(mdbxKeys), len(fileKeys), domain, datadirPath)

	return &snapBenchSetup{
		tx:       tx,
		aggTx:    aggTx,
		domain:   domain,
		mdbxKeys: mdbxKeys,
		fileKeys: fileKeys,
	}
}

// pickRealDatadirKeys walks the existing datadir to find up to
// `target` MDBX-resident and `target` file-resident keys for the
// given domain.
//
// Strategy A — cursor-walk the per-domain values table for
// MDBX-resident keys (those rows ARE in MDBX by definition). Strip
// the trailing 8-byte inverted-step from each row's key.
//
// Strategy B — for file-resident keys, randomly sample address-sized
// or storage-key-sized byte strings from the cursor walk's MDBX keys
// (deterministic via seeded RNG): for each candidate, query
// DebugGetLatestFromFiles. If the file path returns ok and MDBX does
// NOT, the key is file-resident. This is approximate — keys that
// exist BOTH in MDBX and files (during prune transitions) are
// classified as MDBX-resident, which is what we want for bench
// timing accuracy (MDBX always wins on getLatest).
//
// Practical note: in production datadirs the MDBX values table
// usually has fewer rows than the snapshot files combined, so we
// expect mdbxKeys to fill faster than fileKeys. To find file-only
// keys we'd need to walk the .kv files directly via seg.Decompressor
// — that's a follow-on once Strategy B is shown to be insufficient.
func pickRealDatadirKeys(b *testing.B, aggTx *state.AggregatorRoTx, tx kv.TemporalTx, domain kv.Domain, target int) (mdbxKeys, fileKeys [][]byte) {
	b.Helper()
	table := domainValsTable(b, domain)

	c, err := tx.Cursor(table)
	require.NoError(b, err)
	defer c.Close()

	const stepSuffixLen = 8 // values-table key = realKey || invertedStep
	seen := make(map[string]struct{})
	var rowsScanned, dbOkCount int

	// First pass: collect MDBX-resident keys directly from the values table.
	for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
		require.NoError(b, err)
		rowsScanned++
		if len(k) <= stepSuffixLen {
			continue // unexpected; skip
		}
		realKey := append([]byte(nil), k[:len(k)-stepSuffixLen]...)
		ks := string(realKey)
		if _, dup := seen[ks]; dup {
			continue
		}
		seen[ks] = struct{}{}

		// Confirm MDBX residency via DebugGetLatestFromDB so we don't
		// include keys that, despite having a row here, don't actually
		// resolve in the read path (edge cases around step bounds).
		_, _, dbOk, err := aggTx.DebugGetLatestFromDB(domain, realKey, tx)
		require.NoError(b, err)
		if dbOk {
			dbOkCount++
			mdbxKeys = append(mdbxKeys, realKey)
			if len(mdbxKeys) >= target {
				break
			}
		}
	}
	b.Logf("first-pass scan: table=%s rows=%d unique_keys=%d dbOk=%d",
		table, rowsScanned, len(seen), dbOkCount)

	// Second pass: walk the same cursor again looking for keys that
	// MISS MDBX but HIT files. Iterating the values table catches
	// keys that left a row but have all data in files (delete
	// markers, post-prune residue), which is approximate but cheap.
	// For a full file-only walk we'd open the .kv decompressor; this
	// is good enough for the H0 first cut.
	for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
		if len(fileKeys) >= target {
			break
		}
		require.NoError(b, err)
		if len(k) <= stepSuffixLen {
			continue
		}
		realKey := append([]byte(nil), k[:len(k)-stepSuffixLen]...)

		_, _, dbOk, err := aggTx.DebugGetLatestFromDB(domain, realKey, tx)
		require.NoError(b, err)
		if dbOk {
			continue // MDBX already serves this; not file-only
		}
		_, fileOk, _, _, err := aggTx.DebugGetLatestFromFiles(domain, realKey, 0)
		require.NoError(b, err)
		if fileOk {
			fileKeys = append(fileKeys, realKey)
		}
	}

	return mdbxKeys, fileKeys
}

// domainValsTable returns the MDBX values-table name for the given
// domain. Used by pickRealDatadirKeys to cursor-walk MDBX directly.
func domainValsTable(b *testing.B, domain kv.Domain) string {
	b.Helper()
	switch domain {
	case kv.AccountsDomain:
		return kv.TblAccountVals
	case kv.StorageDomain:
		return kv.TblStorageVals
	case kv.CodeDomain:
		return kv.TblCodeVals
	case kv.CommitmentDomain:
		return kv.TblCommitmentVals
	default:
		b.Fatalf("domainValsTable: domain %v not mapped", domain)
		return ""
	}
}

// openSnapBenchSynthetic builds an in-memory aggregator with a known
// key distribution and returns a setup whose two key sets are
// guaranteed to live on the file path and the MDBX path respectively.
//
// Sequence:
//
//  1. testDbAndAggregatorBench builds a fresh agg + tempdir.
//  2. Phase 1 — write keysetSize "old" keys at txNums spanning steps
//     [0..stepsToFlush-1], deterministically generated from seed
//     phase1Seed so the same bench reproduces.
//  3. Flush + commit + agg.BuildFiles(stepsToFlush*aggStep) so those
//     keys land in .kv files. PruneSmallBatches removes them from
//     MDBX. After this, phase-1 keys are file-resident.
//  4. Phase 2 — write keysetSize "new" keys at txNums spanning steps
//     [stepsToFlush..stepsToFlush+1], from disjoint phase2Seed so no
//     overlap with phase 1. NO BuildFiles for these. They stay in
//     MDBX.
//  5. Begin a RO temporal tx, return setup.
//
// We verify each key actually lives where we expect via
// DebugGetLatestFromDB / DebugGetLatestFromFiles — fail loudly if a
// key isn't where the test plan says it should be (catches setup
// regressions).
func openSnapBenchSynthetic(b *testing.B, domain kv.Domain) *snapBenchSetup {
	b.Helper()

	const (
		// aggStep small + many full steps mirrors how the fuzz test sets
		// up data. BuildFiles only builds completed steps, so phase-1 has
		// to span an integer number of full steps.
		aggStep      = 16
		stepsToFlush = 64                  // phase 1 spans this many full steps
		keysetSize   = stepsToFlush * aggStep // = 1024; one key per txNum, every step is full
		phase1Seed   = 0xC0FFEE
		phase2Seed   = 0xDECAFB
		// phase-2 starts well past the built step boundary so its keys
		// can't be lumped into a future build.
		phase2BaseTxNum = uint64(stepsToFlush) * aggStep
	)

	// keySize varies per domain; values are 8-byte counters (txNum
	// encoded big-endian) so put/lookup paths are exercised.
	var keySize int
	switch domain {
	case kv.AccountsDomain:
		keySize = length.Addr
	case kv.StorageDomain:
		keySize = length.Addr + length.Hash
	default:
		b.Fatalf("openSnapBenchSynthetic: domain %v not yet wired", domain)
	}

	db, agg := testDbAndAggregatorBench(b, aggStep)
	logger := log.New()

	// ---- Phase 1: keys destined for files. Each key gets its own
	// txNum so per-tx history entries are well-formed.
	phase1Keys := genKeys(phase1Seed, keysetSize, keySize)

	{
		rwTx, err := db.BeginTemporalRw(b.Context())
		require.NoError(b, err)

		domains, err := execctx.NewSharedDomains(b.Context(), rwTx, logger)
		require.NoError(b, err)

		val := make([]byte, 8)
		for i, k := range phase1Keys {
			// One key per txNum, packing every txNum in the
			// stepsToFlush * aggStep range exactly once. This guarantees
			// every step is full so BuildFiles emits a .kv per step.
			txNum := uint64(i)
			binary.BigEndian.PutUint64(val, txNum+1) // +1 so all values are non-zero
			err := domains.DomainPut(domain, rwTx, k, val, txNum, nil)
			require.NoError(b, err)
		}

		// ComputeCommitment writes the trie root into the CommitmentDomain
		// so a follow-up NewSharedDomains can SeekCommitment without
		// erroring "commitment state out of date".
		lastTxNum := uint64(keysetSize - 1)
		_, err = domains.ComputeCommitment(b.Context(), rwTx, true /*save*/, 0, lastTxNum, "h0-bench", nil)
		require.NoError(b, err)

		require.NoError(b, domains.Flush(b.Context(), rwTx))
		domains.Close()
		require.NoError(b, rwTx.Commit())
	}

	// Build files for the phase-1 range.
	require.NoError(b, agg.BuildFiles(uint64(stepsToFlush)*aggStep))

	// Prune phase-1 entries out of MDBX so they're file-only.
	// PruneSmallBatches may report haveMore=true when batch limits stop
	// it short of fully draining; loop until drained or we hit a sane
	// safety bound. time.Hour keeps it in "furious" prune mode (large
	// per-iteration limit), so this is fast in practice.
	for round := 0; round < 32; round++ {
		rwTx, err := db.BeginTemporalRw(b.Context())
		require.NoError(b, err)
		haveMore, err := rwTx.PruneSmallBatches(b.Context(), time.Hour)
		require.NoError(b, err)
		require.NoError(b, rwTx.Commit())
		if !haveMore {
			break
		}
	}

	// ---- Phase 2: keys destined for MDBX (no BuildFiles after).
	phase2Keys := genKeys(phase2Seed, keysetSize, keySize)

	{
		rwTx, err := db.BeginTemporalRw(b.Context())
		require.NoError(b, err)

		domains, err := execctx.NewSharedDomains(b.Context(), rwTx, logger)
		require.NoError(b, err)

		val := make([]byte, 8)
		for i, k := range phase2Keys {
			// Phase-2 keys live well past the built-step boundary so a
			// hypothetical follow-up BuildFiles wouldn't sweep them in.
			txNum := phase2BaseTxNum + uint64(i)
			binary.BigEndian.PutUint64(val, txNum+1)
			err := domains.DomainPut(domain, rwTx, k, val, txNum, nil)
			require.NoError(b, err)
		}

		require.NoError(b, domains.Flush(b.Context(), rwTx))
		domains.Close()
		require.NoError(b, rwTx.Commit())
	}

	// ---- Open a RO tx for benching.
	tx, err := db.BeginTemporalRo(b.Context())
	require.NoError(b, err)
	b.Cleanup(func() { tx.Rollback() })

	aggTx, ok := tx.AggTx().(*state.AggregatorRoTx)
	require.True(b, ok, "tx.AggTx() must be *state.AggregatorRoTx")

	// Partition each phase set by actual residency. We expect:
	//   phase 1 -> almost all "file", with a tail of tip-step keys still
	//              in MDBX because Prune retains the most-recent step;
	//   phase 2 -> all "mdbx" (no BuildFiles called for it).
	// The bench uses only the keys that landed where we want, so any
	// misroute just shrinks the keyset rather than corrupting timings.
	mdbxFromP2, fileFromP2, missingP2 := partitionByResidency(b, aggTx, tx, domain, phase2Keys)
	mdbxFromP1, fileFromP1, missingP1 := partitionByResidency(b, aggTx, tx, domain, phase1Keys)
	if missingP1 > 0 || missingP2 > 0 {
		b.Fatalf("residency partition: missing keys p1=%d p2=%d", missingP1, missingP2)
	}
	b.Logf("residency: phase1 file=%d mdbx=%d  phase2 file=%d mdbx=%d",
		len(fileFromP1), len(mdbxFromP1), len(fileFromP2), len(mdbxFromP2))

	mdbxKeys := mdbxFromP2 // bench's mdbx-resident set
	fileKeys := fileFromP1 // bench's file-resident set
	require.NotEmpty(b, mdbxKeys, "synthetic fixture produced no MDBX-resident keys")
	require.NotEmpty(b, fileKeys, "synthetic fixture produced no file-resident keys")

	return &snapBenchSetup{
		tx:       tx,
		aggTx:    aggTx,
		domain:   domain,
		mdbxKeys: mdbxKeys,
		fileKeys: fileKeys,
	}
}

// genKeys produces n deterministic keys of size keySize bytes from
// the given seed. Same seed -> same keys -> reproducible benches.
func genKeys(seed uint64, n, keySize int) [][]byte {
	rnd := newRnd(seed)
	keys := make([][]byte, n)
	for i := range keys {
		k := make([]byte, keySize)
		_, _ = rnd.Read(k)
		keys[i] = k
	}
	return keys
}

// partitionByResidency walks keys and groups them by which read path
// returns the value: "mdbx" (DebugGetLatestFromDB ok), "file"
// (DebugGetLatestFromDB miss + DebugGetLatestFromFiles ok), or
// "missing" (neither). missing is reported as a count for the caller
// to fail on — no key in our synthetic setup should be unreachable.
func partitionByResidency(b *testing.B, aggTx *state.AggregatorRoTx, tx kv.TemporalTx, domain kv.Domain, keys [][]byte) (mdbxKeys, fileKeys [][]byte, missing int) {
	b.Helper()
	for _, k := range keys {
		_, _, dbOk, err := aggTx.DebugGetLatestFromDB(domain, k, tx)
		require.NoError(b, err)
		if dbOk {
			mdbxKeys = append(mdbxKeys, k)
			continue
		}
		_, fileOk, _, _, err := aggTx.DebugGetLatestFromFiles(domain, k, 0)
		require.NoError(b, err)
		if fileOk {
			fileKeys = append(fileKeys, k)
			continue
		}
		missing++
	}
	return
}

// warmup reads every key in keys via tx.GetLatest so the OS page cache
// is hot for all relevant snapshot/MDBX pages before timing starts.
// Required because the H0 claim only holds for warm pages — we want to
// measure software overhead, not disk I/O.
func warmup(tx kv.TemporalTx, domain kv.Domain, keys [][]byte) {
	for _, k := range keys {
		_, _, _ = tx.GetLatest(domain, k)
	}
}

// skipIfEmpty short-circuits the calling sub-bench when the key set
// is empty (real-datadir mode often has no MDBX-resident keys after
// aggressive pruning).
func skipIfEmpty(b *testing.B, name string, keys [][]byte) bool {
	if len(keys) == 0 {
		b.Skipf("no keys for %s sub-bench", name)
		return true
	}
	return false
}

// runH0 is the H0 measurement body. Four sub-benches:
//
//   - MDBX_path: tx.GetLatest with key in MDBX. Hits getLatestFromDb
//     and returns; never touches files. Baseline cost.
//
//   - File_path: tx.GetLatest with key only in files. Misses MDBX,
//     then walks getLatestFromFiles (Bloom -> recsplit -> seg
//     decompress). Baseline-with-files cost.
//
//   - Forced_file_path: same MDBX-resident key set as MDBX_path but
//     routed through DebugGetLatestFromFiles, forcing the file path
//     even though the key would have hit MDBX. Isolates per-key
//     file-path cost from key-distribution effects.
//
//   - Forced_db_path: DebugGetLatestFromDB on the MDBX key set —
//     mirror of Forced_file_path, isolates the MDBX-only path cost.
//
// Headline: file_ns_per_op / mdbx_ns_per_op. Forced_* sub-benches
// confirm the ratio isn't a key-set artifact.
func runH0(b *testing.B, setup *snapBenchSetup) {
	b.Helper()

	// Two warmup passes: first lands pages in the OS page cache,
	// second absorbs L3/TLB effects so we time steady-state.
	warmup(setup.tx, setup.domain, setup.mdbxKeys)
	warmup(setup.tx, setup.domain, setup.fileKeys)
	warmup(setup.tx, setup.domain, setup.mdbxKeys)
	warmup(setup.tx, setup.domain, setup.fileKeys)

	b.Run("MDBX_path", func(b *testing.B) {
		if skipIfEmpty(b, "MDBX_path", setup.mdbxKeys) {
			return
		}
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _ = setup.tx.GetLatest(setup.domain, setup.mdbxKeys[i%len(setup.mdbxKeys)])
		}
	})

	b.Run("File_path", func(b *testing.B) {
		if skipIfEmpty(b, "File_path", setup.fileKeys) {
			return
		}
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _ = setup.tx.GetLatest(setup.domain, setup.fileKeys[i%len(setup.fileKeys)])
		}
	})

	// Forced_file_path: file-resident keys via the file-only debug path.
	// Strips MDBX-miss cost from File_path so we see pure file-side
	// work (Bloom -> recsplit -> seg decompress).
	b.Run("Forced_file_path", func(b *testing.B) {
		if skipIfEmpty(b, "Forced_file_path", setup.fileKeys) {
			return
		}
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _, _, _ = setup.aggTx.DebugGetLatestFromFiles(
				setup.domain, setup.fileKeys[i%len(setup.fileKeys)], 0)
		}
	})

	// Forced_db_path: MDBX-resident keys via the DB-only debug path.
	// Strips routing/dispatch overhead from MDBX_path so we see pure
	// MDBX cursor work.
	b.Run("Forced_db_path", func(b *testing.B) {
		if skipIfEmpty(b, "Forced_db_path", setup.mdbxKeys) {
			return
		}
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _, _ = setup.aggTx.DebugGetLatestFromDB(
				setup.domain, setup.mdbxKeys[i%len(setup.mdbxKeys)], setup.tx)
		}
	})

	// Bloom_miss_path: MDBX-resident keys against the file-only debug
	// path. These keys are NOT in any .kv file so each lookup is a
	// pure xorfilter "not present" probe per file. Gives the Bloom-miss
	// cost in isolation — useful for H1 (per-file Bloom probe overhead).
	b.Run("Bloom_miss_path", func(b *testing.B) {
		if skipIfEmpty(b, "Bloom_miss_path", setup.mdbxKeys) {
			return
		}
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _, _, _ = setup.aggTx.DebugGetLatestFromFiles(
				setup.domain, setup.mdbxKeys[i%len(setup.mdbxKeys)], 0)
		}
	})
}

// BenchmarkSnapVsMDBX_H0_Accounts measures the warm-cache file-vs-MDBX
// per-key gap on the AccountsDomain. Headline: file_ns/op : mdbx_ns/op.
func BenchmarkSnapVsMDBX_H0_Accounts(b *testing.B) {
	runH0(b, openSnapBench(b, kv.AccountsDomain))
}

// runHGetAsOf measures HistorySeek-via-GetAsOf cost on file-resident
// keys. The motivation (per H0 finding): `getLatestFromFiles` is fast
// (~30 ns isolated); the bloat-workload pprof showed real file-read
// pressure that this can't account for. The calculator's
// HistoryStateReader.Read calls tx.GetAsOf, which goes through
// HistorySeek and walks .ef history files — a different code path
// than getLatestFromFiles.
//
// Sub-benches:
//
//   - GetLatest_baseline: tx.GetLatest on file-resident keys; mirrors
//     H0's File_path so we can confirm the same setup against a fresh
//     comparator.
//
//   - GetAsOf_recent: tx.GetAsOf at asOfTxNum = endTxNum - 1. Most
//     keys won't have a history record post-asOf, so HistorySeek
//     short-circuits to the latest path. Lower bound on GetAsOf cost.
//
//   - GetAsOf_mid: tx.GetAsOf at asOfTxNum = endTxNum / 2. Forces
//     the .ef walk to seek through more history. This is the cost
//     shape the calculator pays when reading historic state.
//
//   - GetAsOf_zero: tx.GetAsOf at asOfTxNum = HistoryStartFrom() + 1
//     (just past the visible window start). Maximally adversarial —
//     full .ef walk depth. The calculator's old asOfReader.txNum=0
//     bug (PR #21010) hit this path when the window started past 0.
//
// All sub-benches use the same fileKeys set so per-key cost is
// directly comparable across the four.
func runHGetAsOf(b *testing.B, setup *snapBenchSetup) {
	b.Helper()
	if skipIfEmpty(b, "H_GetAsOf", setup.fileKeys) {
		return
	}

	// Snapshot the visible-history window so all four sub-benches use
	// the same anchors and so we report them in the bench log. We
	// don't probe historyStartFrom from here (no public accessor on
	// AggregatorRoTx for per-domain history range); instead use simple
	// fixed fractions of endTxNum. Adjust asOfFloor=1 to avoid
	// hitting txNum=0 which can error on snapshot-loaded chains
	// (PR #21010 was that bug; we don't want H_GetAsOf to trip on it).
	endTxNum := setup.aggTx.EndTxNumNoCommitment()
	asOfRecent := endTxNum
	if endTxNum > 0 {
		asOfRecent = endTxNum - 1
	}
	asOfMid := endTxNum / 2
	asOfFloor := uint64(1)
	b.Logf("H_GetAsOf anchors: endTxNum=%d -> asOfRecent=%d asOfMid=%d asOfFloor=%d",
		endTxNum, asOfRecent, asOfMid, asOfFloor)

	// Two warmup passes so the OS page cache holds the .ef files we
	// care about. Touch each anchor txNum so the relevant history
	// segments are mmap'd in.
	for _, asOf := range []uint64{asOfRecent, asOfMid, asOfFloor} {
		for _, k := range setup.fileKeys {
			_, _, _ = setup.tx.GetAsOf(setup.domain, k, asOf)
		}
		for _, k := range setup.fileKeys {
			_, _, _ = setup.tx.GetAsOf(setup.domain, k, asOf)
		}
	}

	b.Run("GetLatest_baseline", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _ = setup.tx.GetLatest(setup.domain, setup.fileKeys[i%len(setup.fileKeys)])
		}
	})

	b.Run("GetAsOf_recent", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _ = setup.tx.GetAsOf(setup.domain, setup.fileKeys[i%len(setup.fileKeys)], asOfRecent)
		}
	})

	b.Run("GetAsOf_mid", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _ = setup.tx.GetAsOf(setup.domain, setup.fileKeys[i%len(setup.fileKeys)], asOfMid)
		}
	})

	b.Run("GetAsOf_floor", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _, _ = setup.tx.GetAsOf(setup.domain, setup.fileKeys[i%len(setup.fileKeys)], asOfFloor)
		}
	})
}

// BenchmarkSnapVsMDBX_HGetAsOf_Accounts measures HistorySeek cost on
// real file-resident keys via tx.GetAsOf. Confirms the H0 finding
// that the production bloat-workload bottleneck is in .ef history
// walking, not .kv latest-state reads.
func BenchmarkSnapVsMDBX_HGetAsOf_Accounts(b *testing.B) {
	runHGetAsOf(b, openSnapBench(b, kv.AccountsDomain))
}

// BenchmarkSnapVsMDBX_HGetAsOf_Storage — same for StorageDomain.
// Storage tends to dominate in the bloat workload.
func BenchmarkSnapVsMDBX_HGetAsOf_Storage(b *testing.B) {
	runHGetAsOf(b, openSnapBench(b, kv.StorageDomain))
}

// BenchmarkSnapVsMDBX_H0_Storage — same harness for StorageDomain.
// Storage dominates file-read traffic on bloat workloads (per
// runs-step9-cache-behind-sd memory), so getting the gap for both
// Accounts and Storage is the minimum useful H0 output.
func BenchmarkSnapVsMDBX_H0_Storage(b *testing.B) {
	runH0(b, openSnapBench(b, kv.StorageDomain))
}
