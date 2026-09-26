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

package storage

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

const testStepSize uint64 = 390625

func items(m map[string]string) snapcfg.PreverifiedItems {
	out := make(snapcfg.PreverifiedItems, 0, len(m))
	for name, hash := range m {
		out = append(out, snapcfg.PreverifiedItem{Name: name, Hash: hash})
	}
	out.Sort()
	return out
}

func namesOf(items []snapcfg.PreverifiedItem) []string {
	names := make([]string, len(items))
	for i, it := range items {
		names[i] = it.Name
	}
	sort.Strings(names)
	return names
}

// The mode-B compute walks accounts/storage/code history only. The filter
// must include exactly those under history/, idx/, and accessor/ whose
// step range overlaps (baselineStep, walkEndStep] — and exclude every
// pre-baseline file plus every non-walked domain (tracesfrom, tracesto,
// logaddrs, logtopics, rcache, commitment) plus every non-history file
// (domain/*.kv, top-level block segs, chain.toml). Receipt IS in the
// walked set as of 2026-08-16 — mode-C split-emit needs its history so
// the aligned receipt file can be regenerated cleanly (see walkDomains
// docstring).
func TestNeededPreverifiedHistoryForWalk_FiltersByDomainAndOverlap(t *testing.T) {
	t.Parallel()

	all := items(map[string]string{
		// Walked domains — post-baseline: INCLUDE.
		"history/v2.0-accounts.256-272.v":    "aaa2",
		"history/v2.0-accounts.272-280.v":    "aaa3",
		"history/v2.0-accounts.280-284.v":    "aaa4",
		"history/v2.0-accounts.284-286.v":    "aaa5",
		"history/v2.0-accounts.286-287.v":    "aaa6",
		"history/v2.0-storage.256-272.v":     "bbb2",
		"history/v2.0-code.256-272.v":        "ccc2",
		"history/v3.0-receipt.256-272.v":     "rcpt",
		"idx/v3.0-accounts.256-272.ef":       "iii2",
		"accessor/v1.1-accounts.256-272.vi":  "vvv2",
		"accessor/v2.1-accounts.256-272.efi": "eff2",
		// Walked domains — pre-baseline: EXCLUDE (data already in trie baseline).
		"history/v2.0-accounts.0-256.v":   "aaa1",
		"history/v2.0-storage.0-256.v":    "bbb1",
		"history/v2.0-code.0-256.v":       "ccc1",
		"idx/v3.0-accounts.0-256.ef":      "iii1",
		"accessor/v1.1-accounts.0-256.vi": "vvv1",
		// Non-walked domains: EXCLUDE.
		"idx/v3.0-logaddrs.256-272.ef":     "la",
		"idx/v3.0-logtopics.256-272.ef":    "lt",
		"idx/v3.0-tracesfrom.256-272.ef":   "tf",
		"idx/v3.0-tracesto.256-272.ef":     "tt",
		"accessor/v2.0-rcache.256-272.efi": "rc",
		// Non-history categories: EXCLUDE.
		"domain/v2.0-accounts.256-272.kv":   "kv",
		"domain/v2.0-commitment.256-272.kv": "cmt",
		"v1.1-000000-000100-headers.seg":    "blk0",
		"chain.v2.abc.toml":                 "toml0",
	})

	// baselineStep=256, walkEndStep=287.
	got := namesOf(neededPreverifiedHistoryForWalk(all, 256, 287, testStepSize))

	want := []string{
		"accessor/v1.1-accounts.256-272.vi",
		"accessor/v2.1-accounts.256-272.efi",
		"history/v2.0-accounts.256-272.v",
		"history/v2.0-accounts.272-280.v",
		"history/v2.0-accounts.280-284.v",
		"history/v2.0-accounts.284-286.v",
		"history/v2.0-accounts.286-287.v",
		"history/v2.0-code.256-272.v",
		"history/v2.0-storage.256-272.v",
		"history/v3.0-receipt.256-272.v",
		"idx/v3.0-accounts.256-272.ef",
	}
	require.Equal(t, want, got)
}

func TestNeededPreverifiedHistoryForWalk_ExcludesStepsPastEnd(t *testing.T) {
	t.Parallel()

	all := items(map[string]string{
		"history/v2.0-accounts.256-272.v": "in",
		"history/v2.0-accounts.288-296.v": "outStart", // fromStep=288 > walkEndStep=287
	})

	got := namesOf(neededPreverifiedHistoryForWalk(all, 256, 287, testStepSize))
	require.Equal(t, []string{"history/v2.0-accounts.256-272.v"}, got)
}

func TestNeededPreverifiedHistoryForWalk_BaselineAtOrBeyondWalkEndReturnsNil(t *testing.T) {
	t.Parallel()

	all := items(map[string]string{"history/v2.0-accounts.256-272.v": "x"})
	require.Nil(t, neededPreverifiedHistoryForWalk(all, 287, 287, testStepSize))
	require.Nil(t, neededPreverifiedHistoryForWalk(all, 300, 287, testStepSize))
}

func TestFilterMissingOnDisk_SplitsPresentAndMissing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "history"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "history", "already-here.v"), []byte("x"), 0o644))

	inputs := []snapcfg.PreverifiedItem{
		{Name: "history/already-here.v", Hash: "hash-present"},
		{Name: "history/needed-1.v", Hash: "hash1"},
		{Name: "history/needed-2.v", Hash: "hash2"},
	}

	missing, paths, names := filterMissingOnDisk(inputs, dir)
	require.Len(t, missing, 2)
	require.Equal(t, "history/needed-1.v", missing[0].Path)
	require.Equal(t, "hash1", missing[0].TorrentHash)
	require.Equal(t, "history/needed-2.v", missing[1].Path)
	require.Equal(t, filepath.Join(dir, "history", "needed-1.v"), paths[0])
	require.Equal(t, filepath.Join(dir, "history", "needed-2.v"), paths[1])
	require.Equal(t, "history/needed-1.v", names[0])
	require.Equal(t, "history/needed-2.v", names[1])
}

func TestFilterMissingOnDisk_AllPresentReturnsEmpty(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "idx"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "idx", "one.ef"), []byte("x"), 0o644))

	missing, paths, names := filterMissingOnDisk([]snapcfg.PreverifiedItem{
		{Name: "idx/one.ef", Hash: "h"},
	}, dir)
	require.Empty(t, missing)
	require.Empty(t, paths)
	require.Empty(t, names)
}

// stubDeleteCounter satisfies just enough of downloader.Client to
// capture the names passed to Delete during a discard call. Never
// calls into a real downloader — the goal is to prove the ensure
// cleanup ALSO invokes Delete (dropping the torrents from the client's
// internal state), not just the FS-unlink.
type stubDeleteCounter struct {
	deletes [][]string
}

func (s *stubDeleteCounter) Seed(_ context.Context, _ []string) error { return nil }
func (s *stubDeleteCounter) Delete(_ context.Context, paths []string) error {
	s.deletes = append(s.deletes, append([]string(nil), paths...))
	return nil
}
func (s *stubDeleteCounter) Download(_ context.Context, _ *downloaderproto.DownloadRequest) error {
	return nil
}

// TestDiscardDownloadedHistory_InvokesDownloaderDelete regressions the
// mode-B bootstrap-loop bug: after the ensure step downloaded history
// files for iter N's compute walk, the cleanup callback unlinked the
// files but did NOT tell the torrent client to drop the torrents.
// Iter N+1's ensure call then saw the torrents as "have complete"
// from N's download, returned instantly with files=12/12 100.00%
// even though the actual files were gone, and the compute produced
// the baseline root instead of the target root (mismatch → setHead
// mode-B fail on any deep unwind after a prior mode-B in the same
// session).
func TestDiscardDownloadedHistory_InvokesDownloaderDelete(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "history"), 0o755))
	f := filepath.Join(dir, "history", "a.v")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))

	stub := &stubDeleteCounter{}
	p := &Provider{downloaderClient: stub}
	names := []string{"history/a.v"}
	p.discardDownloadedHistory(context.Background(), []string{f}, names)

	require.Len(t, stub.deletes, 1, "downloader.Delete must be called exactly once")
	require.Equal(t, names, stub.deletes[0], "downloader.Delete must receive the snap-dir-relative names")
	_, err := os.Stat(f)
	require.True(t, os.IsNotExist(err), "file must also be unlinked from disk")
}

func TestDiscardDownloadedHistory_RemovesFilesAndTorrents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "history"), 0o755))
	f1 := filepath.Join(dir, "history", "a.v")
	f2 := filepath.Join(dir, "history", "b.v")
	require.NoError(t, os.WriteFile(f1, []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(f2, []byte("y"), 0o644))
	require.NoError(t, os.WriteFile(f1+".torrent", []byte("t"), 0o644))
	// f2.torrent intentionally missing — cleanup must not error on that.

	p := &Provider{}
	// Pass nil names — Provider.downloaderClient is nil so the Delete
	// step is a no-op; the FS-unlink path is what this test exercises.
	p.discardDownloadedHistory(context.Background(), []string{f1, f2}, nil)

	for _, path := range []string{f1, f2, f1 + ".torrent"} {
		_, err := os.Stat(path)
		require.True(t, os.IsNotExist(err), "%s should have been removed, got err=%v", path, err)
	}
}

func TestParseStateFileStepRange_LegacyVersion(t *testing.T) {
	t.Parallel()

	from, to, ok := parseStateFileStepRange("history/v2.0-accounts.256-272.v", testStepSize)
	require.True(t, ok)
	require.Equal(t, uint64(256), from)
	require.Equal(t, uint64(272), to)
}

func TestIsWalkDomain(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"history/v2.0-accounts.256-272.v":    true,
		"history/v2.0-storage.256-272.v":     true,
		"history/v2.0-code.256-272.v":        true,
		"idx/v3.0-accounts.256-272.ef":       true,
		"accessor/v2.1-accounts.256-272.efi": true,
		"history/v3.0-receipt.256-272.v":     true,
		"idx/v3.0-logaddrs.256-272.ef":       false,
		"idx/v3.0-logtopics.256-272.ef":      false,
		"idx/v3.0-tracesfrom.256-272.ef":     false,
		"idx/v3.0-tracesto.256-272.ef":       false,
		"accessor/v2.0-rcache.256-272.efi":   false,
		"domain/v2.0-commitment.0-256.kv":    false,
	}
	for name, want := range cases {
		require.Equal(t, want, isWalkDomain(name), name)
	}
}

// TestLocalHistoryCoversWalk pins the fast-path guard added to
// ensureHistoryForUnwindWalk: when every walked step × walk domain has a
// local .v file covering it, the ensure step must return true so the
// caller skips the preverified-registry starvation error.
//
// Motivation: cycle 6 iter 5 mode_a #2 hit the starvation error because
// preverified had no entries for steps 310-314 (past preverified's
// horizon), even though local retire had produced them. The fast path
// prevents that class of false-positive.
func TestLocalHistoryCoversWalk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	historyDir := filepath.Join(dir, "history")
	require.NoError(t, os.MkdirAll(historyDir, 0o755))

	// Full coverage of steps 310-313 for all four walk domains.
	fullSet := []string{
		"v2.1-accounts.310-311.v", "v2.1-accounts.311-312.v", "v2.1-accounts.312-313.v", "v2.1-accounts.313-314.v",
		"v2.1-storage.310-311.v", "v2.1-storage.311-312.v", "v2.1-storage.312-313.v", "v2.1-storage.313-314.v",
		"v2.1-code.310-311.v", "v2.1-code.311-312.v", "v2.1-code.312-313.v", "v2.1-code.313-314.v",
		"v2.1-receipt.310-311.v", "v2.1-receipt.311-312.v", "v2.1-receipt.312-313.v", "v2.1-receipt.313-314.v",
	}
	for _, name := range fullSet {
		require.NoError(t, os.WriteFile(filepath.Join(historyDir, name), []byte("x"), 0o644))
	}

	// baseline=310 walkEnd=314 — covers full range, all four domains present.
	require.True(t, localHistoryCoversWalk(dir, 310, 314, testStepSize))

	// baseline=310 walkEnd=315 — extends past local coverage.
	require.False(t, localHistoryCoversWalk(dir, 310, 315, testStepSize))

	// baseline==walkEnd → trivially covered (empty range).
	require.True(t, localHistoryCoversWalk(dir, 310, 310, testStepSize))
	require.True(t, localHistoryCoversWalk(dir, 313, 310, testStepSize))
}

// TestLocalHistoryCoversWalk_MissingDomainFails pins the per-domain
// coverage requirement: even if accounts/storage/code are fully covered,
// missing receipt coverage counts as starvation.
func TestLocalHistoryCoversWalk_MissingDomainFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	historyDir := filepath.Join(dir, "history")
	require.NoError(t, os.MkdirAll(historyDir, 0o755))
	// Only 3 of the 4 walk domains present.
	for _, name := range []string{
		"v2.1-accounts.310-311.v",
		"v2.1-storage.310-311.v",
		"v2.1-code.310-311.v",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(historyDir, name), []byte("x"), 0o644))
	}
	require.False(t, localHistoryCoversWalk(dir, 310, 311, testStepSize),
		"receipt domain missing → coverage incomplete")
}

// TestLocalHistoryCoversWalk_MergedFileCoversRange pins that a merged
// multi-step .v file (v2.2-accounts.288-304.v) satisfies coverage for
// every step in its [288, 304) range, not just one — retire+merge
// widen files and both shapes contribute to local coverage.
func TestLocalHistoryCoversWalk_MergedFileCoversRange(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	historyDir := filepath.Join(dir, "history")
	require.NoError(t, os.MkdirAll(historyDir, 0o755))
	// One merged file per domain spanning steps 288-303 (4 steps).
	for _, dom := range []string{"accounts", "storage", "code", "receipt"} {
		name := "v2.2-" + dom + ".288-304.v"
		require.NoError(t, os.WriteFile(filepath.Join(historyDir, name), []byte("x"), 0o644))
	}
	// Walk over any subrange of the merged file's coverage — should pass.
	require.True(t, localHistoryCoversWalk(dir, 288, 304, testStepSize))
	require.True(t, localHistoryCoversWalk(dir, 290, 300, testStepSize))
}

func TestLocalCommitmentBaselineStep(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	domainDir := filepath.Join(dir, "domain")
	require.NoError(t, os.MkdirAll(domainDir, 0o755))
	// Two commitment .kv files present; the widest ≤ walkEndStep wins.
	for _, name := range []string{
		"v2.0-commitment.0-256.kv",
		"v2.1-commitment.256-288.kv",
		"v2.0-accounts.0-256.kv", // non-commitment .kv — must be ignored.
	} {
		require.NoError(t, os.WriteFile(filepath.Join(domainDir, name), []byte("x"), 0o644))
	}

	// walkEndStep=287 → 256-288 excluded (toStep=288 > 287); best = 256.
	got, ok := localCommitmentBaselineStep(dir, 287, testStepSize)
	require.True(t, ok)
	require.Equal(t, uint64(256), got)

	// walkEndStep=288 → 256-288 eligible (toStep=288 ≤ 288); best = 288.
	got, ok = localCommitmentBaselineStep(dir, 288, testStepSize)
	require.True(t, ok)
	require.Equal(t, uint64(288), got)

	// walkEndStep=100 → neither file eligible.
	_, ok = localCommitmentBaselineStep(dir, 100, testStepSize)
	require.False(t, ok)

	// Missing domain dir → false.
	_, ok = localCommitmentBaselineStep(t.TempDir(), 287, testStepSize)
	require.False(t, ok)
}

// TestBaselineMaxStepFor pins the maxStep the compute uses to pick its
// baseline commitment file. Ensure must choose the SAME baseline, or it
// reasons about a walk the compute is not doing.
func TestBaselineMaxStepFor(t *testing.T) {
	t.Parallel()

	const step = uint64(390625)
	require.Equal(t, uint64(330), baselineMaxStepFor(129_287_388, step),
		"a target inside step 330 leaves the baseline lookup capped at 330")
	require.Equal(t, uint64(331), baselineMaxStepFor(331*step-1, step),
		"a target on the step boundary admits a file ending at 331")
}

// TestCoverageEndStepFor pins the exclusive end of the step range needing
// history. It differs from the baseline cap whenever the target is mid-step,
// which is the normal shape once v4 files are cut mid-step.
//
// v4 files are cut mid-step, so an unwind target usually sits inside a step
// rather than on its boundary. That step is touched and must be included;
// deriving the end as (target+1)/stepSize collapses such a walk to an empty
// step range, and ensureHistoryForUnwindWalk then fetches nothing — the
// compute later refuses the unwind with "zero touches".
func TestCoverageEndStepFor(t *testing.T) {
	t.Parallel()

	const step = uint64(390625)
	require.Equal(t, uint64(331), coverageEndStepFor(129_256_174, step),
		"a target inside step 330 must leave step 330 in the range")
	require.Equal(t, uint64(331), coverageEndStepFor(128_934_742, step),
		"the live mode-C case: 28493 txNums into step 330")
	require.Equal(t, uint64(331), coverageEndStepFor(331*step-1, step),
		"the last txNum of step 330 is still step 330")
	require.Equal(t, uint64(332), coverageEndStepFor(331*step, step),
		"the first txNum of step 331 moves the end on")
}

// TestWalkNeedsHistoryFiles pins what the ensure step is actually for.
//
// The compute reads history from snapshot files AND from MDBX. Files are only
// needed for a walk starting below what is already readable. A shallow unwind
// lands in the step the node is currently executing, whose history lives in
// MDBX and which no publisher can have a file for — demanding one there is
// starvation that can never be satisfied.
func TestWalkNeedsHistoryFiles(t *testing.T) {
	t.Parallel()

	const historyStart = uint64(129_296_875) // first txNum readable

	require.False(t, walkNeedsHistoryFiles(129_687_500, historyStart),
		"a walk in the current step is readable from MDBX; no file exists or ever will")
	require.False(t, walkNeedsHistoryFiles(historyStart, historyStart),
		"a walk starting exactly where history begins is readable")
	require.True(t, walkNeedsHistoryFiles(128_906_250, historyStart),
		"the deep mode-C case: the walk starts below what files+DB provide")
	require.True(t, walkNeedsHistoryFiles(0, historyStart),
		"a walk from genesis needs files")
}

// TestWalkStepConsumersAgree pins that the two coverage consumers require the
// SAME steps for the same walk.
//
// They are asked the same question — is the walk's history available — one
// against files on disk, one against the preverified registry. When their step
// ranges differ, files that satisfy one starve the other, and which of the two
// fails depends on where the unwind target happens to land.
func TestWalkStepConsumersAgree(t *testing.T) {
	t.Parallel()

	// A walk whose baseline ends at step 330 and whose target sits inside
	// step 330: the live mode-C shape. Exactly one step is touched — 330.
	const baselineStep, walkEndStep = uint64(330), uint64(331)

	dir := t.TempDir()
	historyDir := filepath.Join(dir, "history")
	require.NoError(t, os.MkdirAll(historyDir, 0o755))
	names := map[string]string{}
	for _, dom := range []string{"accounts", "storage", "code", "receipt"} {
		// Cover exactly the touched step, nothing beyond it.
		base := "v2.1-" + dom + ".330-331.v"
		require.NoError(t, os.WriteFile(filepath.Join(historyDir, base), []byte("x"), 0o644))
		names["history/"+base] = dom
	}

	require.True(t, localHistoryCoversWalk(dir, baselineStep, walkEndStep, testStepSize),
		"files covering the touched step must satisfy the on-disk check")
	require.Empty(t, findStarvedCoverage(items(names), baselineStep, walkEndStep),
		"the same files must satisfy the preverified check — a consumer asking "+
			"for a different step reports starvation the other cannot see")
}
