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
// logaddrs, logtopics, receipt, rcache, commitment) plus every non-
// history file (domain/*.kv, top-level block segs, chain.toml).
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
		"history/v3.0-receipt.256-272.v":   "rcpt",
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
		"history/v3.0-receipt.256-272.v":     false,
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
