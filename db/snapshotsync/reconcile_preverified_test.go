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

package snapshotsync

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapcfg"
)

// touch creates an empty file at path; tests use it to mark a
// preverified entry as "present on disk" without staging real content.
func touch(t *testing.T, dir, name string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	f, err := os.Create(filepath.Join(dir, name))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func names(reqs []DownloadRequestLite) []string {
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.Name)
	}
	sort.Strings(out)
	return out
}

// TestReconcileEmptyPreverified: an empty preverified list yields no
// missing files.
func TestReconcileEmptyPreverified(t *testing.T) {
	dir := t.TempDir()
	missing := ReconcilePreverifiedAgainstDisk(snapcfg.PreverifiedItems{}, dir)
	require.Empty(t, missing)
}

// TestReconcileAllPresent: every preverified entry is on disk → no
// missing files.
func TestReconcileAllPresent(t *testing.T) {
	dir := t.TempDir()
	items := snapcfg.PreverifiedItems{
		{Name: "v1.1-000000-000001-headers.seg", Hash: "h1"},
		{Name: "v1.1-000000-000001-bodies.seg", Hash: "h2"},
		{Name: "v1.1-000000-000001-transactions.seg", Hash: "h3"},
	}
	for _, it := range items {
		touch(t, dir, it.Name)
	}
	missing := ReconcilePreverifiedAgainstDisk(items, dir)
	require.Empty(t, missing,
		"all preverified entries are on disk; reconciliation should report zero missing")
}

// TestReconcileOneMissing is the wedge-repro: preverified.toml lists a
// file that never landed on disk. Reconciliation must report it.
func TestReconcileOneMissing(t *testing.T) {
	dir := t.TempDir()
	items := snapcfg.PreverifiedItems{
		{Name: "v1.1-000000-000001-headers.seg", Hash: "h1"},
		{Name: "v1.1-000001-000002-headers.seg", Hash: "h2"}, // intentionally NOT touched
		{Name: "v1.1-000002-000003-headers.seg", Hash: "h3"},
	}
	touch(t, dir, items[0].Name)
	touch(t, dir, items[2].Name)
	missing := ReconcilePreverifiedAgainstDisk(items, dir)
	require.Equal(t,
		[]string{"v1.1-000001-000002-headers.seg"},
		names(missing),
		"the single missing entry must be reported with its preverified hash")
	require.Equal(t, "h2", missing[0].Hash,
		"the hash from preverified.toml must be carried into the download request")
}

// TestReconcileSkipsCoveredByLocalWiderFile pins the post-bootstrap
// invariant: once the local node has produced a wider file that fully
// covers a preverified entry's [From, To) range (same subdir +
// version + type + extension), the reconcile pass must NOT re-request
// the narrower preverified file. Without this, every reconcile pass
// undoes the local merge: the broad locally-built file remains AND the
// publisher-narrow files reappear, leaving the aggregator visibility
// set with both — manifesting as wrong-trie-root ~209 blocks past a
// Mode-B unwind target.
func TestReconcileSkipsCoveredByLocalWiderFile(t *testing.T) {
	dir := t.TempDir()
	domainDir := filepath.Join(dir, "domain")
	// Local merge produced a broad 272-280 .kv (covers all of 272-279).
	touch(t, domainDir, "v2.0-accounts.272-280.kv")
	// Preverified.toml advertises three narrow chunks that union to
	// match the broad file's coverage. They were on disk at boot
	// (from OtterSync) but were consumed by the local merge.
	items := snapcfg.PreverifiedItems{
		{Name: "domain/v2.0-accounts.272-276.kv", Hash: "h-narrow-272"},
		{Name: "domain/v2.0-accounts.276-278.kv", Hash: "h-narrow-276"},
		{Name: "domain/v2.0-accounts.278-279.kv", Hash: "h-narrow-278"},
	}
	missing := ReconcilePreverifiedAgainstDisk(items, dir)
	require.Empty(t, missing,
		"narrow preverified entries fully covered by the local wider file must not be re-requested")
}

// TestReconcilePartialCoverageStillReports pins the boundary case:
// a local file that only partially covers the preverified range does
// NOT subsume it. The missing preverified entry must still be
// reported so the downloader can fetch it.
func TestReconcilePartialCoverageStillReports(t *testing.T) {
	dir := t.TempDir()
	domainDir := filepath.Join(dir, "domain")
	// Local file ends at step 278; preverified entry extends to 279.
	touch(t, domainDir, "v2.0-accounts.272-278.kv")
	items := snapcfg.PreverifiedItems{
		{Name: "domain/v2.0-accounts.272-279.kv", Hash: "h-wider"},
	}
	missing := ReconcilePreverifiedAgainstDisk(items, dir)
	require.Equal(t,
		[]string{"domain/v2.0-accounts.272-279.kv"},
		names(missing),
		"partial coverage (local 272-278 vs preverified 272-279) must NOT mask the missing wider entry")
}

// TestReconcileCoverageRespectsClass pins: a local file of a different
// type/extension does NOT cover a preverified entry. Same range but
// different domain (e.g., v2.0-storage vs v2.0-accounts) is independent
// coverage. Without this guard, the local accounts file would suppress
// a missing storage file.
func TestReconcileCoverageRespectsClass(t *testing.T) {
	dir := t.TempDir()
	domainDir := filepath.Join(dir, "domain")
	// Local has accounts data covering 272-280.
	touch(t, domainDir, "v2.0-accounts.272-280.kv")
	// Preverified asks for storage data at a covered range — must still
	// report missing because the local file is a DIFFERENT class.
	items := snapcfg.PreverifiedItems{
		{Name: "domain/v2.0-storage.272-276.kv", Hash: "h-storage"},
	}
	missing := ReconcilePreverifiedAgainstDisk(items, dir)
	require.Equal(t,
		[]string{"domain/v2.0-storage.272-276.kv"},
		names(missing),
		"a local accounts file must not be treated as coverage for a preverified storage entry")
}

// TestFilterPreverifiedBySubsumingLocal_HoodiIter2ModeBLayout pins the
// exact wedge from the 2026-07-01 iter-2 mode_b soak: hoodi's
// preverified list carries BOTH a broad 100k-block file and its
// constituent 10k + 1k narrower files under distinct torrent hashes.
// The header-chain OtterSync fallback re-requests every listed name
// individually — so when the broad is already on disk locally, the
// narrows must be dropped from the request queue. Without the filter,
// downloader accepts them all and retire's next merge feeds the
// union set into recsplit → duplicate tx hashes → runaway retry.
func TestFilterPreverifiedBySubsumingLocal_HoodiIter2ModeBLayout(t *testing.T) {
	dir := t.TempDir()
	// Broad 100k file locally (from initial fresh sync bootstrap).
	touch(t, dir, "v1.1-003000-003100-headers.seg")
	touch(t, dir, "v1.1-003000-003100-bodies.seg")
	touch(t, dir, "v1.1-003000-003100-transactions.seg")

	// Preverified advertises the broad AND overlapping narrow files.
	items := snapcfg.PreverifiedItems{
		{Name: "v1.1-003000-003100-headers.seg", Hash: "h-broad-headers"},
		{Name: "v1.1-003000-003100-bodies.seg", Hash: "h-broad-bodies"},
		{Name: "v1.1-003000-003100-transactions.seg", Hash: "h-broad-txs"},
		{Name: "v1.1-003000-003010-headers.seg", Hash: "h-narrow-10k-1"},
		{Name: "v1.1-003000-003010-bodies.seg", Hash: "h-narrow-10k-1b"},
		{Name: "v1.1-003060-003061-headers.seg", Hash: "h-narrow-1k-1"},
		{Name: "v1.1-003064-003065-headers.seg", Hash: "h-narrow-1k-2"},
		// Distinct-range entry NOT covered by the broad — must survive.
		{Name: "v1.1-003100-003110-headers.seg", Hash: "h-post-broad"},
	}

	filtered := FilterPreverifiedBySubsumingLocal(items, dir)

	got := make([]string, 0, len(filtered))
	for _, p := range filtered {
		got = append(got, p.Name)
	}
	sort.Strings(got)
	require.Equal(t,
		[]string{"v1.1-003100-003110-headers.seg"},
		got,
		"only entries neither already on disk nor subsumed by a wider local file survive; broad files self-subsume (on-disk) so they drop, narrow files are subsumed by the broad so they drop, and the disjoint post-broad file is the sole request")
}

// TestFilterPreverifiedBySubsumingLocal_NilOrEmpty defensively covers
// snapDir=="" (test/degenerate configs) and empty inputs.
func TestFilterPreverifiedBySubsumingLocal_NilOrEmpty(t *testing.T) {
	// snapDir empty: filter is a pass-through.
	items := snapcfg.PreverifiedItems{{Name: "v1.1-000000-000001-headers.seg", Hash: "h1"}}
	require.Equal(t, items, FilterPreverifiedBySubsumingLocal(items, ""))

	// Empty input: empty output.
	require.Empty(t, FilterPreverifiedBySubsumingLocal(nil, t.TempDir()))
}

// TestReconcileAllMissing: nothing on disk → every preverified entry
// reported as missing.
func TestReconcileAllMissing(t *testing.T) {
	dir := t.TempDir()
	items := snapcfg.PreverifiedItems{
		{Name: "v1.1-000000-000001-headers.seg", Hash: "h1"},
		{Name: "v1.1-000000-000001-bodies.seg", Hash: "h2"},
	}
	missing := ReconcilePreverifiedAgainstDisk(items, dir)
	require.Equal(t,
		[]string{
			"v1.1-000000-000001-bodies.seg",
			"v1.1-000000-000001-headers.seg",
		},
		names(missing))
}
