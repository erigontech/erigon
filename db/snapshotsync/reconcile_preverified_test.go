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
