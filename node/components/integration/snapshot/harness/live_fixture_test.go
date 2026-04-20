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

package harness

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestLoadLiveFixture_ParsesRecognisedFiles writes a small handful of valid
// erigon snapshot file names into a tmpdir, invokes the loader, and verifies
// the returned Fixture has entries for each file with correct Domain / range.
func TestLoadLiveFixture_ParsesRecognisedFiles(t *testing.T) {
	dir := t.TempDir()

	files := []struct {
		relPath string
		bytes   int
	}{
		{"domain/v1.0-accounts.0-128.kv", 100},
		{"domain/v1.0-accounts.0-128.kvi", 20},
		{"domain/v1.0-storage.0-128.kv", 200},
		{"v1.0-000000-000500-headers.seg", 300},
		{"v1.0-000000-000500-bodies.seg", 400},

		// Unparseable: should be silently skipped.
		{"random-file.txt", 5},
		{"README.md", 10},
	}
	for _, f := range files {
		full := filepath.Join(dir, f.relPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, make([]byte, f.bytes), 0o644))
	}

	fixture, err := LoadLiveFixture("test", dir)
	require.NoError(t, err)
	require.Equal(t, "test", fixture.Name)

	require.Len(t, fixture.Domains[snapshot.DomainAccounts], 2, "accounts .kv + .kvi")
	require.Len(t, fixture.Domains[snapshot.DomainStorage], 1, "storage .kv")
	require.Len(t, fixture.Blocks, 2, "headers + bodies")

	// Sizes reflect actual disk stat'd values.
	for _, e := range fixture.Domains[snapshot.DomainAccounts] {
		switch e.Name {
		case "v1.0-accounts.0-128.kv":
			require.Equal(t, int64(100), e.Size)
		case "v1.0-accounts.0-128.kvi":
			require.Equal(t, int64(20), e.Size)
		default:
			t.Errorf("unexpected accounts file %q", e.Name)
		}
	}

	// Step range extracted correctly.
	for _, e := range fixture.Domains[snapshot.DomainAccounts] {
		require.Equal(t, uint64(0), e.FromStep)
		require.Equal(t, uint64(128), e.ToStep)
	}
}

// TestLoadLiveFixture_EmptyDirError reports a clear error when no recognised
// files are found, rather than returning an empty fixture silently.
func TestLoadLiveFixture_EmptyDirError(t *testing.T) {
	dir := t.TempDir()

	_, err := LoadLiveFixture("test", dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no parseable snapshot files")
}

// TestLoadLiveFixture_NonExistentDirError surfaces a meaningful error for a
// missing path rather than a nil fixture.
func TestLoadLiveFixture_NonExistentDirError(t *testing.T) {
	_, err := LoadLiveFixture("test", filepath.Join(t.TempDir(), "does-not-exist"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "stat")
}

// TestLoadLiveFixture_NotADirectoryError errors when the path is a file.
func TestLoadLiveFixture_NotADirectoryError(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "a-file")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0o644))

	_, err := LoadLiveFixture("test", filePath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a directory")
}
