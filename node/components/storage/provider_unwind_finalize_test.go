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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// stageOneFile is a test helper that creates `name` on disk under
// `dir`, then stages it on the Provider's pendingTrim list (mirroring
// what unwindSnapshotsPastBlock does at the end of Provider.Unwind).
func stageOneFile(t *testing.T, p *Provider, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("test contents"), 0o600))
	p.pendingTrim = &pendingTrimState{
		names: []string{name},
		paths: []string{path},
	}
	return path
}

// TestProvider_AbortUnwind_LeavesFSUnchanged pins the W3.11 core
// contract: when a mode-B attempt errors out before tx.Commit,
// AbortUnwind drops the staged trim ops without touching the
// filesystem. The datadir is unchanged and retriable.
//
// Without W3.11 the old inline FS deletes ran during Provider.Unwind;
// a downstream failure (ensureCommitmentAtBlock, WipeWritableShadowPast,
// tx.Commit) left the deleted files gone even though the DB tx rolled
// back — making the datadir permanently inconsistent.
func TestProvider_AbortUnwind_LeavesFSUnchanged(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	p := &Provider{snapDir: tmpDir}

	path := stageOneFile(t, p, tmpDir, "accounts.0-128.kv")
	require.FileExists(t, path, "stub file must exist before Abort")

	p.AbortUnwind()

	require.FileExists(t, path, "AbortUnwind must NOT delete staged files — the rolled-back tx leaves the datadir unchanged")
	require.Nil(t, p.pendingTrim, "AbortUnwind must drop the staged list")
}

// TestProvider_FinalizeUnwind_DeletesStagedFiles pins the happy
// path: after tx.Commit succeeds, FinalizeUnwind executes the
// deferred FS deletions.
func TestProvider_FinalizeUnwind_DeletesStagedFiles(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	p := &Provider{snapDir: tmpDir}

	path := stageOneFile(t, p, tmpDir, "accounts.0-128.kv")
	require.FileExists(t, path)

	require.NoError(t, p.FinalizeUnwind())

	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "FinalizeUnwind must delete staged files post-commit")
	require.Nil(t, p.pendingTrim, "FinalizeUnwind must drain the staged list")
}

// TestProvider_FinalizeUnwind_NothingStaged pins that calling
// FinalizeUnwind with an empty stage is a safe no-op — covers the
// Provider-with-no-Inventory path in setHeadModeB where Unwind
// short-circuits and stages nothing.
func TestProvider_FinalizeUnwind_NothingStaged(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	require.NoError(t, p.FinalizeUnwind())
	require.Nil(t, p.pendingTrim)
}

// TestProvider_AbortUnwind_NothingStaged pins symmetric no-op
// behavior for AbortUnwind.
func TestProvider_AbortUnwind_NothingStaged(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	p.AbortUnwind()
	require.Nil(t, p.pendingTrim)
}
