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

package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMoveFileAcrossFS covers the same-filesystem path: a plain rename
// moves the bytes and removes the source. (The EXDEV copy fallback
// needs two real filesystems and is exercised by the harness.)
func TestMoveFileAcrossFS(t *testing.T) {
	t.Parallel()
	d := t.TempDir()
	src := filepath.Join(d, "src.kv")
	dst := filepath.Join(d, "sub", "dst.kv")
	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
	require.NoError(t, os.WriteFile(src, []byte("canonical-bytes"), 0o644))

	require.NoError(t, MoveFileAcrossFS(src, dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, "canonical-bytes", string(got))
	_, statErr := os.Stat(src)
	require.True(t, os.IsNotExist(statErr), "source must be gone after the move")

	require.Error(t, MoveFileAcrossFS(filepath.Join(d, "missing.kv"), dst))
}

// TestCutoverStagedDir drives the stopped-node cutover: staged files
// land at their kind-subdir live locations, a superseded file's stale
// .torrent sidecar is removed, and the staging directory is cleaned
// up. The dry run reports the same set without moving anything.
func TestCutoverStagedDir(t *testing.T) {
	t.Parallel()
	liveDir := t.TempDir()
	stagingDir := filepath.Join(t.TempDir(), "adoption-5")
	require.NoError(t, os.MkdirAll(stagingDir, 0o755))

	blockName := "v1.0-000000-000500-headers.seg" // top-level layout
	domainName := "v1.0-accounts.0-2048.kv"       // domain/ subdir
	require.NoError(t, os.WriteFile(filepath.Join(stagingDir, blockName), []byte("canon-block"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(stagingDir, domainName), []byte("canon-domain"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(stagingDir, AdoptionReadyMarker), nil, 0o644))

	// A superseded live block file plus its now-stale .torrent sidecar.
	require.NoError(t, os.WriteFile(PathForName(liveDir, blockName), []byte("minority"), 0o644))
	require.NoError(t, os.WriteFile(PathForName(liveDir, blockName)+".torrent", []byte("stale"), 0o644))

	files, err := CutoverStagedDir(liveDir, stagingDir, true /* dryRun */, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{blockName, domainName}, files)
	require.FileExists(t, filepath.Join(stagingDir, blockName), "dry run must not move anything")

	files, err = CutoverStagedDir(liveDir, stagingDir, false, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{blockName, domainName}, files)

	gotBlock, err := os.ReadFile(PathForName(liveDir, blockName))
	require.NoError(t, err)
	require.Equal(t, "canon-block", string(gotBlock))

	gotDomain, err := os.ReadFile(PathForName(liveDir, domainName))
	require.NoError(t, err)
	require.Equal(t, "canon-domain", string(gotDomain))

	_, statErr := os.Stat(PathForName(liveDir, blockName) + ".torrent")
	require.True(t, os.IsNotExist(statErr), "stale .torrent sidecar must be removed")

	_, statErr = os.Stat(PathForName(liveDir, AdoptionReadyMarker))
	require.True(t, os.IsNotExist(statErr), "the ready marker must not be cut over as a snapshot file")

	_, statErr = os.Stat(stagingDir)
	require.True(t, os.IsNotExist(statErr), "staging dir must be removed after a real cutover")
}

// TestRecoverStagedAdoptions covers the startup auto-recovery scan: a
// marked batch is cut over, an unmarked (interrupted) batch is left
// alone, a non-adoption temp directory is ignored, and a missing temp
// directory is not an error.
func TestRecoverStagedAdoptions(t *testing.T) {
	t.Parallel()

	// Missing temp dir → nothing staged, no error.
	got, err := RecoverStagedAdoptions(t.TempDir(), filepath.Join(t.TempDir(), "absent"), false, nil)
	require.NoError(t, err)
	require.Empty(t, got)

	liveDir := t.TempDir()
	tmpDir := t.TempDir()

	// A marked, validated batch — must be cut over.
	marked := filepath.Join(tmpDir, "adoption-7")
	require.NoError(t, os.MkdirAll(marked, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(marked, "v1.0-accounts.0-2048.kv"), []byte("canon"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(marked, AdoptionReadyMarker), nil, 0o644))

	// An unmarked batch — an interrupted fetch, must be left alone.
	unmarked := filepath.Join(tmpDir, "adoption-8")
	require.NoError(t, os.MkdirAll(unmarked, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(unmarked, "v1.0-storage.0-2048.kv"), []byte("partial"), 0o644))

	// A non-adoption temp directory — must be ignored.
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "something-else"), 0o755))

	recovered, err := RecoverStagedAdoptions(liveDir, tmpDir, false, nil)
	require.NoError(t, err)
	require.Len(t, recovered, 1, "only the marked batch is recovered")
	require.Equal(t, "adoption-7", recovered[0].Name)
	require.ElementsMatch(t, []string{"v1.0-accounts.0-2048.kv"}, recovered[0].Files)

	// The marked batch's file landed live; its staging dir is gone.
	gotFile, err := os.ReadFile(PathForName(liveDir, "v1.0-accounts.0-2048.kv"))
	require.NoError(t, err)
	require.Equal(t, "canon", string(gotFile))
	_, statErr := os.Stat(marked)
	require.True(t, os.IsNotExist(statErr), "recovered staging dir must be removed")

	// The unmarked batch is untouched — file still staged, dir present.
	require.FileExists(t, filepath.Join(unmarked, "v1.0-storage.0-2048.kv"))

	// A dry run reports the unmarked-skip-aside set without moving it.
	require.NoError(t, os.WriteFile(filepath.Join(unmarked, AdoptionReadyMarker), nil, 0o644))
	dry, err := RecoverStagedAdoptions(liveDir, tmpDir, true /* dryRun */, nil)
	require.NoError(t, err)
	require.Len(t, dry, 1)
	require.Equal(t, "adoption-8", dry[0].Name)
	require.FileExists(t, filepath.Join(unmarked, "v1.0-storage.0-2048.kv"), "dry run must not move anything")
}
