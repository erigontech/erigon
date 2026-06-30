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

// TestProvider_RecoverOrphanRegenSidecars_NoOrphans confirms the
// recovery sweep is a no-op when nothing's on disk to clean up.
func TestProvider_RecoverOrphanRegenSidecars_NoOrphans(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	p := &Provider{snapDir: tmpDir}
	p.recoverOrphanRegenSidecars() // must not panic or error
}

// TestProvider_RecoverOrphanRegenSidecars_RegenOrphan covers the case
// where a mid-Provider.Unwind crash left a .regen scratch file behind
// without ever reaching FinalizeUnwind / AbortUnwind. The .regen is
// unconditionally removed; next mode-B will write a fresh one.
func TestProvider_RecoverOrphanRegenSidecars_RegenOrphan(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	regenPath := filepath.Join(tmpDir, "v1.1-accounts.272-278.kv.regen")
	require.NoError(t, os.WriteFile(regenPath, []byte("incomplete regen content"), 0o600))

	p := &Provider{snapDir: tmpDir}
	p.recoverOrphanRegenSidecars()

	_, err := os.Stat(regenPath)
	require.True(t, os.IsNotExist(err), ".regen orphan must be removed at startup")
}

// TestProvider_RecoverOrphanRegenSidecars_OldFinalizeCompleted covers
// the case where FinalizeUnwind's swap completed (regen content is
// in place at .kv) but the .old unlink at the end of the block
// crashed / was interrupted. Recovery: drop the .old; the .kv is
// canonical.
func TestProvider_RecoverOrphanRegenSidecars_OldFinalizeCompleted(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	kvPath := filepath.Join(tmpDir, "v1.1-accounts.272-280.kv")
	oldPath := kvPath + ".old"
	require.NoError(t, os.WriteFile(kvPath, []byte("post-finalize regen-content"), 0o600))
	require.NoError(t, os.WriteFile(oldPath, []byte("pre-finalize broad content"), 0o600))

	p := &Provider{snapDir: tmpDir}
	p.recoverOrphanRegenSidecars()

	_, err := os.Stat(oldPath)
	require.True(t, os.IsNotExist(err), ".old must be removed when corresponding .kv exists (finalize completed)")
	kvContents, err := os.ReadFile(kvPath)
	require.NoError(t, err, ".kv must be preserved unchanged")
	require.Equal(t, "post-finalize regen-content", string(kvContents))
}

// TestProvider_RecoverOrphanRegenSidecars_OldRestoresBroad covers the
// case where FinalizeUnwind crashed AFTER renaming broad→.old but
// BEFORE renaming regen→truncated.kv (or before the truncated.kv
// landed under a different name). The original .kv is missing; the
// broad is preserved only in the .old. Recovery: restore by renaming
// .old back to the original path. The next mode-B will redo the
// regen.
func TestProvider_RecoverOrphanRegenSidecars_OldRestoresBroad(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	kvPath := filepath.Join(tmpDir, "v1.1-accounts.272-280.kv")
	oldPath := kvPath + ".old"
	require.NoError(t, os.WriteFile(oldPath, []byte("pre-finalize broad content"), 0o600))

	p := &Provider{snapDir: tmpDir}
	p.recoverOrphanRegenSidecars()

	_, err := os.Stat(oldPath)
	require.True(t, os.IsNotExist(err), ".old must be consumed (renamed back to .kv)")
	kvContents, err := os.ReadFile(kvPath)
	require.NoError(t, err, "original .kv must be restored from .old")
	require.Equal(t, "pre-finalize broad content", string(kvContents))
}

// TestProvider_RecoverOrphanRegenSidecars_BothOrphansPresent covers
// the case where BOTH a .regen and a .old exist on disk simultaneously
// — e.g. a crash mid-FinalizeUnwind between the broad→.old rename
// and the regen→truncated rename, AND the truncated.kv hadn't landed
// yet. The .regen is dropped (it's incomplete state); the .old is
// restored to its original .kv (pre-mode-B state).
func TestProvider_RecoverOrphanRegenSidecars_BothOrphansPresent(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	broadKV := filepath.Join(tmpDir, "v1.1-accounts.272-280.kv")
	broadOld := broadKV + ".old"
	truncatedRegen := filepath.Join(tmpDir, "v1.1-accounts.272-278.kv.regen")

	require.NoError(t, os.WriteFile(broadOld, []byte("pre-finalize broad content"), 0o600))
	require.NoError(t, os.WriteFile(truncatedRegen, []byte("partial regen content"), 0o600))

	p := &Provider{snapDir: tmpDir}
	p.recoverOrphanRegenSidecars()

	_, err := os.Stat(truncatedRegen)
	require.True(t, os.IsNotExist(err), ".regen must be removed")
	_, err = os.Stat(broadOld)
	require.True(t, os.IsNotExist(err), ".old must be consumed (renamed back to .kv)")
	kvContents, err := os.ReadFile(broadKV)
	require.NoError(t, err, "broad .kv must be restored")
	require.Equal(t, "pre-finalize broad content", string(kvContents))
}

// TestProvider_RecoverOrphanRegenSidecars_SubdirSweep confirms the
// sweep also covers per-kind subdirs (domain/, history/, idx/,
// accessor/) where state-domain files actually live in production.
// The top-level sweep would miss them without explicit walk.
func TestProvider_RecoverOrphanRegenSidecars_SubdirSweep(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	domainDir := filepath.Join(tmpDir, "domain")
	require.NoError(t, os.MkdirAll(domainDir, 0o755))
	regenPath := filepath.Join(domainDir, "v1.1-accounts.272-278.kv.regen")
	require.NoError(t, os.WriteFile(regenPath, []byte("subdir regen orphan"), 0o600))

	p := &Provider{snapDir: tmpDir}
	p.recoverOrphanRegenSidecars()

	_, err := os.Stat(regenPath)
	require.True(t, os.IsNotExist(err), "subdir .regen orphans must also be removed")
}
