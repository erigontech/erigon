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
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/dbservices"
	downloaderproto "github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

// recordingDownloaderClient is the test stub for dbservices.DownloaderClient.
// It records every Delete call so a test can assert that the regen
// path notified the downloader to drop the regenerated file from its
// torrent set.
type recordingDownloaderClient struct {
	mu      sync.Mutex
	deletes [][]string
}

var _ dbservices.DownloaderClient = (*recordingDownloaderClient)(nil)

func (c *recordingDownloaderClient) Seed(_ context.Context, _ []string) error { return nil }

func (c *recordingDownloaderClient) Delete(_ context.Context, paths []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes = append(c.deletes, append([]string(nil), paths...))
	return nil
}

func (c *recordingDownloaderClient) Download(_ context.Context, _ *downloaderproto.DownloadRequest) error {
	return nil
}

func (c *recordingDownloaderClient) snapshotDeletes() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]string, len(c.deletes))
	for i, d := range c.deletes {
		out[i] = append([]string(nil), d...)
	}
	return out
}

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

// TestProvider_FinalizeUnwind_RegenStripsTorrentAndNotifiesDownloader
// pins the cleanup the iter-3 soak wedge surfaced: when mode-B's
// boundary-step regen rewrites a .kv, the stale .torrent sidecar must
// be unlinked AND the downloader must be told via Delete so it stops
// trying to re-fetch the original-hashed content (which it would
// otherwise rename to .kv.part, leaving the rebuilt .kvi accessor
// pointing at a missing file and the next process restart panicking
// in decompress.go).
func TestProvider_FinalizeUnwind_RegenStripsTorrentAndNotifiesDownloader(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	finalName := "v2.1-commitment.272-280.kv"
	finalPath := filepath.Join(tmpDir, finalName)
	regenPath := finalPath + ".regen"
	torrentPath := finalPath + ".torrent"

	require.NoError(t, os.WriteFile(finalPath, []byte("pre-regen"), 0o600))
	require.NoError(t, os.WriteFile(regenPath, []byte("regen-content"), 0o600))
	require.NoError(t, os.WriteFile(torrentPath, []byte("stale-torrent"), 0o600))

	stub := &recordingDownloaderClient{}
	p := &Provider{
		snapDir:          tmpDir,
		downloaderClient: stub,
	}
	p.pendingRegen = &pendingRegenState{
		pairs: []regenPair{{
			regenPath:    regenPath,
			finalPath:    finalPath,
			oldBroadPath: finalPath, // aligned case: regen overwrites in place
		}},
	}

	require.NoError(t, p.FinalizeUnwind())

	contents, err := os.ReadFile(finalPath)
	require.NoError(t, err, "regen .kv must be in place after rename")
	require.Equal(t, "regen-content", string(contents), "finalPath must hold the regenerated bytes")

	_, err = os.Stat(torrentPath)
	require.True(t, os.IsNotExist(err), "stale .torrent sidecar must be removed so the downloader stops policing the regenerated .kv")

	deletes := stub.snapshotDeletes()
	require.Len(t, deletes, 1, "downloaderClient.Delete must be called exactly once for the regen batch")
	require.Equal(t, []string{finalName}, deletes[0], "Delete must carry the basename of the regenerated .kv")

	require.Nil(t, p.pendingRegen, "FinalizeUnwind must drain pendingRegen")
}

// TestProvider_FinalizeUnwind_RemovesEntirelyPastFiles is the load-
// bearing integration test for the post-iter-3-mode_b fix. State-
// domain .kv files entirely past the unwind boundary (per
// planStateFileActions's actionRemove classification) MUST be unlinked
// by FinalizeUnwind alongside their accessors + .torrent sidecar +
// Inventory entry. Pre-fix, these files persisted on disk and served
// stale post-boundary state, producing the ~4,800-gas mismatch at
// block 3,091,971 we caught on hoodi.
//
// Fixture: three .kv files staged for removal (mimicking the on-disk
// shape of accounts.278-279, 280-282, 280-284 from the iter-3 wedge).
// Each with a fake .torrent sidecar + accessor (.bt) to exercise the
// full cleanup path. After FinalizeUnwind:
//   - The .kv files must be gone.
//   - Their .torrent sidecars must be gone.
//   - The .bt accessor files must be gone.
//   - The downloader Delete batch must include all three basenames.
//   - pendingRegen must be drained.
func TestProvider_FinalizeUnwind_RemovesEntirelyPastFiles(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	type pastFile struct {
		name        string
		path        string
		torrentPath string
		btPath      string
	}
	pastFiles := []pastFile{
		{name: "v1.1-accounts.278-279.kv"},
		{name: "v1.1-accounts.280-282.kv"},
		{name: "v1.1-accounts.280-284.kv"},
	}
	for i := range pastFiles {
		pastFiles[i].path = filepath.Join(tmpDir, pastFiles[i].name)
		pastFiles[i].torrentPath = pastFiles[i].path + ".torrent"
		pastFiles[i].btPath = strings.TrimSuffix(pastFiles[i].path, ".kv") + ".bt"
		require.NoError(t, os.WriteFile(pastFiles[i].path, []byte("stale past-boundary content"), 0o600))
		require.NoError(t, os.WriteFile(pastFiles[i].torrentPath, []byte("stale torrent"), 0o600))
		require.NoError(t, os.WriteFile(pastFiles[i].btPath, []byte("stale accessor"), 0o600))
	}

	stub := &recordingDownloaderClient{}
	p := &Provider{
		snapDir:          tmpDir,
		downloaderClient: stub,
	}
	removals := make([]removalEntry, 0, len(pastFiles))
	for _, f := range pastFiles {
		removals = append(removals, removalEntry{
			path: f.path,
			name: f.name,
		})
	}
	p.pendingRegen = &pendingRegenState{removals: removals}

	require.NoError(t, p.FinalizeUnwind())

	for _, f := range pastFiles {
		_, err := os.Stat(f.path)
		require.True(t, os.IsNotExist(err), "past-boundary .kv must be removed: %s", f.path)
		_, err = os.Stat(f.torrentPath)
		require.True(t, os.IsNotExist(err), "past-boundary .torrent must be removed: %s", f.torrentPath)
		_, err = os.Stat(f.btPath)
		require.True(t, os.IsNotExist(err), "past-boundary .bt accessor must be removed: %s", f.btPath)
	}

	deletes := stub.snapshotDeletes()
	require.Len(t, deletes, 1, "downloaderClient.Delete must be called exactly once for the removal batch")
	wantNames := []string{
		"v1.1-accounts.278-279.kv",
		"v1.1-accounts.280-282.kv",
		"v1.1-accounts.280-284.kv",
	}
	require.ElementsMatch(t, wantNames, deletes[0],
		"Delete batch must cover every past-boundary basename so any in-flight torrent is cancelled")

	require.Nil(t, p.pendingRegen, "FinalizeUnwind must drain pendingRegen")
}

// TestProvider_FinalizeUnwind_RegenAndRemovalsTogether covers the
// composite case the iter-3 mode_b layout produces: BOTH a regen
// straddler (with truncation) AND multiple files entirely past the
// boundary, all in the same FinalizeUnwind call. The two paths must
// compose cleanly — both must finish, regardless of order.
func TestProvider_FinalizeUnwind_RegenAndRemovalsTogether(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Straddler that gets regen+truncate (272-280 → 272-278).
	broadPath := filepath.Join(tmpDir, "v1.1-accounts.272-280.kv")
	truncPath := filepath.Join(tmpDir, "v1.1-accounts.272-278.kv")
	regenPath := truncPath + ".regen"
	require.NoError(t, os.WriteFile(broadPath, []byte("pre-regen broad"), 0o600))
	require.NoError(t, os.WriteFile(regenPath, []byte("regen truncated"), 0o600))

	// Past-boundary file to remove.
	pastPath := filepath.Join(tmpDir, "v1.1-accounts.280-282.kv")
	require.NoError(t, os.WriteFile(pastPath, []byte("stale past"), 0o600))

	stub := &recordingDownloaderClient{}
	p := &Provider{
		snapDir:          tmpDir,
		downloaderClient: stub,
	}
	p.pendingRegen = &pendingRegenState{
		pairs: []regenPair{{
			regenPath:    regenPath,
			finalPath:    truncPath,
			oldBroadPath: broadPath,
		}},
		removals: []removalEntry{{
			path: pastPath,
			name: "v1.1-accounts.280-282.kv",
		}},
	}

	require.NoError(t, p.FinalizeUnwind())

	// Truncated regen output landed:
	contents, err := os.ReadFile(truncPath)
	require.NoError(t, err)
	require.Equal(t, "regen truncated", string(contents))

	// Broad file removed:
	_, err = os.Stat(broadPath)
	require.True(t, os.IsNotExist(err), "broad straddler must be removed")

	// Past-boundary file removed:
	_, err = os.Stat(pastPath)
	require.True(t, os.IsNotExist(err), "past-boundary file must be removed")

	require.Nil(t, p.pendingRegen, "FinalizeUnwind must drain pendingRegen")
}

// TestProvider_FinalizeUnwind_RegenTruncatedRenameRemovesBroadFile
// pins the truncated-rename path that the 2026-06-30 iter-4 mode-B
// soak surfaced: when the boundary file's ToStep extends past the
// unwind-target step boundary, the regen output is written under a
// truncated filename (e.g. v1.1-accounts.272-280.kv.regen →
// v1.1-accounts.272-278.kv), and FinalizeUnwind must
//
//   - move the regen content to the truncated final path
//   - remove the original broad .kv (it now over-claims coverage
//     for steps the regen didn't write)
//   - drop the broad's .torrent + Inventory entry
//   - issue downloader Delete for the broad basename so any
//     in-flight fetch for the retired file gets cancelled
//
// Without this, the broad and truncated files co-exist and the
// fileset rule's default direction (M-A: narrower loses) picks the
// broad — serving stale state for the truncated portion and wedging
// exec at the next block that reads from that range.
func TestProvider_FinalizeUnwind_RegenTruncatedRenameRemovesBroadFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	broadName := "v1.1-accounts.272-280.kv"
	truncatedName := "v1.1-accounts.272-278.kv"
	broadPath := filepath.Join(tmpDir, broadName)
	finalPath := filepath.Join(tmpDir, truncatedName)
	regenPath := finalPath + ".regen"
	broadTorrent := broadPath + ".torrent"

	require.NoError(t, os.WriteFile(broadPath, []byte("pre-regen-broad"), 0o600))
	require.NoError(t, os.WriteFile(regenPath, []byte("regen-truncated-content"), 0o600))
	require.NoError(t, os.WriteFile(broadTorrent, []byte("stale-broad-torrent"), 0o600))

	stub := &recordingDownloaderClient{}
	p := &Provider{
		snapDir:          tmpDir,
		downloaderClient: stub,
	}
	p.pendingRegen = &pendingRegenState{
		pairs: []regenPair{{
			regenPath:    regenPath,
			finalPath:    finalPath,
			oldBroadPath: broadPath,
		}},
	}

	require.NoError(t, p.FinalizeUnwind())

	contents, err := os.ReadFile(finalPath)
	require.NoError(t, err, "truncated .kv must exist post-finalize")
	require.Equal(t, "regen-truncated-content", string(contents))

	_, err = os.Stat(broadPath)
	require.True(t, os.IsNotExist(err), "broad .kv must be removed (its content is superseded by the truncated regen)")
	_, err = os.Stat(broadTorrent)
	require.True(t, os.IsNotExist(err), "broad .torrent must be removed (advertises the retired file)")

	deletes := stub.snapshotDeletes()
	require.Len(t, deletes, 1, "downloaderClient.Delete must be called once for the regen batch")
	require.ElementsMatch(t, []string{truncatedName, broadName}, deletes[0],
		"Delete must carry BOTH the new truncated name and the retired broad name")

	require.Nil(t, p.pendingRegen, "FinalizeUnwind must drain pendingRegen")
}
