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

// TestProvider_FinalizeUnwind_StagedTrimRemovesAccessorSiblings pins
// the follow-up-3 fix: the staged (pendingTrim) path removes the .kv
// AND every accessor sibling (.bt / .kvi / .kvei plus their .torrent
// sidecars). Pre-fix, only .kv + .kv.torrent were unlinked, leaving
// accessor siblings orphaned on disk. Mode-C's snapshot-trim of files
// past the target step exhibited this at scale: leg P v5 iter 5 left
// 45 orphaned accessors across step ranges 296-300 and 300-302.
//
// Orphaned accessors are more than clutter: a subsequent retire round
// re-emits a .kv at the same step range with different content; the
// stale .bt / .kvi / .kvei point at the old file's byte offsets and,
// once the aggregator's mmap references them, produce wrong reads.
func TestProvider_FinalizeUnwind_StagedTrimRemovesAccessorSiblings(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	kvName := "v2.2-accounts.296-300.kv"
	kvPath := filepath.Join(tmpDir, kvName)
	torrentPath := kvPath + ".torrent"
	prefix := strings.TrimSuffix(kvPath, ".kv")
	btPath := prefix + ".bt"
	kviPath := prefix + ".kvi"
	kveiPath := prefix + ".kvei"
	btTorrentPath := btPath + ".torrent"
	kviTorrentPath := kviPath + ".torrent"

	for _, path := range []string{kvPath, torrentPath, btPath, kviPath, kveiPath, btTorrentPath, kviTorrentPath} {
		require.NoError(t, os.WriteFile(path, []byte("test"), 0o600))
	}

	stub := &recordingDownloaderClient{}
	p := &Provider{snapDir: tmpDir, downloaderClient: stub}
	p.pendingTrim = &pendingTrimState{
		names: []string{kvName},
		paths: []string{kvPath},
	}

	require.NoError(t, p.FinalizeUnwind())

	for _, path := range []string{kvPath, torrentPath, btPath, kviPath, kveiPath, btTorrentPath, kviTorrentPath} {
		_, err := os.Stat(path)
		require.True(t, os.IsNotExist(err),
			"staged-trim must unlink accessor sibling: %s", filepath.Base(path))
	}

	deletes := stub.snapshotDeletes()
	require.Len(t, deletes, 1, "downloaderClient.Delete must fire once")
	require.Contains(t, deletes[0], kvName,
		"Delete batch must announce the .kv basename")
	require.Contains(t, deletes[0], filepath.Base(btPath),
		"Delete batch must announce accessor basenames so in-flight torrents are cancelled")
	require.Contains(t, deletes[0], filepath.Base(kviPath))
	require.Contains(t, deletes[0], filepath.Base(kveiPath))
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

// TestProvider_AbortUnwind_UnlinksHistoryRegenFiles pins that on
// rollback, historyRegenPair .regen files (the paired v4 .v / .ef
// mode-C emits) are unlinked just like the regular .kv regens. Without
// this the pre-mode-B datadir has orphaned .v.regen / .ef.regen
// leftovers that a subsequent Provider.Unwind attempt would trip over.
func TestProvider_AbortUnwind_UnlinksHistoryRegenFiles(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	efRegen := filepath.Join(tmpDir, "v4.0-accounts.100-200.ef.regen")
	vRegen := filepath.Join(tmpDir, "v4.0-accounts.100-200.v.regen")
	require.NoError(t, os.WriteFile(efRegen, []byte("ef-regen"), 0o600))
	require.NoError(t, os.WriteFile(vRegen, []byte("v-regen"), 0o600))

	p := &Provider{snapDir: tmpDir}
	p.pendingRegen = &pendingRegenState{
		historyPairs: []historyRegenPair{{
			efRegenPath: efRegen,
			vRegenPath:  vRegen,
		}},
	}

	p.AbortUnwind()

	_, err := os.Stat(efRegen)
	require.True(t, os.IsNotExist(err), "AbortUnwind must unlink .ef.regen")
	_, err = os.Stat(vRegen)
	require.True(t, os.IsNotExist(err), "AbortUnwind must unlink .v.regen")
	require.Nil(t, p.pendingRegen, "AbortUnwind must drain pendingRegen")
}

// TestProvider_FinalizeUnwind_HistoryPairSwapAndStraddlerRemoval pins
// the FS layer of stage 5. Given a staged historyRegenPair pointing at
// a straddler .v/.ef pair and their .regen replacements, FinalizeUnwind
// must:
//
//  1. Rename the .regen files into their final v4-named locations.
//  2. Remove the straddler .v/.ef files (they were the .old sidecars).
//  3. Remove the .torrent sidecars for the straddlers.
//
// Accessor building (BuildHistoryAccessors / BuildIndexAccessors) is
// exercised end-to-end by TestFinalizeUnwind_RenamesPairedHistoryV4 in
// the integration test suite — this unit test uses a nil Aggregator so
// the FS-only behaviour is isolated from the aggregator's real
// side-effects.
func TestProvider_FinalizeUnwind_HistoryPairSwapAndStraddlerRemoval(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Straddler on disk under history/ and idx/ subdirs.
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "history"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "idx"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "accessor"), 0o755))

	efOldName := "idx/v3.1-accounts.310-311.ef"
	efOldPath := filepath.Join(tmpDir, efOldName)
	efOldTorrent := efOldPath + ".torrent"
	vOldName := "history/v2.1-accounts.310-311.v"
	vOldPath := filepath.Join(tmpDir, vOldName)
	vOldTorrent := vOldPath + ".torrent"
	require.NoError(t, os.WriteFile(efOldPath, []byte("old-ef"), 0o600))
	require.NoError(t, os.WriteFile(efOldTorrent, []byte("stale-ef-torrent"), 0o600))
	require.NoError(t, os.WriteFile(vOldPath, []byte("old-v"), 0o600))
	require.NoError(t, os.WriteFile(vOldTorrent, []byte("stale-v-torrent"), 0o600))

	// Regen counterparts.
	efFinalPath := filepath.Join(tmpDir, "idx", "v4.0-accounts.121093750-121175226.ef")
	vFinalPath := filepath.Join(tmpDir, "history", "v4.0-accounts.121093750-121175226.v")
	efRegenPath := efFinalPath + ".regen"
	vRegenPath := vFinalPath + ".regen"
	require.NoError(t, os.WriteFile(efRegenPath, []byte("regen-ef"), 0o600))
	require.NoError(t, os.WriteFile(vRegenPath, []byte("regen-v"), 0o600))

	stub := &recordingDownloaderClient{}
	p := &Provider{
		snapDir:          tmpDir,
		downloaderClient: stub,
	}
	p.pendingRegen = &pendingRegenState{
		historyPairs: []historyRegenPair{{
			efRegenPath: efRegenPath,
			efFinalPath: efFinalPath,
			efOldName:   efOldName,
			efOldPath:   efOldPath,
			vRegenPath:  vRegenPath,
			vFinalPath:  vFinalPath,
			vOldName:    vOldName,
			vOldPath:    vOldPath,
		}},
	}

	require.NoError(t, p.FinalizeUnwind())

	// Final v4 pair in place.
	efFinal, err := os.ReadFile(efFinalPath)
	require.NoError(t, err, "regen .ef must be promoted to final")
	require.Equal(t, "regen-ef", string(efFinal))
	vFinal, err := os.ReadFile(vFinalPath)
	require.NoError(t, err, "regen .v must be promoted to final")
	require.Equal(t, "regen-v", string(vFinal))

	// Straddlers gone (renamed to .old, then unlinked).
	_, err = os.Stat(efOldPath)
	require.True(t, os.IsNotExist(err), "straddler .ef must be removed")
	_, err = os.Stat(vOldPath)
	require.True(t, os.IsNotExist(err), "straddler .v must be removed")
	_, err = os.Stat(efOldPath + ".old")
	require.True(t, os.IsNotExist(err), ".ef.old sidecar must be unlinked")
	_, err = os.Stat(vOldPath + ".old")
	require.True(t, os.IsNotExist(err), ".v.old sidecar must be unlinked")

	// Straddler .torrent sidecars removed.
	_, err = os.Stat(efOldTorrent)
	require.True(t, os.IsNotExist(err), "stale .ef.torrent must be removed")
	_, err = os.Stat(vOldTorrent)
	require.True(t, os.IsNotExist(err), "stale .v.torrent must be removed")

	// Downloader Delete carries both new and old basenames.
	deletes := stub.snapshotDeletes()
	require.Len(t, deletes, 1)
	// Order isn't asserted — we just need all four basenames present.
	require.ElementsMatch(t, []string{
		filepath.Base(efFinalPath),
		filepath.Base(vFinalPath),
		filepath.Base(efOldPath),
		filepath.Base(vOldPath),
	}, deletes[0])

	require.Nil(t, p.pendingRegen, "FinalizeUnwind must drain pendingRegen")
}

// TestProvider_FinalizeUnwind_HistoryPairWithoutStraddler pins the
// no-straddler skip: when historyPairs.efOldPath / vOldPath are empty
// (fresh sync hasn't retired that step yet), FinalizeUnwind lands the
// new v4 .v/.ef without trying to rename absent straddlers.
func TestProvider_FinalizeUnwind_HistoryPairWithoutStraddler(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "history"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "idx"), 0o755))

	efFinalPath := filepath.Join(tmpDir, "idx", "v4.0-accounts.100-200.ef")
	vFinalPath := filepath.Join(tmpDir, "history", "v4.0-accounts.100-200.v")
	efRegenPath := efFinalPath + ".regen"
	vRegenPath := vFinalPath + ".regen"
	require.NoError(t, os.WriteFile(efRegenPath, []byte("regen-ef"), 0o600))
	require.NoError(t, os.WriteFile(vRegenPath, []byte("regen-v"), 0o600))

	p := &Provider{snapDir: tmpDir}
	p.pendingRegen = &pendingRegenState{
		historyPairs: []historyRegenPair{{
			efRegenPath: efRegenPath,
			efFinalPath: efFinalPath,
			vRegenPath:  vRegenPath,
			vFinalPath:  vFinalPath,
			// no efOldPath, no vOldPath
		}},
	}

	require.NoError(t, p.FinalizeUnwind())

	_, err := os.Stat(efFinalPath)
	require.NoError(t, err, "regen .ef promoted to final")
	_, err = os.Stat(vFinalPath)
	require.NoError(t, err, "regen .v promoted to final")
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

	// Every past-boundary .kv comes with the full accessor family that
	// a real state-domain file has on disk (.bt / .kvi / .kvei) plus a
	// .torrent sidecar for each. All of them must be removed together —
	// leaving any accessor .torrent behind produces the orphan class the
	// datadir-consistency check catches at Phase 5 (an accessor .torrent
	// whose primary payload no longer exists).
	type pastFile struct {
		name        string
		path        string
		torrentPath string
		btPath      string
		btTorrent   string
		kviPath     string
		kviTorrent  string
		kveiPath    string
		kveiTorrent string
	}
	pastFiles := []pastFile{
		{name: "v1.1-accounts.278-279.kv"},
		{name: "v1.1-accounts.280-282.kv"},
		{name: "v1.1-accounts.280-284.kv"},
	}
	for i := range pastFiles {
		pastFiles[i].path = filepath.Join(tmpDir, pastFiles[i].name)
		pastFiles[i].torrentPath = pastFiles[i].path + ".torrent"
		stem := strings.TrimSuffix(pastFiles[i].path, ".kv")
		pastFiles[i].btPath = stem + ".bt"
		pastFiles[i].btTorrent = stem + ".bt.torrent"
		pastFiles[i].kviPath = stem + ".kvi"
		pastFiles[i].kviTorrent = stem + ".kvi.torrent"
		pastFiles[i].kveiPath = stem + ".kvei"
		pastFiles[i].kveiTorrent = stem + ".kvei.torrent"
		require.NoError(t, os.WriteFile(pastFiles[i].path, []byte("stale past-boundary content"), 0o600))
		require.NoError(t, os.WriteFile(pastFiles[i].torrentPath, []byte("stale torrent"), 0o600))
		for _, p := range []string{
			pastFiles[i].btPath, pastFiles[i].btTorrent,
			pastFiles[i].kviPath, pastFiles[i].kviTorrent,
			pastFiles[i].kveiPath, pastFiles[i].kveiTorrent,
		} {
			require.NoError(t, os.WriteFile(p, []byte("stale accessor"), 0o600))
		}
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
		checks := []struct {
			path string
			what string
		}{
			{f.path, ".kv"},
			{f.torrentPath, ".kv.torrent"},
			{f.btPath, ".bt"},
			{f.btTorrent, ".bt.torrent"},
			{f.kviPath, ".kvi"},
			{f.kviTorrent, ".kvi.torrent"},
			{f.kveiPath, ".kvei"},
			{f.kveiTorrent, ".kvei.torrent"},
		}
		for _, c := range checks {
			_, err := os.Stat(c.path)
			require.True(t, os.IsNotExist(err), "past-boundary %s must be removed: %s", c.what, c.path)
		}
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

// TestProvider_FinalizeUnwind_SplitEmitAlignedPlusStub pins mode-C's
// split-emit finalize path: one straddler produces TWO pairs — an
// aligned wide file (step-aligned name) plus a stub v4 (mid-step v4
// name confined to the target step). Only the aligned pair carries
// oldBroadPath — it owns the .old-dance + broad retire. The stub pair
// leaves oldBroadPath empty so FinalizeUnwind skips the rename and
// only lands the new file. Both new files must exist post-finalize,
// broad file removed, downloader.Delete carries all three basenames.
// Without the split, a single wide v4 mid-step file overlaps with the
// retire that fires when forward-exec crosses the next step boundary
// → aggregator visible-set has overlapping data files → SIGBUS in
// getLatestFromFile's accessor-into-mmap read (leg-M iter 4 mode_a
// 2026-08-15 repro).
func TestProvider_FinalizeUnwind_SplitEmitAlignedPlusStub(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	broadName := "v2.2-accounts.272-292.kv"
	alignedName := "v2.2-accounts.272-289.kv"
	stubName := "v4.0-accounts.112890625-113250001.kv"
	broadPath := filepath.Join(tmpDir, broadName)
	alignedFinal := filepath.Join(tmpDir, alignedName)
	stubFinal := filepath.Join(tmpDir, stubName)
	alignedRegen := alignedFinal + ".regen"
	stubRegen := stubFinal + ".regen"
	broadTorrent := broadPath + ".torrent"

	require.NoError(t, os.WriteFile(broadPath, []byte("pre-split-broad"), 0o600))
	require.NoError(t, os.WriteFile(alignedRegen, []byte("aligned-content"), 0o600))
	require.NoError(t, os.WriteFile(stubRegen, []byte("stub-content"), 0o600))
	require.NoError(t, os.WriteFile(broadTorrent, []byte("stale-broad-torrent"), 0o600))

	stub := &recordingDownloaderClient{}
	p := &Provider{
		snapDir:          tmpDir,
		downloaderClient: stub,
	}
	p.pendingRegen = &pendingRegenState{
		pairs: []regenPair{
			{
				regenPath:    alignedRegen,
				finalPath:    alignedFinal,
				oldBroadPath: broadPath, // aligned owns the broad retire
			},
			{
				regenPath:    stubRegen,
				finalPath:    stubFinal,
				oldBroadPath: "", // stub is additive; peer already retires broad
			},
		},
	}

	require.NoError(t, p.FinalizeUnwind())

	alignedBytes, err := os.ReadFile(alignedFinal)
	require.NoError(t, err, "aligned .kv must exist post-finalize")
	require.Equal(t, "aligned-content", string(alignedBytes))

	stubBytes, err := os.ReadFile(stubFinal)
	require.NoError(t, err, "stub v4 .kv must exist post-finalize")
	require.Equal(t, "stub-content", string(stubBytes))

	_, err = os.Stat(broadPath)
	require.True(t, os.IsNotExist(err), "broad .kv must be removed (superseded by aligned + stub)")
	_, err = os.Stat(broadTorrent)
	require.True(t, os.IsNotExist(err), "broad .torrent must be removed")

	deletes := stub.snapshotDeletes()
	require.Len(t, deletes, 1, "downloaderClient.Delete called once for the regen batch")
	require.ElementsMatch(t, []string{alignedName, stubName, broadName}, deletes[0],
		"Delete must carry aligned + stub new basenames AND the retired broad basename")

	require.Nil(t, p.pendingRegen, "FinalizeUnwind must drain pendingRegen")
}
