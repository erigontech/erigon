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

package integrity

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func writeTorrentPair(t *testing.T, dir, name string, corrupt bool) {
	t.Helper()
	dataPath := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(dataPath), 0o755))
	require.NoError(t, os.WriteFile(dataPath, bytes.Repeat([]byte{0xAA}, 1024), 0o644))

	info := metainfo.Info{PieceLength: 16 * 1024}
	require.NoError(t, info.BuildFromFilePath(dataPath))
	infoBytes, err := bencode.Marshal(info)
	require.NoError(t, err)

	f, err := os.Create(dataPath + ".torrent")
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, (&metainfo.MetaInfo{InfoBytes: infoBytes}).Write(f))

	if corrupt {
		require.NoError(t, os.WriteFile(dataPath, bytes.Repeat([]byte{0xBB}, 1024), 0o644))
	}
}

// A corrupt pair in a subdir must fail verification — proving the scan
// descends below the top level rather than globbing it flat.
func TestVerifyTorrentFilesVisitsSubdirs(t *testing.T) {
	dir := t.TempDir()
	writeTorrentPair(t, dir, filepath.Join("caplin", "test.seg"), true)

	err := VerifyTorrentFiles(context.Background(), dir, true, log.New())
	require.Error(t, err)
}

func TestVerifyTorrentFilesVisitsTopLevel(t *testing.T) {
	dir := t.TempDir()
	writeTorrentPair(t, dir, "test.seg", true)

	err := VerifyTorrentFiles(context.Background(), dir, true, log.New())
	require.Error(t, err)
}

func TestVerifyTorrentFilesValidPairs(t *testing.T) {
	dir := t.TempDir()
	writeTorrentPair(t, dir, "test.seg", false)
	writeTorrentPair(t, dir, filepath.Join("caplin", "test.seg"), false)

	err := VerifyTorrentFiles(context.Background(), dir, true, log.New())
	require.NoError(t, err)
}

// Without failFast every file is still verified, but the run must report the
// failures it collected instead of exiting as a clean verification.
func TestVerifyTorrentFilesWithoutFailFast(t *testing.T) {
	dir := t.TempDir()
	writeTorrentPair(t, dir, "corrupt.seg", true)
	writeTorrentPair(t, dir, "alsoCorrupt.seg", true)
	writeTorrentPair(t, dir, filepath.Join("caplin", "valid.seg"), false)

	err := VerifyTorrentFiles(context.Background(), dir, false, log.New())
	require.Error(t, err)
	// Both corrupt files counted: the run went past the first failure.
	require.ErrorContains(t, err, "2 file(s) failed verification")
}

// An interrupted run must not be reported as a successful verification: without
// failFast the per-piece cancellation error is only warned about, so the
// cancellation has to be re-checked after the workers finish.
func TestVerifyTorrentFilesCancelled(t *testing.T) {
	dir := t.TempDir()
	writeTorrentPair(t, dir, "test.seg", false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := VerifyTorrentFiles(ctx, dir, false, log.New())
	require.ErrorIs(t, err, context.Canceled)
}

// An unreadable root must not be reported as a successful verification.
func TestVerifyTorrentFilesUnreadableRoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")

	err := VerifyTorrentFiles(context.Background(), dir, false, log.New())
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// Putting the snapshots dir on another disk via a symlink is a supported
// layout, so the scan must resolve it instead of reporting an empty run.
func TestVerifyTorrentFilesSymlinkedRoot(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target")
	writeTorrentPair(t, target, "test.seg", true)

	link := filepath.Join(tmp, "snapshots")
	require.NoError(t, os.Symlink(target, link))

	err := VerifyTorrentFiles(context.Background(), link, true, log.New())
	require.Error(t, err)
}

// Same for an individual subtree: a symlinked caplin/ must still be descended into.
func TestVerifyTorrentFilesSymlinkedSubdir(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target")
	writeTorrentPair(t, target, "test.seg", true)

	dir := filepath.Join(tmp, "snapshots")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "caplin")))

	err := VerifyTorrentFiles(context.Background(), dir, true, log.New())
	require.Error(t, err)
}

// A path the scan cannot resolve may hide any number of torrents, so the run
// must fail rather than report a complete verification over what it did reach —
// while still verifying the files it could reach.
func TestVerifyTorrentFilesUnresolvableSubdir(t *testing.T) {
	dir := t.TempDir()
	writeTorrentPair(t, dir, "corrupt.seg", true)
	require.NoError(t, os.Symlink(filepath.Join(dir, "gone"), filepath.Join(dir, "caplin")))

	err := VerifyTorrentFiles(context.Background(), dir, false, log.New())
	require.Error(t, err)
	require.ErrorContains(t, err, "1 unreadable path(s)")
	require.ErrorContains(t, err, "1 file(s) failed verification")
}

// failFast governs the mismatch behaviour only, so an unresolvable path must
// still leave everything the scan did reach verified.
func TestVerifyTorrentFilesUnresolvableSubdirFailFast(t *testing.T) {
	dir := t.TempDir()
	writeTorrentPair(t, dir, "corrupt.seg", true)
	require.NoError(t, os.Symlink(filepath.Join(dir, "gone"), filepath.Join(dir, "caplin")))

	err := VerifyTorrentFiles(context.Background(), dir, true, log.New())
	require.Error(t, err)
	require.ErrorContains(t, err, "hash mismatch")
}

// A symlink pointing back at an ancestor must not send the scan into unbounded
// recursion; the resolved-path guard stops it and the reachable files still verify.
func TestVerifyTorrentFilesSymlinkCycle(t *testing.T) {
	dir := t.TempDir()
	writeTorrentPair(t, dir, "test.seg", true)
	require.NoError(t, os.Symlink(dir, filepath.Join(dir, "loop")))

	err := VerifyTorrentFiles(context.Background(), dir, true, log.New())
	require.Error(t, err)
	require.ErrorContains(t, err, "hash mismatch")
}
