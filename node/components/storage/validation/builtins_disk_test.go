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

package validation

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// makeFileWithTorrent writes a file at <dir>/<name> with the supplied
// content and builds a real .torrent sidecar at <dir>/<name>.torrent
// using the production BuildTorrentIfNeed path. The returned name
// matches the file's base name (no path prefix). This is the
// "downloaded file landed cleanly" baseline state every disk-aware
// validation test starts from.
func makeFileWithTorrent(t *testing.T, dir, name string, content []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), content, 0o644))
	tf := downloader.NewAtomicTorrentFS(dir)
	_, err := downloader.BuildTorrentIfNeed(context.Background(), name, dir, tf)
	require.NoError(t, err)
}

// truncateFile shortens an on-disk file to the requested size.
// Simulates a partial / interrupted download — the producer side's
// canonical "manufactured failure" mechanic.
func truncateFile(t *testing.T, path string, newSize int64) {
	t.Helper()
	require.NoError(t, os.Truncate(path, newSize))
}

// appendBytes writes extra bytes onto the end of an on-disk file.
// Simulates inflation tampering — same shape as truncation but in
// the other direction.
func appendBytes(t *testing.T, path string, extra []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Write(extra)
	require.NoError(t, err)
}

func TestSizeMatchesTorrent_CleanFileAccepts(t *testing.T) {
	dir := t.TempDir()
	makeFileWithTorrent(t, dir, "v1.0-clean.kv", []byte(
		"a sample snapshot file payload — the actual bytes don't matter, only that the .torrent agrees with the file on disk"))

	v := SizeMatchesTorrent{SnapDir: dir}
	file := &snapshot.FileEntry{Name: "v1.0-clean.kv"}
	content := FileContent{Path: filepath.Join(dir, "v1.0-clean.kv")}

	require.NoError(t, v.Validate(file, content),
		"clean file with matching .torrent must pass")
}

func TestSizeMatchesTorrent_TruncatedFileRejects(t *testing.T) {
	dir := t.TempDir()
	original := []byte(
		"a multi-piece-friendly payload that is comfortably larger than zero so a truncation is unambiguously detectable downstream of the torrent metainfo's declared length")
	name := "v1.0-truncated.kv"
	makeFileWithTorrent(t, dir, name, original)

	// Manufacture the failure: cut the file in half (simulates a
	// partial download that crashed mid-transfer).
	truncateFile(t, filepath.Join(dir, name), int64(len(original)/2))

	v := SizeMatchesTorrent{SnapDir: dir}
	file := &snapshot.FileEntry{Name: name}
	content := FileContent{Path: filepath.Join(dir, name)}

	err := v.Validate(file, content)
	require.Error(t, err, "truncated file must be rejected")
	require.Contains(t, err.Error(), "likely truncated or tampered")
}

func TestSizeMatchesTorrent_InflatedFileRejects(t *testing.T) {
	dir := t.TempDir()
	original := []byte("payload-bytes-exact-length-matters-here")
	name := "v1.0-inflated.kv"
	makeFileWithTorrent(t, dir, name, original)

	// Manufacture the failure: append extra bytes (tampered file).
	appendBytes(t, filepath.Join(dir, name), []byte("garbage-tail"))

	v := SizeMatchesTorrent{SnapDir: dir}
	file := &snapshot.FileEntry{Name: name}
	content := FileContent{Path: filepath.Join(dir, name)}

	err := v.Validate(file, content)
	require.Error(t, err, "inflated file must be rejected")
	require.Contains(t, err.Error(), "likely truncated or tampered")
}

func TestSizeMatchesTorrent_MissingTorrentSilentlyAccepts(t *testing.T) {
	dir := t.TempDir()
	// File present, NO .torrent sidecar.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "v1.0-no-torrent.kv"),
		[]byte("payload"), 0o644))

	v := SizeMatchesTorrent{SnapDir: dir}
	file := &snapshot.FileEntry{Name: "v1.0-no-torrent.kv"}
	content := FileContent{Path: filepath.Join(dir, "v1.0-no-torrent.kv")}

	require.NoError(t, v.Validate(file, content),
		"missing .torrent should be a silent accept (no torrent → can't measure)")
}

func TestSizeMatchesTorrent_EmptySnapDirErrors(t *testing.T) {
	v := SizeMatchesTorrent{SnapDir: ""}
	err := v.Validate(&snapshot.FileEntry{Name: "x"}, BytesContent("y"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty SnapDir")
}

func TestSizeMatchesTorrent_NilContentErrors(t *testing.T) {
	dir := t.TempDir()
	makeFileWithTorrent(t, dir, "v1.0-x.kv", []byte("payload"))

	v := SizeMatchesTorrent{SnapDir: dir}
	err := v.Validate(&snapshot.FileEntry{Name: "v1.0-x.kv"}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil content")
}

// TestSizeMatchesTorrent_ProducerGateBlocksTruncatedFile is the
// motivating end-to-end shape: a producer-side validation chain that
// includes SizeMatchesTorrent rejects a truncated file at
// MarkAdvertisable time. The Advertisable flag stays false; the file
// never reaches the published manifest. This is the swarm-blast-
// radius firewall the validation phase exists to provide.
func TestSizeMatchesTorrent_ProducerGateBlocksTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	name := "v1.0-account.0-1024.kv"
	original := make([]byte, 4096)
	for i := range original {
		original[i] = byte(i)
	}
	makeFileWithTorrent(t, dir, name, original)

	inv := snapshot.NewInventory()
	file := &snapshot.FileEntry{
		Name: name, Domain: "accounts",
		FromStep: 0, ToStep: 1024, Kind: snapshot.KindKV,
		Local: true,
	}
	inv.AddFile(file)

	producer := &Producer{Chain: DefaultStage1ChainWithDisk(dir)}

	// Clean state — pass.
	changed, err := producer.MarkAdvertisable(inv, file, FileContent{Path: filepath.Join(dir, name)})
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, file.Advertisable, "clean file is advertised")

	// Add a second file, then truncate it before producer-gate runs.
	corrupt := "v1.0-account.1024-2048.kv"
	makeFileWithTorrent(t, dir, corrupt, original)
	truncateFile(t, filepath.Join(dir, corrupt), 1024)

	corruptEntry := &snapshot.FileEntry{
		Name: corrupt, Domain: "accounts",
		FromStep: 1024, ToStep: 2048, Kind: snapshot.KindKV,
		Local: true,
	}
	inv.AddFile(corruptEntry)

	changed, err = producer.MarkAdvertisable(inv, corruptEntry, FileContent{Path: filepath.Join(dir, corrupt)})
	require.Error(t, err, "truncated file must be rejected by producer gate")
	require.Contains(t, err.Error(), "size_matches_torrent")
	require.Contains(t, err.Error(), "likely truncated or tampered")
	require.False(t, changed)
	require.False(t, corruptEntry.Advertisable,
		"validation rejection must leave Advertisable=false — file never enters the published manifest")
}
