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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// bumpMtime advances a file's modification time so the validator's
// size+mtime memo treats the content as changed.
func bumpMtime(t *testing.T, path string) {
	t.Helper()
	di, err := os.Stat(path)
	require.NoError(t, err)
	future := di.ModTime().Add(time.Second)
	require.NoError(t, os.Chtimes(path, future, future))
}

// writeTorrentFor builds a .torrent next to the data file, capturing the
// file's piece hashes — what the publisher would advertise it under.
func writeTorrentFor(t *testing.T, dataPath string) {
	t.Helper()
	info := metainfo.Info{PieceLength: 64 * 1024}
	require.NoError(t, info.BuildFromFilePath(dataPath))
	var mi metainfo.MetaInfo
	var err error
	mi.InfoBytes, err = bencode.Marshal(info)
	require.NoError(t, err)
	f, err := os.Create(dataPath + ".torrent")
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, mi.Write(f))
}

func TestInfoHashValidatorCheckFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	name := "v1-000000-000500-headers.seg"
	dataPath := filepath.Join(dir, name)
	content := bytes.Repeat([]byte("erigon-snapshot-content"), 5000) // ~115 KiB → multi-piece
	require.NoError(t, os.WriteFile(dataPath, content, 0o644))

	v := &InfoHashValidator{SnapDir: dir}
	ctx := context.Background()

	// No .torrent yet (the seeder writes it asynchronously) → ErrPause,
	// so the lifecycle driver retries rather than quarantining.
	err := v.checkFile(ctx, name)
	require.Error(t, err)
	require.ErrorIs(t, err, validation.ErrPause)

	// Valid .torrent + intact content → pass.
	writeTorrentFor(t, dataPath)
	require.NoError(t, v.checkFile(ctx, name))

	// Corrupt one byte → hard mismatch, NOT a pause (must count toward
	// quarantine). Touch the mtime so the size+mtime memo does not mask
	// the corruption.
	corrupt := bytes.Clone(content)
	corrupt[len(corrupt)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(dataPath, corrupt, 0o644))
	bumpMtime(t, dataPath)
	err = v.checkFile(ctx, name)
	require.Error(t, err)
	require.NotErrorIs(t, err, validation.ErrPause)
}

func TestInfoHashValidator_ValidateStep(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	name := "v1-000000-000500-headers.seg"
	dataPath := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(dataPath, bytes.Repeat([]byte("x"), 100_000), 0o644))
	writeTorrentFor(t, dataPath)

	v := &InfoHashValidator{SnapDir: dir}
	ctx := context.Background()

	// A Local file is piece-hashed and passes.
	require.NoError(t, v.ValidateStep(ctx, []*snapshot.FileEntry{
		{Name: name, Local: true},
	}))

	// A non-Local entry is skipped — no on-disk bytes to speak to.
	require.NoError(t, v.ValidateStep(ctx, []*snapshot.FileEntry{
		{Name: "v1-000500-001000-headers.seg", Local: false},
	}))
}
