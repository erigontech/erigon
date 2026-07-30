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
