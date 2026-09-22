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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
)

func TestTorrentHashesReturnsErrorOnUnreadableTorrent(t *testing.T) {
	dataDir := t.TempDir()
	dirs := datadir.New(dataDir)
	corrupt := filepath.Join(dirs.SnapDomain, "v1.1-accounts.0-64.kv.torrent")
	require.NoError(t, os.WriteFile(corrupt, []byte("not a torrent"), 0o644))

	rootCmd.SetArgs([]string{"torrent_hashes", "--datadir", dataDir, "--chain", "mainnet", "--log.dir.disable"})
	err := rootCmd.ExecuteContext(t.Context())

	require.ErrorContains(t, err, corrupt)
}
