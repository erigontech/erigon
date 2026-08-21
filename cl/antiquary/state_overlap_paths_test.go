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

package antiquary

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
)

func TestAbsPathsReRootOntoTheDownloaderKey(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	const name = "v1.1-000000-000050-pending_deposits_dump.seg"

	got := absPaths(dirs.SnapCaplin, []string{name})

	require.Len(t, got, 1)
	require.True(t, filepath.IsAbs(got[0]), "the downloader re-roots only absolute paths")
	require.Equal(t, filepath.Join(dirs.SnapCaplin, name), got[0])

	// What RpcClient.fixPath then computes: the torrent key the downloader stores.
	rel, err := filepath.Rel(dirs.Snap, got[0])
	require.NoError(t, err)
	require.Equal(t, filepath.Join("caplin", name), rel)
}
