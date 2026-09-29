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

package downloader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// State files are named to the downloader both ways: the flow uses the
// layout-relative path, the aggregator's merge and unwind callbacks use the
// bare basename. Both name one file, so a delete under either spelling must
// take the sidecar and the registration with it — the data file is the
// aggregator's to remove. A delete that finds nothing leaves the torrent
// registered, and the client goes on fetching a file the node has deliberately
// retired, putting a merged range back on disk beside the finer files that
// replaced it.
func TestDeleteResolvesBasenameThroughLayout(t *testing.T) {
	test := newDownloaderTest(t)
	d := test.downloader

	const base = "v2.0-code.320-328.kv"
	const rel = "domain/" + base
	path := filepath.Join(test.dirs.SnapDomain, base)
	require.NoError(t, os.WriteFile(path, []byte("retired range"), 0o644))
	require.NoError(t, d.AddNewSeedableFile(t.Context(), rel))
	require.Len(t, d.torrentClient.Torrents(), 1)

	require.NoError(t, d.Delete(base))

	require.Empty(t, d.torrentClient.Torrents())
	require.NoFileExists(t, path+".torrent")
}
