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

// Only files the torrent client can serve are advertised, and a node that
// bootstrapped from preverified holds a complete set minutes before the client
// finishes registering it. The startup publishes all run inside that window, so
// without a trigger on the set becoming servable the node advertises nothing
// until it happens to build a file of its own — a step boundary away, which on
// hoodi measured 7h39m.
func TestPublishesWhenTheServableSetGrows(t *testing.T) {
	test := newDownloaderTest(t)
	d := test.downloader

	// Nothing servable yet: nothing to say, and nothing should be claimed.
	d.republishOnServableGrowth()

	const base = "v2.0-accounts.0-1.kv"
	path := filepath.Join(test.dirs.SnapDomain, base)
	require.NoError(t, os.WriteFile(path, []byte("bootstrapped payload"), 0o644))
	require.NoError(t, d.AddNewSeedableFile(t.Context(), "domain/"+base))
	require.NotEmpty(t, d.torrentClient.Torrents(), "the file is now servable")

	d.republishOnServableGrowth()

	got, err := LoadChainToml(d.snapDir())
	require.NoError(t, err)
	require.Contains(t, string(got), base,
		"a set that became servable must be advertised without waiting for a local build")
}
