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

package freezeblocks

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/node/ethconfig"
)

// CaplinSnapshots never calls RemoveOverlaps, so nothing on a running node sweeps the .tmp left
// by a killed blob compression. Each rerun picks a new random suffix and one blob range can be
// several GB, so they accumulate until the disk fills. Sweep once at construction, before the
// antiquary compresses anything.
func TestNewCaplinSnapshotsSweepsItsOwnStaleTmp(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	cfg := clparams.MainnetBeaconConfig

	stale := filepath.Join(dirs.Snap, "v1.1-014790-014800-blobsidecars.seg.2774720258.tmp")
	alsoStale := filepath.Join(dirs.Snap, "v1.1-014790-014800-beaconblocks.seg.99.tmp")
	foreign := filepath.Join(dirs.Snap, "v1.0-000000-000500-headers.seg.123.tmp")
	for _, p := range []string{stale, alsoStale, foreign} {
		require.NoError(t, os.WriteFile(p, []byte("leftover"), 0o644))
	}

	c := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(c.Close)

	require.NoFileExists(t, stale, "a stale blobsidecars .tmp must be swept at startup")
	require.NoFileExists(t, alsoStale, "a stale beaconblocks .tmp must be swept at startup")
	require.FileExists(t, foreign, "a block-snapshot .tmp is not caplin's to remove")
}
