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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

func writeBlockRootSeg(t *testing.T, dirs datadir.Dirs, from, to uint64, words [][]byte) {
	t.Helper()
	name := snaptype.BeaconBlocks.FileName(version.ZeroVersion, from, to)
	name = strings.ReplaceAll(name, "beaconblocks", "BlockRoot")
	c, err := seg.NewCompressor(context.Background(), "test", filepath.Join(dirs.SnapCaplin, name), dirs.Tmp, seg.DefaultCfg, log.LvlCrit, log.New())
	require.NoError(t, err)
	defer c.Close()
	for _, w := range words {
		require.NoError(t, c.AddWord(w))
	}
	require.NoError(t, c.Compress())
}

func TestCheckCaplinStateRoots(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.SnapCaplin, 0o755))
	require.NoError(t, os.MkdirAll(dirs.Tmp, 0o755))
	ctx, logger := context.Background(), log.New()

	root := make([]byte, length.Hash)
	full := make([][]byte, 0, 20)
	for range 20 {
		full = append(full, root)
	}

	writeBlockRootSeg(t, dirs, 0, 50000, full)
	require.NoError(t, CheckCaplinStateRoots(ctx, dirs, true, logger))

	holed := append([][]byte{}, full...)
	holed[7] = nil
	writeBlockRootSeg(t, dirs, 50000, 100000, holed)
	err := CheckCaplinStateRoots(ctx, dirs, true, logger)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no root")
}
