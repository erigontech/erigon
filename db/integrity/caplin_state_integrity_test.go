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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

func writeRootSeg(t *testing.T, dirs datadir.Dirs, table string, from, to uint64, words [][]byte) {
	t.Helper()
	name := snaptype.BeaconBlocks.FileName(version.ZeroVersion, from, to)
	name = strings.ReplaceAll(name, "beaconblocks", table)
	c, err := seg.NewCompressor(context.Background(), "test", filepath.Join(dirs.SnapCaplin, name), dirs.Tmp, seg.DefaultCfg, log.LvlCrit, log.New())
	require.NoError(t, err)
	defer c.Close()
	for _, w := range words {
		require.NoError(t, c.AddWord(w))
	}
	require.NoError(t, c.Compress())
}

func TestCheckPtcWindowEpoch(t *testing.T) {
	setup := func(t *testing.T, wordCount int, forkRow []byte) (datadir.Dirs, *clparams.BeaconChainConfig) {
		t.Helper()
		dirs := datadir.New(t.TempDir())
		require.NoError(t, os.MkdirAll(dirs.SnapCaplin, 0o755))
		require.NoError(t, os.MkdirAll(dirs.Tmp, 0o755))
		words := make([][]byte, wordCount)
		for slot := 4; slot < wordCount; slot += 4 {
			words[slot] = forkRow
		}
		writeRootSeg(t, dirs, kv.PtcWindowEpochTable, 0, 8, words)
		cfg := clparams.MainnetBeaconConfig
		cfg.SlotsPerEpoch = 4
		cfg.GloasForkEpoch = 1
		return dirs, &cfg
	}

	t.Run("complete", func(t *testing.T) {
		dirs, cfg := setup(t, 8, []byte("compressed-window"))
		require.NoError(t, CheckPtcWindowEpoch(context.Background(), dirs, cfg))
	})

	t.Run("missing post-fork epoch", func(t *testing.T) {
		dirs, cfg := setup(t, 8, nil)
		err := CheckPtcWindowEpoch(context.Background(), dirs, cfg)
		require.Error(t, err)
		require.ErrorContains(t, err, "slot 4")
	})

	t.Run("truncated segment range", func(t *testing.T) {
		dirs, cfg := setup(t, 5, []byte("compressed-window"))
		err := CheckPtcWindowEpoch(context.Background(), dirs, cfg)
		require.Error(t, err)
		require.ErrorContains(t, err, "word count 5, expected 8")
	})

	t.Run("overlong segment range", func(t *testing.T) {
		dirs, cfg := setup(t, 9, []byte("compressed-window"))
		err := CheckPtcWindowEpoch(context.Background(), dirs, cfg)
		require.Error(t, err)
		require.ErrorContains(t, err, "word count 9, expected 8")
	})
}

func TestCheckCaplinStateRoots(t *testing.T) {
	ctx, logger := context.Background(), log.New()

	fullRoots := func() [][]byte {
		root := make([]byte, length.Hash)
		w := make([][]byte, 0, 20)
		for range 20 {
			w = append(w, root)
		}
		return w
	}
	holed := func() [][]byte {
		w := fullRoots()
		w[7] = nil
		return w
	}
	setup := func(t *testing.T) datadir.Dirs {
		t.Helper()
		dirs := datadir.New(t.TempDir())
		require.NoError(t, os.MkdirAll(dirs.SnapCaplin, 0o755))
		require.NoError(t, os.MkdirAll(dirs.Tmp, 0o755))
		return dirs
	}

	t.Run("all roots present", func(t *testing.T) {
		dirs := setup(t)
		writeRootSeg(t, dirs, "BlockRoot", 0, 50000, fullRoots())
		writeRootSeg(t, dirs, "StateRoot", 0, 50000, fullRoots())
		require.NoError(t, CheckCaplinStateRoots(ctx, dirs, true, logger))
	})

	t.Run("hole in BlockRoot", func(t *testing.T) {
		dirs := setup(t)
		writeRootSeg(t, dirs, "BlockRoot", 50000, 100000, holed())
		err := CheckCaplinStateRoots(ctx, dirs, true, logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "BlockRoot")
		require.Contains(t, err.Error(), "invalid root")
	})

	t.Run("hole in StateRoot", func(t *testing.T) {
		dirs := setup(t)
		writeRootSeg(t, dirs, "BlockRoot", 0, 50000, fullRoots())
		writeRootSeg(t, dirs, "StateRoot", 50000, 100000, holed())
		err := CheckCaplinStateRoots(ctx, dirs, true, logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "StateRoot")
		require.Contains(t, err.Error(), "invalid root")
	})
}
