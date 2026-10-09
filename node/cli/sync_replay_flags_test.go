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

package cli

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChainTipModeFlag(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		cfg := buildEthCfg(t, nil)
		require.False(t, cfg.Sync.ChainTipMode)
		require.EqualValues(t, 5_000, cfg.Sync.LoopBlockLimit)
	})

	t.Run("one block per cycle with changesets", func(t *testing.T) {
		cfg := buildEthCfg(t, []string{"--pfb.sync.mode.chaintip", "--sync.loop.block.limit", "500"})
		require.True(t, cfg.Sync.ChainTipMode)
		require.EqualValues(t, 1, cfg.Sync.LoopBlockLimit)
		require.True(t, cfg.Sync.AlwaysGenerateChangesets)
	})
}

func TestExecStopAtBlockFlag(t *testing.T) {
	require.Zero(t, buildEthCfg(t, nil).Sync.ExecStopAtBlock)
	require.EqualValues(t, 25_640_187, buildEthCfg(t, []string{"--pfb.exec.stop-at-block", "25640187"}).Sync.ExecStopAtBlock)
}

func TestPruneBlocksDistanceDefaultKeepsReplayRange(t *testing.T) {
	const replayRange = 10_000_000
	head := uint64(30_000_000)
	for _, mode := range []string{"full", "minimal"} {
		t.Run(mode+" defaults to the replay range", func(t *testing.T) {
			cfg := buildEthCfg(t, []string{"--prune.mode", mode})
			require.EqualValues(t, head-replayRange, cfg.Prune.Blocks.PruneTo(head))
		})
	}
	t.Run("explicit distance wins", func(t *testing.T) {
		cfg := buildEthCfg(t, []string{"--prune.mode", "full", "--prune.distance.blocks", "300000"})
		require.EqualValues(t, head-300_000, cfg.Prune.Blocks.PruneTo(head))
	})
	t.Run("archive keeps all blocks", func(t *testing.T) {
		require.Zero(t, buildEthCfg(t, []string{"--prune.mode", "archive"}).Prune.Blocks.PruneTo(head))
	})
}

func TestOfflineBALFlags(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		cfg := buildEthCfg(t, nil)
		require.False(t, cfg.Sync.GenerateOfflineBALs)
		require.False(t, cfg.Sync.UseOfflineBALs)
		require.False(t, cfg.ExperimentalBAL)
		require.Equal(t, filepath.Join(cfg.Dirs.DataDir, "offline-bal"), cfg.Sync.OfflineBALDir)
	})

	t.Run("generate implies experimental BAL", func(t *testing.T) {
		cfg := buildEthCfg(t, []string{"--generate-offline-bals", "--offline-bal.dir", "/bal"})
		require.True(t, cfg.Sync.GenerateOfflineBALs)
		require.True(t, cfg.ExperimentalBAL)
		require.Equal(t, "/bal", cfg.Sync.OfflineBALDir)
	})

	t.Run("use", func(t *testing.T) {
		cfg := buildEthCfg(t, []string{"--use-offline-bals"})
		require.True(t, cfg.Sync.UseOfflineBALs)
		require.False(t, cfg.ExperimentalBAL)
	})
}
