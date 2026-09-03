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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/blockmetrics"
	"github.com/erigontech/erigon/node/ethconfig"
)

func buildEthCfg(t *testing.T, args []string) ethconfig.Config {
	t.Helper()

	var result ethconfig.Config
	app := &cli.Command{}
	app.Flags = DefaultFlags
	app.Action = func(_ context.Context, ctx *cli.Command) error {
		cfg := ethconfig.Defaults
		cfg.Dirs.DataDir = t.TempDir()
		applyRemainingEthFlags(ctx, &cfg, log.New())
		result = cfg
		return nil
	}
	require.NoError(t, app.Run(context.Background(), append([]string{"erigon"}, args...)))
	return result
}

func TestSlowBlockThresholdFlag(t *testing.T) {
	t.Run("off unless asked", func(t *testing.T) {
		require.Equal(t, blockmetrics.Disabled, buildEthCfg(t, nil).Sync.SlowBlockThreshold)
	})

	t.Run("zero means every block", func(t *testing.T) {
		cfg := buildEthCfg(t, []string{"--debug.slow-block-threshold", "0"})
		require.Zero(t, cfg.Sync.SlowBlockThreshold)
	})

	t.Run("duration reaches the sync config", func(t *testing.T) {
		cfg := buildEthCfg(t, []string{"--debug.slow-block-threshold", "250ms"})
		require.Equal(t, 250*time.Millisecond, cfg.Sync.SlowBlockThreshold)
	})

	t.Run("threshold enables the read counters", func(t *testing.T) {
		buildEthCfg(t, []string{"--debug.slow-block-threshold", "250ms"})
		require.True(t, dbg.KVReadLevelledMetrics)
	})
}
