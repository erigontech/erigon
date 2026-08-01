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

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/nodecfg"
)

// buildHttpCfg runs the full node flag pipeline (ApplyFlagsForNodeConfig) with
// the given CLI args and returns the resulting embedded HTTP config.
func buildHttpCfg(t *testing.T, args []string) nodecfg.Config {
	t.Helper()

	var result nodecfg.Config
	app := &cli.Command{}
	app.Flags = DefaultFlags
	app.Action = func(nodeCtx context.Context, ctx *cli.Command) error {
		cfg := nodecfg.Config{}
		cfg.Dirs.DataDir = t.TempDir()
		ApplyFlagsForNodeConfig(ctx, &cfg, log.New())
		result = cfg
		return nil
	}
	require.NoError(t, app.Run(context.Background(), append([]string{"erigon"}, args...)))
	return result
}

func TestWitnessCacheFlags_Defaults(t *testing.T) {
	cfg := buildHttpCfg(t, nil)

	require.EqualValues(t, 0, cfg.Http.WitnessCacheBlocks)
	require.False(t, cfg.Http.WitnessCacheHeadCapture)
	require.EqualValues(t, 0, cfg.Http.WitnessCacheMaxMB)
}

func TestWitnessCacheFlags_Set(t *testing.T) {
	cfg := buildHttpCfg(t, []string{
		"--witness.cache.blocks", "48",
		"--witness.cache.head-capture",
		"--witness.cache.maxmb", "512",
	})

	require.EqualValues(t, 48, cfg.Http.WitnessCacheBlocks)
	require.True(t, cfg.Http.WitnessCacheHeadCapture)
	require.EqualValues(t, 512, cfg.Http.WitnessCacheMaxMB)
}

func TestWitnessCacheFlags_BlocksUnchangedByNewFlags(t *testing.T) {
	// Setting only blocks must not toggle head-capture or the byte cap.
	cfg := buildHttpCfg(t, []string{"--witness.cache.blocks", "10"})

	require.EqualValues(t, 10, cfg.Http.WitnessCacheBlocks)
	require.False(t, cfg.Http.WitnessCacheHeadCapture)
	require.EqualValues(t, 0, cfg.Http.WitnessCacheMaxMB)
}
