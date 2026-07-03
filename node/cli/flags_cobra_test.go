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
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestApplyFlagsForEthConfigCobra_Defaults(t *testing.T) {
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	cfg := &ethconfig.Config{}

	ApplyFlagsForEthConfigCobra(flags, cfg)

	require.Equal(t, prune.FullMode, cfg.Prune)
	require.True(t, cfg.StateStream)
	require.False(t, cfg.ExperimentalBAL)
}

func TestApplyFlagsForEthConfigCobra_UsesProvidedFlags(t *testing.T) {
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String(PruneModeFlag.Name, PruneModeFlag.Value, PruneModeFlag.Usage)
	flags.Bool(StateStreamDisableFlag.Name, StateStreamDisableFlag.Value, StateStreamDisableFlag.Usage)
	flags.Bool(ExperimentalBALFlag.Name, ExperimentalBALFlag.Value, ExperimentalBALFlag.Usage)

	require.NoError(t, flags.Set(PruneModeFlag.Name, prune.ArchiveMode.String()))
	require.NoError(t, flags.Set(StateStreamDisableFlag.Name, "true"))
	require.NoError(t, flags.Set(ExperimentalBALFlag.Name, "true"))

	cfg := &ethconfig.Config{}
	ApplyFlagsForEthConfigCobra(flags, cfg)

	require.Equal(t, prune.ArchiveMode, cfg.Prune)
	require.False(t, cfg.StateStream)
	require.True(t, cfg.ExperimentalBAL)
}

func TestApplyFlagsForEthConfigCobra_BlocksDistanceValues(t *testing.T) {
	cases := []struct {
		name       string
		blocksFlag string
		want       prune.BlockAmount
	}{
		{name: "alias keep-post-merge", blocksFlag: "keep-post-merge", want: prune.KeepPostMergeBlocksPruneMode},
		{name: "alias keep-all", blocksFlag: "keep-all", want: prune.KeepAllBlocksPruneMode},
		{name: "numeric sentinel still works", blocksFlag: "18446744073709551615", want: prune.KeepPostMergeBlocksPruneMode},
		{name: "finite numeric", blocksFlag: "262144", want: prune.Distance(262_144)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			flags.String(PruneModeFlag.Name, PruneModeFlag.Value, PruneModeFlag.Usage)
			flags.String(PruneBlocksDistanceFlag.Name, PruneBlocksDistanceFlag.Value, PruneBlocksDistanceFlag.Usage)
			require.NoError(t, flags.Set(PruneModeFlag.Name, "archive"))
			require.NoError(t, flags.Set(PruneBlocksDistanceFlag.Name, tc.blocksFlag))

			cfg := &ethconfig.Config{}
			ApplyFlagsForEthConfigCobra(flags, cfg)
			require.Equal(t, tc.want, cfg.Prune.Blocks)
		})
	}
}

func TestApplyFlagsForEthConfigCobra_CommitmentHistoryDistanceValues(t *testing.T) {
	cases := []struct {
		name string
		flag string
		want prune.BlockAmount
	}{
		{name: "alias keep-all", flag: "keep-all", want: prune.KeepAllBlocksPruneMode},
		{name: "finite numeric", flag: "100000", want: prune.Distance(100_000)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			flags.String(PruneModeFlag.Name, PruneModeFlag.Value, PruneModeFlag.Usage)
			flags.String(utils.CommitmentHistoryDistanceFlag.Name, utils.CommitmentHistoryDistanceFlag.Value, utils.CommitmentHistoryDistanceFlag.Usage)
			require.NoError(t, flags.Set(PruneModeFlag.Name, "archive"))
			require.NoError(t, flags.Set(utils.CommitmentHistoryDistanceFlag.Name, tc.flag))

			cfg := &ethconfig.Config{}
			ApplyFlagsForEthConfigCobra(flags, cfg)
			require.Equal(t, tc.want, cfg.Prune.CommitmentHistory)
		})
	}
}
