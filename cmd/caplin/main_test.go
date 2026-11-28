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

package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/cmd/caplin/caplincli"
	"github.com/erigontech/erigon/cmd/caplin/caplinflags"
	"github.com/erigontech/erigon/cmd/sentinel/sentinelcli"
	"github.com/erigontech/erigon/cmd/sentinel/sentinelflags"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

func TestCaplinSentinelBindFlags(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		host string
		port uint64
	}{
		{name: "defaults", host: "localhost", port: 7777},
		{name: "custom", args: []string{"--sentinel.addr=127.0.0.2", "--sentinel.port=8888"}, host: "127.0.0.2", port: 8888},
		{name: "ephemeral", args: []string{"--sentinel.addr=127.0.0.1", "--sentinel.port=0"}, host: "127.0.0.1", port: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			addr := sentinelflags.SentinelServerAddr
			port := sentinelflags.SentinelServerPort
			cmd := &cli.Command{
				Flags: []cli.Flag{&addr, &port},
				Action: func(_ context.Context, cmd *cli.Command) error {
					cliConfig := &caplincli.CaplinCliCfg{
						SentinelCliCfg: &sentinelcli.SentinelCliCfg{},
					}
					config, err := newCaplinConfig(cmd, cliConfig)
					require.NoError(t, err)
					require.Equal(t, test.host, config.SentinelAddr)
					require.Equal(t, test.port, config.SentinelPort)
					return nil
				},
			}
			require.NoError(t, cmd.Run(t.Context(), append([]string{"caplin"}, test.args...)))
		})
	}
}

func TestNewCaplinConfigArchiveRetention(t *testing.T) {
	mode := utils.CaplinPruneModeFlag
	chain := utils.ChainFlag
	cmd := &cli.Command{
		Flags: []cli.Flag{&mode, &chain},
		Action: func(_ context.Context, cmd *cli.Command) error {
			cfg := &caplincli.CaplinCliCfg{SentinelCliCfg: &sentinelcli.SentinelCliCfg{}}
			config, err := newCaplinConfig(cmd, cfg)
			require.NoError(t, err)
			require.Equal(t, clparams.CaplinPruneModeArchive, config.RequestedRetention.Mode)
			require.True(t, config.ArchiveBlocks)
			require.True(t, config.ArchiveBlobs)
			require.True(t, config.ArchiveStates)
			return nil
		},
	}
	require.NoError(t, cmd.Run(t.Context(), []string{"caplin", "--chain=mainnet", "--caplin.prune.mode=archive"}))
}

func TestNewCaplinConfigRejectsUnknownRetentionChain(t *testing.T) {
	mode := utils.CaplinPruneModeFlag
	chain := utils.ChainFlag
	cmd := &cli.Command{
		Flags: []cli.Flag{&mode, &chain},
		Action: func(_ context.Context, cmd *cli.Command) error {
			_, err := newCaplinConfig(cmd, &caplincli.CaplinCliCfg{SentinelCliCfg: &sentinelcli.SentinelCliCfg{}})
			return err
		},
	}
	err := cmd.Run(t.Context(), []string{"caplin", "--chain=unknown", "--caplin.prune.mode=archive"})
	require.ErrorContains(t, err, "beacon chain config not found")
}

func TestEmptySidecarFlagRequiresPrunedMode(t *testing.T) {
	cmd := app.MakeApp("caplin", func(_ context.Context, cmd *cli.Command) error {
		cfg, err := newCaplinConfig(cmd, &caplincli.CaplinCliCfg{SentinelCliCfg: &sentinelcli.SentinelCliCfg{}})
		if err == nil {
			t.Fatalf("explicit --caplin.prune.sidecars= accepted with mode=%s", cfg.RequestedRetention.Mode)
		}
		return nil
	}, append(caplinflags.CliFlags, sentinelflags.CliFlags...))
	if err := cmd.Run(t.Context(), []string{"caplin", "--caplin.prune.sidecars="}); err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneRetentionRefusal(t *testing.T) {
	const envDir = "CAPLIN_TEST_REFUSAL_DATADIR"
	if root := os.Getenv(envDir); root != "" {
		mode := utils.CaplinPruneModeFlag
		chain := utils.ChainFlag
		data := utils.DataDirFlag
		cmd := &cli.Command{Flags: []cli.Flag{&mode, &chain, &data}, Action: func(ctx context.Context, cmd *cli.Command) error {
			config, err := newCaplinConfig(cmd, &caplincli.CaplinCliCfg{SentinelCliCfg: &sentinelcli.SentinelCliCfg{}})
			require.NoError(t, err)
			config.DisabledCheckpointSync = true
			config.CaplinDiscoveryAddr = "127.0.0.1"
			config.CaplinDisableQUIC = true
			config.MaxPeerCount = 5
			network := clparams.NetworkConfigs[config.NetworkId]
			network.BootNodes = nil
			clparams.NetworkConfigs[config.NetworkId] = network
			return caplin1.RunCaplinService(ctx, nil, config, datadir.New(cmd.String(data.Name)), nil, nil, nil, nil)
		}}
		err := cmd.Run(t.Context(), []string{"caplin", "--chain=mainnet", "--caplin.prune.mode=archive", "--datadir=" + root})
		require.ErrorContains(t, err, "caplin retention request moves away")
		t.Log(err)
		return
	}
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	spec, err := (clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec}).Normalize(&clparams.MainnetBeaconConfig)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error { return beacon_indicies.WriteCaplinRetention(tx, spec) }))
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStandaloneRetentionRefusal$", "-test.v")
	child.Env = append(os.Environ(), envDir+"="+dirs.DataDir)
	output, err := child.CombinedOutput()
	require.NoError(t, err, "%s", output)

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		got, found, err := beacon_indicies.ReadCaplinRetention(tx)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, spec, got)
		n, err := tx.Count(kv.BeaconBlocks)
		require.NoError(t, err)
		if n != 0 {
			t.Errorf("refused start: BeaconBlocks rows=%d, want 0", n)
		}
		return nil
	}))
	for line := range strings.SplitSeq(string(output), "\n") {
		if _, hint, ok := strings.Cut(line, "moves away from the stored regime: "); ok {
			for command := range strings.SplitSeq(hint, "; ") {
				if !strings.Contains(command, dirs.DataDir) {
					t.Errorf("missing datadir: %s", command)
				}
			}
			if !strings.Contains(hint, "; caplin ") {
				t.Errorf("standalone restart: %s", hint)
			}
			return
		}
	}
	t.Fatal("missing refusal in child output")
}
