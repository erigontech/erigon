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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/genesisdb"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	cliutils "github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/snaptype"
	erigoncli "github.com/erigontech/erigon/node/cli"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
)

func writeRegimeFixture(t *testing.T, retention clparams.CaplinRetention) datadir.Dirs {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(123, common.Hash{1}), []byte{1}); err != nil {
			return err
		}
		return beacon_indicies.WriteCaplinRetention(tx, retention)
	}))
	db.Close()
	return dirs
}

func runRegime(t *testing.T, dirs datadir.Dirs, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	args = append([]string{"capcli", "seg", "regime"}, args...)
	args = append(args, "--datadir", dirs.DataDir, "--chain", "mainnet")
	err := app.Run(t.Context(), args)
	return output.String(), err
}

func TestCaplinRegimeShow(t *testing.T) {
	dirs := writeRegimeFixture(t, clparams.CaplinRetention{Mode: clparams.CaplinPruneModePruned, Blocks: clparams.CaplinAllFloor()})
	output, err := runRegime(t, dirs, "show")
	require.NoError(t, err)
	require.Contains(t, output, "stored regime pruned")
	require.Contains(t, output, "floor blocks all at slot 0")
	require.Contains(t, output, "oldest blocks 123")
	require.Contains(t, output, "legacy flags --caplin.blocks-archive")
}

func TestCaplinRegimeSetUsesSharedHintAndPersistsFloors(t *testing.T) {
	dirs := writeRegimeFixture(t, clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec})
	output, err := runRegime(t, dirs, "set", "archive")
	require.NoError(t, err)
	currentSlot, err := caplinCurrentSlotForDatadir(t.Context(), dirs, "mainnet", &clparams.MainnetBeaconConfig)
	require.NoError(t, err)
	oldest := clparams.OldestStoredResult{Blocks: clparams.StoredSlot{Slot: 123, Holds: true}}
	archive, err := (clparams.CaplinRetention{Mode: clparams.CaplinPruneModeArchive}).Normalize(&clparams.MainnetBeaconConfig)
	require.NoError(t, err)
	wantHint := clparams.CaplinRetentionHint(archive, oldest, currentSlot, &clparams.MainnetBeaconConfig, "caplin", dirs.DataDir)
	require.Equal(t, wantHint+"\n", output)

	output, err = runRegime(t, dirs, "set", "spec")
	require.NoError(t, err)
	require.Equal(t, "stored regime spec\n", output)
	_, err = runRegime(t, dirs, "set", "archive")
	require.NoError(t, err)

	output, err = runRegime(t, dirs, "set", "pruned", "--distance=all", "--sidecars=slot:14000000")
	require.NoError(t, err)
	require.Equal(t, "stored regime pruned\n", output)

	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	stored, found, err := caplinRetentionStored(t.Context(), db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, clparams.CaplinSlotFloor(14_000_000), stored.Sidecars)
	db.Close()

	_, err = runRegime(t, dirs, "set", "pruned", "--distance=33024", "--sidecars=all")
	require.EqualError(t, err, "sidecars reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.sidecars")
	db, err = caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	stored, found, err = caplinRetentionStored(t.Context(), db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, clparams.CaplinSlotFloor(14_000_000), stored.Sidecars)
	db.Close()
	require.False(t, strings.Contains(output, "capcli seg reset"))
}

func regimeNodeConfig(t *testing.T, dirs datadir.Dirs, args ...string) clparams.CaplinConfig {
	t.Helper()
	cfg := ethconfig.Defaults
	require.NoError(t, os.MkdirAll(filepath.Join(dirs.DataDir, "logs"), 0o700))
	flags := make([]cli.Flag, 0, len(erigoncli.DefaultFlags))
	for _, flag := range erigoncli.DefaultFlags {
		value := reflect.ValueOf(flag).Elem()
		copy := reflect.New(value.Type())
		copy.Elem().Set(value)
		flags = append(flags, copy.Interface().(cli.Flag))
	}
	app := &cli.Command{Name: "erigon", Flags: flags, Action: func(ctx context.Context, c *cli.Command) error {
		cliutils.SetEthConfig(ctx, c, &nodecfg.Config{Dirs: dirs}, &cfg, log.Root())
		return nil
	}}
	require.NoError(t, app.Run(t.Context(), append([]string{"erigon"}, args...)))
	return cfg.CaplinConfig
}

func TestRegimeSetEmbeddedRestart(t *testing.T) {
	stored := clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec}
	dirs := writeRegimeFixture(t, stored)
	writeRegimeExecutionChain(t, dirs)
	node := regimeNodeConfig(t, dirs, "--caplin.prune.mode=archive")
	oldest := clparams.OldestStoredResult{Blocks: clparams.StoredSlot{Slot: 123, Holds: true}}
	_, action, refusal := clparams.ResolveRetention(stored, true, node.RequestedRetention, node.RetentionSource, oldest, 15_000_000, &clparams.MainnetBeaconConfig, node.NodeBinary, dirs.DataDir)
	require.Equal(t, clparams.CaplinRetentionActionRefuse, action)
	require.Error(t, refusal)
	_, sequence, found := strings.Cut(refusal.Error(), ": ")
	require.True(t, found)
	output, err := runRegime(t, dirs, "set", "archive")
	require.NoError(t, err)
	if strings.TrimSuffix(output, "\n") != sequence {
		last := strings.Split(output, "; ")
		t.Fatalf("restart executable = %q, want erigon", strings.Fields(last[len(last)-1])[0])
	}
}

func TestRegimeShowAfterFixedBlockSet(t *testing.T) {
	dirs := writeRegimeFixture(t, clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec})
	_, err := runRegime(t, dirs, "set", "pruned", "--distance=slot:10000")
	require.NoError(t, err)
	output, err := runRegime(t, dirs, "show")
	if err != nil {
		t.Fatalf("show after successful set: %v", err)
	}
	require.Contains(t, output, "oldest blocks 123")
}

func TestRegimeShowLegacyColumns(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	for _, original := range []clparams.CaplinConfig{
		{ColumnKeepSlots: 262144},
		{ColumnKeepSlots: 262144, ArchiveBlocks: true, ArchiveStates: true},
	} {
		stored, exact, err := clparams.LegacyRetention(original, cfg)
		require.NoError(t, err)
		require.True(t, exact)
		dirs := writeRegimeFixture(t, stored)
		output, err := runRegime(t, dirs, "show")
		require.NoError(t, err)
		_, rest, found := strings.Cut(output, "legacy flags ")
		require.True(t, found)
		line, _, _ := strings.Cut(rest, "\n")
		node := regimeNodeConfig(t, dirs, strings.Fields(line)...)
		if node.RequestedRetention != stored || node.RetentionSource != clparams.CaplinRetentionSourceLegacyExact {
			t.Errorf("%s maps to %s, want %s", line, node.RequestedRetention, stored)
		}
	}
}

func TestRegimeSetNestingBeforeDirectoryWrites(t *testing.T) {
	dirs := datadir.Open(t.TempDir())
	unlock, err := dirs.TryFlock()
	require.NoError(t, err)
	unlock()
	_, err = runRegime(t, dirs, "set", "pruned", "--distance=33024", "--sidecars=all")
	require.ErrorContains(t, err, "sidecars reach below blocks")
	entries, err := os.ReadDir(dirs.DataDir)
	require.NoError(t, err)
	if len(entries) != 1 || entries[0].Name() != "LOCK" {
		t.Fatalf("datadir entries after nesting refusal = %d, want 1 (LOCK)", len(entries))
	}
}

func TestRegimeSetNestingWithoutStoredRegime(t *testing.T) {
	dirs := datadir.Open(t.TempDir())
	_, err := runRegime(t, dirs, "set", "pruned", "--distance=33024", "--sidecars=all")
	require.EqualError(t, err, "sidecars reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.sidecars")
	entries, err := os.ReadDir(dirs.DataDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "LOCK", entries[0].Name())
	_, err = os.Stat(dirs.CaplinIndexing)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRegimeGnosisClock(t *testing.T) {
	_, cfg, network, err := clparams.GetConfigsByNetworkName("gnosis")
	require.NoError(t, err)
	genesis, err := initial_state.GetGenesisState(t.Context(), network)
	require.NoError(t, err)
	clock := eth_clock.NewEthereumClock(genesis.GenesisTime(), genesis.GenesisValidatorsRoot(), cfg)
	before := clock.GetCurrentSlot()
	boundary := (before - 2*cfg.MinEpochsForBlobSidecarsRequests*cfg.SlotsPerEpoch) / snaptype.CaplinMergeLimit * snaptype.CaplinMergeLimit
	distance := (before-boundary)/cfg.SlotsPerEpoch + 32
	stored := clparams.CaplinRetention{Mode: clparams.CaplinPruneModePruned, Blocks: clparams.CaplinAllFloor(), Sidecars: clparams.CaplinDistanceFloor(distance)}
	dirs := writeRegimeFixture(t, stored)
	run := func(args ...string) string {
		var output bytes.Buffer
		app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output}
		args = append(append([]string{"capcli", "seg", "regime"}, args...), "--datadir", dirs.DataDir, "--chain", "gnosis")
		require.NoError(t, app.Run(t.Context(), args))
		return output.String()
	}

	output := run("show")
	after := clock.GetCurrentSlot()
	_, line, found := strings.Cut(output, "current slot ")
	require.True(t, found)
	var got uint64
	_, err = fmt.Sscan(line, &got)
	require.NoError(t, err)
	if got < before || got > after {
		t.Errorf("show slot = %d, node clock = [%d, %d]", got, before, after)
	}

	floors, err := stored.Floors(cfg, after)
	require.NoError(t, err)
	requested := stored
	requested.Sidecars = clparams.CaplinSlotFloor(floors.Sidecars)
	oldest := clparams.OldestStoredResult{Blocks: clparams.StoredSlot{Slot: 123, Holds: true}}
	_, action, err := clparams.ResolveRetention(stored, true, requested, clparams.CaplinRetentionSourceNewFlags, oldest, after, cfg, "caplin", dirs.DataDir)
	require.NoError(t, err)
	require.Equal(t, clparams.CaplinRetentionActionStore, action)
	output = run("set", "pruned", "--distance=all", fmt.Sprintf("--sidecars=slot:%d", floors.Sidecars))
	if output != "stored regime pruned\n" {
		t.Errorf("equal node floors: output starts with %q, reset printed = %t", strings.Fields(output)[0], strings.Contains(output, "capcli seg reset"))
	}
}

func TestRegimeShowDevConfig(t *testing.T) {
	dirs := writeRegimeFixture(t, clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec})
	node := regimeNodeConfig(t, dirs, "--chain=dev", "--dev-validator-count=4")
	cfg, err := clparams.ResolveBeaconConfig(uint64(node.NetworkId), node.CustomConfigPath, node.IsDevnet())
	require.NoError(t, err)
	require.FileExists(t, node.CustomGenesisStatePath)
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output}
	err = app.Run(t.Context(), []string{"capcli", "seg", "regime", "show", "--datadir", dirs.DataDir, "--chain", "dev"})
	if err != nil {
		t.Fatalf("show with node config (%d slots/epoch): %v", cfg.SlotsPerEpoch, err)
	}
	require.Contains(t, output.String(), "stored regime spec")
}

func TestRegimeSetBeforeGenesis(t *testing.T) {
	_, cfg, network, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	state, err := initial_state.GetGenesisState(t.Context(), network)
	require.NoError(t, err)
	state.SetGenesisTime(uint64(time.Now().Add(24 * time.Hour).Unix()))
	dirs := datadir.New(t.TempDir())
	db := genesisdb.NewGenesisDB(cfg, dirs.CaplinGenesis)
	require.NoError(t, db.Initialize(state))
	state, err = db.ReadGenesisState()
	require.NoError(t, err)
	clock := eth_clock.NewEthereumClock(state.GenesisTime(), state.GenesisValidatorsRoot(), cfg)
	require.Zero(t, clock.GetCurrentSlot())
	request := clparams.CaplinRetention{Mode: clparams.CaplinPruneModePruned, Blocks: clparams.CaplinSlotFloor(10000)}
	_, nesting := request.Floors(cfg, clock.GetCurrentSlot())
	require.ErrorContains(t, nesting, "sidecars reach below blocks")
	_, err = runRegime(t, dirs, "set", "pruned", "--distance=slot:10000")
	if err == nil {
		index, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
		require.NoError(t, err)
		defer index.Close()
		value, found, err := caplinRetentionStored(t.Context(), index)
		require.NoError(t, err)
		require.True(t, found)
		t.Fatalf("set stored %s; node at slot 0 rejects it: %v", value, nesting)
	}
	require.EqualError(t, err, nesting.Error())
}

func TestRegimeSetRepairsStaleNesting(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	previous := clparams.CaplinRetention{Mode: clparams.CaplinPruneModePruned, Sidecars: clparams.CaplinSlotFloor(10000)}
	previous, err := previous.Normalize(cfg)
	require.NoError(t, err)
	_, err = previous.Floors(cfg, 1_000_000)
	require.NoError(t, err)
	dirs := writeRegimeFixture(t, previous)
	_, err = runRegime(t, dirs, "set", "archive")
	if err != nil {
		t.Fatalf("set archive on previously valid regime: %v", err)
	}
	index, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	defer index.Close()
	value, found, err := caplinRetentionStored(t.Context(), index)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, clparams.CaplinPruneModeArchive, value.Mode)
}
