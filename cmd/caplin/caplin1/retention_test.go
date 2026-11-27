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

package caplin1

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/urfave/cli/v3"
)

func TestResolveAndStoreRetention(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.CaplinDB)
	t.Cleanup(db.Close)
	beaconCfg := clparams.MainnetBeaconConfig
	dirs := datadir.New(t.TempDir())
	defaultConfig := clparams.CaplinConfig{RequestedRetention: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec}, RetentionSource: clparams.CaplinRetentionSourceDefault}
	require.NoError(t, resolveAndStoreRetention(t.Context(), db, clparams.OldestStoredResult{}, &defaultConfig, &beaconCfg, 15_660_000, dirs, log.New()))

	var stored clparams.CaplinRetention
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var found bool
		var err error
		stored, found, err = beacon_indicies.ReadCaplinRetention(tx)
		require.NoError(t, err)
		require.True(t, found)
		return nil
	}))
	want, err := defaultConfig.RequestedRetention.Normalize(&beaconCfg)
	require.NoError(t, err)
	require.Equal(t, want, stored)

	secondConfig := clparams.CaplinConfig{RequestedRetention: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec}, RetentionSource: clparams.CaplinRetentionSourceDefault}
	require.NoError(t, resolveAndStoreRetention(t.Context(), db, clparams.OldestStoredResult{}, &secondConfig, &beaconCfg, 15_660_000, dirs, log.New()))
	require.Equal(t, clparams.CaplinPruneModeSpec, secondConfig.RequestedRetention.Mode)

	awayConfig := clparams.CaplinConfig{RequestedRetention: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeArchive}, RetentionSource: clparams.CaplinRetentionSourceNewFlags}
	err = resolveAndStoreRetention(t.Context(), db, clparams.OldestStoredResult{}, &awayConfig, &beaconCfg, 15_660_000, dirs, log.New())
	require.ErrorContains(t, err, "capcli seg regime set")
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		got, found, err := beacon_indicies.ReadCaplinRetention(tx)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, stored, got)
		return nil
	}))
}

func TestStartupRetentionMatrix(t *testing.T) {
	if os.Getenv("CAPLIN_RETENTION_MATRIX_CHILD") == "1" {
		args := strings.Split(os.Getenv("CAPLIN_RETENTION_MATRIX_ARGS"), "\x1f")
		parseStartupRetentionConfig(t, datadir.New(t.TempDir()), args...)
		return
	}
	beaconCfg := clparams.MainnetBeaconConfig
	sidecarMin := max(beaconCfg.MinEpochsForBlobSidecarsRequests, beaconCfg.MinEpochsForDataColumnSidecarsRequests)
	columnsValues := []uint64{0, sidecarMin * beaconCfg.SlotsPerEpoch, 1, sidecarMin*beaconCfg.SlotsPerEpoch + 1}
	type testCase struct {
		name    string
		args    []string
		want    clparams.CaplinConfig
		inexact bool
	}
	var cases []testCase
	for bits := range 16 {
		blocks := bits&1 != 0
		states := bits&2 != 0
		blobs := bits&4 != 0
		noPruning := bits&8 != 0
		for _, from := range []struct {
			set   bool
			value uint64
		}{{}, {set: true}, {set: true, value: 12345}} {
			for _, columns := range columnsValues {
				for _, snapgen := range []bool{false, true} {
					if from.value > 0 && snapgen {
						continue
					}
					archiveBlobs := blobs || from.set
					want := clparams.CaplinConfig{
						ArchiveBlocks:             blocks || states || archiveBlobs,
						ArchiveBlobs:              archiveBlobs,
						ArchiveStates:             states,
						BlobPruningDisabled:       noPruning,
						ColumnKeepSlots:           columns,
						SnapshotGenerationEnabled: snapgen,
					}
					if from.set {
						want.BlobsArchiveFromSlot = from.value
					}
					cases = append(cases, testCase{
						name:    fmt.Sprintf("%d/from-%t-%d/columns-%d/snapgen-%t", bits, from.set, from.value, columns, snapgen),
						args:    startupRetentionArgs(bits, from.set, from.value, columns, snapgen),
						want:    want,
						inexact: columns != 0 || noPruning && (!archiveBlobs || from.value > 0),
					})
				}
			}
		}
	}

	for _, test := range cases {
		if test.want.BlobPruningDisabled && !test.want.ArchiveBlocks {
			t.Run(test.name, func(t *testing.T) {
				child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestStartupRetentionMatrix$", "-test.count=1")
				child.Env = append(os.Environ(),
					"CAPLIN_RETENTION_MATRIX_CHILD=1",
					"CAPLIN_RETENTION_MATRIX_ARGS="+strings.Join(test.args, "\x1f"),
				)
				output, err := child.CombinedOutput()
				require.Error(t, err)
				require.Contains(t, string(output), "--caplin.blobs-no-pruning requires --caplin.blocks-archive")
			})
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			for _, nonEmpty := range []bool{false, true} {
				t.Run(fmt.Sprintf("non-empty-%t", nonEmpty), func(t *testing.T) {
					dirs := datadir.New(t.TempDir())
					cfg := parseStartupRetentionConfig(t, dirs, test.args...)
					db := mdbxtest.NewTestDB(t, dbcfg.CaplinDB)
					oldest := clparams.OldestStoredResult{}
					if nonEmpty {
						require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
							return tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(0, common.Hash{1}), []byte{1})
						}))
						oldest = clparams.OldestStoredResult{
							Blocks:   clparams.StoredSlot{Holds: true},
							Sidecars: clparams.StoredSlot{Holds: true},
							States:   clparams.StoredSlot{Holds: true},
						}
					}
					err := resolveAndStoreRetention(t.Context(), db, oldest, &cfg.CaplinConfig, &beaconCfg, 15_660_000, dirs, log.New())
					if !nonEmpty && test.inexact {
						require.ErrorContains(t, err, "capcli seg regime set")
						return
					}
					require.NoError(t, err)
					require.Equal(t, test.want.ArchiveBlocks, cfg.CaplinConfig.ArchiveBlocks)
					require.Equal(t, test.want.ArchiveBlobs, cfg.CaplinConfig.ArchiveBlobs)
					require.Equal(t, test.want.ArchiveStates, cfg.CaplinConfig.ArchiveStates)
					require.Equal(t, test.want.BlobPruningDisabled, cfg.CaplinConfig.BlobPruningDisabled)
					require.Equal(t, test.want.BlobsArchiveFromSlot, cfg.CaplinConfig.BlobsArchiveFromSlot)
					require.Equal(t, test.want.ColumnKeepSlots, cfg.CaplinConfig.ColumnKeepSlots)
					require.Equal(t, test.want.SnapshotGenerationEnabled, cfg.CaplinConfig.SnapshotGenerationEnabled)
				})
			}
		})
	}
}

func startupRetentionArgs(bits int, fromSet bool, fromSlot, columns uint64, snapgen bool) []string {
	args := []string{"--chain=mainnet"}
	flags := []struct {
		bit  int
		name string
	}{
		{1, "--caplin.blocks-archive"},
		{2, "--caplin.states-archive"},
		{4, "--caplin.blobs-archive"},
		{8, "--caplin.blobs-no-pruning"},
	}
	for _, flag := range flags {
		if bits&flag.bit != 0 {
			args = append(args, flag.name)
		}
	}
	if fromSet {
		args = append(args, fmt.Sprintf("--caplin.blobs-archive-from-slot=%d", fromSlot))
	}
	if columns != 0 {
		args = append(args, fmt.Sprintf("--caplin.columns-keep-slots=%d", columns))
	}
	if snapgen {
		args = append(args, "--caplin.snapgen")
	}
	return args
}

func parseStartupRetentionConfig(t *testing.T, dirs datadir.Dirs, args ...string) ethconfig.Config {
	t.Helper()
	flagValues := []cli.Flag{
		&utils.CaplinArchiveBlocksFlag,
		&utils.CaplinArchiveStatesFlag,
		&utils.CaplinArchiveBlobsFlag,
		&utils.CaplinArchiveBlobsFromSlotFlag,
		&utils.CaplinDisableBlobPruningFlag,
		&utils.CaplinColumnKeepSlotsFlag,
		&utils.CaplinEnableSnapshotGeneration,
		&utils.ChainFlag,
	}
	flags := make([]cli.Flag, 0, len(flagValues))
	for _, flag := range flagValues {
		value := reflect.ValueOf(flag).Elem()
		copy := reflect.New(value.Type())
		copy.Elem().Set(value)
		flags = append(flags, copy.Interface().(cli.Flag))
	}
	cfg := ethconfig.Defaults
	app := &cli.Command{
		Name:  "erigon",
		Flags: flags,
		Action: func(ctx context.Context, cmd *cli.Command) error {
			utils.SetEthConfig(ctx, cmd, &nodecfg.Config{Dirs: dirs}, &cfg, log.Root())
			return nil
		},
	}
	require.NoError(t, app.Run(t.Context(), append([]string{"erigon"}, args...)))
	return cfg
}

func TestRefusedInferenceLeavesDBUnchanged(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.CaplinDB)
	chain := &clparams.MainnetBeaconConfig
	cfg := clparams.CaplinConfig{ColumnKeepSlots: 1}
	requested, exact, err := clparams.LegacyRetention(cfg, chain)
	require.NoError(t, err)
	require.False(t, exact)
	cfg.RequestedRetention, cfg.RetentionSource = requested, clparams.CaplinRetentionSourceLegacyInexact
	oldest := clparams.OldestStoredResult{
		Blocks:   clparams.StoredSlot{Slot: 14_000_000, Holds: true},
		Sidecars: clparams.StoredSlot{Slot: 13_000_000, Holds: true},
	}
	err = resolveAndStoreRetention(t.Context(), db, oldest, &cfg, chain, 15_660_000, datadir.New(t.TempDir()), log.New())
	require.ErrorContains(t, err, "sidecars reach below blocks")
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		got, found, err := beacon_indicies.ReadCaplinRetention(tx)
		require.NoError(t, err)
		if found {
			t.Fatalf("refused start stored %s %s", got.Mode, got)
		}
		return nil
	}))
}

func TestInheritedApplyFailureKeepsFields(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.CaplinDB)
	target := clparams.CaplinRetention{Mode: clparams.CaplinPruneModePruned, Blocks: clparams.CaplinSlotFloor(100_000), Sidecars: clparams.CaplinSlotFloor(200_000)}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error { return beacon_indicies.WriteCaplinRetention(tx, target) }))
	cfg := clparams.CaplinConfig{EnableEngineAPI: true, ColumnKeepSlots: 99}
	var buf bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&buf, log.LogfmtFormat()))
	err := resolveAndStoreRetention(t.Context(), db, clparams.OldestStoredResult{}, &cfg, &clparams.MainnetBeaconConfig, 15_660_000, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "kept parsed")
	if cfg.ColumnKeepSlots != 99 {
		t.Fatalf("logged kept parsed: ColumnKeepSlots=%d, want 99", cfg.ColumnKeepSlots)
	}
}

func TestRestartUsesRequestedColumnWindow(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.CaplinDB)
	dirs := datadir.New(t.TempDir())
	chain := &clparams.MainnetBeaconConfig
	for _, epochs := range []uint64{8193, 8192} {
		cfg := clparams.CaplinConfig{RequestedRetention: clparams.CaplinRetention{
			Mode: clparams.CaplinPruneModePruned, Blocks: clparams.CaplinAllFloor(), Sidecars: clparams.CaplinDistanceFloor(epochs),
		}, RetentionSource: clparams.CaplinRetentionSourceNewFlags}
		require.NoError(t, resolveAndStoreRetention(t.Context(), db, clparams.OldestStoredResult{}, &cfg, chain, 15_660_000, dirs, log.New()))
		if cfg.ColumnKeepSlots != epochs*chain.SlotsPerEpoch {
			t.Fatalf("explicit %d epochs: ColumnKeepSlots=%d, want %d", epochs, cfg.ColumnKeepSlots, epochs*chain.SlotsPerEpoch)
		}
	}
}

func TestInexactMigrationLogsArchiveCommand(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.CaplinDB)
	chain := &clparams.MainnetBeaconConfig
	cfg := clparams.CaplinConfig{ColumnKeepSlots: 1}
	requested, exact, err := clparams.LegacyRetention(cfg, chain)
	require.NoError(t, err)
	require.False(t, exact)
	cfg.RequestedRetention, cfg.RetentionSource = requested, clparams.CaplinRetentionSourceLegacyInexact
	var buf bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&buf, log.LogfmtFormat()))
	oldest := clparams.OldestStoredResult{Blocks: clparams.StoredSlot{Slot: 50_000, Holds: true}}
	require.NoError(t, resolveAndStoreRetention(t.Context(), db, oldest, &cfg, chain, 15_660_000, datadir.New(t.TempDir()), logger))
	if !strings.Contains(buf.String(), "capcli seg regime set archive") {
		t.Fatalf("missing archive command: %s", buf.String())
	}
}

func TestArchiveRetireHintNamesDatadir(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db, err := OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	var buf bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&buf, log.LogfmtFormat()))
	cfg := clparams.CaplinConfig{RetentionSource: clparams.CaplinRetentionSourceNewFlags, RequestedRetention: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeArchive}}
	oldest := clparams.OldestStoredResult{Blocks: clparams.StoredSlot{Slot: 1, Holds: true}}
	require.NoError(t, resolveAndStoreRetention(t.Context(), db, oldest, &cfg, &clparams.MainnetBeaconConfig, 15_660_000, dirs, logger))
	require.Contains(t, buf.String(), "mdbx_size=")
	if !strings.Contains(buf.String(), dirs.DataDir) {
		t.Fatalf("missing node datadir in retire hint: %s", buf.String())
	}
}

func TestArchiveLogsPartialStateDB(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db, err := OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error { return tx.Put(kv.StateEvents, base_encoding.Encode64ToBytes4(123), []byte{1}) }))
	oldest, err := antiquary.OldestStored(t.Context(), db, nil, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, oldest.StateTables[kv.StateEvents].Holds)
	var buf bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&buf, log.LogfmtFormat()))
	cfg := clparams.CaplinConfig{RequestedRetention: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeArchive}, RetentionSource: clparams.CaplinRetentionSourceNewFlags}
	require.NoError(t, resolveAndStoreRetention(t.Context(), db, oldest, &cfg, &clparams.MainnetBeaconConfig, 15_660_000, dirs, logger))
	if !strings.Contains(buf.String(), "mdbx_size=") || !strings.Contains(buf.String(), "capcli seg retire") {
		t.Fatalf("nonempty archive log: %s", buf.String())
	}
}

func TestInheritedBlobBoundAndSnapgen(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := mdbxtest.NewTestDB(t, dbcfg.CaplinDB)
	chain := &clparams.MainnetBeaconConfig
	bounded := clparams.CaplinConfig{ArchiveBlocks: true, ArchiveBlobs: true, BlobsArchiveFromSlot: 14_000_000}
	requested, exact, err := clparams.LegacyRetention(bounded, chain)
	require.NoError(t, err)
	require.True(t, exact)
	bounded.RequestedRetention, bounded.RetentionSource = requested, clparams.CaplinRetentionSourceLegacyExact
	require.NoError(t, resolveAndStoreRetention(t.Context(), db, clparams.OldestStoredResult{}, &bounded, chain, 15_660_000, dirs, log.New()))
	cfg := clparams.CaplinConfig{SnapshotGenerationEnabled: true}
	err = resolveAndStoreRetention(t.Context(), db, clparams.OldestStoredResult{}, &cfg, chain, 15_660_000, dirs, log.New())
	if err == nil && cfg.BlobsArchiveFromSlot > 0 && cfg.SnapshotGenerationEnabled {
		t.Fatalf("accepted snapgen=true with BlobsArchiveFromSlot=%d", cfg.BlobsArchiveFromSlot)
	}
}

func TestEngineAPIRestartKeepsBackfillDisabled(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.CaplinDB)
	dirs := datadir.New(t.TempDir())
	chain := &clparams.MainnetBeaconConfig
	cfg := clparams.CaplinConfig{RetentionSource: clparams.CaplinRetentionSourceNewFlags, RequestedRetention: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeArchive}}
	require.NoError(t, resolveAndStoreRetention(t.Context(), db, clparams.OldestStoredResult{}, &cfg, chain, 15_660_000, dirs, log.New()))
	cfg = clparams.CaplinConfig{EnableEngineAPI: true}
	err := resolveAndStoreRetention(t.Context(), db, clparams.OldestStoredResult{}, &cfg, chain, 15_660_000, dirs, log.New())
	if err == nil && (cfg.ArchiveBlocks || cfg.ArchiveStates || cfg.ArchiveBlobs) {
		t.Fatalf("engine API backfill: blocks=%t states=%t blobs=%t", cfg.ArchiveBlocks, cfg.ArchiveStates, cfg.ArchiveBlobs)
	}
}

func TestFreshGenesisRefusesInexact(t *testing.T) {
	const childDir = "CAPLIN_TEST_FRESH_DATADIR"
	root := os.Getenv(childDir)
	if root == "" {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFreshGenesisRefusesInexact$")
		child.Env = append(os.Environ(), childDir+"="+t.TempDir())
		output, err := child.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, output)
		}
		return
	}
	_, chain, networkID, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	network := clparams.NetworkConfigs[networkID]
	network.BootNodes = nil
	clparams.NetworkConfigs[networkID] = network
	cfg := clparams.CaplinConfig{NetworkId: networkID, DisabledCheckpointSync: true, CaplinDiscoveryAddr: "127.0.0.1", CaplinDisableQUIC: true, MaxPeerCount: 5, ColumnKeepSlots: 1}
	requested, exact, err := clparams.LegacyRetention(cfg, chain)
	require.NoError(t, err)
	require.False(t, exact)
	cfg.RequestedRetention, cfg.RetentionSource = requested, clparams.CaplinRetentionSourceLegacyInexact
	log.Root().SetHandler(log.FilterHandler(func(r *log.Record) bool {
		if r.Msg == "Caplin retention" && slices.Contains(r.Ctx, any("store")) {
			t.Fatalf("fresh datadir accepted: %v", r.Ctx)
		}
		return false
	}, log.DiscardHandler()))
	err = RunCaplinService(t.Context(), nil, cfg, datadir.New(root), nil, nil, nil, nil)
	require.ErrorContains(t, err, "empty datadir")
}
