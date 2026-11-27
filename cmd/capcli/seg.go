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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/persistence/genesisdb"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	cliutils "github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	datadirreset "github.com/erigontech/erigon/db/datadir/reset"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func segCommand() *cli.Command {
	return &cli.Command{
		Name:        "seg",
		Usage:       "inspect and manage snapshot segments",
		Description: "Read and manage Caplin snapshot files and retention metadata. Read-only commands do not take the datadir lock; commands that write segments, the index, or the stored regime take it.",
		Commands:    []*cli.Command{caplinDuCommand(), caplinRetireCommand(), caplinResetCommand(), caplinRegimeCommand(), caplinCheckManifestCommand()},
	}
}

func caplinRegimeCommand() *cli.Command {
	chainFlag := cli.StringFlag{Name: cliutils.ChainFlag.Name, Usage: cliutils.ChainFlag.Usage}
	return &cli.Command{
		Name:        "regime",
		Usage:       "show or set the Caplin retention regime; show reads without a lock, set writes with the datadir lock",
		Description: "Show reads the stored Caplin regime, oldest stored data, and local sidecar files without writing or taking the datadir lock. Set validates and writes a regime while holding the datadir lock.",
		Commands: []*cli.Command{
			{
				Name:        "show",
				Usage:       "show the stored Caplin retention regime; reads the index and local files without the datadir lock",
				Description: "Reads the stored regime, floors, oldest stored data, and matching legacy flags. It does not write the datadir or take its lock.",
				Flags:       []cli.Flag{&chainFlag, &cliutils.DataDirFlag},
				Action:      runCaplinRegimeShow,
			},
			{
				Name:        "set",
				Usage:       "set the Caplin retention regime; writes the index with the datadir lock",
				Description: "Validates and writes the requested Caplin regime and takes the datadir lock for the full operation.",
				ArgsUsage:   "<mode>",
				Flags: []cli.Flag{
					&chainFlag,
					&cliutils.DataDirFlag,
					&cli.StringFlag{Name: "distance", Usage: "block distance in epochs or all"},
					&cli.StringFlag{Name: "sidecars", Usage: "sidecar distance in epochs, all or slot:N"},
					&cli.StringFlag{Name: "states", Usage: "state distance in epochs or all"},
				},
				Action: runCaplinRegimeSet,
			},
		},
	}
}

func caplinResetCommand() *cli.Command {
	chainFlag := cli.StringFlag{Name: cliutils.ChainFlag.Name, Usage: cliutils.ChainFlag.Usage}
	return &cli.Command{
		Name:        "reset",
		Usage:       "reset Caplin snapshot segments; reads and writes files and the index with the datadir lock",
		Description: "Prints or applies a Caplin reset plan. Both dry runs and writes take the datadir lock; only a write changes files or index rows.",
		Flags: []cli.Flag{
			&chainFlag,
			&cliutils.DataDirFlag,
			&cli.BoolFlag{Name: "dry-run", Usage: "print the Caplin reset plan without changing the datadir"},
			&cli.BoolFlag{Name: "force", Usage: "delete Caplin files that have no preverified replacement"},
		},
		Action: runCaplinReset,
	}
}

func runCaplinReset(ctx context.Context, cmd *cli.Command) error {
	folder := outputFolder{}
	if err := folder.fromCmd(cmd); err != nil {
		return err
	}
	dirs, unlock, err := lockCaplinDirs(folder.Datadir, false)
	if err != nil {
		return err
	}
	defer unlock()

	chainName, _, err := caplinDuChainConfig(ctx, cmd, dirs)
	if err != nil {
		return err
	}
	items, err := caplinResetManifest(ctx, dirs, chainName)
	if err != nil {
		return err
	}
	db, err := datadirreset.OpenCaplinDB(ctx, dirs.CaplinIndexing, log.Root(), cmd.Bool("dry-run"))
	if err != nil {
		return err
	}
	if db != nil {
		defer db.Close()
	}
	plan, err := datadirreset.BuildCaplinResetPlan(ctx, dirs, items, db, true, log.Root())
	if err != nil {
		return err
	}
	writer := cmd.Writer
	if cmd.Bool("dry-run") {
		plan.Format(writer)
		return nil
	}
	if err := plan.Apply(ctx, db, dir.RemoveFile, cmd.Bool("force")); err != nil {
		return err
	}
	for _, file := range plan.Files {
		fmt.Fprintf(writer, "deleted %s %s\n", file.Family, file.ReportName())
	}
	for _, rows := range plan.DBRows {
		fmt.Fprintf(writer, "deleted db rows %s %d..%d %d\n", rows.Table, rows.From, rows.To, rows.Rows)
	}
	if plan.Progress != plan.ProgressAfter {
		fmt.Fprintf(writer, "progress %d -> %d\n", plan.Progress, plan.ProgressAfter)
	}
	fmt.Fprintln(writer, "stored regime kept")
	return nil
}

func caplinOldestStored(ctx context.Context, dirs datadir.Dirs, chainName string, beaconConfig *clparams.BeaconChainConfig, db kv.RoDB) (clparams.OldestStoredResult, error) {
	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = chainName
	var snapshots *freezeblocks.CaplinSnapshots
	if info, err := os.Stat(dirs.Snap); err == nil && info.IsDir() {
		snapshots = freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
		if err := snapshots.OpenFolder(); err != nil {
			return clparams.OldestStoredResult{}, err
		}
		defer snapshots.Close()
	}
	var stateSnapshots *snapshotsync.CaplinStateSnapshots
	if info, err := os.Stat(dirs.SnapCaplin); err == nil && info.IsDir() {
		stateSnapshots = snapshotsync.NewCaplinStateSnapshots(freezingCfg, dirs, snapshotsync.MakeCaplinStateSnapshotsTypes(db, beaconConfig), log.Root())
		if err := stateSnapshots.OpenFolder(); err != nil {
			return clparams.OldestStoredResult{}, err
		}
		defer stateSnapshots.Close()
	}
	var blobStorage blob_storage.BlobStorage
	blobPath := filepath.Join(dirs.CaplinBlobs, "chaindata")
	if fileExists(filepath.Join(blobPath, "mdbx.dat")) {
		blobDB, err := antiquary.OpenCaplinDuIndexDB(ctx, blobPath, log.Root())
		if err != nil {
			return clparams.OldestStoredResult{}, err
		}
		defer blobDB.Close()
		blobStorage = blob_storage.NewBlobStore(blobDB, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinBlobs))
	}
	var columnStorage blob_storage.DataColumnStorage
	if info, err := os.Stat(dirs.CaplinColumnData); err == nil && info.IsDir() {
		columnStorage = blob_storage.NewDataColumnStore(afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinColumnData), beaconConfig, beaconevents.NewEventEmitter())
	}
	oldest, err := antiquary.OldestStored(ctx, db, snapshots, stateSnapshots, blobStorage, columnStorage)
	if err != nil {
		return clparams.OldestStoredResult{}, err
	}
	hotSidecars, err := antiquary.CaplinDuOldestHotSlot(dirs.CaplinBlobs)
	if err != nil {
		return clparams.OldestStoredResult{}, err
	}
	if hotSidecars.Holds && (!oldest.Sidecars.Holds || hotSidecars.Slot < oldest.Sidecars.Slot) {
		oldest.Sidecars = hotSidecars
	}
	return oldest, nil
}

func caplinGenesisState(ctx context.Context, dirs datadir.Dirs, chainName string, beaconConfig *clparams.BeaconChainConfig) (*state.CachingBeaconState, error) {
	genesis := genesisdb.NewGenesisDB(beaconConfig, dirs.CaplinGenesis)
	initialized, err := genesis.IsInitialized()
	if err != nil {
		return nil, err
	}
	if initialized {
		return genesis.ReadGenesisState()
	}
	if chainName == networkname.Dev {
		data, err := os.ReadFile(filepath.Join(dirs.DataDir, "dev-beacon", "genesis.ssz"))
		if err != nil {
			return nil, err
		}
		genesisState := state.New(beaconConfig)
		version := beaconConfig.GetCurrentStateVersion(beaconConfig.GenesisEpoch)
		if err := genesisState.DecodeSSZ(data, int(version)); err != nil {
			return nil, err
		}
		return genesisState, nil
	}
	_, _, networkID, err := clparams.GetConfigsByNetworkName(chainName)
	if err != nil {
		return nil, err
	}
	return initial_state.GetGenesisState(ctx, networkID)
}

func caplinCurrentSlotForDatadir(ctx context.Context, dirs datadir.Dirs, chainName string, beaconConfig *clparams.BeaconChainConfig) (uint64, error) {
	genesisState, err := caplinGenesisState(ctx, dirs, chainName, beaconConfig)
	if err != nil {
		return 0, err
	}
	return eth_clock.NewEthereumClock(genesisState.GenesisTime(), genesisState.GenesisValidatorsRoot(), beaconConfig).GetCurrentSlot(), nil
}

func caplinRetentionBinary(dirs datadir.Dirs) string {
	if fileExists(filepath.Join(dirs.Chaindata, "mdbx.dat")) {
		return "erigon"
	}
	return "caplin"
}

func caplinRetentionStored(ctx context.Context, db kv.RoDB) (clparams.CaplinRetention, bool, error) {
	if db == nil {
		return clparams.CaplinRetention{}, false, nil
	}
	var stored clparams.CaplinRetention
	var found bool
	err := db.View(ctx, func(tx kv.Tx) error {
		var err error
		stored, found, err = beacon_indicies.ReadCaplinRetention(tx)
		return err
	})
	return stored, found, err
}

func runCaplinRegimeShow(ctx context.Context, cmd *cli.Command) error {
	folder := outputFolder{}
	if err := folder.fromCmd(cmd); err != nil {
		return err
	}
	dirs := datadir.Open(folder.Datadir)
	chainName, beaconConfig, err := caplinDuChainConfig(ctx, cmd, dirs)
	if err != nil {
		return err
	}
	indexDB, err := antiquary.OpenCaplinDuIndexDB(ctx, dirs.CaplinIndexing, log.Root())
	if err != nil {
		return err
	}
	if indexDB != nil {
		defer indexDB.Close()
	}
	stored, found, err := caplinRetentionStored(ctx, indexDB)
	if err != nil {
		return err
	}
	writer := cmd.Writer
	fmt.Fprintf(writer, "chain %s\n", chainName)
	if !found {
		fmt.Fprintln(writer, "stored regime none stored")
	} else {
		if beaconConfig == nil {
			return errors.New("caplin retention needs a beacon config; pass --chain")
		}
		stored, err = stored.Normalize(beaconConfig)
		if err != nil {
			return err
		}
		currentSlot, err := caplinCurrentSlotForDatadir(ctx, dirs, chainName, beaconConfig)
		if err != nil {
			return err
		}
		floors, err := stored.Floors(beaconConfig, currentSlot)
		if err != nil {
			return err
		}
		fmt.Fprintf(writer, "stored regime %s\n", stored.Mode)
		fmt.Fprintf(writer, "current slot %d\n", currentSlot)
		fmt.Fprintf(writer, "floor blocks %s at slot %d\n", stored.Blocks, floors.Blocks)
		fmt.Fprintf(writer, "floor sidecars %s at slot %d\n", stored.Sidecars, floors.Sidecars)
		fmt.Fprintf(writer, "floor states %s at slot %d\n", stored.States, floors.States)
		flags, exact, err := clparams.LegacyRetentionFlags(stored, beaconConfig)
		if err != nil {
			return err
		}
		if !exact || len(flags) == 0 {
			fmt.Fprintln(writer, "legacy flags none")
		} else {
			fmt.Fprintf(writer, "legacy flags %s\n", strings.Join(flags, " "))
		}
	}
	if beaconConfig == nil {
		fmt.Fprintln(writer, "current slot unavailable")
		fmt.Fprintln(writer, "oldest blocks none")
		fmt.Fprintln(writer, "oldest sidecars none")
		fmt.Fprintln(writer, "oldest states none")
		return nil
	}
	oldest, err := caplinOldestStored(ctx, dirs, chainName, beaconConfig, indexDB)
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "oldest blocks %s\n", caplinStoredSlotText(oldest.Blocks))
	fmt.Fprintf(writer, "oldest sidecars %s\n", caplinStoredSlotText(oldest.Sidecars))
	fmt.Fprintf(writer, "oldest states %s\n", caplinStoredSlotText(oldest.States))
	return nil
}

func caplinStoredSlotText(slot clparams.StoredSlot) string {
	if !slot.Holds {
		return "none"
	}
	return strconv.FormatUint(slot.Slot, 10)
}

func runCaplinRegimeSet(ctx context.Context, cmd *cli.Command) error {
	folder := outputFolder{}
	if err := folder.fromCmd(cmd); err != nil {
		return err
	}
	if cmd.Args().Len() != 1 {
		return errors.New("caplin seg regime set requires one mode")
	}
	dirs, unlock, err := lockCaplinDirs(folder.Datadir, false)
	if err != nil {
		return err
	}
	defer unlock()
	chainName, beaconConfig, err := caplinDuChainConfig(ctx, cmd, dirs)
	if err != nil {
		return err
	}
	if beaconConfig == nil {
		return errors.New("caplin retention needs a beacon config; pass --chain")
	}
	requested, err := clparams.ParseCaplinRetentionWithSlots(
		cmd.Args().First(),
		cmd.String("distance"),
		cmd.String("sidecars"),
		cmd.String("states"),
		cmd.IsSet("distance"),
		cmd.IsSet("sidecars"),
		cmd.IsSet("states"),
	)
	if err != nil {
		return err
	}
	requested, err = requested.Normalize(beaconConfig)
	if err != nil {
		return err
	}
	currentSlot, err := caplinCurrentSlotForDatadir(ctx, dirs, chainName, beaconConfig)
	if err != nil {
		return err
	}
	requestedFloors, err := requested.Floors(beaconConfig, currentSlot)
	if err != nil {
		return err
	}
	dirs = datadir.New(dirs.DataDir)
	db, err := caplin1.OpenCaplinIndexDb(ctx, dirs.CaplinIndexing)
	if err != nil {
		return err
	}
	defer db.Close()
	stored, found, err := caplinRetentionStored(ctx, db)
	if err != nil {
		return err
	}
	oldest, err := caplinOldestStored(ctx, dirs, chainName, beaconConfig, db)
	if err != nil {
		return err
	}
	var away bool
	if found {
		stored, err = stored.Normalize(beaconConfig)
		if err != nil {
			return err
		}
		if stored != requested {
			storedFloors, storedErr := stored.Floors(beaconConfig, currentSlot)
			away = storedErr != nil || clparams.CaplinRetentionMovesAwayFromTip(requestedFloors, storedFloors)
		}
	}
	if err := db.Update(ctx, func(tx kv.RwTx) error { return beacon_indicies.WriteCaplinRetention(tx, requested) }); err != nil {
		return err
	}
	writer := cmd.Writer
	if away {
		fmt.Fprintln(writer, clparams.CaplinRetentionHint(requested, oldest, currentSlot, beaconConfig, caplinRetentionBinary(dirs), dirs.DataDir))
		return nil
	}
	fmt.Fprintf(writer, "stored regime %s\n", requested.Mode)
	return nil
}

func caplinRetireCommand() *cli.Command {
	chainFlag := cli.StringFlag{Name: cliutils.ChainFlag.Name, Usage: cliutils.ChainFlag.Usage}
	return &cli.Command{
		Name:        "retire",
		Usage:       "retire Caplin snapshots; reads and writes files and the index with the datadir lock",
		Description: "Retires Caplin block, blob, and state data and may compact the Caplin index. It takes the datadir lock for the full operation.",
		Flags: []cli.Flag{
			&chainFlag,
			&cliutils.DataDirFlag,
			&cli.BoolFlag{Name: "no-compact", Usage: "skip Caplin database compaction"},
		},
		Action: runCaplinRetire,
	}
}

func runCaplinRetire(ctx context.Context, cmd *cli.Command) error {
	folder := outputFolder{}
	if err := folder.fromCmd(cmd); err != nil {
		return err
	}
	dirs, unlock, err := lockCaplinDirs(folder.Datadir, true)
	if err != nil {
		return err
	}
	defer unlock()

	chainName, beaconConfig, err := caplinDuChainConfig(ctx, cmd, dirs)
	if err != nil {
		return err
	}
	if beaconConfig == nil {
		return nil
	}
	freezingCfg := ethconfig.Defaults.Snapshot
	freezingCfg.ChainName = chainName
	snapshots := freezeblocks.NewCaplinSnapshots(freezingCfg, beaconConfig, dirs, log.Root())
	if err := snapshots.OpenFolder(); err != nil {
		return err
	}
	defer snapshots.Close()
	probe, err := antiquary.OpenCaplinDuIndexDB(ctx, dirs.CaplinIndexing, log.Root())
	if err != nil {
		return err
	}
	if probe == nil {
		return nil
	}
	var indexProgress uint64
	if err := probe.View(ctx, func(tx kv.Tx) error {
		var err error
		indexProgress, err = beacon_indicies.ReadLastBeaconSnapshot(tx)
		return err
	}); err != nil {
		probe.Close()
		return err
	}
	if indexProgress < snapshots.BlocksAvailable() {
		probe.Close()
		return fmt.Errorf("snapshot index progress %d is below snapshot coverage %d; start the node until its index pass completes", indexProgress, snapshots.BlocksAvailable())
	}
	needsWrite, err := caplinRetireNeedsWrite(ctx, probe, snapshots)
	probe.Close()
	if err != nil {
		return err
	}
	if !needsWrite {
		needsWrite, err = caplinBlobDataPresent(ctx, dirs)
		if err != nil {
			return err
		}
	}
	writer := cmd.Writer
	if !needsWrite {
		return compactCaplinDatabase(ctx, dirs, writer, cmd.Bool("no-compact"), false)
	}
	db, err := caplin1.OpenCaplinIndexDb(ctx, dirs.CaplinIndexing)
	if err != nil {
		return err
	}
	defer db.Close()
	blobStorage, closeBlobStorage, err := openCaplinBlobStore(ctx, dirs, false)
	if err != nil {
		return err
	}
	defer closeBlobStorage()

	databaseChanged := false
	view := snapshots.View()
	hasBlockSegments := len(view.BeaconBlocks()) != 0
	hasFrozenBlockSegments := hasBlockSegments
	coveredTo := snapshots.BlocksAvailable()
	view.Close()
	if !hasBlockSegments {
		var firstSlot uint64
		var found bool
		err = db.View(ctx, func(tx kv.Tx) error {
			var err error
			firstSlot, found, err = antiquary.CaplinBeaconBlocksFirstSlot(tx)
			return err
		})
		if err != nil {
			return err
		}
		if !found || firstSlot > coveredTo+1 {
			if found {
				fmt.Fprintf(writer, "skip blocks: hot beacon blocks start at %d, frozen range starts at %d\n", firstSlot, coveredTo+1)
			} else {
				fmt.Fprintf(writer, "skip blocks: no hot beacon blocks at frozen range start %d\n", coveredTo+1)
			}
		} else {
			hasBlockSegments = true
		}
	}
	if hasBlockSegments {
		linked, gap, err := caplinFrozenBlockLink(ctx, db, snapshots, hasFrozenBlockSegments)
		if err != nil {
			return err
		}
		if !linked {
			fmt.Fprintf(writer, "skip blocks: backfill gap at %d after frozen range ending %d\n", gap, coveredTo)
			hasBlockSegments = false
		}
	}
	for hasBlockSegments {
		result, err := antiquary.RetireBeaconBlocks(ctx, db, snapshots, dirs, log.Root(), nil, nil)
		if err != nil {
			return err
		}
		databaseChanged = databaseChanged || result.RowsPruned != 0
		if result.Files == 0 {
			break
		}
		fmt.Fprintf(writer, "freeze blocks %d..%d %d files\n", result.From, result.To, result.Files)
	}
	for {
		result, err := antiquary.MergeBeaconBlocks(ctx, snapshots, dirs.Tmp, log.Root(), nil, nil, nil)
		if err != nil {
			return err
		}
		if result.From == result.To {
			break
		}
		fmt.Fprintf(writer, "merge blocks %d..%d %d files\n", result.From, result.To, result.Files)
	}
	if err := snapshots.RemoveOverlaps(nil); err != nil {
		return err
	}
	removed, err := antiquary.PrunePublishedBlobSidecars(ctx, db, blobStorage, snapshots, nil)
	if err != nil {
		return err
	}
	databaseChanged = databaseChanged || removed != 0
	reader := freezeblocks.NewBeaconSnapshotReader(snapshots, nil, beaconConfig)
	var checkedTo uint64
	for {
		result, err := antiquary.RetireBlobSidecars(ctx, db, blobStorage, snapshots, reader, beaconConfig, dirs, snapshots.Salt, log.Root(), antiquary.BlobRetireHooks{CheckedTo: &checkedTo})
		if err != nil {
			return err
		}
		databaseChanged = databaseChanged || result.SidecarsRemoved != 0
		if result.Blocked {
			fmt.Fprintf(writer, "blob gap slot %d\n", result.Gap)
			break
		}
		if result.From == result.To {
			break
		}
		fmt.Fprintf(writer, "retire blob sidecars %d..%d %d files\n", result.From, result.To, result.Files)
	}
	view = snapshots.View()
	hasBlockSegments = len(view.BeaconBlocks()) != 0
	view.Close()
	if hasBlockSegments {
		if err := checkSnapshots(ctx, db, snapshots); err != nil {
			return err
		}
	}
	if err := checkBlobsSnapshots(ctx, db, blobStorage, snapshots, beaconConfig); err != nil {
		return err
	}
	var stateProgress uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		var err error
		stateProgress, err = state_accessors.GetStateProcessingProgress(tx)
		return err
	}); err != nil {
		return err
	}
	if stateProgress != 0 {
		stateTypes := snapshotsync.MakeCaplinStateSnapshotsTypes(db, beaconConfig)
		stateSnapshots := snapshotsync.NewCaplinStateSnapshots(freezingCfg, dirs, stateTypes, log.Root())
		if err := stateSnapshots.OpenFolder(); err != nil {
			return err
		}
		defer stateSnapshots.Close()
		for {
			result, err := antiquary.RetireCaplinState(ctx, db, stateSnapshots, stateProgress, snapshots.Salt, dirs, runtime.NumCPU(), log.Root(), antiquary.StateRetireHooks{})
			if err != nil {
				return err
			}
			databaseChanged = databaseChanged || result.DatabaseChanged
			if result.IncompleteTable != "" {
				fmt.Fprintf(writer, "incomplete state table %s at slot %d\n", result.IncompleteTable, result.IncompleteSlot)
				break
			}
			if result.From == result.To {
				break
			}
			fmt.Fprintf(writer, "retire state %d..%d %d files\n", result.From, result.To, result.Files)
		}
	}
	db.Close()
	return compactCaplinDatabase(ctx, dirs, writer, cmd.Bool("no-compact"), databaseChanged)
}

func compactCaplinDatabase(ctx context.Context, dirs datadir.Dirs, writer io.Writer, noCompact, databaseChanged bool) error {
	if noCompact {
		return nil
	}
	if !databaseChanged {
		compact, err := caplinNeedsCompaction(dirs.CaplinIndexing)
		if err != nil {
			return err
		}
		if !compact {
			return nil
		}
	}
	dataPath := filepath.Join(dirs.CaplinIndexing, "mdbx.dat")
	before, err := os.Stat(dataPath)
	if err != nil {
		return err
	}
	if err := backup.CompactInPlace(ctx, dirs.CaplinIndexing, dbcfg.CaplinDB, log.Root()); err != nil {
		return err
	}
	after, err := os.Stat(dataPath)
	if err != nil {
		return err
	}
	reclaimed := int64(before.Size()) - int64(after.Size())
	fmt.Fprintf(writer, "compact caplin mdbx %d -> %d bytes, %d bytes reclaimed\n", before.Size(), after.Size(), reclaimed)
	return nil
}

var caplinNeedsCompaction = backup.NeedsCompaction

func caplinFrozenBlockLink(ctx context.Context, db kv.RoDB, snapshots *freezeblocks.CaplinSnapshots, hasFrozenSegments bool) (bool, uint64, error) {
	coveredTo := snapshots.BlocksAvailable()
	var frozenRoot common.Hash
	var foundFrozen bool
	if hasFrozenSegments {
		view := snapshots.View()
		segments := view.BeaconBlocks()
		lastSlot := segments[len(segments)-1].To()
		view.Close()
		for lastSlot > 0 {
			lastSlot--
			lastFrozen, _, _, err := snapshots.ReadHeader(lastSlot, nil)
			if err != nil {
				return false, coveredTo + 1, err
			}
			if lastFrozen == nil {
				continue
			}
			frozenRoot, err = lastFrozen.Header.HashSSZ()
			if err != nil {
				return false, coveredTo + 1, err
			}
			foundFrozen = true
			break
		}
		if !foundFrozen {
			return false, coveredTo + 1, nil
		}
	}
	var linked bool
	gap := coveredTo + 1
	err := db.View(ctx, func(tx kv.Tx) error {
		from, to, err := antiquary.CaplinBlockRetireRange(tx, snapshots)
		if err != nil {
			return err
		}
		if from == to {
			linked = true
			return nil
		}
		cursor, err := tx.Cursor(kv.CanonicalBlockRoots)
		if err != nil {
			return err
		}
		defer cursor.Close()
		key, value, err := cursor.Seek(base_encoding.Encode64ToBytes4(from))
		if err != nil {
			return err
		}
		previous := frozenRoot
		hasPrevious := hasFrozenSegments
		for ; key != nil; key, value, err = cursor.Next() {
			if err != nil {
				return err
			}
			if len(key) != 4 {
				continue
			}
			slot := base_encoding.Decode64FromBytes4(key)
			root := common.BytesToHash(value)
			if !hasPrevious {
				previous = root
				hasPrevious = true
				if slot >= to {
					break
				}
				continue
			}
			parent, err := beacon_indicies.ReadParentBlockRoot(tx, root)
			if err != nil {
				return err
			}
			if parent != previous {
				gap = slot
				return nil
			}
			if slot >= to {
				break
			}
			previous = root
		}
		linked = true
		return nil
	})
	return linked, gap, err
}

func caplinRetireNeedsWrite(ctx context.Context, db kv.RoDB, snapshots *freezeblocks.CaplinSnapshots) (bool, error) {
	needsWrite := false
	err := db.View(ctx, func(tx kv.Tx) error {
		from, to, err := antiquary.CaplinBlockRetireRange(tx, snapshots)
		if err != nil {
			return err
		}
		needsWrite = from != to
		if needsWrite {
			return nil
		}
		coveredTo := snapshots.BlocksAvailable()
		needsWrite, err = antiquary.CaplinBeaconBlocksBelow(tx, coveredTo+1)
		if err != nil || needsWrite {
			return err
		}
		progress, err := state_accessors.GetStateProcessingProgress(tx)
		needsWrite = progress != 0
		return err
	})
	if err != nil || needsWrite {
		return needsWrite, err
	}
	view := snapshots.View()
	needsWrite = len(view.BlobSidecars()) != 0
	view.Close()
	return needsWrite, nil
}

func caplinBlobDataPresent(ctx context.Context, dirs datadir.Dirs) (bool, error) {
	path := filepath.Join(dirs.CaplinBlobs, "chaindata")
	if !fileExists(filepath.Join(path, "mdbx.dat")) {
		return false, nil
	}
	db, err := antiquary.OpenCaplinDuIndexDB(ctx, path, log.Root())
	if err != nil {
		return false, err
	}
	defer db.Close()
	var present bool
	err = db.View(ctx, func(tx kv.Tx) error {
		names, err := tx.ListTables()
		if err != nil {
			return err
		}
		if slices.Contains(names, kv.BlockRootToKzgCommitments) {
			hasRows, err := caplinDuTableHasRows(tx, kv.BlockRootToKzgCommitments)
			if err != nil {
				return err
			}
			present = hasRows
		}
		return nil
	})
	return present, err
}

func caplinDuCommand() *cli.Command {
	chainFlag := cli.StringFlag{Name: cliutils.ChainFlag.Name, Usage: cliutils.ChainFlag.Usage}
	return &cli.Command{
		Name:        "du",
		Usage:       "show Caplin disk usage; reads files and the index without the datadir lock",
		Description: "Reads local Caplin files, hot stores, the Caplin index, and a manifest when supplied. It does not write the datadir or take its lock.",
		Flags: []cli.Flag{
			&chainFlag,
			&cliutils.DataDirFlag,
			&cli.BoolFlag{Name: "json", Usage: "write JSON"},
			&cli.BoolFlag{Name: "v", Usage: "show every Caplin MDBX table"},
			&cli.StringFlag{Name: "manifest", Usage: "snapshot manifest TOML"},
		},
		Action: runCaplinDu,
	}
}

func runCaplinDu(ctx context.Context, cmd *cli.Command) error {
	folder := outputFolder{}
	if err := folder.fromCmd(cmd); err != nil {
		return err
	}
	dirs := datadir.Open(folder.Datadir)
	chainName, beaconConfig, err := caplinDuChainConfig(ctx, cmd, dirs)
	if err != nil {
		return err
	}
	indexDB, err := antiquary.OpenCaplinDuIndexDB(ctx, dirs.CaplinIndexing, log.Root())
	if err != nil {
		return err
	}
	if indexDB != nil {
		defer indexDB.Close()
	}

	manifestPath := cmd.String("manifest")
	var manifest snapcfg.PreverifiedItems
	if manifestPath != "" {
		data, err := os.ReadFile(manifestPath)
		if err != nil {
			return fmt.Errorf("read manifest: %w", err)
		}
		manifest, err = snapcfg.ParseToml(data)
		if err != nil {
			return fmt.Errorf("parse manifest: %w", err)
		}
	}
	result, err := antiquary.CollectCaplinDu(ctx, antiquary.CaplinDuRequest{
		Chain:        chainName,
		Dirs:         dirs,
		BeaconConfig: beaconConfig,
		IndexDB:      indexDB,
		Manifest:     manifest,
		Verbose:      cmd.Bool("v"),
	})
	if err != nil {
		return err
	}
	writer := cmd.Writer
	if cmd.Bool("json") {
		return antiquary.FormatCaplinDuJSON(writer, result)
	}
	antiquary.FormatCaplinDu(writer, result)
	return nil
}

func caplinDuChainConfig(ctx context.Context, cmd *cli.Command, dirs datadir.Dirs) (string, *clparams.BeaconChainConfig, error) {
	chainName := cmd.String(cliutils.ChainFlag.Name)
	if !cmd.IsSet(cliutils.ChainFlag.Name) {
		switch {
		case fileExists(filepath.Join(dirs.Chaindata, "mdbx.dat")):
			var err error
			chainName, err = caplinDuChainName(ctx, dirs)
			if err != nil {
				return "", nil, err
			}
		case caplinDuDataPresent(ctx, dirs):
			return "", nil, errors.New("caplin data found but chain is unknown; pass --chain")
		default:
			return "unknown", nil, nil
		}
	}
	if chainName == networkname.Dev {
		path := filepath.Join(dirs.DataDir, "dev-beacon", "config.yaml")
		beaconConfig, _, err := clparams.CustomConfig(path)
		return chainName, &beaconConfig, err
	}
	_, beaconConfig, _, err := clparams.GetConfigsByNetworkName(chainName)
	return chainName, beaconConfig, err
}

func caplinDuChainName(ctx context.Context, dirs datadir.Dirs) (string, error) {
	if !fileExists(filepath.Join(dirs.Chaindata, "mdbx.dat")) {
		return "", errors.New("caplin data found but chain is unknown; pass --chain")
	}
	db, err := mdbx.New(dbcfg.ChainDB, log.Root()).Path(dirs.Chaindata).Accede(true).Readonly(true).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).Open(ctx)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var name string
	err = db.View(ctx, func(tx kv.Tx) error {
		genesis, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		cfg, err := rawdb.ReadChainConfig(tx, genesis)
		if err != nil {
			return err
		}
		if cfg == nil || cfg.ChainName == "" {
			return errors.New("caplin data found but chain is unknown; pass --chain")
		}
		name = cfg.ChainName
		return nil
	})
	return name, err
}

func caplinDuDataPresent(ctx context.Context, dirs datadir.Dirs) bool {
	for _, prefix := range []string{"", "caplin"} {
		dir := filepath.Join(dirs.Snap, prefix)
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			name = filepath.ToSlash(filepath.Join(prefix, name))
			info, _, ok := snaptype.ParseFileName("", name)
			if ok && (info.Ext == ".seg" || info.Ext == ".idx") {
				if _, ok := snapshotsync.CaplinFileInfo(name, info); ok {
					return true
				}
			}
		}
	}
	if fileExists(filepath.Join(dirs.CaplinIndexing, "mdbx.dat")) {
		db, err := antiquary.OpenCaplinDuIndexDB(ctx, dirs.CaplinIndexing, log.Root())
		if err != nil {
			return true
		}
		if db == nil {
			return false
		}
		defer db.Close()
		var present bool
		if err := db.View(ctx, func(tx kv.Tx) error {
			names, err := tx.ListTables()
			if err != nil {
				return err
			}
			for _, name := range names {
				hasRows, err := caplinDuTableHasRows(tx, name)
				if err != nil {
					return err
				}
				if hasRows {
					present = true
					break
				}
			}
			return nil
		}); err != nil {
			return true
		}
		return present
	}
	for _, root := range []string{dirs.CaplinBlobs, dirs.CaplinColumnData} {
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			bucket, err := strconv.ParseUint(entry.Name(), 10, 64)
			if err != nil || strconv.FormatUint(bucket, 10) != entry.Name() {
				continue
			}
			if caplinDuCompletedFiles(filepath.Join(root, entry.Name())) {
				return true
			}
		}
	}
	return false
}

func caplinDuTableHasRows(tx kv.Tx, name string) (bool, error) {
	key, err := kv.FirstKey(tx, name)
	return key != nil, err
}

func caplinDuCompletedFiles(root string) bool {
	found := false
	err := fs.WalkDir(os.DirFS(root), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if strings.HasSuffix(entry.Name(), ".tmp") || strings.HasPrefix(filepath.Ext(entry.Name()), ".torrent") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			found = true
			return fs.SkipAll
		}
		return nil
	})
	return err == nil && found
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func lockCaplinDirs(path string, create bool) (datadir.Dirs, func(), error) {
	dirs := datadir.Open(path)
	unlock, err := dirs.TryFlock()
	if err != nil {
		return dirs, func() {}, err
	}
	if create {
		dirs = datadir.New(path)
	}
	return dirs, unlock, nil
}

func caplinResetManifest(ctx context.Context, dirs datadir.Dirs, chainName string) (snapcfg.PreverifiedItems, error) {
	items, err := snapcfg.PreverifiedItemsForDatadir(&dirs, chainName)
	if err != nil {
		return nil, err
	}
	if len(items) != 0 {
		return items, nil
	}
	if _, err := os.Stat(dirs.PreverifiedPath()); err == nil {
		return items, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if !caplinResetHasFiles(dirs) {
		return items, nil
	}
	if err := snapcfg.LoadRemotePreverified(ctx, chainName, ""); err != nil {
		return nil, err
	}
	return snapcfg.PreverifiedItemsForDatadir(&dirs, chainName)
}

func caplinResetHasFiles(dirs datadir.Dirs) bool {
	for _, root := range []string{dirs.Snap, dirs.SnapCaplin} {
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				return true
			}
		}
	}
	return false
}
