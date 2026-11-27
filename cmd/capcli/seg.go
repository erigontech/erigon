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
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/clparams"
	cliutils "github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain/networkname"
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
	return &cli.Command{
		Usage:       "show or set the Caplin retention regime; show reads without a lock, set writes with the datadir lock",
		Description: "Show reads the stored Caplin regime, oldest stored data, and local sidecar files without writing or taking the datadir lock. Set validates and writes a regime while holding the datadir lock.",
	}
}

func caplinResetCommand() *cli.Command {
	return &cli.Command{
		Usage:       "reset Caplin snapshot segments; reads and writes files and the index with the datadir lock",
		Description: "Prints or applies a Caplin reset plan. Both dry runs and writes take the datadir lock; only a write changes files or index rows.",
	}
}

func caplinRetireCommand() *cli.Command {
	return &cli.Command{
		Usage:       "retire Caplin snapshots; reads and writes files and the index with the datadir lock",
		Description: "Retires Caplin block, blob, and state data and may compact the Caplin index. It takes the datadir lock for the full operation.",
	}
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
