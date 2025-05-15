// Copyright 2024 The Erigon Authors
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

package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/cmd/utils"
	snaptype2 "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

type Sqeeze string

var (
	SqeezeCommitment Sqeeze = "commitment"
	SqeezeStorage    Sqeeze = "storage"
	SqeezeCode       Sqeeze = "code"
	SqeezeBlocks     Sqeeze = "blocks"
)

func doSqueeze(cliCtx *cli.Context) error {
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	logger, _, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	ctx := cliCtx.Context
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	t := Sqeeze(cliCtx.String("type"))

	start := time.Now()
	log.Info("[sqeeze] start", "t", t)
	defer func() { logger.Info("[sqeeze] done", "t", t, "took", time.Since(start)) }()

	switch {
	case t == SqeezeCommitment:
		return squeezeCommitment(ctx, dirs, logger)
	case t == SqeezeStorage:
		return squeezeStorage(ctx, dirs, logger)
	case t == SqeezeCode:
		return squeezeCode(ctx, dirs, logger)
	case t == SqeezeBlocks:
		return squeezeBlocks(ctx, dirs, logger)
	default:

		return fmt.Errorf("unknown type: %s", t)
	}
}

func squeezeCommitment(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
	db := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()
	cfg := ethconfig.NewSnapCfg(false, true, true, fromdb.ChainConfig(db).ChainName)

	_, _, _, _, agg, clean, err := openSnaps(ctx, cfg, dirs, db, logger)
	if err != nil {
		return err
	}
	defer clean()
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	if err := agg.OpenFolder(); err != nil {
		return err
	}
	if err := agg.BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers()); err != nil {
		return err
	}
	ac := agg.BeginFilesRo()
	defer ac.Close()
	if err := state.SqueezeCommitmentFiles(ac, logger); err != nil {
		return err
	}
	ac.Close()
	if err := agg.BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers()); err != nil {
		return err
	}
	return nil
}

func squeezeStorage(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
	db := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()
	cfg := ethconfig.NewSnapCfg(false, true, true, fromdb.ChainConfig(db).ChainName)
	_, _, _, _, agg, clean, err := openSnaps(ctx, cfg, dirs, db, logger)
	if err != nil {
		return err
	}
	defer clean()
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	dirsOld := dirs
	dirsOld.SnapDomain += "_old"
	dir.MustExist(dirsOld.SnapDomain, dirs.SnapDomain+"_backup")
	if err := agg.Sqeeze(ctx, kv.StorageDomain); err != nil {
		return err
	}

	if err := agg.OpenFolder(); err != nil {
		return err
	}
	if err := agg.BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers()); err != nil {
		return err
	}
	ac := agg.BeginFilesRo()
	defer ac.Close()

	aggOld, err := state.NewAggregator(ctx, dirsOld, config3.DefaultStepSize, db, logger)
	if err != nil {
		panic(err)
	}
	defer aggOld.Close()
	if err = aggOld.OpenFolder(); err != nil {
		panic(err)
	}
	aggOld.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	if err := aggOld.BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers()); err != nil {
		return err
	}
	if err := agg.BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers()); err != nil {
		return err
	}

	acOld := aggOld.BeginFilesRo()
	defer acOld.Close()

	if err = state.SqueezeCommitmentFiles(acOld, logger); err != nil {
		return err
	}
	acOld.Close()
	ac.Close()
	if err := agg.BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers()); err != nil {
		return err
	}
	if err := aggOld.BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers()); err != nil {
		return err
	}
	agg.Close()
	aggOld.Close()

	log.Info("[sqeeze] removing", "dir", dirsOld.SnapDomain)
	_ = os.RemoveAll(dirsOld.SnapDomain)
	log.Info("[sqeeze] success", "please_remove", dirs.SnapDomain+"_backup")
	return nil
}
func squeezeCode(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
	db := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()
	agg, err := state.NewAggregator(ctx, dirs, config3.DefaultStepSize, db, logger)
	if err != nil {
		return err
	}
	defer agg.Close()
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())

	log.Info("[sqeeze] start")
	if err := agg.Sqeeze(ctx, kv.CodeDomain); err != nil {
		return err
	}
	if err = agg.OpenFolder(); err != nil {
		return err
	}
	if err := agg.BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers()); err != nil {
		return err
	}
	return nil
}

func squeezeBlocks(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
	for _, f := range ls(dirs.Snap, ".seg") {
		good := strings.Contains(f, snaptype2.Transactions.Name()) ||
			strings.Contains(f, snaptype2.Headers.Name())
		if !good {
			continue
		}
		_, name := filepath.Split(f)
		in, _, ok := snaptype.ParseFileName(dirs.Snap, name)
		if !ok {
			continue
		}
		good = in.To-in.From == snaptype.Erigon2OldMergeLimit || in.To-in.From == snaptype.Erigon2MergeLimit
		if !good {
			continue
		}
		if err := freezeblocks.Sqeeze(ctx, dirs, f, f, logger); err != nil {
			return err
		}
		_ = os.Remove(strings.ReplaceAll(f, ".seg", ".seg.torrent"))
		_ = os.Remove(strings.ReplaceAll(f, ".seg", ".idx"))
		_ = os.Remove(strings.ReplaceAll(f, ".seg", ".idx.torrent"))
	}

	db := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()
	chainConfig := fromdb.ChainConfig(db)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	_, _, _, br, _, clean, err := openSnaps(ctx, cfg, dirs, db, logger)
	if err != nil {
		return err
	}
	defer clean()

	if err := br.BuildMissedIndicesIfNeed(ctx, "retire", nil); err != nil {
		return err
	}
	return nil
}

func ls(dirPath string, ext string) []string {
	res, err := dir.ListFiles(dirPath, ext)
	if err != nil {
		panic(err)
	}
	return res
}
