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

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/turbo/debug"
)

// Build snapshot indexes for given snapshot files.
// Sample usage:
// build_idx --datadir erigon-1 --snapshot_path /snapshots/v1.0-000000-000500-headers.seg,/snapshots/v1.0-000500-001000-headers.seg

func main() {

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "datadir",
				Value: "./dev",
				Usage: "node data directory",
			},
			&cli.StringSliceFlag{
				Name:  "snapshot_path",
				Usage: "pathname of the snapshot file",
			},
		},
		Action: func(cCtx *cli.Context) error {
			return buildIndex(cCtx, cCtx.String("datadir"), cCtx.StringSlice("snapshot_path"), 0)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Crit(err.Error())
	}
}

func FindIf(segments []snaptype.FileInfo, predicate func(snaptype.FileInfo) bool) (snaptype.FileInfo, bool) {
	for _, segment := range segments {
		if predicate(segment) {
			return segment, true
		}
	}
	return snaptype.FileInfo{}, false // Return zero value and false if not found
}

func buildIndex(cliCtx *cli.Context, dataDir string, snapshotPaths []string, minBlock uint64) error {
	logger, _, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	logLevel := log.LvlInfo

	ps := background.NewProgressSet()

	workers := 4
	g, ctx := errgroup.WithContext(cliCtx.Context)
	g.SetLimit(workers)

	dirs := datadir.New(dataDir)

	chainDB := mdbx.New(kv.ChainDB, logger).Path(dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	chainConfig := fromdb.ChainConfig(chainDB)

	segments, _, err := freezeblocks.Segments(dirs.Snap, minBlock)
	if err != nil {
		return err
	}

	fmt.Printf("Building indexes:\n- dataDir: %s\n- snapshots: %s\n", dataDir, snapshotPaths)
	start := time.Now()

	for _, snapshotPath := range snapshotPaths {
		segment, found := FindIf(segments, func(s snaptype.FileInfo) bool {
			return s.Path == snapshotPath
		})
		if !found {
			return fmt.Errorf("segment %s not found", snapshotPath)
		}

		switch segment.Type.Enum() {
		case snaptype2.Enums.Headers, snaptype2.Enums.Bodies, snaptype2.Enums.Transactions:
			g.Go(func() error {
				jobProgress := &background.Progress{}
				ps.Add(jobProgress)
				defer ps.Delete(jobProgress)
				return segment.Type.BuildIndexes(ctx, segment, nil, chainConfig, dirs.Tmp, jobProgress, logLevel, logger)
			})
		}
	}

	if err := g.Wait(); err != nil {
		return err
	}

	elapsed := time.Since(start)
	fmt.Printf("Indexes for %d snapshots built in %d ms\n", len(snapshotPaths), elapsed.Milliseconds())

	return nil
}
