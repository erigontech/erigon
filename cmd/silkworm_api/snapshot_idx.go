package main

import (
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	coresnaptype "github.com/ledgerwatch/erigon/core/snaptype"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

// Build snapshot indexes for given snapshot files.
// Sample usage:
// build_idx --datadir erigon-1 --snapshot_path /snapshots/v1-000000-000500-headers.seg,/snapshots/v1-000500-001000-headers.seg

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
	logger, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	logLevel := log.LvlInfo

	ps := background.NewProgressSet()

	workers := 4
	g, ctx := errgroup.WithContext(cliCtx.Context)
	g.SetLimit(workers)

	dirs := datadir.New(dataDir)

	chainDB := mdbx.NewMDBX(logger).Path(dirs.Chaindata).MustOpen()
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
		case coresnaptype.Enums.Headers, coresnaptype.Enums.Bodies, coresnaptype.Enums.Transactions:
			g.Go(func() error {
				jobProgress := &background.Progress{}
				ps.Add(jobProgress)
				defer ps.Delete(jobProgress)
				return segment.Type.BuildIndexes(ctx, segment, chainConfig, dirs.Tmp, jobProgress, logLevel, logger)
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
