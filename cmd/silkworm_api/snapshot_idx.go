package main

import (
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"os"
	"path/filepath"
	"time"
)

func main() {

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "datadir",
				Value: "./dev",
				Usage: "node data directory",
			},
			&cli.StringFlag{
				Name:  "snapshot_path",
				Value: ".",
				Usage: "pathname of the snapshot file",
			},
		},
		Action: func(cCtx *cli.Context) error {
			return buildIndex(cCtx, cCtx.String("datadir"), cCtx.String("snapshot_path"))
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

func buildIndex(cliCtx *cli.Context, dataDir string, snapshotPath string) error {
	logger, err := debug.Setup(cliCtx, true /* rootLogger */)
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

	segments, _, err := freezeblocks.Segments(dirs.Snap)
	if err != nil {
		return err
	}

	segment, found := FindIf(segments, func(s snaptype.FileInfo) bool {
		return s.Path == snapshotPath
	})
	if !found {
		fmt.Printf("Segment %s not found\n", snapshotPath)
		return nil
	}

	fmt.Printf("Building index:\ndataDir: %s, \nsnapshotPath: %s\n", dataDir, snapshotPath)
	start := time.Now()

	switch segment.T {
	case snaptype.Headers:
		jobProgress := &background.Progress{}
		ps.Add(jobProgress)
		err = freezeblocks.HeadersIdx(ctx, chainConfig, segment.Path, segment.From, dirs.Tmp, jobProgress, logLevel, logger)
	case snaptype.Bodies:
		jobProgress := &background.Progress{}
		ps.Add(jobProgress)
		err = freezeblocks.BodiesIdx(ctx, segment.Path, segment.From, dirs.Tmp, jobProgress, logLevel, logger)
	case snaptype.Transactions:
		jobProgress := &background.Progress{}
		ps.Add(jobProgress)
		dir, _ := filepath.Split(segment.Path)
		err = freezeblocks.TransactionsIdx(ctx, chainConfig, segment.From, segment.To, dir, dirs.Tmp, jobProgress, logLevel, logger)
	}

	if err != nil {
		return err
	}

	elapsed := time.Since(start)
	fmt.Printf("Index for snapshot %s built in %d ms\n", snapshotPath, elapsed.Milliseconds())

	return nil
}
