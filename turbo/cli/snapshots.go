package cli

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli"
)

const ASSERT = false

var snapshotCommand = cli.Command{
	Name:        "snapshots",
	Description: `Managing snapshots (historical data partitions)`,
	Subcommands: []cli.Command{
		{
			Name:   "create",
			Action: doSnapshotCommand,
			Usage:  "Create snapshots for given range of blocks",
			Before: func(ctx *cli.Context) error { return debug.Setup(ctx) },
			Flags: append([]cli.Flag{
				utils.DataDirFlag,
				SnapshotFromFlag,
				SnapshotToFlag,
				SnapshotSegmentSizeFlag,
			}, debug.Flags...),
		},
		{
			Name:   "index",
			Action: doIndicesCommand,
			Usage:  "Create all indices for snapshots",
			Before: func(ctx *cli.Context) error { return debug.Setup(ctx) },
			Flags: append([]cli.Flag{
				utils.DataDirFlag,
				SnapshotRebuildFlag,
			}, debug.Flags...),
		},
	},
}

var (
	SnapshotFromFlag = cli.Uint64Flag{
		Name:  "from",
		Usage: "From block number",
		Value: 0,
	}
	SnapshotToFlag = cli.Uint64Flag{
		Name:  "to",
		Usage: "To block number. Zero - means unlimited.",
		Value: 0,
	}
	SnapshotSegmentSizeFlag = cli.Uint64Flag{
		Name:  "segment.size",
		Usage: "Amount of blocks in each segment",
		Value: 500_000,
	}
	SnapshotRebuildFlag = cli.BoolFlag{
		Name:  "rebuild",
		Usage: "Force rebuild",
	}
)

func doIndicesCommand(cliCtx *cli.Context) error {
	ctx, cancel := utils.RootContext()
	defer cancel()

	dataDir := cliCtx.String(utils.DataDirFlag.Name)
	snapshotDir := path.Join(dataDir, "snapshots")
	tmpDir := path.Join(dataDir, etl.TmpDirName)
	rebuild := cliCtx.Bool(SnapshotRebuildFlag.Name)

	chainDB := mdbx.NewMDBX(log.New()).Path(path.Join(dataDir, "chaindata")).Readonly().MustOpen()
	defer chainDB.Close()

	if rebuild {
		if err := rebuildIndices(ctx, chainDB, snapshotDir, tmpDir); err != nil {
			log.Error("Error", "err", err)
		}
	}
	return nil
}
func doSnapshotCommand(cliCtx *cli.Context) error {
	ctx, cancel := utils.RootContext()
	defer cancel()

	fromBlock := cliCtx.Uint64(SnapshotFromFlag.Name)
	toBlock := cliCtx.Uint64(SnapshotToFlag.Name)
	segmentSize := cliCtx.Uint64(SnapshotSegmentSizeFlag.Name)
	if segmentSize < 1000 {
		return fmt.Errorf("too small --segment.size %d", segmentSize)
	}
	dataDir := cliCtx.String(utils.DataDirFlag.Name)
	snapshotDir := path.Join(dataDir, "snapshots")
	tmpDir := path.Join(dataDir, etl.TmpDirName)
	_ = os.MkdirAll(tmpDir, 0744)

	chainDB := mdbx.NewMDBX(log.New()).Path(path.Join(dataDir, "chaindata")).Readonly().MustOpen()
	defer chainDB.Close()

	if err := snapshotBlocks(ctx, chainDB, fromBlock, toBlock, segmentSize, snapshotDir, tmpDir); err != nil {
		log.Error("Error", "err", err)
	}
	return nil
}

func rebuildIndices(ctx context.Context, chainDB kv.RoDB, snapshotDir, tmpDir string) error {
	chainConfig := tool.ChainConfigFromDB(chainDB)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)
	_ = chainID
	_ = os.MkdirAll(snapshotDir, 0744)

	allSnapshots := snapshotsync.NewAllSnapshots(snapshotDir, snapshothashes.KnownConfig(chainConfig.ChainName))
	if err := allSnapshots.ReopenSegments(); err != nil {
		return err
	}
	idxFilesList, err := snapshotsync.IdxFiles(snapshotDir)
	if err != nil {
		return err
	}
	for _, f := range idxFilesList {
		_ = os.Remove(f)
	}
	if err := allSnapshots.BuildIndices(ctx, *chainID, tmpDir); err != nil {
		return err
	}
	return nil
}

func snapshotBlocks(ctx context.Context, chainDB kv.RoDB, fromBlock, toBlock, blocksPerFile uint64, snapshotDir, tmpDir string) error {
	var last uint64

	if toBlock > 0 {
		last = toBlock
	} else {
		lastChunk := func(tx kv.Tx, blocksPerFile uint64) (uint64, error) {
			c, err := tx.Cursor(kv.BlockBody)
			if err != nil {
				return 0, err
			}
			k, _, err := c.Last()
			if err != nil {
				return 0, err
			}
			last := binary.BigEndian.Uint64(k)
			if last > params.FullImmutabilityThreshold {
				last -= params.FullImmutabilityThreshold
			} else {
				last = 0
			}
			last = last - last%blocksPerFile
			return last, nil
		}

		if err := chainDB.View(context.Background(), func(tx kv.Tx) (err error) {
			last, err = lastChunk(tx, blocksPerFile)
			return err
		}); err != nil {
			return err
		}
	}

	chainConfig := tool.ChainConfigFromDB(chainDB)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)
	_ = chainID
	_ = os.MkdirAll(snapshotDir, fs.ModePerm)

	log.Info("Last body number", "last", last)
	workers := runtime.NumCPU() - 1
	if workers < 1 {
		workers = 1
	}

	for i := fromBlock; i < last; i += blocksPerFile {
		fileName := snapshotsync.FileName(i, i+blocksPerFile, snapshotsync.Bodies)
		tmpFilePath, segmentFile := filepath.Join(tmpDir, fileName)+".dat", filepath.Join(snapshotDir, fileName)+".seg"
		log.Info("Creating", "file", fileName)

		if err := snapshotsync.DumpBodies(ctx, chainDB, segmentFile, tmpDir, i, int(blocksPerFile), workers); err != nil {
			panic(err)
		}
		_ = os.Remove(tmpFilePath)

		fileName = snapshotsync.FileName(i, i+blocksPerFile, snapshotsync.Headers)
		tmpFilePath, segmentFile = filepath.Join(tmpDir, fileName)+".dat", filepath.Join(snapshotDir, fileName)+".seg"
		log.Info("Creating", "file", fileName)
		if err := snapshotsync.DumpHeaders(ctx, chainDB, segmentFile, tmpDir, i, int(blocksPerFile), workers); err != nil {
			panic(err)
		}
		_ = os.Remove(tmpFilePath)

		fileName = snapshotsync.FileName(i, i+blocksPerFile, snapshotsync.Transactions)
		tmpFilePath, segmentFile = filepath.Join(tmpDir, fileName)+".dat", filepath.Join(snapshotDir, fileName)+".seg"
		log.Info("Creating", "file", fileName)
		if _, err := snapshotsync.DumpTxs(ctx, chainDB, segmentFile, tmpDir, i, int(blocksPerFile), workers); err != nil {
			panic(err)
		}
		_ = os.Remove(tmpFilePath)

		//nolint
		//break // TODO: remove me - useful for tests
	}
	return nil
}

//nolint
func checkBlockSnapshot(chaindata string) error {
	database := mdbx.MustOpen(chaindata)
	defer database.Close()
	dataDir := path.Dir(chaindata)
	chainConfig := tool.ChainConfigFromDB(database)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)
	_ = chainID

	snapshots := snapshotsync.NewAllSnapshots(path.Join(dataDir, "snapshots"), snapshothashes.KnownConfig(chainConfig.ChainName))
	snapshots.ReopenSegments()
	snapshots.ReopenIndices()
	//if err := snapshots.BuildIndices(context.Background(), *chainID); err != nil {
	//	panic(err)
	//}

	snBlockReader := snapshotsync.NewBlockReaderWithSnapshots(snapshots)
	tx, err := database.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i := uint64(0); i < snapshots.BlocksAvailable(); i++ {
		hash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			return err
		}
		blockFromDB := rawdb.ReadBlock(tx, hash, i)
		blockFromSnapshot, _, err := snBlockReader.BlockWithSenders(context.Background(), tx, hash, i)
		if err != nil {
			return err
		}

		if blockFromSnapshot.Hash() != blockFromDB.Hash() {
			panic(i)
		}
		if i%1_000 == 0 {
			log.Info(fmt.Sprintf("Block Num: %dK", i/1_000))
		}
	}
	return nil
}
