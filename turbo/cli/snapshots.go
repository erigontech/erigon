package cli

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/parallelcompress"
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
			Flags: []cli.Flag{
				utils.DataDirFlag,
				SnapshotFromFlag,
				SnapshotToFlag,
				SnapshotSegmentSizeFlag,
			},
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
)

func doSnapshotCommand(ctx *cli.Context) error {
	fromBlock := ctx.Uint64(SnapshotFromFlag.Name)
	toBlock := ctx.Uint64(SnapshotToFlag.Name)
	segmentSize := ctx.Uint64(SnapshotSegmentSizeFlag.Name)
	if segmentSize < 1000 {
		return fmt.Errorf("too small --segment.size %d", segmentSize)
	}
	dataDir := ctx.String(utils.DataDirFlag.Name)
	snapshotDir := path.Join(dataDir, "snapshots")

	chainDB := mdbx.MustOpen(path.Join(dataDir, "chaindata"))
	defer chainDB.Close()

	if err := snapshotBlocks(chainDB, fromBlock, toBlock, segmentSize, snapshotDir); err != nil {
		log.Error("Error", "err", err)
	}
	return nil
}

func snapshotBlocks(chainDB kv.RoDB, fromBlock, toBlock, blocksPerFile uint64, snapshotDir string) error {
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
	for i := fromBlock; i < last; i += blocksPerFile {
		fileName := snapshotsync.FileName(i, i+blocksPerFile, snapshotsync.Bodies)

		log.Info("Creating", "file", fileName+".seg")
		if err := snapshotsync.DumpBodies(chainDB, "", i, int(blocksPerFile)); err != nil {
			panic(err)
		}
		segmentFile := path.Join(snapshotDir, fileName) + ".seg"
		if err := parallelcompress.Compress("Bodies", fileName, segmentFile); err != nil {
			panic(err)
		}
		_ = os.Remove(fileName + ".dat")

		fileName = snapshotsync.FileName(i, i+blocksPerFile, snapshotsync.Headers)
		log.Info("Creating", "file", fileName+".seg")
		if err := snapshotsync.DumpHeaders(chainDB, "", i, int(blocksPerFile)); err != nil {
			panic(err)
		}
		segmentFile = path.Join(snapshotDir, fileName) + ".seg"
		if err := parallelcompress.Compress("Headers", fileName, segmentFile); err != nil {
			panic(err)
		}
		_ = os.Remove(fileName + ".dat")

		fileName = snapshotsync.FileName(i, i+blocksPerFile, snapshotsync.Transactions)
		log.Info("Creating", "file", fileName+".seg")
		_, err := snapshotsync.DumpTxs(chainDB, "", i, int(blocksPerFile))
		if err != nil {
			panic(err)
		}
		segmentFile = path.Join(snapshotDir, fileName) + ".seg"
		if err := parallelcompress.Compress("Transactions", fileName, segmentFile); err != nil {
			panic(err)
		}
		_ = os.Remove(fileName + ".dat")

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
