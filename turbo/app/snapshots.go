package app

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
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
			Name:   "recompress",
			Action: doRecompressCommand,
			Usage:  "Recompress existing .seg files to apply new compression rules",
			Before: func(ctx *cli.Context) error { return debug.Setup(ctx) },
			Flags: append([]cli.Flag{
				utils.DataDirFlag,
			}, debug.Flags...),
		},
		{
			Name:   "index",
			Action: doIndicesCommand,
			Usage:  "Create all indices for snapshots",
			Before: func(ctx *cli.Context) error { return debug.Setup(ctx) },
			Flags: append([]cli.Flag{
				utils.DataDirFlag,
				SnapshotFromFlag,
				SnapshotRebuildFlag,
			}, debug.Flags...),
		},
		{
			Name:   "retire",
			Action: doRetireCommand,
			Usage:  "erigon snapshots uncompress a.seg | erigon snapshots compress b.seg",
			Before: func(ctx *cli.Context) error { return debug.Setup(ctx) },
			Flags: append([]cli.Flag{
				utils.DataDirFlag,
				SnapshotFromFlag,
				SnapshotToFlag,
				SnapshotEveryFlag,
			}, debug.Flags...),
		},
		{
			Name:   "uncompress",
			Action: doUncompress,
			Usage:  "erigon snapshots uncompress a.seg | erigon snapshots compress b.seg",
			Before: func(ctx *cli.Context) error { return debug.Setup(ctx) },
			Flags:  append([]cli.Flag{}, debug.Flags...),
		},
		{
			Name:   "compress",
			Action: doCompress,
			Before: func(ctx *cli.Context) error { return debug.Setup(ctx) },
			Flags:  append([]cli.Flag{utils.DataDirFlag}, debug.Flags...),
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
	SnapshotEveryFlag = cli.Uint64Flag{
		Name:  "every",
		Usage: "Do operation every N blocks",
		Value: 1_000,
	}
	SnapshotSegmentSizeFlag = cli.Uint64Flag{
		Name:  "segment.size",
		Usage: "Amount of blocks in each segment",
		Value: snapshotsync.DEFAULT_SEGMENT_SIZE,
	}
	SnapshotRebuildFlag = cli.BoolFlag{
		Name:  "rebuild",
		Usage: "Force rebuild",
	}
)

func doIndicesCommand(cliCtx *cli.Context) error {
	ctx, cancel := common.RootContext()
	defer cancel()

	datadir := cliCtx.String(utils.DataDirFlag.Name)
	snapshotDir := filepath.Join(datadir, "snapshots")
	tmpDir := filepath.Join(datadir, etl.TmpDirName)
	rebuild := cliCtx.Bool(SnapshotRebuildFlag.Name)
	from := cliCtx.Uint64(SnapshotFromFlag.Name)

	chainDB := mdbx.NewMDBX(log.New()).Path(path.Join(datadir, "chaindata")).Readonly().MustOpen()
	defer chainDB.Close()

	if rebuild {
		cfg := ethconfig.NewSnapshotCfg(true, true)
		rwSnapshotDir, err := dir.OpenRw(snapshotDir)
		if err != nil {
			return err
		}
		defer rwSnapshotDir.Close()
		if err := rebuildIndices(ctx, chainDB, cfg, rwSnapshotDir, tmpDir, from); err != nil {
			log.Error("Error", "err", err)
		}
	}
	return nil
}

func doUncompress(cliCtx *cli.Context) error {
	ctx, cancel := common.RootContext()
	defer cancel()
	args := cliCtx.Args()
	if len(args) != 1 {
		return fmt.Errorf("expecting .seg file path")
	}
	f := args[0]
	decompressor, err := compress.NewDecompressor(f)
	if err != nil {
		return err
	}
	wr := bufio.NewWriterSize(os.Stdout, 16*etl.BufIOSize)
	defer wr.Flush()
	var numBuf [binary.MaxVarintLen64]byte
	if err := decompressor.WithReadAhead(func() error {
		g := decompressor.MakeGetter()
		buf := make([]byte, 0, 16*etl.BufIOSize)
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
			n := binary.PutUvarint(numBuf[:], uint64(len(buf)))
			if _, err := wr.Write(numBuf[:n]); err != nil {
				return err
			}
			if _, err := wr.Write(buf); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	}); err != nil {
		return err
	}
	_ = ctx
	return nil
}
func doCompress(cliCtx *cli.Context) error {
	ctx, cancel := common.RootContext()
	defer cancel()
	args := cliCtx.Args()
	if len(args) != 1 {
		return fmt.Errorf("expecting .seg file path")
	}
	f := args[0]
	datadir := cliCtx.String(utils.DataDirFlag.Name)
	tmpDir := filepath.Join(datadir, etl.TmpDirName)
	c, err := compress.NewCompressor(ctx, "", f, tmpDir, compress.MinPatternScore, runtime.NumCPU()-1, log.LvlInfo)
	if err != nil {
		return err
	}
	r := bufio.NewReaderSize(os.Stdin, 16*etl.BufIOSize)
	buf := make([]byte, 0, 32*1024*1024)
	var l uint64
	for l, err = binary.ReadUvarint(r); err == nil; l, err = binary.ReadUvarint(r) {
		if cap(buf) < int(l) {
			buf = make([]byte, l)
		} else {
			buf = buf[:l]
		}
		if _, err = io.ReadFull(r, buf); err != nil {
			return err
		}
		if err = c.AddWord(buf); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if err := c.Compress(); err != nil {
		return err
	}

	return nil
}
func doRetireCommand(cliCtx *cli.Context) error {
	ctx, cancel := common.RootContext()
	defer cancel()

	datadir := cliCtx.String(utils.DataDirFlag.Name)
	snapshotDir := filepath.Join(datadir, "snapshots")
	tmpDir := filepath.Join(datadir, etl.TmpDirName)
	from := cliCtx.Uint64(SnapshotFromFlag.Name)
	to := cliCtx.Uint64(SnapshotToFlag.Name)
	every := cliCtx.Uint64(SnapshotEveryFlag.Name)

	chainDB := mdbx.NewMDBX(log.New()).Label(kv.ChainDB).Path(path.Join(datadir, "chaindata")).Readonly().MustOpen()
	defer chainDB.Close()

	cfg := ethconfig.NewSnapshotCfg(true, true)
	rwSnapshotDir, err := dir.OpenRw(snapshotDir)
	if err != nil {
		return err
	}
	defer rwSnapshotDir.Close()
	chainConfig := tool.ChainConfigFromDB(chainDB)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)
	snapshots := snapshotsync.NewRoSnapshots(cfg, snapshotDir)
	snapshots.Reopen()

	br := snapshotsync.NewBlockRetire(runtime.NumCPU()-1, tmpDir, snapshots, rwSnapshotDir, chainDB, nil, nil)

	for i := from; i < to; i += every {
		br.RetireBlocksInBackground(ctx, i, i+every, *chainID, log.LvlInfo)
		br.Wait()
		res := br.Result()
		if res.Err != nil {
			panic(res.Err)
		}
	}
	return nil
}

func doSnapshotCommand(cliCtx *cli.Context) error {
	ctx, cancel := common.RootContext()
	defer cancel()

	fromBlock := cliCtx.Uint64(SnapshotFromFlag.Name)
	toBlock := cliCtx.Uint64(SnapshotToFlag.Name)
	segmentSize := cliCtx.Uint64(SnapshotSegmentSizeFlag.Name)
	if segmentSize < 1000 {
		return fmt.Errorf("too small --segment.size %d", segmentSize)
	}
	datadir := cliCtx.String(utils.DataDirFlag.Name)
	snapshotDir := filepath.Join(datadir, "snapshots")
	tmpDir := filepath.Join(datadir, etl.TmpDirName)
	dir.MustExist(tmpDir)

	chainDB := mdbx.NewMDBX(log.New()).Label(kv.ChainDB).Path(filepath.Join(datadir, "chaindata")).Readonly().MustOpen()
	defer chainDB.Close()

	if err := snapshotBlocks(ctx, chainDB, fromBlock, toBlock, segmentSize, snapshotDir, tmpDir); err != nil {
		log.Error("Error", "err", err)
	}
	return nil
}
func doRecompressCommand(cliCtx *cli.Context) error {
	ctx, cancel := common.RootContext()
	defer cancel()

	datadir := cliCtx.String(utils.DataDirFlag.Name)
	snapshotDir, err := dir.OpenRw(filepath.Join(datadir, "snapshots"))
	if err != nil {
		return err
	}
	tmpDir := filepath.Join(datadir, etl.TmpDirName)
	dir.MustExist(tmpDir)

	if err := snapshotsync.RecompressSegments(ctx, snapshotDir, tmpDir); err != nil {
		log.Error("Error", "err", err)
	}
	return nil
}
func rebuildIndices(ctx context.Context, chainDB kv.RoDB, cfg ethconfig.Snapshot, snapshotDir *dir.Rw, tmpDir string, from uint64) error {
	chainConfig := tool.ChainConfigFromDB(chainDB)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	allSnapshots := snapshotsync.NewRoSnapshots(cfg, snapshotDir.Path)
	if err := allSnapshots.Reopen(); err != nil {
		return err
	}
	if err := snapshotsync.BuildIndices(ctx, allSnapshots, snapshotDir, *chainID, tmpDir, from, log.LvlInfo); err != nil {
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

	dir.MustExist(snapshotDir)

	log.Info("Last body number", "last", last)
	workers := runtime.NumCPU() - 1
	if workers < 1 {
		workers = 1
	}
	if err := snapshotsync.DumpBlocks(ctx, fromBlock, last, blocksPerFile, tmpDir, snapshotDir, chainDB, workers, log.LvlInfo); err != nil {
		return fmt.Errorf("DumpBlocks: %w", err)
	}
	return nil
}

//nolint
func checkBlockSnapshot(chaindata string) error {
	database := mdbx.MustOpen(chaindata)
	defer database.Close()
	datadir := path.Dir(chaindata)
	chainConfig := tool.ChainConfigFromDB(database)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)
	_ = chainID

	cfg := ethconfig.NewSnapshotCfg(true, true)
	snapshots := snapshotsync.NewRoSnapshots(cfg, filepath.Join(datadir, "snapshots"))
	snapshots.Reopen()
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
