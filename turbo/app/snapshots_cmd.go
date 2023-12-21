package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/metrics"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/diagnostics"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

func joinFlags(lists ...[]cli.Flag) (res []cli.Flag) {
	lists = append(lists, debug.Flags, logging.Flags, utils.MetricFlags)
	for _, list := range lists {
		res = append(res, list...)
	}
	return res
}

var snapshotCommand = cli.Command{
	Name:  "snapshots",
	Usage: `Managing snapshots (historical data partitions)`,
	Before: func(context *cli.Context) error {
		_, _, err := debug.Setup(context, true /* rootLogger */)
		if err != nil {
			return err
		}
		return nil
	},
	Subcommands: []*cli.Command{
		{
			Name:   "index",
			Action: doIndicesCommand,
			Usage:  "Create all indices for snapshots",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&SnapshotFromFlag,
				&SnapshotRebuildFlag,
			}),
		},
		{
			Name:   "retire",
			Action: doRetireCommand,
			Usage:  "erigon snapshots uncompress a.seg | erigon snapshots compress b.seg",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&SnapshotFromFlag,
				&SnapshotToFlag,
				&SnapshotEveryFlag,
			}),
		},
		{
			Name:   "uploader",
			Action: doUploaderCommand,
			Usage:  "run erigon in snapshot upload mode (no execution)",
			Flags: uploaderCommandFlags([]cli.Flag{
				&SnapshotVersionFlag,
				&erigoncli.UploadLocationFlag,
				&erigoncli.UploadFromFlag,
				&erigoncli.FrozenBlockLimitFlag,
			}),
		},
		{
			Name:   "uncompress",
			Action: doUncompress,
			Usage:  "erigon snapshots uncompress a.seg | erigon snapshots compress b.seg",
			Flags:  joinFlags([]cli.Flag{}),
		},
		{
			Name:   "compress",
			Action: doCompress,
			Flags:  joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
		{
			Name:   "ram",
			Action: doRam,
			Flags:  joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
		{
			Name:   "decompress_speed",
			Action: doDecompressSpeed,
			Flags:  joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
		{
			Name:   "diff",
			Action: doDiff,
			Flags: joinFlags([]cli.Flag{
				&cli.PathFlag{
					Name:     "src",
					Required: true,
				},
				&cli.PathFlag{
					Name:     "dst",
					Required: true,
				},
			}),
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
	SnapshotVersionFlag = cli.IntFlag{
		Name:  "snapshot.version",
		Usage: "Snapshot files version.",
		Value: 1,
	}
	SnapshotRebuildFlag = cli.BoolFlag{
		Name:  "rebuild",
		Usage: "Force rebuild",
	}
)

func doDiff(cliCtx *cli.Context) error {
	defer log.Info("Done")
	srcF, dstF := cliCtx.String("src"), cliCtx.String("dst")
	src, err := compress.NewDecompressor(srcF)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := compress.NewDecompressor(dstF)
	if err != nil {
		return err
	}
	defer dst.Close()

	i := 0
	srcG, dstG := src.MakeGetter(), dst.MakeGetter()
	var srcBuf, dstBuf []byte
	for srcG.HasNext() {
		i++
		srcBuf, _ = srcG.Next(srcBuf[:0])
		dstBuf, _ = dstG.Next(dstBuf[:0])

		if !bytes.Equal(srcBuf, dstBuf) {
			log.Error(fmt.Sprintf("found difference: %d, %x, %x\n", i, srcBuf, dstBuf))
			return nil
		}
	}
	return nil
}

func doDecompressSpeed(cliCtx *cli.Context) error {
	logger, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	args := cliCtx.Args()
	if args.Len() < 1 {
		return fmt.Errorf("expecting file path as a first argument")
	}
	f := args.First()

	decompressor, err := compress.NewDecompressor(f)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	func() {
		defer decompressor.EnableReadAhead().DisableReadAhead()

		t := time.Now()
		g := decompressor.MakeGetter()
		buf := make([]byte, 0, 16*etl.BufIOSize)
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
		}
		logger.Info("decompress speed", "took", time.Since(t))
	}()
	func() {
		defer decompressor.EnableReadAhead().DisableReadAhead()

		t := time.Now()
		g := decompressor.MakeGetter()
		for g.HasNext() {
			_, _ = g.Skip()
		}
		log.Info("decompress skip speed", "took", time.Since(t))
	}()
	return nil
}
func doRam(cliCtx *cli.Context) error {
	var logger log.Logger
	var err error
	if logger, _, err = debug.Setup(cliCtx, true /* rootLogger */); err != nil {
		return err
	}
	defer logger.Info("Done")
	args := cliCtx.Args()
	if args.Len() < 1 {
		return fmt.Errorf("expecting file path as a first argument")
	}
	f := args.First()
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	before := m.Alloc
	logger.Info("RAM before open", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	decompressor, err := compress.NewDecompressor(f)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	dbg.ReadMemStats(&m)
	logger.Info("RAM after open", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys), "diff", common.ByteCount(m.Alloc-before))
	return nil
}

func doIndicesCommand(cliCtx *cli.Context) error {
	logger, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	defer logger.Info("Done")
	ctx := cliCtx.Context

	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	rebuild := cliCtx.Bool(SnapshotRebuildFlag.Name)
	//from := cliCtx.Uint64(SnapshotFromFlag.Name)

	chainDB := mdbx.NewMDBX(logger).Path(dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	dir.MustExist(dirs.SnapHistory)
	chainConfig := fromdb.ChainConfig(chainDB)

	if rebuild {
		panic("not implemented")
	}

	cfg := ethconfig.NewSnapCfg(true, false, true)
	blockSnaps, borSnaps, br, agg, err := openSnaps(ctx, cfg, dirs, snapcfg.KnownCfg(chainConfig.ChainName, 0).Version, chainDB, logger)

	if err != nil {
		return err
	}
	defer blockSnaps.Close()
	defer borSnaps.Close()
	defer agg.Close()
	if err := br.BuildMissedIndicesIfNeed(ctx, "Indexing", nil, chainConfig); err != nil {
		return err
	}
	err = agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers())
	if err != nil {
		return err
	}

	return nil
}

func openSnaps(ctx context.Context, cfg ethconfig.BlocksFreezing, dirs datadir.Dirs, version uint8, chainDB kv.RwDB, logger log.Logger) (
	blockSnaps *freezeblocks.RoSnapshots, borSnaps *freezeblocks.BorRoSnapshots, br *freezeblocks.BlockRetire, agg *libstate.AggregatorV3, err error,
) {
	blockSnaps = freezeblocks.NewRoSnapshots(cfg, dirs.Snap, version, logger)
	if err = blockSnaps.ReopenFolder(); err != nil {
		return
	}
	blockSnaps.LogStat("open")

	borSnaps = freezeblocks.NewBorRoSnapshots(cfg, dirs.Snap, version, logger)
	if err = borSnaps.ReopenFolder(); err != nil {
		return
	}
	borSnaps.LogStat("open")

	agg, err = libstate.NewAggregatorV3(ctx, dirs.SnapHistory, dirs.Tmp, ethconfig.HistoryV3AggregationStep, chainDB, logger)
	if err != nil {
		return
	}
	agg.SetWorkers(estimate.CompressSnapshot.Workers())
	err = agg.OpenFolder()
	if err != nil {
		return
	}

	blockReader := freezeblocks.NewBlockReader(blockSnaps, borSnaps)
	blockWriter := blockio.NewBlockWriter(fromdb.HistV3(chainDB))
	chainConfig := fromdb.ChainConfig(chainDB)
	br = freezeblocks.NewBlockRetire(estimate.CompressSnapshot.Workers(), dirs, blockReader, blockWriter, chainDB, chainConfig, nil, logger)
	return
}

func doUncompress(cliCtx *cli.Context) error {
	var logger log.Logger
	var err error
	if logger, _, err = debug.Setup(cliCtx, true /* rootLogger */); err != nil {
		return err
	}
	ctx := cliCtx.Context

	args := cliCtx.Args()
	if args.Len() < 1 {
		return fmt.Errorf("expecting file path as a first argument")
	}
	f := args.First()

	decompressor, err := compress.NewDecompressor(f)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.EnableReadAhead().DisableReadAhead()

	wr := bufio.NewWriterSize(os.Stdout, int(128*datasize.MB))
	defer wr.Flush()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	var i uint
	var numBuf [binary.MaxVarintLen64]byte

	g := decompressor.MakeGetter()
	buf := make([]byte, 0, 1*datasize.MB)
	for g.HasNext() {
		buf, _ = g.Next(buf[:0])
		n := binary.PutUvarint(numBuf[:], uint64(len(buf)))
		if _, err := wr.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err := wr.Write(buf); err != nil {
			return err
		}
		i++
		select {
		case <-logEvery.C:
			_, fileName := filepath.Split(decompressor.FilePath())
			progress := 100 * float64(i) / float64(decompressor.Count())
			logger.Info("[uncompress] ", "progress", fmt.Sprintf("%.2f%%", progress), "file", fileName)
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}
func doCompress(cliCtx *cli.Context) error {
	var err error
	var logger log.Logger
	if logger, _, err = debug.Setup(cliCtx, true /* rootLogger */); err != nil {
		return err
	}
	ctx := cliCtx.Context

	args := cliCtx.Args()
	if args.Len() < 1 {
		return fmt.Errorf("expecting file path as a first argument")
	}
	f := args.First()
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	logger.Info("file", "datadir", dirs.DataDir, "f", f)
	c, err := compress.NewCompressor(ctx, "compress", f, dirs.Tmp, compress.MinPatternScore, estimate.CompressSnapshot.Workers(), log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer c.Close()
	r := bufio.NewReaderSize(os.Stdin, int(128*datasize.MB))
	buf := make([]byte, 0, int(1*datasize.MB))
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
	var logger log.Logger
	var err error
	if logger, _, err = debug.Setup(cliCtx, true /* rootLogger */); err != nil {
		return err
	}
	defer logger.Info("Done")
	ctx := cliCtx.Context

	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	from := cliCtx.Uint64(SnapshotFromFlag.Name)
	to := cliCtx.Uint64(SnapshotToFlag.Name)
	every := cliCtx.Uint64(SnapshotEveryFlag.Name)
	version := uint8(cliCtx.Int(SnapshotVersionFlag.Name))

	db := mdbx.NewMDBX(logger).Label(kv.ChainDB).Path(dirs.Chaindata).MustOpen()
	defer db.Close()

	cfg := ethconfig.NewSnapCfg(true, false, true)
	blockSnaps, borSnaps, br, agg, err := openSnaps(ctx, cfg, dirs, version, db, logger)
	if err != nil {
		return err
	}
	defer blockSnaps.Close()
	defer borSnaps.Close()
	defer agg.Close()

	chainConfig := fromdb.ChainConfig(db)
	if err := br.BuildMissedIndicesIfNeed(ctx, "retire", nil, chainConfig); err != nil {
		return err
	}

	agg.CleanDir()

	var forwardProgress uint64
	if to == 0 {
		db.View(ctx, func(tx kv.Tx) error {
			forwardProgress, err = stages.GetStageProgress(tx, stages.Senders)
			return err
		})
		blockReader, _ := br.IO()
		from2, to2, ok := freezeblocks.CanRetire(forwardProgress, blockReader.FrozenBlocks())
		if ok {
			from, to, every = from2, to2, to2-from2
		}
	}

	logger.Info("Params", "from", from, "to", to, "every", every)
	if _, err := br.RetireBlocks(ctx, 0, forwardProgress, log.LvlInfo, nil, nil); err != nil {
		return err
	}

	if err := db.Update(ctx, func(tx kv.RwTx) error {
		blockReader, _ := br.IO()
		if err := rawdb.WriteSnapshots(tx, blockReader.FrozenFiles(), agg.Files()); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	for j := 0; j < 10_000; j++ { // prune happens by small steps, so need many runs
		if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
			if err := br.PruneAncientBlocks(tx, 100); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if !kvcfg.HistoryV3.FromDB(db) {
		return nil
	}

	logger.Info("Prune state history")
	for i := 0; i < 1024; i++ {
		if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
			agg.SetTx(tx)
			if err = agg.Prune(ctx, ethconfig.HistoryV3AggregationStep/2); err != nil {
				return err
			}
			return err
		}); err != nil {
			return err
		}
	}

	logger.Info("Work on state history blockSnapshots")
	indexWorkers := estimate.IndexSnapshot.Workers()
	if err = agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}

	var lastTxNum uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		execProgress, _ := stages.GetStageProgress(tx, stages.Execution)
		lastTxNum, err = rawdbv3.TxNums.Max(tx, execProgress)
		if err != nil {
			return err
		}
		agg.SetTxNum(lastTxNum)
		return nil
	}); err != nil {
		return err
	}

	logger.Info("Build state history blockSnapshots")
	if err = agg.BuildFiles(lastTxNum); err != nil {
		return err
	}

	if err = agg.MergeLoop(ctx, estimate.CompressSnapshot.Workers()); err != nil {
		return err
	}
	if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteSnapshots(tx, blockSnaps.Files(), agg.Files())
	}); err != nil {
		return err
	}

	logger.Info("Prune state history")
	for i := 0; i < 1024; i++ {
		if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
			agg.SetTx(tx)
			if err = agg.Prune(ctx, ethconfig.HistoryV3AggregationStep/10); err != nil {
				return err
			}
			return err
		}); err != nil {
			return err
		}
	}
	logger.Info("Prune state history")
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteSnapshots(tx, blockSnaps.Files(), agg.Files())
	}); err != nil {
		return err
	}

	return nil
}

func uploaderCommandFlags(flags []cli.Flag) []cli.Flag {
	erigoncli.SyncLoopBreakAfterFlag.Value = "Senders"
	erigoncli.SyncLoopBlockLimitFlag.Value = 100000
	erigoncli.SyncLoopPruneLimitFlag.Value = 100000
	erigoncli.FrozenBlockLimitFlag.Value = 1500000
	utils.NoDownloaderFlag.Value = true
	utils.HTTPEnabledFlag.Value = false
	utils.TxPoolDisableFlag.Value = true
	return joinFlags(erigoncli.DefaultFlags, flags, []cli.Flag{
		&erigoncli.SyncLoopBreakAfterFlag,
		&erigoncli.SyncLoopBlockLimitFlag,
		&erigoncli.SyncLoopPruneLimitFlag,
	})
}

func doUploaderCommand(cliCtx *cli.Context) error {
	var logger log.Logger
	var err error
	var metricsMux *http.ServeMux

	if logger, metricsMux, err = debug.Setup(cliCtx, true /* root logger */); err != nil {
		return err
	}

	// initializing the node and providing the current git commit there

	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)
	erigonInfoGauge := metrics.GetOrCreateGauge(fmt.Sprintf(`erigon_info{version="%s",commit="%s"}`, params.Version, params.GitCommit))
	erigonInfoGauge.Set(1)

	if version := uint8(cliCtx.Int(SnapshotVersionFlag.Name)); version != 0 {
		snapcfg.SnapshotVersion(version)
	}

	nodeCfg := node.NewNodConfigUrfave(cliCtx, logger)
	if err := datadir.ApplyMigrations(nodeCfg.Dirs); err != nil {
		return err
	}

	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg, logger)

	ethNode, err := node.New(cliCtx.Context, nodeCfg, ethCfg, logger)
	if err != nil {
		log.Error("Erigon startup", "err", err)
		return err
	}

	if metricsMux != nil {
		diagnostics.Setup(cliCtx, metricsMux, ethNode)
	}

	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving an Erigon node", "err", err)
	}
	return err
}
