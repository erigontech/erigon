package app

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/dir"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/systemcontracts"

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
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
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
			Name:   "decompress-speed",
			Action: doDecompressSpeed,
			Flags:  joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
		{
			Name:   "bt-search",
			Action: doBtSearch,
			Flags: joinFlags([]cli.Flag{
				&cli.PathFlag{
					Name:     "src",
					Required: true,
				},
				&cli.StringFlag{
					Name:     "key",
					Required: true,
				},
			}),
		},
		{
			Name: "rm-all-state-snapshots",
			Action: func(cliCtx *cli.Context) error {
				dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
				return dir.DeleteFiles(dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors)
			},
			Flags: joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
		{
			Name: "rm-state-snapshots",
			Action: func(cliCtx *cli.Context) error {
				dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
				steprm := cliCtx.String("step")
				if steprm == "" {
					return errors.New("step to remove is required (eg 0-2)")
				}
				steprm = fmt.Sprintf(".%s.", steprm)

				removed := 0
				for _, dir := range []string{dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors} {
					files, err := os.ReadDir(dir)
					if err != nil {
						return err
					}
					for _, file := range files {
						if !file.IsDir() && strings.Contains(file.Name(), steprm) {
							if err := os.Remove(filepath.Join(dir, file.Name())); err != nil {
								return fmt.Errorf("failed to remove %s: %w", file.Name(), err)
							}
							removed++
						}
					}
				}
				fmt.Printf("removed %d state snapshot files\n", removed)
				return nil
			},
			Flags: joinFlags([]cli.Flag{&utils.DataDirFlag, &cli.StringFlag{Name: "step", Required: true}}),
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
	SnapshotRebuildFlag = cli.BoolFlag{
		Name:  "rebuild",
		Usage: "Force rebuild",
	}
)

func doBtSearch(cliCtx *cli.Context) error {
	logger, _, err := debug.Setup(cliCtx, true /* root logger */)
	if err != nil {
		return err
	}

	srcF := cliCtx.String("src")
	dataFilePath := strings.TrimRight(srcF, ".bt") + ".kv"

	runtime.GC()
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info("before open", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	idx, err := libstate.OpenBtreeIndex(srcF, dataFilePath, libstate.DefaultBtreeM, libstate.CompressKeys|libstate.CompressVals, false)
	if err != nil {
		return err
	}
	defer idx.Close()

	runtime.GC()
	dbg.ReadMemStats(&m)
	logger.Info("after open", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

	seek := common.FromHex(cliCtx.String("key"))

	cur, err := idx.SeekDeprecated(seek)
	if err != nil {
		return err
	}
	if cur != nil {
		fmt.Printf("seek: %x, -> %x, %x\n", seek, cur.Key(), cur.Value())
	} else {
		fmt.Printf("seek: %x, -> nil\n", seek)
	}

	return nil
}

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

func doIndicesCommand(cliCtx *cli.Context) error {
	logger, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	defer logger.Info("Done")
	ctx := cliCtx.Context

	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	rebuild := cliCtx.Bool(SnapshotRebuildFlag.Name)
	chainDB := mdbx.NewMDBX(logger).Path(dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	if rebuild {
		panic("not implemented")
	}
	cfg := ethconfig.NewSnapCfg(true, true, false)

	allSnapshots := freezeblocks.NewRoSnapshots(cfg, dirs.Snap, logger)
	if err := allSnapshots.ReopenFolder(); err != nil {
		return err
	}
	allSnapshots.LogStat()
	indexWorkers := estimate.IndexSnapshot.Workers()
	chainConfig := fromdb.ChainConfig(chainDB)
	if err := freezeblocks.BuildMissedIndices("Indexing", ctx, dirs, chainConfig, indexWorkers, logger); err != nil {
		return err
	}
	agg, err := libstate.NewAggregatorV3(ctx, dirs, ethconfig.HistoryV3AggregationStep, chainDB, logger)
	if err != nil {
		return err
	}
	if err = agg.OpenFolder(); err != nil {
		return err
	}
	chainDB.View(ctx, func(tx kv.Tx) error {
		ac := agg.MakeContext()
		defer ac.Close()
		ac.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
			_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
			return histBlockNumProgress
		})
		return nil
	})
	if err = agg.BuildOptionalMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}
	if err = agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}

	return nil
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
	db := mdbx.NewMDBX(logger).Label(kv.ChainDB).Path(dirs.Chaindata).MustOpen()
	defer db.Close()

	cfg := ethconfig.NewSnapCfg(true, false, true)
	blockSnapshots := freezeblocks.NewRoSnapshots(cfg, dirs.Snap, logger)
	borSnapshots := freezeblocks.NewBorRoSnapshots(cfg, dirs.Snap, logger)
	if err := blockSnapshots.ReopenFolder(); err != nil {
		return err
	}
	blockReader := freezeblocks.NewBlockReader(blockSnapshots, borSnapshots)
	blockWriter := blockio.NewBlockWriter(fromdb.HistV3(db))

	br := freezeblocks.NewBlockRetire(estimate.CompressSnapshot.Workers(), dirs, blockReader, blockWriter, db, nil, logger)
	agg, err := libstate.NewAggregatorV3(ctx, dirs, ethconfig.HistoryV3AggregationStep, db, logger)
	if err != nil {
		return err
	}
	err = agg.OpenFolder()
	if err != nil {
		return err
	}
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())

	var cc *chain.Config
	if err := db.View(ctx, func(tx kv.Tx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		cc, err = rawdb.ReadChainConfig(tx, genesisHash)
		return err
	}); err != nil {
		return err
	}

	db, err = temporal.New(db, agg, systemcontracts.SystemContractCodeLookup[cc.ChainName])
	if err != nil {
		return err
	}

	//agg.KeepStepsInDB(0)

	db.View(ctx, func(tx kv.Tx) error {
		blockSnapshots.LogStat()
		ac := agg.MakeContext()
		defer ac.Close()
		ac.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
			_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
			return histBlockNumProgress
		})
		return nil
	})

	if to == 0 {
		var forwardProgress uint64
		db.View(ctx, func(tx kv.Tx) error {
			forwardProgress, err = stages.GetStageProgress(tx, stages.Senders)
			return err
		})
		from2, to2, ok := freezeblocks.CanRetire(forwardProgress, blockReader.FrozenBlocks())
		if ok {
			from, to, every = from2, to2, to2-from2
		}
	}

	logger.Info("Params", "from", from, "to", to, "every", every)
	{
		logEvery := time.NewTicker(10 * time.Second)
		defer logEvery.Stop()

		for j := 0; j < 10_000; j++ { // prune happens by small steps, so need many runs
			if err := db.Update(ctx, func(tx kv.RwTx) error {
				if err := br.PruneAncientBlocks(tx, 100, false /* includeBor */); err != nil {
					return err
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					firstNonGenesisHeader, err := rawdbv3.SecondKey(tx, kv.Headers)
					if err != nil {
						return err
					}
					if len(firstNonGenesisHeader) > 0 {
						logger.Info("Prunning old blocks", "progress", binary.BigEndian.Uint64(firstNonGenesisHeader))
					}
				default:
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}

	for i := from; i < to; i += every {
		if err := br.RetireBlocks(ctx, i, i+every, log.LvlInfo, nil, nil); err != nil {
			panic(err)
		}
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			ac := agg.MakeContext()
			defer ac.Close()
			if err := rawdb.WriteSnapshots(tx, blockReader.FrozenFiles(), ac.Files()); err != nil {
				return err
			}
			for j := 0; j < 10_000; j++ { // prune happens by small steps, so need many runs
				if err := br.PruneAncientBlocks(tx, 100, false /* includeBor */); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if !kvcfg.HistoryV3.FromDB(db) {
		return nil
	}

	logger.Info("Compute commitment")
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		if casted, ok := tx.(kv.CanWarmupDB); ok {
			if err := casted.WarmupDB(false); err != nil {
				return err
			}
		}
		ac := agg.MakeContext()
		defer ac.Close()
		sd := libstate.NewSharedDomains(tx)
		defer sd.Close()
		if _, err = sd.ComputeCommitment(ctx, true, false, sd.BlockNum()); err != nil {
			return err
		}
		if err := sd.Flush(ctx, tx); err != nil {
			return err
		}
		return err
	}); err != nil {
		return err
	}

	logger.Info("Prune state history")
	for i := 0; i < 1; i++ {
		if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
			ac := agg.MakeContext()
			defer ac.Close()
			if ac.CanPrune(tx) {
				if err = ac.PruneWithTimeout(ctx, time.Hour, tx); err != nil {
					return err
				}
			}
			return err
		}); err != nil {
			return err
		}
	}

	logger.Info("Work on state history snapshots")
	indexWorkers := estimate.IndexSnapshot.Workers()
	if err = agg.BuildOptionalMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}
	if err = agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}

	var lastTxNum uint64
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		execProgress, _ := stages.GetStageProgress(tx, stages.Execution)
		lastTxNum, err = rawdbv3.TxNums.Max(tx, execProgress)
		if err != nil {
			return err
		}

		ac := agg.MakeContext()
		defer ac.Close()
		return nil
	}); err != nil {
		return err
	}

	logger.Info("Build state history snapshots")
	if err = agg.BuildFiles(lastTxNum); err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
			ac := agg.MakeContext()
			defer ac.Close()
			if ac.CanPrune(tx) {
				if err = ac.PruneWithTimeout(ctx, time.Hour, tx); err != nil {
					return err
				}
			}
			return err
		}); err != nil {
			return err
		}
	}

	if err = agg.MergeLoop(ctx, estimate.AlmostAllCPUs()); err != nil {
		return err
	}
	if err = agg.BuildOptionalMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}
	if err = agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}
	if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
		ac := agg.MakeContext()
		defer ac.Close()
		return rawdb.WriteSnapshots(tx, blockSnapshots.Files(), ac.Files())
	}); err != nil {
		return err
	}
	logger.Info("Prune state history")
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		ac := agg.MakeContext()
		defer ac.Close()
		return rawdb.WriteSnapshots(tx, blockSnapshots.Files(), ac.Files())
	}); err != nil {
		return err
	}

	return nil
}
