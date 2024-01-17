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
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/eth/integrity"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/semaphore"

	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/metrics"
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
				&SnapshotVersionFlag,
			}),
		},
		{
			Name:   "uploader",
			Action: doUploaderCommand,
			Usage:  "run erigon in snapshot upload mode (no execution)",
			Flags: joinFlags(erigoncli.DefaultFlags,
				[]cli.Flag{
					&SnapshotVersionFlag,
					&erigoncli.UploadLocationFlag,
					&erigoncli.UploadFromFlag,
					&erigoncli.FrozenBlockLimitFlag,
				}),
			Before: func(ctx *cli.Context) error {
				ctx.Set(erigoncli.SyncLoopBreakAfterFlag.Name, "Senders")
				ctx.Set(utils.NoDownloaderFlag.Name, "true")
				ctx.Set(utils.HTTPEnabledFlag.Name, "false")
				ctx.Set(utils.TxPoolDisableFlag.Name, "true")

				if !ctx.IsSet(erigoncli.SyncLoopBlockLimitFlag.Name) {
					ctx.Set(erigoncli.SyncLoopBlockLimitFlag.Name, "100000")
				}

				if !ctx.IsSet(erigoncli.FrozenBlockLimitFlag.Name) {
					ctx.Set(erigoncli.FrozenBlockLimitFlag.Name, "1500000")
				}

				if !ctx.IsSet(erigoncli.SyncLoopPruneLimitFlag.Name) {
					ctx.Set(erigoncli.SyncLoopPruneLimitFlag.Name, "100000")
				}

				return nil
			},
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
				&cli.PathFlag{Name: "src", Required: true},
				&cli.StringFlag{Name: "key", Required: true},
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
				for _, dirPath := range []string{dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors} {
					filePaths, err := dir.ListFiles(dirPath)
					if err != nil {
						return err
					}
					for _, filePath := range filePaths {
						_, fName := filepath.Split(filePath)
						if !strings.Contains(fName, steprm) {
							continue
						}

						if err := os.Remove(filePath); err != nil {
							return fmt.Errorf("failed to remove %s: %w", fName, err)
						}
						removed++
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
				&cli.PathFlag{Name: "src", Required: true},
				&cli.PathFlag{Name: "dst", Required: true},
			}),
		},
		{
			Name:   "meta",
			Action: doMeta,
			Flags: joinFlags([]cli.Flag{
				&cli.PathFlag{Name: "src", Required: true},
			}),
		},
		{
			Name:   "debug",
			Action: doDebugKey,
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.StringFlag{Name: "key", Required: true},
				&cli.StringFlag{Name: "domain", Required: true},
			}),
		},
		{
			Name:   "integrity",
			Action: doIntegrity,
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
			}),
		},
		//{
		//	Name:   "bodies_decrement_datafix",
		//	Action: doBodiesDecrement,
		//	Flags: joinFlags([]cli.Flag{
		//		&utils.DataDirFlag,
		//	}),
		//},
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
	//var a = accounts.Account{}
	//accounts.DeserialiseV3(&a, cur.Value())
	//fmt.Printf("a: nonce=%d\n", a.Nonce)
	return nil
}

func doDebugKey(cliCtx *cli.Context) error {
	logger, _, err := debug.Setup(cliCtx, true /* root logger */)
	if err != nil {
		return err
	}
	key := common.FromHex(cliCtx.String("key"))
	var domain kv.Domain
	var idx kv.InvertedIdx
	ds := cliCtx.String("domain")
	switch ds {
	case "accounts":
		domain, idx = kv.AccountsDomain, kv.AccountsHistoryIdx
	case "storage":
		domain, idx = kv.StorageDomain, kv.StorageHistoryIdx
	case "code":
		domain, idx = kv.CodeDomain, kv.CodeHistoryIdx
	case "commitment":
		domain, idx = kv.CommitmentDomain, kv.CommitmentHistoryIdx
	default:
		panic(ds)
	}
	_ = idx

	ctx := cliCtx.Context
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	chainDB := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	agg := openAgg(ctx, dirs, chainDB, logger)

	view := agg.MakeContext()
	defer view.Close()
	if err := view.DebugKey(domain, key); err != nil {
		return err
	}
	if err := view.DebugEFKey(domain, key); err != nil {
		return err
	}
	return nil
}

func doIntegrity(cliCtx *cli.Context) error {
	logger, _, err := debug.Setup(cliCtx, true /* root logger */)
	if err != nil {
		return err
	}

	ctx := cliCtx.Context
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	chainDB := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	cfg := ethconfig.NewSnapCfg(true, false, true)
	chainConfig := fromdb.ChainConfig(chainDB)
	blockSnaps, borSnaps, blockRetire, agg, err := openSnaps(ctx, cfg, dirs, snapcfg.KnownCfg(chainConfig.ChainName, 0).Version, chainDB, logger)
	if err != nil {
		return err
	}
	defer blockSnaps.Close()
	defer borSnaps.Close()
	defer agg.Close()

	blockReader, _ := blockRetire.IO()
	if err := blockReader.(*freezeblocks.BlockReader).IntegrityTxnID(false); err != nil {
		return err
	}

	if err := integrity.E3HistoryNoSystemTxs(ctx, chainDB, agg); err != nil {
		return err
	}

	return nil
}

func doDiff(cliCtx *cli.Context) error {
	log.Info("staring")
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

func doMeta(cliCtx *cli.Context) error {
	fname := cliCtx.String("src")
	if strings.HasSuffix(fname, ".seg") {
		src, err := compress.NewDecompressor(fname)
		if err != nil {
			return err
		}
		defer src.Close()
		log.Info("meta", "count", src.Count(), "size", datasize.ByteSize(src.Size()).String(), "name", src.FileName())
	} else if strings.HasSuffix(fname, ".bt") {
		kvFPath := strings.TrimSuffix(fname, ".bt") + ".kv"
		src, err := compress.NewDecompressor(kvFPath)
		if err != nil {
			return err
		}
		defer src.Close()
		bt, err := libstate.OpenBtreeIndexWithDecompressor(fname, libstate.DefaultBtreeM, src, libstate.CompressNone)
		if err != nil {
			return err
		}
		defer bt.Close()

		distances, err := bt.Distances()
		if err != nil {
			return err
		}
		for i := range distances {
			distances[i] /= 100_000
		}
		for i := range distances {
			if distances[i] == 0 {
				delete(distances, i)
			}
		}

		log.Info("meta", "distances(*100K)", fmt.Sprintf("%v", distances))
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
	chainDB := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	if rebuild {
		panic("not implemented")
	}

	cfg := ethconfig.NewSnapCfg(true, false, true)
	chainConfig := fromdb.ChainConfig(chainDB)
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
	agg = openAgg(ctx, dirs, chainDB, logger)
	err = chainDB.View(ctx, func(tx kv.Tx) error {
		ac := agg.MakeContext()
		defer ac.Close()
		ac.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
			_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
			return histBlockNumProgress
		})
		return nil
	})
	if err != nil {
		return
	}

	blockReader := freezeblocks.NewBlockReader(blockSnaps, borSnaps)
	blockWriter := blockio.NewBlockWriter(fromdb.HistV3(chainDB))
	chainConfig := fromdb.ChainConfig(chainDB)

	blockSnapBuildSema := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	br = freezeblocks.NewBlockRetire(estimate.CompressSnapshot.Workers(), dirs, blockReader, blockWriter, chainDB, chainConfig, nil, blockSnapBuildSema, logger)
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
	if version != 0 {
		snapcfg.SnapshotVersion(version)
	}

	db := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()

	cfg := ethconfig.NewSnapCfg(true, false, true)
	blockSnaps, borSnaps, br, agg, err := openSnaps(ctx, cfg, dirs, version, db, logger)
	if err != nil {
		return err
	}

	// `erigon retire` command is designed to maximize resouces utilization. But `Erigon itself` does minimize background impact (because not in rush).
	agg.SetCollateAndBuildWorkers(estimate.StateV3Collate.Workers())
	agg.SetMergeWorkers(estimate.AlmostAllCPUs())
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())

	defer blockSnaps.Close()
	defer borSnaps.Close()
	defer agg.Close()

	chainConfig := fromdb.ChainConfig(db)
	if err := br.BuildMissedIndicesIfNeed(ctx, "retire", nil, chainConfig); err != nil {
		return err
	}

	//agg.KeepStepsInDB(0)

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
	if err := br.RetireBlocks(ctx, 0, forwardProgress, log.LvlInfo, nil, nil); err != nil {
		return err
	}

	if err := db.Update(ctx, func(tx kv.RwTx) error {
		blockReader, _ := br.IO()
		ac := agg.MakeContext()
		defer ac.Close()
		if err := rawdb.WriteSnapshots(tx, blockReader.FrozenFiles(), ac.Files()); err != nil {
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

	db, err = temporal.New(db, agg, systemcontracts.SystemContractCodeLookup[chainConfig.ChainName])
	if err != nil {
		return err
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
		sd := libstate.NewSharedDomains(tx, logger)
		defer sd.Close()
		if _, err = sd.ComputeCommitment(ctx, true, sd.BlockNum(), ""); err != nil {
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
				if err = ac.PruneSmallBatches(ctx, time.Hour, tx); err != nil {
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
				if err = ac.PruneSmallBatches(ctx, time.Hour, tx); err != nil {
					return err
				}
			}
			return err
		}); err != nil {
			return err
		}
	}

	if err = agg.MergeLoop(ctx); err != nil {
		return err
	}
	if err = agg.BuildOptionalMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}
	if err = agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}
	if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
		blockReader, _ := br.IO()
		ac := agg.MakeContext()
		defer ac.Close()
		return rawdb.WriteSnapshots(tx, blockReader.FrozenFiles(), ac.Files())
	}); err != nil {
		return err
	}
	logger.Info("Prune state history")
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		ac := agg.MakeContext()
		defer ac.Close()
		return rawdb.WriteSnapshots(tx, blockSnaps.Files(), ac.Files())
	}); err != nil {
		return err
	}

	return nil
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

/*

func doBodiesDecrement(cliCtx *cli.Context) error {
	logger, _, err := debug.Setup(cliCtx, true)
	if err != nil {
		return err
	}
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	ctx := cliCtx.Context
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	list, err := snaptype.Segments(dirs.Snap, 1)
	if err != nil {
		return err
	}
	var l []snaptype.FileInfo
	for _, f := range list {
		if f.T != snaptype.Bodies {
			continue
		}
		if f.From < 18_000_000 {
			continue
		}
		l = append(l, f)
	}
	migrateSingleBody := func(srcF, dstF string) error {
		src, err := compress.NewDecompressor(srcF)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := compress.NewCompressor(ctx, "compress", dstF, dirs.Tmp, compress.MinPatternScore, estimate.CompressSnapshot.Workers(), log.LvlInfo, logger)
		if err != nil {
			return err
		}
		defer dst.Close()

		i := 0
		srcG := src.MakeGetter()
		var buf []byte
		log.Info("start", "file", src.FileName())
		dstBuf := bytes.NewBuffer(nil)
		for srcG.HasNext() {
			i++
			if buf == nil {
				panic(fmt.Sprintf("nil val at file: %s\n", srcG.FileName()))
			}
			buf, _ = srcG.Next(buf[:0])
			if buf == nil {
				panic(fmt.Sprintf("nil val at file: %s\n", srcG.FileName()))
			}
			body := &types.BodyForStorage{}
			if err := rlp.Decode(bytes.NewReader(buf), body); err != nil {
				return err
			}
			body.BaseTxId -= 1
			dstBuf.Reset()
			if err := rlp.Encode(dstBuf, body); err != nil {
				return err
			}

			if err := dst.AddWord(dstBuf.Bytes()); err != nil {
				return err
			}

			select {
			case <-logEvery.C:
				logger.Info("[bodies] progress", "f", src.FileName(), "progress", fmt.Sprintf("%dK/%dK", i/1_000, src.Count()/1_000))
			default:
			}
		}
		if err := dst.Compress(); err != nil {
			return err
		}
		src.Close()
		dst.Close()
		os.Rename(srcF, srcF+".back")
		os.Rename(dstF, srcF)
		os.Remove(srcF + ".torrent")
		os.Remove(srcF + ".idx")
		ext := filepath.Ext(srcF)
		withoutExt := srcF[:len(srcF)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
		log.Info("done", "file", src.FileName())
		return nil
	}
	for _, f := range l {
		srcF, dstF := f.Path, f.Path+"2"
		if err := migrateSingleBody(srcF, dstF); err != nil {
			return err
		}
	}

	return nil
}
*/

func dbCfg(label kv.Label, path string) mdbx.MdbxOpts {
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	opts := mdbx.NewMDBX(log.New()).Path(path).Label(label).RoTxsLimiter(limiterB)
	// integration tool don't intent to create db, then easiest way to open db - it's pass mdbx.Accede flag, which allow
	// to read all options from DB, instead of overriding them
	opts = opts.Accede()
	return opts
}
func openAgg(ctx context.Context, dirs datadir.Dirs, chainDB kv.RwDB, logger log.Logger) *libstate.AggregatorV3 {
	agg, err := libstate.NewAggregatorV3(ctx, dirs, ethconfig.HistoryV3AggregationStep, chainDB, logger)
	if err != nil {
		panic(err)
	}
	if err = agg.OpenFolder(true); err != nil {
		panic(err)
	}
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	return agg
}
