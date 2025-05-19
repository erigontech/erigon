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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/compress"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/disk"
	"github.com/erigontech/erigon-lib/common/mem"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/downloader"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/state/stats"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core/rawdb/blockio"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/diagnostics"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/ethconfig/features"
	"github.com/erigontech/erigon/eth/integrity"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	erigoncli "github.com/erigontech/erigon/turbo/cli"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/logging"
	"github.com/erigontech/erigon/turbo/node"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

func joinFlags(lists ...[]cli.Flag) (res []cli.Flag) {
	lists = append(lists, debug.Flags, logging.Flags, utils.MetricFlags)
	for _, list := range lists {
		res = append(res, list...)
	}
	return res
}

var snapshotCommand = cli.Command{
	Name:    "seg",
	Aliases: []string{"snapshots", "segments"},
	Usage:   `Managing historical data segments (partitions)`,
	Before: func(cliCtx *cli.Context) error {
		go mem.LogMemStats(cliCtx.Context, log.New())
		go disk.UpdateDiskStats(cliCtx.Context, log.New())
		_, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
		if err != nil {
			return err
		}
		return nil
	},
	Subcommands: []*cli.Command{
		{
			Name: "ls",
			Action: func(c *cli.Context) error {
				dirs := datadir.New(c.String(utils.DataDirFlag.Name))
				return doLS(c, dirs)
			},
			Usage: "List all files with their words count",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
			}),
		},
		{
			Name:    "accessor",
			Aliases: []string{"index"},
			Action: func(c *cli.Context) error {
				dirs, l, err := datadir.New(c.String(utils.DataDirFlag.Name)).MustFlock()
				if err != nil {
					return err
				}
				defer l.Unlock()

				return doIndicesCommand(c, dirs)
			},
			Usage: "Create all missed indices for snapshots. It also removing unsupported versions of existing indices and re-build them",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&SnapshotRebuildFlag,
			}),
		},
		{
			Name: "retire",
			Action: func(c *cli.Context) error {
				dirs, l, err := datadir.New(c.String(utils.DataDirFlag.Name)).MustFlock()
				if err != nil {
					return err
				}
				defer l.Unlock()

				return doRetireCommand(c, dirs)
			},
			Usage: "create snapshots from the specified block number",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
			}),
		},
		{
			Name: "unmerge",
			Action: func(c *cli.Context) error {
				dirs, l, err := datadir.New(c.String(utils.DataDirFlag.Name)).MustFlock()
				if err != nil {
					return err
				}
				defer l.Unlock()

				return doUnmerge(c, dirs)
			},
			Usage: "unmerge a particular snapshot file (to 1 step files).",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&SnapshotFileFlag,
			}),
		},
		{
			Name:   "uploader",
			Action: doUploaderCommand,
			Usage:  "run erigon in snapshot upload mode (no execution)",
			Flags: joinFlags(erigoncli.DefaultFlags,
				[]cli.Flag{
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

				return nil
			},
		},
		{
			Name:   "uncompress",
			Action: doUncompress,
			Usage:  "erigon seg uncompress a.seg | erigon seg compress b.seg",
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
			Name:    "rm-state-snapshots",
			Aliases: []string{"rm-state-segments", "rm-state"},
			Action:  doRmStateSnapshots,
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.StringFlag{Name: "step"},
				&cli.BoolFlag{Name: "latest"},
				&cli.StringSliceFlag{Name: "domain"},
			},
			),
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
			Flags:  joinFlags([]cli.Flag{}),
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
			Name:   "sqeeze",
			Action: doSqueeze,
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.StringFlag{Name: "type", Required: true},
			}),
		},
		{
			Name:        "integrity",
			Action:      doIntegrity,
			Description: "run slow validation of files. use --check to run single",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.StringFlag{Name: "check", Usage: fmt.Sprintf("one of: %s", integrity.AllChecks)},
				&cli.BoolFlag{Name: "failFast", Value: true, Usage: "to stop after 1st problem or print WARN log and continue check"},
				&cli.Uint64Flag{Name: "fromStep", Value: 0, Usage: "skip files before given step"},
			}),
		},
		{
			Name: "publishable",
			Action: func(cliCtx *cli.Context) error {
				if err := doPublishable(cliCtx); err != nil {
					log.Error("[publishable]", "err", err)
					return err
				}
				log.Info("[publishable] snapshots are publishable")
				return nil
			},
			Description: "Check if snapshot is publishable by a webseed client",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
			}),
		},
		{
			Name:        "clearIndexing",
			Action:      doClearIndexing,
			Description: "Clear all indexing data",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
			}),
		},
	},
}

var (
	SnapshotRebuildFlag = cli.BoolFlag{
		Name:  "rebuild",
		Usage: "Force rebuild",
	}
	SnapshotFileFlag = cli.StringFlag{
		Name:  "file",
		Usage: "Snapshot file",
	}
)

func doRmStateSnapshots(cliCtx *cli.Context) error {
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))

	removeLatest := cliCtx.Bool("latest")

	_maxFrom := uint64(0)
	files := make([]snaptype.FileInfo, 0)
	for _, dirPath := range []string{dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors} {
		filePaths, err := dir.ListFiles(dirPath)
		if err != nil {
			return err
		}
		for _, filePath := range filePaths {
			_, fName := filepath.Split(filePath)
			res, isStateFile, ok := snaptype.ParseFileName(dirPath, fName)
			if !ok || !isStateFile {
				continue
			}
			if res.From == 0 && res.To == 0 {
				parts := strings.Split(fName, ".")
				if len(parts) == 3 || len(parts) == 4 {
					fsteps := strings.Split(parts[1], "-")
					res.From, err = strconv.ParseUint(fsteps[0], 10, 64)
					if err != nil {
						return err
					}
					res.To, err = strconv.ParseUint(fsteps[1], 10, 64)
					if err != nil {
						return err
					}
				}
			}

			files = append(files, res)
			if removeLatest {
				_maxFrom = max(_maxFrom, res.From)
			}
		}
	}

	if cliCtx.IsSet("step") {
		steprm := cliCtx.String("step")
		if steprm == "" && !removeLatest {
			return errors.New("step to remove is required (eg 0-2) OR flag --latest provided")
		}
		if steprm != "" {
			removeLatest = false // --step has higher priority
		}

		var minS, maxS uint64
		if removeLatest {
		AllowPruneSteps:
			fmt.Printf("remove latest snapshot files with stepFrom=%d?\n1) Remove\n2) Exit\n (pick number): ", _maxFrom)
			var ans uint8
			_, err := fmt.Scanf("%d\n", &ans)
			if err != nil {
				return err
			}
			switch ans {
			case 1:
				minS, maxS = _maxFrom, math.MaxUint64
				break
			case 2:
				return nil
			default:
				fmt.Printf("invalid input: %d; Just an answer number expected.\n", ans)
				goto AllowPruneSteps
			}
		} else if steprm != "" {
			parseStep := func(step string) (uint64, uint64, error) {
				var from, to uint64
				if _, err := fmt.Sscanf(step, "%d-%d", &from, &to); err != nil {
					return 0, 0, fmt.Errorf("step expected in format from-to, got %s", step)
				}
				return from, to, nil
			}
			var err error
			minS, maxS, err = parseStep(steprm)
			if err != nil {
				return err
			}
		} else {
			panic("unexpected arguments")
		}

		var removed int
		for _, res := range files {
			if res.From >= minS && res.To <= maxS {
				if err := os.Remove(res.Path); err != nil {
					return fmt.Errorf("failed to remove %s: %w", res.Path, err)
				}
				removed++
			}
		}
		fmt.Printf("removed %d state segments files\n", removed)
	}
	if cliCtx.IsSet("domain") {
		domainNames := cliCtx.StringSlice("domain")
		for _, domainName := range domainNames {
			_, err := kv.String2InvertedIdx(domainName)
			if err != nil {
				_, err = kv.String2Domain(domainName)
				if err != nil {
					return err
				}
			}

			var removed int
			for _, res := range files {
				if !strings.Contains(res.Name(), domainName) {
					continue
				}
				if err := os.Remove(res.Path); err != nil {
					return fmt.Errorf("failed to remove %s: %w", res.Path, err)
				}
				removed++
			}
			fmt.Printf("removed %d state segments files of %s\n", removed, domainName)
		}

	}

	return nil
}

func doBtSearch(cliCtx *cli.Context) error {
	logger, _, _, err := debug.Setup(cliCtx, true /* root logger */)
	if err != nil {
		return err
	}

	srcF := cliCtx.String("src")
	dataFilePath := strings.TrimRight(srcF, ".bt") + ".kv"

	runtime.GC()
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info("before open", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	compress := seg.CompressKeys | seg.CompressVals
	kv, idx, err := libstate.OpenBtreeIndexAndDataFile(srcF, dataFilePath, libstate.DefaultBtreeM, compress, false)
	if err != nil {
		return err
	}
	defer idx.Close()
	defer kv.Close()

	runtime.GC()
	dbg.ReadMemStats(&m)
	logger.Info("after open", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

	seek := common.FromHex(cliCtx.String("key"))

	getter := seg.NewReader(kv.MakeGetter(), compress)

	cur, err := idx.Seek(getter, seek)
	if err != nil {
		return err
	}
	defer cur.Close()
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
	logger, _, _, err := debug.Setup(cliCtx, true /* root logger */)
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
	case "receipt":
		domain, idx = kv.ReceiptDomain, kv.ReceiptHistoryIdx
	case "rcache":
		domain, idx = kv.RCacheDomain, kv.RCacheHistoryIdx
	default:
		panic(ds)
	}
	_ = idx

	ctx := cliCtx.Context
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	chainDB := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	_, _, _, _, agg, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()

	view := agg.BeginFilesRo()
	defer view.Close()
	if err := view.IntegrityKey(domain, key); err != nil {
		return err
	}
	if err := view.IntegirtyInvertedIndexKey(domain, key); err != nil {
		return err
	}
	return nil
}

func doIntegrity(cliCtx *cli.Context) error {
	logger, _, _, err := debug.Setup(cliCtx, true /* root logger */)
	if err != nil {
		return err
	}

	ctx := cliCtx.Context
	requestedCheck := integrity.Check(cliCtx.String("check"))
	failFast := cliCtx.Bool("failFast")
	fromStep := cliCtx.Uint64("fromStep")
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	chainDB := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	_, borSnaps, _, blockRetire, agg, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()

	defer blockRetire.MadvNormal().DisableReadAhead()
	defer agg.MadvNormal().DisableReadAhead()

	db := temporal.New(chainDB, agg)
	defer db.Close()

	checks := append([]integrity.Check{}, integrity.AllChecks...)
	nonDefaultCheck := requestedCheck != "" &&
		!slices.Contains(integrity.AllChecks, requestedCheck) &&
		slices.Contains(integrity.NonDefaultChecks, requestedCheck)
	if nonDefaultCheck {
		checks = append(checks, integrity.NonDefaultChecks...)
	}

	blockReader, _ := blockRetire.IO()
	for _, chk := range checks {
		if requestedCheck != "" && requestedCheck != chk {
			continue
		}
		switch chk {
		case integrity.BlocksTxnID:
			if err := blockReader.(*freezeblocks.BlockReader).IntegrityTxnID(failFast); err != nil {
				return err
			}
		case integrity.Blocks:
			if err := integrity.SnapBlocksRead(ctx, db, blockReader, 0, 0, failFast); err != nil {
				return err
			}
		case integrity.InvertedIndex:
			if err := integrity.E3EfFiles(ctx, db, failFast, fromStep); err != nil {
				return err
			}
		case integrity.HistoryNoSystemTxs:
			if err := integrity.HistoryCheckNoSystemTxs(ctx, db, blockReader); err != nil {
				return err
			}
		case integrity.BorEvents:
			if err := integrity.ValidateBorEvents(ctx, db, blockReader, 0, 0, failFast); err != nil {
				return err
			}
		case integrity.BorSpans:
			if err := integrity.ValidateBorSpans(ctx, logger, dirs, borSnaps, failFast); err != nil {
				return err
			}
		case integrity.BorCheckpoints:
			if err := integrity.ValidateBorCheckpoints(ctx, logger, dirs, borSnaps, failFast); err != nil {
				return err
			}
		case integrity.BorMilestones:
			if err := integrity.ValidateBorMilestones(ctx, logger, dirs, borSnaps, failFast); err != nil {
				return err
			}
		case integrity.ReceiptsNoDups:
			if err := integrity.ReceiptsNoDuplicates(ctx, db, blockReader, failFast); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown check: %s", chk)
		}
	}

	return nil
}

func checkIfBlockSnapshotsPublishable(snapDir string) error {
	var sum uint64
	var maxTo uint64
	// Check block sanity
	if err := filepath.Walk(snapDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}
		// Skip CL files
		if !strings.Contains(info.Name(), "headers") || !strings.HasSuffix(info.Name(), ".seg") {
			return nil
		}
		// Do the range check
		res, _, ok := snaptype.ParseFileName(snapDir, info.Name())
		if !ok {
			return nil
		}
		sum += res.To - res.From
		headerSegName := info.Name()
		// check that all files exist
		for _, snapType := range []string{"transactions", "bodies"} {
			segName := strings.Replace(headerSegName, "headers", snapType, 1)
			// check that the file exist
			if _, err := os.Stat(filepath.Join(snapDir, segName)); err != nil {
				return fmt.Errorf("missing file %s", segName)
			}
			// check that the index file exist
			idxName := strings.Replace(segName, ".seg", ".idx", 1)
			if _, err := os.Stat(filepath.Join(snapDir, idxName)); err != nil {
				return fmt.Errorf("missing index file %s", idxName)
			}
			if snapType == "transactions" {
				// check that the tx index file exist
				txIdxName := strings.Replace(segName, "transactions.seg", "transactions-to-block.idx", 1)
				if _, err := os.Stat(filepath.Join(snapDir, txIdxName)); err != nil {
					return fmt.Errorf("missing tx index file %s", txIdxName)
				}
			}
		}

		maxTo = max(maxTo, res.To)

		return nil
	}); err != nil {
		return err
	}
	if sum != maxTo {
		return fmt.Errorf("sum %d != maxTo %d", sum, maxTo)
	}
	if err := doBlockSnapshotsRangeCheck(snapDir, ".seg", "headers"); err != nil {
		return err
	}
	if err := doBlockSnapshotsRangeCheck(snapDir, ".seg", "bodies"); err != nil {
		return err
	}
	if err := doBlockSnapshotsRangeCheck(snapDir, ".seg", "transactions"); err != nil {
		return err
	}
	if err := doBlockSnapshotsRangeCheck(snapDir, ".idx", "headers"); err != nil {
		return err
	}
	if err := doBlockSnapshotsRangeCheck(snapDir, ".idx", "bodies"); err != nil {
		return err
	}
	if err := doBlockSnapshotsRangeCheck(snapDir, ".idx", "transactions"); err != nil {
		return fmt.Errorf("failed to check transactions idx: %w", err)
	}
	if err := doBlockSnapshotsRangeCheck(snapDir, ".idx", "transactions-to-block"); err != nil {
		return fmt.Errorf("failed to check transactions-to-block idx: %w", err)
	}
	// Iterate over all fies in snapDir
	return nil
}

func checkIfStateSnapshotsPublishable(dirs datadir.Dirs) error {
	var stepSum uint64
	var maxStep uint64
	if err := filepath.Walk(dirs.SnapDomain, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && path != dirs.SnapDomain {
			return fmt.Errorf("unexpected directory in domain (%s) check %s", dirs.SnapDomain, path)
		}
		if path == dirs.SnapDomain {
			return nil
		}
		rangeString := strings.Split(info.Name(), ".")[1]
		rangeNums := strings.Split(rangeString, "-")
		// convert the range to uint64
		from, err := strconv.ParseUint(rangeNums[0], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse to %s: %w", rangeNums[1], err)
		}

		to, err := strconv.ParseUint(rangeNums[1], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse to %s: %w", rangeNums[1], err)
		}
		maxStep = max(maxStep, to)

		if !strings.HasSuffix(info.Name(), ".kv") || !strings.Contains(info.Name(), "accounts") {
			return nil
		}

		stepSum += to - from
		// do a range check over all snapshots types (sanitizes domain and history folder)
		for _, snapType := range kv.StateDomains {
			expectedFileName := strings.Replace(info.Name(), "accounts", snapType.String(), 1)
			if _, err := os.Stat(filepath.Join(dirs.SnapDomain, expectedFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", expectedFileName, filepath.Join(dirs.SnapDomain, expectedFileName))
			}
			// check that the index file exist
			if libstate.Schema[snapType].Accessors.Has(libstate.AccessorBTree) {
				fileName := strings.Replace(expectedFileName, ".kv", ".bt", 1)
				exists, err := dir.FileExist(filepath.Join(dirs.SnapDomain, fileName))
				if err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("missing file %s", fileName)
				}
			}
			if libstate.Schema[snapType].Accessors.Has(libstate.AccessorExistence) {
				fileName := strings.Replace(expectedFileName, ".kv", ".kvei", 1)
				exists, err := dir.FileExist(filepath.Join(dirs.SnapDomain, fileName))
				if err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("missing file %s", fileName)
				}
			}
			if libstate.Schema[snapType].Accessors.Has(libstate.AccessorHashMap) {
				fileName := strings.Replace(expectedFileName, ".kv", ".kvi", 1)
				exists, err := dir.FileExist(filepath.Join(dirs.SnapDomain, fileName))
				if err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("missing file %s", fileName)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if err := filepath.Walk(dirs.SnapIdx, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() && path != dirs.SnapIdx {
			return fmt.Errorf("unexpected directory in idx (%s) check %s", dirs.SnapIdx, path)

		}
		if path == dirs.SnapIdx {
			return nil
		}
		rangeString := strings.Split(info.Name(), ".")[1]
		rangeNums := strings.Split(rangeString, "-")

		to, err := strconv.ParseUint(rangeNums[1], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse to %s: %w", rangeNums[1], err)
		}
		maxStep = max(maxStep, to)

		if !strings.HasSuffix(info.Name(), ".ef") || !strings.Contains(info.Name(), "accounts") {
			return nil
		}

		viTypes := []string{"accounts", "storage", "code"}

		// do a range check over all snapshots types (sanitizes domain and history folder)
		for _, snapType := range []string{"accounts", "storage", "code", "logtopics", "logaddrs", "tracesfrom", "tracesto"} {
			expectedFileName := strings.Replace(info.Name(), "accounts", snapType, 1)
			if _, err := os.Stat(filepath.Join(dirs.SnapIdx, expectedFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", expectedFileName, filepath.Join(dirs.SnapIdx, expectedFileName))
			}
			// Check accessors
			efiFileName := strings.Replace(expectedFileName, ".ef", ".efi", 1)
			if _, err := os.Stat(filepath.Join(dirs.SnapAccessors, efiFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", efiFileName, filepath.Join(dirs.SnapAccessors, efiFileName))
			}
			if !slices.Contains(viTypes, snapType) {
				continue
			}
			viFileName := strings.Replace(expectedFileName, ".ef", ".vi", 1)
			if _, err := os.Stat(filepath.Join(dirs.SnapAccessors, viFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", viFileName, filepath.Join(dirs.SnapAccessors, viFileName))
			}
			// check that .v
			vFileName := strings.Replace(expectedFileName, ".ef", ".v", 1)
			if _, err := os.Stat(filepath.Join(dirs.SnapHistory, vFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", vFileName, filepath.Join(dirs.SnapHistory, vFileName))
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if stepSum != maxStep {
		return fmt.Errorf("stepSum %d != maxStep %d", stepSum, maxStep)
	}
	return nil
}

func doBlockSnapshotsRangeCheck(snapDir string, suffix string, snapType string) error {
	type interval struct {
		from uint64
		to   uint64
	}

	intervals := []interval{}
	if err := filepath.Walk(snapDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(info.Name(), suffix) || !strings.Contains(info.Name(), snapType+".") {
			return nil
		}
		res, _, ok := snaptype.ParseFileName(snapDir, info.Name())
		if !ok {
			return nil
		}
		intervals = append(intervals, interval{from: res.From, to: res.To})
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].from < intervals[j].from
	})
	// Check that there are no gaps
	for i := 1; i < len(intervals); i++ {
		if intervals[i].from != intervals[i-1].to {
			return fmt.Errorf("gap between (%d-%d) and (%d-%d). snaptype: %s", intervals[i-1].from, intervals[i-1].to, intervals[i].from, intervals[i].to, snapType)
		}
	}
	// Check that there are no overlaps
	for i := 1; i < len(intervals); i++ {
		if intervals[i].from < intervals[i-1].to {
			return fmt.Errorf("overlap between (%d-%d) and (%d-%d). snaptype: %s", intervals[i-1].from, intervals[i-1].to, intervals[i].from, intervals[i].to, snapType)
		}
	}

	return nil

}

func doPublishable(cliCtx *cli.Context) error {
	dat := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	// Check block snapshots sanity
	if err := checkIfBlockSnapshotsPublishable(dat.Snap); err != nil {
		return err
	}
	// Iterate over all fies in dat.Snap
	if err := checkIfStateSnapshotsPublishable(dat); err != nil {
		return err
	}
	// check if salt-state.txt and salt-blocks.txt exist
	exists, err := dir.FileExist(filepath.Join(dat.Snap, "salt-state.txt"))
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("missing file %s", filepath.Join(dat.Snap, "salt-state.txt"))
	}

	exists, err = dir.FileExist(filepath.Join(dat.Snap, "salt-blocks.txt"))
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("missing file %s", filepath.Join(dat.Snap, "salt-blocks.txt"))
	}
	log.Info("All snapshots are publishable")
	return nil
}

func doClearIndexing(cliCtx *cli.Context) error {
	dat := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	accessorsDir := dat.SnapAccessors
	domainDir := dat.SnapDomain
	snapDir := dat.Snap

	// Delete accessorsDir
	if err := os.RemoveAll(accessorsDir); err != nil {
		return fmt.Errorf("failed to delete accessorsDir: %w", err)
	}

	// Delete all files in domainDir with extensions .bt and .bt.torrent
	if err := deleteFilesWithExtensions(domainDir, []string{".bt", ".bt.torrent", ".kvei", ".kvei.torrent", ".kvi", ".kvi.torrent"}); err != nil {
		return fmt.Errorf("failed to delete files in domainDir: %w", err)
	}

	// Delete all files in snapDir with extensions .idx and .idx.torrent
	if err := deleteFilesWithExtensions(snapDir, []string{".idx", ".idx.torrent"}); err != nil {
		return fmt.Errorf("failed to delete files in snapDir: %w", err)
	}

	// remove salt-state.txt and salt-blocks.txt
	os.Remove(filepath.Join(snapDir, "salt-state.txt"))
	os.Remove(filepath.Join(snapDir, "salt-blocks.txt"))

	return nil
}

func deleteFilesWithExtensions(dir string, extensions []string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check file extensions and delete matching files
		for _, ext := range extensions {
			if strings.HasSuffix(info.Name(), ext) {
				if err := os.Remove(path); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func doDiff(cliCtx *cli.Context) error {
	log.Info("staring")
	defer log.Info("Done")
	srcF, dstF := cliCtx.String("src"), cliCtx.String("dst")
	src, err := seg.NewDecompressor(srcF)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := seg.NewDecompressor(dstF)
	if err != nil {
		return err
	}
	defer dst.Close()

	defer src.MadvSequential().DisableReadAhead()
	defer dst.MadvSequential().DisableReadAhead()

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
	args := cliCtx.Args()
	if args.Len() < 1 {
		return errors.New("expecting file path as a first argument")
	}
	fname := args.First()
	if strings.HasSuffix(fname, ".seg") || strings.HasSuffix(fname, ".kv") || strings.HasSuffix(fname, ".v") || strings.HasSuffix(fname, ".ef") {
		src, err := seg.NewDecompressor(fname)
		if err != nil {
			panic(err)
		}
		defer src.Close()
		log.Info("meta", "count", src.Count(), "size", datasize.ByteSize(src.Size()).HumanReadable(), "serialized_dict", datasize.ByteSize(src.SerializedDictSize()).HumanReadable(), "dict_words", src.DictWords(), "name", src.FileName(), "detected_compression_type", seg.DetectCompressType(src.MakeGetter()))
	} else if strings.HasSuffix(fname, ".bt") {
		kvFPath := strings.TrimSuffix(fname, ".bt") + ".kv"
		src, err := seg.NewDecompressor(kvFPath)
		if err != nil {
			panic(err)
		}
		defer src.Close()
		bt, err := libstate.OpenBtreeIndexWithDecompressor(fname, libstate.DefaultBtreeM, src, seg.CompressNone)
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
	} else if strings.HasSuffix(fname, ".kvi") || strings.HasSuffix(fname, ".idx") || strings.HasSuffix(fname, ".efi") || strings.HasSuffix(fname, ".vi") {
		idx, err := recsplit.OpenIndex(fname)
		if err != nil {
			panic(err)
		}
		defer idx.Close()
		total, offsets, ef, golombRice, existence, layer1 := idx.Sizes()
		log.Info("meta", "sz_total", total.HumanReadable(), "sz_offsets", offsets.HumanReadable(), "sz_double_ef", ef.HumanReadable(), "sz_golombRice", golombRice.HumanReadable(), "sz_existence", existence.HumanReadable(), "sz_l1", layer1.HumanReadable(), "keys_count", idx.KeyCount(), "leaf_size", idx.LeafSize(), "bucket_size", idx.BucketSize(), "enums", idx.Enums())
	}
	return nil
}

func doDecompressSpeed(cliCtx *cli.Context) error {
	logger, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	args := cliCtx.Args()
	if args.Len() < 1 {
		return errors.New("expecting file path as a first argument")
	}
	f := args.First()

	decompressor, err := seg.NewDecompressor(f)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	func() {
		defer decompressor.MadvSequential().DisableReadAhead()

		t := time.Now()
		g := decompressor.MakeGetter()
		buf := make([]byte, 0, 16*etl.BufIOSize)
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
		}
		logger.Info("decompress speed", "took", time.Since(t))
	}()
	func() {
		defer decompressor.MadvSequential().DisableReadAhead()

		t := time.Now()
		g := decompressor.MakeGetter()
		for g.HasNext() {
			_, _ = g.Skip()
		}
		log.Info("decompress skip speed", "took", time.Since(t))
	}()
	return nil
}

func doIndicesCommand(cliCtx *cli.Context, dirs datadir.Dirs) error {
	logger, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	defer logger.Info("Done")
	ctx := cliCtx.Context

	rebuild := cliCtx.Bool(SnapshotRebuildFlag.Name)
	chainDB := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	if rebuild {
		panic("not implemented")
	}

	if err := freezeblocks.RemoveIncompatibleIndices(dirs); err != nil {
		return err
	}

	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	_, _, caplinSnaps, br, agg, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()

	if err := br.BuildMissedIndicesIfNeed(ctx, "Indexing", nil); err != nil {
		return err
	}
	if err := caplinSnaps.BuildMissingIndices(ctx, logger); err != nil {
		return err
	}
	err = agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers())
	if err != nil {
		return err
	}

	return nil
}
func doLS(cliCtx *cli.Context, dirs datadir.Dirs) error {
	logger, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	defer logger.Info("Done")
	ctx := cliCtx.Context

	chainDB := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	cfg := ethconfig.NewSnapCfg(false, true, true, fromdb.ChainConfig(chainDB).ChainName)

	blockSnaps, borSnaps, caplinSnaps, _, agg, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()

	blockSnaps.Ls()
	borSnaps.Ls()
	caplinSnaps.LS()
	agg.LS()

	return nil
}

func openSnaps(ctx context.Context, cfg ethconfig.BlocksFreezing, dirs datadir.Dirs, chainDB kv.RwDB, logger log.Logger) (
	blockSnaps *freezeblocks.RoSnapshots,
	borSnaps *heimdall.RoSnapshots,
	csn *freezeblocks.CaplinSnapshots,
	br *freezeblocks.BlockRetire,
	agg *libstate.Aggregator,
	clean func(),
	err error,
) {
	if _, err = features.EnableSyncCfg(chainDB, ethconfig.Sync{}); err != nil {
		return
	}

	chainConfig := fromdb.ChainConfig(chainDB)

	blockSnaps = freezeblocks.NewRoSnapshots(cfg, dirs.Snap, 0, logger)
	if err = blockSnaps.OpenFolder(); err != nil {
		return
	}
	blockSnaps.LogStat("block")

	heimdall.RecordWayPoints(true) // needed to load checkpoints and milestones snapshots
	borSnaps = heimdall.NewRoSnapshots(cfg, dirs.Snap, 0, logger)
	if err = borSnaps.OpenFolder(); err != nil {
		return
	}

	var beaconConfig *clparams.BeaconChainConfig
	_, beaconConfig, _, err = clparams.GetConfigsByNetworkName(chainConfig.ChainName)
	if err == nil {
		csn = freezeblocks.NewCaplinSnapshots(cfg, beaconConfig, dirs, logger)
		if err = csn.OpenFolder(); err != nil {
			return
		}
		csn.LogStat("caplin")
	}

	borSnaps.LogStat("bor")
	var bridgeStore bridge.Store
	var heimdallStore heimdall.Store
	if chainConfig.Bor != nil {
		const PolygonSync = true
		if PolygonSync {
			borSnaps.DownloadComplete() // mark as ready
			bridgeStore = bridge.NewSnapshotStore(bridge.NewMdbxStore(dirs.DataDir, logger, true, 0), borSnaps, chainConfig.Bor)
			heimdallStore = heimdall.NewSnapshotStore(heimdall.NewMdbxStore(logger, dirs.DataDir, true, 0), borSnaps)
		} else {
			bridgeStore = bridge.NewSnapshotStore(bridge.NewDbStore(chainDB), borSnaps, chainConfig.Bor)
			heimdallStore = heimdall.NewSnapshotStore(heimdall.NewDbStore(chainDB), borSnaps)
		}
	}

	blockReader := freezeblocks.NewBlockReader(blockSnaps, borSnaps, heimdallStore, bridgeStore)
	blockWriter := blockio.NewBlockWriter()
	blockSnapBuildSema := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))
	br = freezeblocks.NewBlockRetire(estimate.CompressSnapshot.Workers(), dirs, blockReader, blockWriter, chainDB, heimdallStore, bridgeStore, chainConfig, &ethconfig.Defaults, nil, blockSnapBuildSema, logger)

	agg = openAgg(ctx, dirs, chainDB, logger)
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	db := temporal.New(chainDB, agg)
	clean = func() {
		defer blockSnaps.Close()
		defer borSnaps.Close()
		defer csn.Close()
		defer agg.Close()
		defer db.Close()
	}
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	stats.LogStats(tx, logger, func(endTxNumMinimax uint64) (uint64, error) {
		_, histBlockNumProgress, err := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, blockReader)).FindBlockNum(tx, endTxNumMinimax)
		return histBlockNumProgress, err
	})

	ls, er := os.Stat(filepath.Join(dirs.Snap, downloader.ProhibitNewDownloadsFileName))
	mtime := time.Time{}
	if er == nil {
		mtime = ls.ModTime()
	}
	logger.Info("[downloads]", "locked", er == nil, "at", mtime.Format("02 Jan 06 15:04 2006"))
	return
}

func doUncompress(cliCtx *cli.Context) error {
	var logger log.Logger
	var err error
	if logger, _, _, err = debug.Setup(cliCtx, true /* rootLogger */); err != nil {
		return err
	}
	ctx := cliCtx.Context

	args := cliCtx.Args()
	if args.Len() < 1 {
		return errors.New("expecting file path as a first argument")
	}
	f := args.First()

	decompressor, err := seg.NewDecompressor(f)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.MadvSequential().DisableReadAhead()

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
	defer func() {
		var m runtime.MemStats
		dbg.ReadMemStats(&m)
		log.Info("done", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	}()

	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	logger, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	ctx := cliCtx.Context

	args := cliCtx.Args()
	if args.Len() < 1 {
		return errors.New("expecting file path as a first argument")
	}
	f := args.First()

	compressCfg := seg.DefaultCfg
	compressCfg.Workers = estimate.CompressSnapshot.Workers()
	compressCfg.MinPatternScore = uint64(dbg.EnvInt("MinPatternScore", int(compressCfg.MinPatternScore)))
	compressCfg.MinPatternLen = dbg.EnvInt("MinPatternLen", compressCfg.MinPatternLen)
	compressCfg.MaxPatternLen = dbg.EnvInt("MaxPatternLen", compressCfg.MaxPatternLen)
	compressCfg.SamplingFactor = uint64(dbg.EnvInt("SamplingFactor", int(compressCfg.SamplingFactor)))
	compressCfg.DictReducerSoftLimit = dbg.EnvInt("DictReducerSoftLimit", compressCfg.DictReducerSoftLimit)
	compressCfg.MaxDictPatterns = dbg.EnvInt("MaxDictPatterns", compressCfg.MaxDictPatterns)
	compression := seg.CompressKeys | seg.CompressVals
	if dbg.EnvBool("OnlyKeys", false) {
		compression = seg.CompressKeys
	}
	if dbg.EnvBool("OnlyVals", false) {
		compression = seg.CompressVals
	}
	if dbg.EnvBool("NoCompress", false) {
		compression = seg.CompressNone
	}

	doSnappyEachWord := dbg.EnvBool("SnappyEachWord", false)
	doUnSnappyEachWord := dbg.EnvBool("UnSnappyEachWord", false)

	justPrint := dbg.EnvBool("JustPrint", false)
	concat := dbg.EnvInt("Concat", 0)

	logger.Info("[compress] file", "datadir", dirs.DataDir, "f", f, "cfg", compressCfg, "SnappyEachWord", doSnappyEachWord)
	c, err := seg.NewCompressor(ctx, "compress", f, dirs.Tmp, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer c.Close()
	w := seg.NewWriter(c, compression)

	r := bufio.NewReaderSize(os.Stdin, int(128*datasize.MB))
	word := make([]byte, 0, int(1*datasize.MB))
	var compressionPageBuf, decompressionPageBuf []byte
	var concatBuf []byte
	concatI := 0

	var l uint64
	for l, err = binary.ReadUvarint(r); err == nil; l, err = binary.ReadUvarint(r) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if cap(word) < int(l) {
			word = make([]byte, l)
		} else {
			word = word[:l]
		}
		if _, err = io.ReadFull(r, word); err != nil {
			return err
		}

		if justPrint {
			fmt.Printf("%x\n\n", word)
			continue
		}

		concatI++
		if concat > 0 {
			if concatI%concat != 0 {
				concatBuf = append(concatBuf, word...)
				continue
			}

			word = concatBuf
			concatBuf = concatBuf[:0]
		}

		compressionPageBuf, word = compress.EncodeZstdIfNeed(compressionPageBuf, word, doSnappyEachWord)
		decompressionPageBuf, word, err = compress.DecodeZstdIfNeed(decompressionPageBuf, word, doUnSnappyEachWord)
		if err != nil {
			return err
		}
		_, _ = compressionPageBuf, decompressionPageBuf

		if _, err := w.Write(word); err != nil {
			return err
		}
	}
	if !errors.Is(err, io.EOF) {
		return err
	}
	if err := c.Compress(); err != nil {
		return err
	}

	return nil
}

func doUnmerge(cliCtx *cli.Context, dirs datadir.Dirs) error {
	logger, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	defer logger.Info("Done")

	ctx := cliCtx.Context
	sourcefile := cliCtx.String(SnapshotFileFlag.Name)
	sourcefile = filepath.Join(dirs.Snap, sourcefile)

	exists, err := dir.FileExist(sourcefile)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("file %s does not exist", sourcefile)
	}

	decomp, err := seg.NewDecompressor(sourcefile)
	if err != nil {
		return err
	}
	defer decomp.Close()
	g := decomp.MakeGetter()
	info, _, ok := snaptype.ParseFileName(dirs.Snap, filepath.Base(sourcefile))
	if !ok {
		return fmt.Errorf("parsing error %s", filepath.Base(sourcefile))
	}
	logger.Info("number of elements", "source", sourcefile, "count", decomp.Count())

	blockFrom, blockTo := info.From, info.To
	var compressor *seg.Compressor
	compresCfg := seg.DefaultCfg
	workers := estimate.CompressSnapshot.Workers()
	compresCfg.Workers = workers
	var word = make([]byte, 0, 4096)

	if info.Type.Enum() == coresnaptype.Enums.Headers || info.Type.Enum() == coresnaptype.Enums.Bodies {
		for g.HasNext() {
			if blockFrom%1000 == 0 {
				if compressor != nil {
					if err = compressor.Compress(); err != nil {
						return err
					}
					compressor.Close()
				}

				unmerged_fileinfo := info.Type.FileInfo(dirs.Snap, blockFrom, blockFrom+1000)
				compressor, err = seg.NewCompressor(ctx, "unmerge", unmerged_fileinfo.Path, dirs.Tmp, compresCfg, log.LvlTrace, logger)
				if err != nil {
					return err
				}
			}

			word, _ = g.Next(word[:0])
			if err := compressor.AddUncompressedWord(word); err != nil {
				return err
			}
			blockFrom++
		}

		if compressor != nil {
			if err := compressor.Compress(); err != nil {
				return err
			}
			compressor.Close()
		}
	} else if info.Type.Enum() != coresnaptype.Enums.Transactions {
		return fmt.Errorf("unsupported type %s", info.Type.Enum().String())
	} else {
		// tx unmerge
		for ; blockFrom < blockTo; blockFrom += 1000 {
			um_fileinfo := coresnaptype.Enums.Bodies.Type().FileInfo(dirs.Snap, blockFrom, blockFrom+1000)
			bodiesSegment, err := seg.NewDecompressor(um_fileinfo.Path)
			if err != nil {
				return err
			}
			defer bodiesSegment.Close()

			_, expectedCount, err := coresnaptype.TxsAmountBasedOnBodiesSnapshots(bodiesSegment, um_fileinfo.Len()-1)
			if err != nil {
				return err
			}

			txfileinfo := um_fileinfo.As(coresnaptype.Enums.Transactions.Type())
			compressor, err = seg.NewCompressor(ctx, "unmerge", txfileinfo.Path, dirs.Tmp, compresCfg, log.LvlTrace, logger)
			if err != nil {
				return err
			}

			for g.HasNext() && expectedCount > 0 {
				word, _ = g.Next(word[:0])
				if err := compressor.AddUncompressedWord(word); err != nil {
					return err
				}
				expectedCount--
			}

			if expectedCount != 0 {
				return fmt.Errorf("unexpected count %d", expectedCount)
			}

			if err = compressor.Compress(); err != nil {
				return err
			}
			compressor.Close()
		}

		blockTo = blockFrom
	}

	if blockFrom != blockTo {
		return fmt.Errorf("unexpected block range %d-%d", blockFrom, blockTo)
	}

	decomp.Close()
	chainDB := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)
	_, _, _, br, _, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()

	if err := br.BuildMissedIndicesIfNeed(ctx, "indexing", nil); err != nil {
		return err
	}

	return nil
}

func doRetireCommand(cliCtx *cli.Context, dirs datadir.Dirs) error {
	logger, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	defer logger.Info("Done")
	ctx := cliCtx.Context

	from := uint64(0)

	db := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()
	chainConfig := fromdb.ChainConfig(db)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	_, _, caplinSnaps, br, agg, clean, err := openSnaps(ctx, cfg, dirs, db, logger)
	if err != nil {
		return err
	}
	defer clean()

	defer br.MadvNormal().DisableReadAhead()
	defer agg.MadvNormal().DisableReadAhead()

	blockSnapBuildSema := semaphore.NewWeighted(max(int64(runtime.NumCPU()), int64(dbg.BuildSnapshotAllowance)))
	agg.SetSnapshotBuildSema(blockSnapBuildSema)

	// `erigon retire` command is designed to maximize resouces utilization. But `Erigon itself` does minimize background impact (because not in rush).
	agg.SetCollateAndBuildWorkers(estimate.StateV3Collate.Workers())
	agg.SetMergeWorkers(estimate.AlmostAllCPUs())
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	agg.PeriodicalyPrintProcessSet(ctx)

	if err := br.BuildMissedIndicesIfNeed(ctx, "retire", nil); err != nil {
		return err
	}
	if err := caplinSnaps.BuildMissingIndices(ctx, logger); err != nil {
		return err
	}

	//agg.LimitRecentHistoryWithoutFiles(0)

	var to uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		to, err = stages.GetStageProgress(tx, stages.Senders)
		return err
	}); err != nil {
		return err
	}

	blockReader, _ := br.IO()

	blocksInSnapshots := blockReader.FrozenBlocks()

	if chainConfig.Bor != nil {
		blocksInSnapshots = min(blocksInSnapshots, blockReader.FrozenBorBlocks())
	}

	from2, to2, ok := freezeblocks.CanRetire(to, blocksInSnapshots, coresnaptype.Enums.Headers, nil)
	if ok {
		from, to = from2, to2
	}

	if err := br.RetireBlocks(ctx, from, to, log.LvlInfo, nil, nil, nil); err != nil {
		return err
	}

	if err := br.RemoveOverlaps(); err != nil {
		return err
	}

	deletedBlocks := math.MaxInt // To pass the first iteration
	allDeletedBlocks := 0
	for deletedBlocks > 0 { // prune happens by small steps, so need many runs
		err = db.UpdateNosync(ctx, func(tx kv.RwTx) error {
			if deletedBlocks, err = br.PruneAncientBlocks(tx, 100); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		allDeletedBlocks += deletedBlocks
	}

	logger.Info("Pruning has ended", "deleted blocks", allDeletedBlocks)

	db = temporal.New(db, agg)

	logger.Info("Prune state history")
	for hasMoreToPrune := true; hasMoreToPrune; {
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			err = tx.(kv.TemporalRwTx).Debug().GreedyPruneHistory(ctx, kv.CommitmentDomain)
			if err != nil {
				return err
			}
			hasMoreToPrune, err = tx.(kv.TemporalRwTx).Debug().PruneSmallBatches(ctx, 2*time.Minute)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}

	logger.Info("Work on state history snapshots")
	indexWorkers := estimate.IndexSnapshot.Workers()
	if err = agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, blockReader))
	var lastTxNum uint64
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		execProgress, _ := stages.GetStageProgress(tx, stages.Execution)
		lastTxNum, err = txNumsReader.Max(tx, execProgress)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	logger.Info("Build state history snapshots")
	if err = agg.BuildFiles(lastTxNum); err != nil {
		return err
	}

	logger.Info("Prune state history")
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		return tx.(kv.TemporalRwTx).Debug().GreedyPruneHistory(ctx, kv.CommitmentDomain)
	}); err != nil {
		return err
	}
	for hasMoreToPrune := true; hasMoreToPrune; {
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			hasMoreToPrune, err = tx.(kv.TemporalRwTx).Debug().PruneSmallBatches(ctx, 30*time.Second)
			return err
		}); err != nil {
			return err
		}
	}

	if err = agg.MergeLoop(ctx); err != nil {
		return err
	}
	if err = agg.RemoveOverlapsAfterMerge(ctx); err != nil {
		return err
	}

	return nil
}

func doUploaderCommand(cliCtx *cli.Context) error {
	var logger log.Logger
	var err error
	var metricsMux *http.ServeMux
	var pprofMux *http.ServeMux

	if logger, metricsMux, pprofMux, err = debug.Setup(cliCtx, true /* root logger */); err != nil {
		return err
	}

	// initializing the node and providing the current git commit there

	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)
	erigonInfoGauge := metrics.GetOrCreateGauge(fmt.Sprintf(`erigon_info{version="%s",commit="%s"}`, params.Version, params.GitCommit))
	erigonInfoGauge.Set(1)

	nodeCfg, err := node.NewNodConfigUrfave(cliCtx, logger)
	if err != nil {
		return err
	}
	if err := datadir.ApplyMigrations(nodeCfg.Dirs); err != nil {
		return err
	}

	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg, logger)

	ethNode, err := node.New(cliCtx.Context, nodeCfg, ethCfg, logger)
	if err != nil {
		log.Error("Erigon startup", "err", err)
		return err
	}
	defer ethNode.Close()

	diagnostics.Setup(cliCtx, ethNode, metricsMux, pprofMux)

	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving an Erigon node", "err", err)
	}
	return err
}

func dbCfg(label kv.Label, path string) mdbx.MdbxOpts {
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	return mdbx.New(label, log.New()).Path(path).
		RoTxsLimiter(limiterB).
		Accede(true) // integration tool: open db without creation and without blocking erigon
}
func openAgg(ctx context.Context, dirs datadir.Dirs, chainDB kv.RwDB, logger log.Logger) *libstate.Aggregator {
	agg, err := libstate.NewAggregator2(ctx, dirs, config3.DefaultStepSize, chainDB, logger)
	if err != nil {
		panic(err)
	}
	if err = agg.OpenFolder(); err != nil {
		panic(err)
	}
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	return agg
}
