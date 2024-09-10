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
	"github.com/erigontech/erigon-lib/seg"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/blockio"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/diagnostics"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/integrity"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/params"
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
	Name:    "snapshots",
	Aliases: []string{"seg"},
	Usage:   `Managing snapshots (historical data partitions)`,
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
			Name: "index",
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
				&SnapshotFromFlag,
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
			Usage: "erigon seg uncompress a.seg | erigon seg compress b.seg",
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

				if !ctx.IsSet(erigoncli.SyncLoopPruneLimitFlag.Name) {
					ctx.Set(erigoncli.SyncLoopPruneLimitFlag.Name, "100000")
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
				os.Remove(filepath.Join(dirs.Snap, "salt-state.txt"))
				return dir.DeleteFiles(dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors)
			},
			Flags: joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
		{
			Name:   "rm-state-snapshots",
			Action: doRmStateSnapshots,
			Flags:  joinFlags([]cli.Flag{&utils.DataDirFlag, &cli.StringFlag{Name: "step", Required: false}, &cli.BoolFlag{Name: "latest", Required: false}}),
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
			Name:        "publishable",
			Action:      doPublishable,
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

func doRmStateSnapshots(cliCtx *cli.Context) error {
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))

	removeLatest := cliCtx.Bool("latest")
	steprm := cliCtx.String("step")
	if steprm == "" && !removeLatest {
		return errors.New("step to remove is required (eg 0-2) OR flag --latest provided")
	}
	if steprm != "" {
		removeLatest = false // --step has higher priority
	}

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
				fmt.Printf("skipping %s\n", filePath)
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
	fmt.Printf("removed %d state snapshot files\n", removed)
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
	default:
		panic(ds)
	}
	_ = idx

	ctx := cliCtx.Context
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	chainDB := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	cr := rawdb.NewCanonicalReader(rawdbv3.TxNums)
	agg := openAgg(ctx, dirs, chainDB, cr, logger)

	view := agg.BeginFilesRo()
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

	cfg := ethconfig.NewSnapCfg(false, true, true)
	from := cliCtx.Uint64(SnapshotFromFlag.Name)

	_, _, _, blockRetire, agg, clean, err := openSnaps(ctx, cfg, dirs, from, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()

	blockReader, _ := blockRetire.IO()
	for _, chk := range integrity.AllChecks {
		if requestedCheck != "" && requestedCheck != chk {
			continue
		}
		switch chk {
		case integrity.BlocksTxnID:
			if err := blockReader.(*freezeblocks.BlockReader).IntegrityTxnID(failFast); err != nil {
				return err
			}
		case integrity.Blocks:
			if err := integrity.SnapBlocksRead(ctx, chainDB, blockReader, 0, 0, failFast); err != nil {
				return err
			}
		case integrity.InvertedIndex:
			if err := integrity.E3EfFiles(ctx, chainDB, agg, failFast, fromStep); err != nil {
				return err
			}
		case integrity.HistoryNoSystemTxs:
			if err := integrity.E3HistoryNoSystemTxs(ctx, chainDB, blockReader, agg); err != nil {
				return err
			}
		case integrity.NoBorEventGaps:
			if err := integrity.NoGapsInBorEvents(ctx, chainDB, blockReader, 0, 0, failFast); err != nil {
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
	if err := doBlockSnapshotsRangeCheck(snapDir, "headers"); err != nil {
		return err
	}
	if err := doBlockSnapshotsRangeCheck(snapDir, "bodies"); err != nil {
		return err
	}
	if err := doBlockSnapshotsRangeCheck(snapDir, "transactions"); err != nil {
		return err
	}
	// Iterate over all fies in snapDir
	return nil
}

func checkIfStateSnapshotsPublishable(dir datadir.Dirs) error {
	var stepSum uint64
	var maxStep uint64
	if err := filepath.Walk(dir.SnapDomain, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && path != dir.SnapDomain {
			return fmt.Errorf("unexpected directory in domain (%s) check %s", dir.SnapDomain, path)
		}
		if path == dir.SnapDomain {
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
		for _, snapType := range []string{"accounts", "storage", "code", "commitment"} {
			expectedFileName := strings.Replace(info.Name(), "accounts", snapType, 1)
			if _, err := os.Stat(filepath.Join(dir.SnapDomain, expectedFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", expectedFileName, filepath.Join(dir.SnapDomain, expectedFileName))
			}
			// check that the index file exist
			btFileName := strings.Replace(expectedFileName, ".kv", ".bt", 1)
			if _, err := os.Stat(filepath.Join(dir.SnapDomain, btFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", btFileName, filepath.Join(dir.SnapDomain, btFileName))
			}

			kveiFileName := strings.Replace(expectedFileName, ".kv", ".kvei", 1)
			if _, err := os.Stat(filepath.Join(dir.SnapDomain, kveiFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", kveiFileName, filepath.Join(dir.SnapDomain, kveiFileName))
			}

		}
		return nil
	}); err != nil {
		return err
	}

	if err := filepath.Walk(dir.SnapIdx, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() && path != dir.SnapIdx {
			return fmt.Errorf("unexpected directory in idx (%s) check %s", dir.SnapIdx, path)

		}
		if path == dir.SnapIdx {
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
			if _, err := os.Stat(filepath.Join(dir.SnapIdx, expectedFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", expectedFileName, filepath.Join(dir.SnapIdx, expectedFileName))
			}
			// Check accessors
			efiFileName := strings.Replace(expectedFileName, ".ef", ".efi", 1)
			if _, err := os.Stat(filepath.Join(dir.SnapAccessors, efiFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", efiFileName, filepath.Join(dir.SnapAccessors, efiFileName))
			}
			if !slices.Contains(viTypes, snapType) {
				continue
			}
			viFileName := strings.Replace(expectedFileName, ".ef", ".vi", 1)
			if _, err := os.Stat(filepath.Join(dir.SnapAccessors, viFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", viFileName, filepath.Join(dir.SnapAccessors, viFileName))
			}
			// check that .v
			vFileName := strings.Replace(expectedFileName, ".ef", ".v", 1)
			if _, err := os.Stat(filepath.Join(dir.SnapHistory, vFileName)); err != nil {
				return fmt.Errorf("missing file %s at path %s", vFileName, filepath.Join(dir.SnapHistory, vFileName))
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

func doBlockSnapshotsRangeCheck(snapDir string, snapType string) error {
	type interval struct {
		from uint64
		to   uint64
	}
	intervals := []interval{}
	if err := filepath.Walk(snapDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(info.Name(), ".seg") || !strings.Contains(info.Name(), snapType) {
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
			return fmt.Errorf("gap between %d and %d. snaptype: %s", intervals[i-1].to, intervals[i].from, snapType)
		}
	}
	// Check that there are no overlaps
	for i := 1; i < len(intervals); i++ {
		if intervals[i].from < intervals[i-1].to {
			return fmt.Errorf("overlap between %d and %d. snaptype: %s", intervals[i-1].to, intervals[i].from, snapType)
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
	// check if salt-state.txt and salt-block.txt exist
	if _, err := os.Stat(filepath.Join(dat.Snap, "salt-state.txt")); err != nil {
		return fmt.Errorf("missing file %s", filepath.Join(dat.Snap, "salt-state.txt"))
	}
	if _, err := os.Stat(filepath.Join(dat.Snap, "salt-blocks.txt")); err != nil {
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
	if err := deleteFilesWithExtensions(domainDir, []string{".bt", ".bt.torrent", ".kvei", ".kvei.torrent"}); err != nil {
		return fmt.Errorf("failed to delete files in domainDir: %w", err)
	}

	// Delete all files in snapDir with extensions .idx and .idx.torrent
	if err := deleteFilesWithExtensions(snapDir, []string{".idx", ".idx.torrent"}); err != nil {
		return fmt.Errorf("failed to delete files in snapDir: %w", err)
	}

	// remove salt-state.txt and salt-block.txt
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

	defer src.EnableReadAhead().DisableReadAhead()
	defer dst.EnableReadAhead().DisableReadAhead()

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
	if strings.Contains(fname, ".seg") || strings.Contains(fname, ".kv") || strings.Contains(fname, ".v") || strings.Contains(fname, ".ef") {
		src, err := seg.NewDecompressor(fname)
		if err != nil {
			return err
		}
		defer src.Close()
		log.Info("meta", "count", src.Count(), "size", datasize.ByteSize(src.Size()).HumanReadable(), "serialized_dict", datasize.ByteSize(src.SerializedDictSize()).HumanReadable(), "dict_words", src.DictWords(), "name", src.FileName())
	} else if strings.Contains(fname, ".bt") {
		kvFPath := strings.TrimSuffix(fname, ".bt") + ".kv"
		src, err := seg.NewDecompressor(kvFPath)
		if err != nil {
			return err
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

	cfg := ethconfig.NewSnapCfg(false, true, true)
	chainConfig := fromdb.ChainConfig(chainDB)
	from := cliCtx.Uint64(SnapshotFromFlag.Name)

	_, _, caplinSnaps, br, agg, clean, err := openSnaps(ctx, cfg, dirs, from, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()

	if err := br.BuildMissedIndicesIfNeed(ctx, "Indexing", nil, chainConfig); err != nil {
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
	cfg := ethconfig.NewSnapCfg(false, true, true)
	from := cliCtx.Uint64(SnapshotFromFlag.Name)
	blockSnaps, borSnaps, caplinSnaps, _, agg, clean, err := openSnaps(ctx, cfg, dirs, from, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()

	blockSnaps.LS()
	borSnaps.LS()
	caplinSnaps.LS()
	agg.LS()

	return nil
}

func openSnaps(ctx context.Context, cfg ethconfig.BlocksFreezing, dirs datadir.Dirs, from uint64, chainDB kv.RwDB, logger log.Logger) (
	blockSnaps *freezeblocks.RoSnapshots, borSnaps *freezeblocks.BorRoSnapshots, csn *freezeblocks.CaplinSnapshots,
	br *freezeblocks.BlockRetire, agg *libstate.Aggregator, clean func(), err error,
) {
	blockSnaps = freezeblocks.NewRoSnapshots(cfg, dirs.Snap, 0, logger)
	if err = blockSnaps.ReopenFolder(); err != nil {
		return
	}
	blockSnaps.LogStat("block")

	borSnaps = freezeblocks.NewBorRoSnapshots(cfg, dirs.Snap, 0, logger)
	if err = borSnaps.ReopenFolder(); err != nil {
		return
	}

	chainConfig := fromdb.ChainConfig(chainDB)

	var beaconConfig *clparams.BeaconChainConfig
	_, beaconConfig, _, err = clparams.GetConfigsByNetworkName(chainConfig.ChainName)
	if err == nil {
		csn = freezeblocks.NewCaplinSnapshots(cfg, beaconConfig, dirs, logger)
		if err = csn.ReopenFolder(); err != nil {
			return
		}
		csn.LogStat("caplin")
	}

	borSnaps.LogStat("bor")
	blockReader := freezeblocks.NewBlockReader(blockSnaps, borSnaps)
	blockWriter := blockio.NewBlockWriter()
	blockSnapBuildSema := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))
	br = freezeblocks.NewBlockRetire(estimate.CompressSnapshot.Workers(), dirs, blockReader, blockWriter, chainDB, chainConfig, nil, blockSnapBuildSema, logger)

	cr := rawdb.NewCanonicalReader(rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, blockReader)))
	agg = openAgg(ctx, dirs, chainDB, cr, logger)
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	clean = func() {
		defer blockSnaps.Close()
		defer borSnaps.Close()
		defer csn.Close()
		defer agg.Close()
	}
	err = chainDB.View(ctx, func(tx kv.Tx) error {
		ac := agg.BeginFilesRo()
		defer ac.Close()
		ac.LogStats(tx, func(endTxNumMinimax uint64) (uint64, error) {
			_, histBlockNumProgress, err := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, blockReader)).FindBlockNum(tx, endTxNumMinimax)
			return histBlockNumProgress, err
		})
		return nil
	})
	if err != nil {
		return
	}

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

	logger.Info("[compress] file", "datadir", dirs.DataDir, "f", f, "cfg", compressCfg)
	c, err := seg.NewCompressor(ctx, "compress", f, dirs.Tmp, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer c.Close()
	w := seg.NewWriter(c, compression)

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
		if err := w.AddWord(buf); err != nil {
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
func doRetireCommand(cliCtx *cli.Context, dirs datadir.Dirs) error {
	logger, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	defer logger.Info("Done")
	ctx := cliCtx.Context

	from := cliCtx.Uint64(SnapshotFromFlag.Name)
	to := cliCtx.Uint64(SnapshotToFlag.Name)
	every := cliCtx.Uint64(SnapshotEveryFlag.Name)

	db := dbCfg(kv.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()

	cfg := ethconfig.NewSnapCfg(false, true, true)

	blockSnaps, _, caplinSnaps, br, agg, clean, err := openSnaps(ctx, cfg, dirs, from, db, logger)
	if err != nil {
		return err
	}
	defer clean()

	// `erigon retire` command is designed to maximize resouces utilization. But `Erigon itself` does minimize background impact (because not in rush).
	agg.SetCollateAndBuildWorkers(estimate.StateV3Collate.Workers())
	agg.SetMergeWorkers(estimate.AlmostAllCPUs())
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())

	chainConfig := fromdb.ChainConfig(db)
	if err := br.BuildMissedIndicesIfNeed(ctx, "retire", nil, chainConfig); err != nil {
		return err
	}
	if err := caplinSnaps.BuildMissingIndices(ctx, logger); err != nil {
		return err
	}

	//agg.LimitRecentHistoryWithoutFiles(0)

	var forwardProgress uint64
	if to == 0 {
		db.View(ctx, func(tx kv.Tx) error {
			forwardProgress, err = stages.GetStageProgress(tx, stages.Senders)
			return err
		})
		blockReader, _ := br.IO()
		from2, to2, ok := freezeblocks.CanRetire(forwardProgress, blockReader.FrozenBlocks(), coresnaptype.Enums.Headers, nil)
		if ok {
			from, to, every = from2, to2, to2-from2
		}
	}

	logger.Info("Params", "from", from, "to", to, "every", every)
	if err := br.RetireBlocks(ctx, 0, forwardProgress, log.LvlInfo, nil, nil, nil); err != nil {
		return err
	}

	blockReader, _ := br.IO()
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		ac := agg.BeginFilesRo()
		defer ac.Close()
		if err := rawdb.WriteSnapshots(tx, blockReader.FrozenFiles(), ac.Files()); err != nil {
			return err
		}
		return nil
	}); err != nil {
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

	db, err = temporal.New(db, agg)
	if err != nil {
		return err
	}

	logger.Info("Prune state history")
	ac := agg.BeginFilesRo()
	defer ac.Close()
	for hasMoreToPrune := true; hasMoreToPrune; {
		hasMoreToPrune, err = ac.PruneSmallBatchesDb(ctx, 2*time.Minute, db)
		if err != nil {
			return err
		}
	}
	ac.Close()

	logger.Info("Work on state history snapshots")
	indexWorkers := estimate.IndexSnapshot.Workers()
	if err = agg.BuildOptionalMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}
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

		ac := agg.BeginFilesRo()
		defer ac.Close()
		return nil
	}); err != nil {
		return err
	}

	logger.Info("Build state history snapshots")
	if err = agg.BuildFiles(lastTxNum); err != nil {
		return err
	}

	if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
		ac := agg.BeginFilesRo()
		defer ac.Close()

		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()

		stat, err := ac.Prune(ctx, tx, math.MaxUint64, logEvery)
		if err != nil {
			return err
		}
		logger.Info("aftermath prune finished", "stat", stat.String())
		return err
	}); err != nil {
		return err
	}

	ac = agg.BeginFilesRo()
	defer ac.Close()
	for hasMoreToPrune := true; hasMoreToPrune; {
		hasMoreToPrune, err = ac.PruneSmallBatchesDb(context.Background(), 2*time.Minute, db)
		if err != nil {
			return err
		}
	}
	ac.Close()

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
		ac := agg.BeginFilesRo()
		defer ac.Close()
		return rawdb.WriteSnapshots(tx, blockReader.FrozenFiles(), ac.Files())
	}); err != nil {
		return err
	}
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		ac := agg.BeginFilesRo()
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
	var pprofMux *http.ServeMux

	if logger, metricsMux, pprofMux, err = debug.Setup(cliCtx, true /* root logger */); err != nil {
		return err
	}

	// initializing the node and providing the current git commit there

	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)
	erigonInfoGauge := metrics.GetOrCreateGauge(fmt.Sprintf(`erigon_info{version="%s",commit="%s"}`, params.Version, params.GitCommit))
	erigonInfoGauge.Set(1)

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
	opts := mdbx.NewMDBX(log.New()).Path(path).Label(label).RoTxsLimiter(limiterB)
	// integration tool don't intent to create db, then easiest way to open db - it's pass mdbx.Accede flag, which allow
	// to read all options from DB, instead of overriding them
	opts = opts.Accede()
	return opts
}
func openAgg(ctx context.Context, dirs datadir.Dirs, chainDB kv.RwDB, cr *rawdb.CanonicalReader, logger log.Logger) *libstate.Aggregator {
	agg, err := libstate.NewAggregator(ctx, dirs, config3.HistoryV3AggregationStep, chainDB, cr, logger)
	if err != nil {
		panic(err)
	}
	if err = agg.OpenFolder(); err != nil {
		panic(err)
	}
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	return agg
}
