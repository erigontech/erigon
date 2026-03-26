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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	g "github.com/anacrolix/generics"
	"github.com/c2h5oh/datasize"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/disk"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/compress"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/downloader/webseeds"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/state/stats"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/diagnostics/mem"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/verify"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/ethconfig/features"
	"github.com/erigontech/erigon/node/logging"
	"github.com/erigontech/erigon/node/rulesconfig"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func joinFlags(lists ...[]cli.Flag) (res []cli.Flag) {
	lists = append(lists, debug.Flags, logging.Flags, utils.MetricFlags)
	for _, list := range lists {
		res = append(res, list...)
	}
	return res
}

// This needs to run *after* subcommand arguments are parsed, in case they alter root flags like data dir.
func commonBeforeSnapshotCommand(cliCtx *cli.Context) error {
	go mem.LogMemStats(cliCtx.Context, log.New())
	go disk.UpdateDiskStats(cliCtx.Context, log.New())
	_, err := debug.SetupSimple(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	// Inject commonBeforeSnapshotCommand into all snapshot subcommands Before handlers.
	for _, cmd := range snapshotCommand.Subcommands {
		oldBefore := cmd.Before
		cmd.Before = func(cliCtx *cli.Context) error {
			err := commonBeforeSnapshotCommand(cliCtx)
			if err != nil {
				return fmt.Errorf("common before snapshot subcommand: %w", err)
			}
			if oldBefore == nil {
				return nil
			}
			return oldBefore(cliCtx)
		}
	}
}

var snapshotCommand = cli.Command{
	Name:    "snapshots",
	Aliases: []string{"seg", "snapshot", "segments", "segment"},
	Usage:   `Managing historical data segments (partitions)`,
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
			Name:  "du",
			Usage: "Report snapshot disk usage by category with estimated sizes per node type",
			Action: func(c *cli.Context) error {
				dirs := datadir.Open(c.String(utils.DataDirFlag.Name))
				if _, err := os.Stat(dirs.DataDir); os.IsNotExist(err) {
					return fmt.Errorf("datadir does not exist: %s", dirs.DataDir)
				}
				return doDU(c, dirs)
			},
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.BoolFlag{Name: "json", Usage: "Output in JSON format"},
				&cli.BoolFlag{Name: "v", Aliases: []string{"verbose"}, Usage: "Show per-domain/per-type subcategory breakdown"},
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
			Name: "remove_overlaps",
			Action: func(c *cli.Context) error {
				dirs, l, err := datadir.New(c.String(utils.DataDirFlag.Name)).MustFlock()
				if err != nil {
					return err
				}
				defer l.Unlock()

				return doRemoveOverlap(c, dirs)
			},
			Usage: "remove overlaps from e3 files",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
			}),
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
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.StringFlag{Name: "from"},
			}),
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
			Description: "Search for a key in a btree index",
		},
		{
			Name:    "rm-all-state-snapshots",
			Aliases: []string{"rm-all-state"},
			Action: func(cliCtx *cli.Context) error {
				dirs, l, err := datadir.New(cliCtx.String(utils.DataDirFlag.Name)).MustFlock()
				if err != nil {
					return err
				}
				defer l.Unlock()

				err = dir2.DeleteFiles(dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors, dirs.SnapForkable)
				if err != nil {
					return err
				}

				fmt.Printf("\n\nRun `integration stage_exec --reset` before restarting Erigon to prune execution remnants from DB to avoid gap between snapshots and DB.\n")
				return nil
			},
			Flags: joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
		{
			Name:  "reset-to-old-ver-format",
			Usage: "change all the snapshots to 3.0 file format",
			Action: func(cliCtx *cli.Context) error {
				dirs, l, err := datadir.New(cliCtx.String(utils.DataDirFlag.Name)).MustFlock()
				if err != nil {
					return err
				}
				defer l.Unlock()
				return dirs.RenameNewVersions()
			},
			Flags: joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
		{
			Name:  "update-to-new-ver-format",
			Usage: "change all the snapshots to 3.1 file ver format",
			Action: func(cliCtx *cli.Context) error {
				dirs, l, err := datadir.New(cliCtx.String(utils.DataDirFlag.Name)).MustFlock()
				if err != nil {
					return err
				}
				defer l.Unlock()
				return dirs.RenameOldVersions(true)
			},
			Flags: joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
		{
			Name:   "reset",
			Usage:  "Reset state to resumable initial sync",
			Action: resetCliAction,
			// Something to alter snapcfg.snapshotGitBranch would go here, or should you set the
			// environment variable? Followup: It would not go here, as it could modify behaviour in
			// parent commands.
			Flags: []cli.Flag{
				&utils.DataDirFlag,
				&dryRunFlag,
				&removeLocalFlag,
				&PreverifiedFlag,
			},
		},
		{
			Name:    "rm-state-snapshots",
			Aliases: []string{"rm-state-segments", "rm-state"},
			Action:  doRmStateSnapshots,
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.StringFlag{Name: "step"},
				&cli.BoolFlag{Name: "recentStep", Aliases: []string{"latest", "latestStep", "recent"}, Usage: "remove minimal possible recent/latest files: and Domain and History. Useful when have 1 corrupted recent file"},
				&cli.BoolFlag{Name: "dry-run"},
				&cli.StringSliceFlag{Name: "domain"},
			},
			),
		},
		{
			Name: "rollback-snapshots-to-block",
			Description: "Rollback the node back to a given block by deleting chaindata and all corresponding " +
				"snapshots that contain data related to the given block and blocks after it. It deletes block " +
				"related seg files and also state files that contain data of its first tx num and later." +
				"It is useful for shadowforks, recovering broken nodes or chains, and/or for doing experiments that " +
				"involve replaying certain blocks.",
			Action: func(cliCtx *cli.Context) error {
				logger := log.Root()
				block := cliCtx.Uint64("block")
				prompt := cliCtx.Bool("prompt")
				dataDir := cliCtx.String(utils.DataDirFlag.Name)
				err := doRollbackSnapshotsToBlock(cliCtx.Context, block, prompt, dataDir, logger)
				if err != nil {
					logger.Error(err.Error())
					return err
				}
				return nil
			},
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.Uint64Flag{Name: "block", Required: true},
				&cli.BoolFlag{Name: "prompt", Value: true},
			}),
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
			Name:   "txnum",
			Action: doBlkTxNum,
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.Int64Flag{Name: "block", Value: -1},
				&cli.Int64Flag{Name: "txnum", Value: -1},
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
			Name:   "squeeze",
			Action: doSqueeze,
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.StringFlag{Name: "type", Required: true, Aliases: []string{"domain"}},
			}),
		},
		{
			Name: "integrity",
			Action: func(cliCtx *cli.Context) error {
				_, l, err := datadir.New(cliCtx.String(utils.DataDirFlag.Name)).MustFlock()
				if err != nil {
					return err
				}
				defer l.Unlock()
				if err := doIntegrity(cliCtx); err != nil {
					log.Error("[integrity]", "err", err)
					return err
				}
				log.Info("[integrity] snapshots are publishable")
				return nil
			},
			Description: "run slow validation of files. use --check to run multiple/single",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.StringFlag{Name: "check", Usage: fmt.Sprintf("comma separated list from: %s", integrity.FastChecks)},
				&cli.StringFlag{Name: "skip-check", Usage: fmt.Sprintf("comma separated list from: %s", integrity.FastChecks)},
				&cli.BoolFlag{Name: "failFast", Value: true, Usage: "to stop after 1st problem or print WARN log and continue check"},
				&cli.Uint64Flag{Name: "fromStep", Value: 0, Usage: "skip files before given step"},
				&cli.StringFlag{Name: "file-integrity-cache", Usage: "path to integrity check cache file (speeds up repeated runs)"},
				&cli.BoolFlag{Name: "skip-torrent-verify", Usage: "skip torrent piece verification when using file-integrity-cache"},
				&cli.Int64Flag{Name: "seed", Usage: "random seed for sampling (auto-generated if not set)"},
				&cli.Float64Flag{Name: "sample", Usage: "fraction of items to check via pseudo-random sampling (0.0-1.0)", Value: 0.01},
			}),
		},
		{
			Name: "check-commitment-hist-at-blk",
			Action: func(cliCtx *cli.Context) error {
				logger := log.Root()
				err := doCheckCommitmentHistAtBlk(cliCtx, logger)
				if err != nil {
					log.Error("[check-commitment-hist-at-blk] failure", "err", err)
					return err
				}
				log.Info("[check-commitment-hist-at-blk] success")
				return nil
			},
			Description: "check if our historical commitment data matches the state root at a given block",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.Uint64Flag{Name: "block", Usage: "block number to verify", Required: true},
			}),
		},
		{
			Name: "check-commitment-hist-at-blk-range",
			Action: func(cliCtx *cli.Context) error {
				logger := log.Root()
				err := doCheckStateRootByHistory(cliCtx, logger)
				if err != nil {
					log.Error("[check-commitment-hist-at-blk-range] failure", "err", err)
					return err
				}
				log.Info("[check-commitment-hist-at-blk-range] success")
				return nil
			},
			Description: "verify block state roots against commitment history snapshots for a given [from,to) block range (no block re-execution)",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.Uint64Flag{Name: "from", Usage: "block number from which to start verifying", Required: true},
				&cli.Uint64Flag{Name: "to", Usage: "block number up to which to verify (exclusive); defaults to latest block with state"},
				&cli.Int64Flag{Name: "seed", Usage: "random seed for block sampling (auto-generated if not set)"},
				&cli.Float64Flag{Name: "sample", Usage: "fraction of blocks to check via pseudo-random sampling (0.0-1.0)", Value: 1.0},
			}),
		},
		{
			Name:        "verify-state",
			Description: "verify correspondence between state snapshots (accounts, storage) and commitment snapshots",
			Action: func(cliCtx *cli.Context) error {
				logger := log.Root()
				err := doVerifyState(cliCtx, logger)
				if err != nil {
					log.Error("[verify-state] failure", "err", err)
					return err
				}
				log.Info("[verify-state] success")
				return nil
			},
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.Uint64Flag{Name: "from-step", Value: 0, Usage: "skip files before given step"},
				&cli.BoolFlag{Name: "failFast", Value: true, Usage: "stop after first problem or print WARN and continue"},
			}),
		},
		{
			Name:        "verify-history",
			Description: "verify history snapshots by re-executing blocks and comparing state changes",
			Action: func(cliCtx *cli.Context) error {
				logger := log.Root()
				err := doVerifyHistory(cliCtx, logger)
				if err != nil {
					log.Error("[verify-history] failure", "err", err)
					return err
				}
				log.Info("[verify-history] success")
				return nil
			},
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.Uint64Flag{Name: "from-step", Value: 0, Usage: "skip files before given step"},
				&cli.BoolFlag{Name: "failFast", Value: true, Usage: "stop after first problem or print WARN and continue"},
				&cli.IntFlag{Name: "workers", Value: 0, Usage: "number of parallel workers (0 = NumCPU/2)"},
			}),
		},
		{
			Name: "publishable",
			Action: func(cliCtx *cli.Context) error {
				if err := doPublishable(cliCtx, nil); err != nil {
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
		{
			Name:        "compareIdx",
			Action:      doCompareIdx,
			Description: "compares to accessors (recsplit) files",
			Flags: joinFlags([]cli.Flag{
				&cli.PathFlag{Name: "first", Required: true},
				&cli.PathFlag{Name: "second", Required: true},
				&cli.BoolFlag{Name: "skip-size-check", Required: false, Value: false},
			}),
		},
		{
			Name:        "step-rebase",
			Action:      stepRebase,
			Description: "Rebase snapshots step size",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.Uint64Flag{Name: "new-step-size", Required: true, DefaultText: strconv.FormatUint(config3.DefaultStepSize, 10)},
			}),
		},
		{
			Name:        "info",
			Action:      segInfo,
			Description: "Show misc information about a segment file",
			Flags: joinFlags([]cli.Flag{
				&utils.DataDirFlag,
				&cli.PathFlag{Name: "file", Required: true},
				&cli.StringFlag{Name: "compress", Required: true, Usage: "Values compression type: all,none,keys,values"},
			}),
		},
		{
			Name:        "domain",
			Description: "Domain related subcommands",
			Subcommands: []*cli.Command{
				{
					Name:   "stat",
					Action: domainStat,
					Usage:  "Calculate statistics for a domain",
					Flags: joinFlags([]cli.Flag{
						&utils.DataDirFlag,
						&cli.UintFlag{Name: "domain", Required: true},
					}),
				},
			},
		},
		{
			Name: "preverified",
			Action: func(cliCtx *cli.Context) (err error) {
				var dataDir string
				// Don't use the default, it must be set to apply.
				if cliCtx.IsSet(utils.DataDirFlag.Name) {
					dataDir = cliCtx.String(utils.DataDirFlag.Name)
				}
				var targetChain g.Option[string]
				// Don't use the default, it must be set to apply.
				if cliCtx.IsSet(VerifyChainFlag.Name) {
					targetChain.Set(VerifyChainFlag.Get(cliCtx))
				}
				return webseeds.Verify(
					cliCtx.Context,
					PreverifiedFlag.Get(cliCtx),
					dataDir,
					ConcurrencyFlag.Get(cliCtx),
					targetChain,
				)
			},
			Flags: []cli.Flag{
				&PreverifiedFlag,
				&VerifyChainFlag,
				&ConcurrencyFlag,
			},
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

// checkCommitmentFileHasRoot checks if a commitment file contains state root key
func checkCommitmentFileHasRoot(filePath string) (hasState, broken bool, err error) {
	const stateKey = "state"
	_, fileName := filepath.Split(filePath)

	// First try with recsplit index (.kvi files)
	derivedKvi := strings.Replace(filePath, ".kv", ".kvi", 1)
	fPathMask, err := version.ReplaceVersionWithMask(derivedKvi)
	if err != nil {
		return false, false, err
	}
	kvi, _, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
	if err != nil {
		return false, false, err
	}
	if ok {
		_, err := os.Stat(kvi)
		if err != nil {
			return false, false, err
		}
		idx, err := recsplit.OpenIndex(kvi)
		if err != nil {
			return false, false, err
		}
		defer idx.Close()

		rd := idx.GetReaderFromPool()
		defer rd.Close()
		if rd.Empty() {
			log.Warn("[dbg] allow files deletion because accessor broken", "accessor", idx.FileName())
			return false, true, nil
		}

		_, found := rd.Lookup([]byte(stateKey))
		if found {
			fmt.Printf("found state key with kvi %s\n", filePath)
			return true, false, nil
		} else {
			fmt.Printf("skipping file because it doesn't have state key %s\n", fileName)
			return true, false, nil
		}
	} else {
		log.Warn("[dbg] not found files for", "pattern", fPathMask)
	}

	// If recsplit index not found, try btree index (.bt files)
	derivedBt := strings.Replace(filePath, ".kv", ".bt", 1)
	fPathMask, err = version.ReplaceVersionWithMask(derivedBt)
	if err != nil {
		return true, false, nil
	}
	bt, _, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
	if err != nil {
		return true, false, nil
	}
	if !ok {
		return false, false, fmt.Errorf("can't find accessor for %s", filePath)
	}
	rd, bti, err := btindex.OpenBtreeIndexAndDataFile(bt, filePath, btindex.DefaultBtreeM, statecfg.Schema.CommitmentDomain.Compression, false)
	if err != nil {
		return false, false, err
	}
	defer rd.Close()
	defer bti.Close()

	getter := seg.NewReader(rd.MakeGetter(), statecfg.Schema.CommitmentDomain.Compression)
	c, err := bti.Seek(getter, []byte(stateKey))
	if err != nil {
		return false, false, err
	}
	defer c.Close()

	if bytes.Equal(c.Key(), []byte(stateKey)) {
		fmt.Printf("found state key using bt %s\n", filePath)
		return true, false, nil
	}
	return false, false, nil
}

type DeleteStateSnapshotsArgs struct {
	Dirs                   datadir.Dirs
	RemoveLatest           bool
	PromptUserBeforeDelete bool
	DryRun                 bool
	StepRange              string
	OnlyDomain             bool
	DomainNames            []string
}

func DeleteStateSnapshots(args DeleteStateSnapshotsArgs) error {
	dirs := args.Dirs
	removeLatest := args.RemoveLatest
	promptUserBeforeDelete := args.PromptUserBeforeDelete
	dryRun := args.DryRun
	stepRange := args.StepRange
	domainNames := args.DomainNames

	_maxFrom := uint64(0)
	_maxTo := uint64(0)
	files := make([]snaptype.FileInfo, 0)
	commitmentFilesWithState := make([]snaptype.FileInfo, 0)

	// Step 1: Collect and parse all candidate state files
	candidateFiles := make([]struct {
		fileInfo snaptype.FileInfo
		dirPath  string
		filePath string
	}, 0)

	scanDirs := []string{dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors, dirs.SnapForkable}
	if args.OnlyDomain {
		scanDirs = []string{dirs.SnapDomain}
	}
	for _, dirPath := range scanDirs {
		filePaths, err := dir2.ListFiles(dirPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return err
		}
		for _, filePath := range filePaths {
			_, fName := filepath.Split(filePath)
			res, isStateFile, ok := snaptype.ParseFileName(dirPath, fName)
			if !ok || !isStateFile {
				fmt.Printf("skipping %s\n", filePath)
				continue
			}
			if res.From == 0 && res.To == 0 { // parse steps from file name
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
			candidateFiles = append(candidateFiles, struct {
				fileInfo snaptype.FileInfo
				dirPath  string
				filePath string
			}{res, dirPath, filePath})
		}
	}

	// Step 2: Process each candidate file (already parsed)
	doesRmCommitment := len(domainNames) == 0 || slices.Contains(domainNames, kv.CommitmentDomain.String())
	for _, candidate := range candidateFiles {
		res := candidate.fileInfo

		// check that commitment file has state in it
		// When domains are "compacted", we want to keep latest commitment file with state key in it
		if doesRmCommitment && strings.Contains(filepath.Base(res.Path), "commitment") && strings.HasSuffix(res.Path, ".kv") {
			hasState, broken, err := checkCommitmentFileHasRoot(res.Path)
			if err != nil {
				return err
			}
			if hasState {
				commitmentFilesWithState = append(commitmentFilesWithState, res)
			}
			if broken {
				commitmentFilesWithState = append(commitmentFilesWithState, res)
			}
		}

		files = append(files, res)
		if removeLatest {
			_maxFrom = max(_maxFrom, res.From)
			_maxTo = max(_maxTo, res.To)
		}
	}

	toRemove := make(map[string]snaptype.FileInfo)
	if len(domainNames) > 0 {
		_maxFrom = 0
		_maxTo = 0
		domainFiles := make([]snaptype.FileInfo, 0, len(files))
		for _, domainName := range domainNames {
			_, err := kv.String2InvertedIdx(domainName)
			if err != nil {
				_, err = kv.String2Domain(domainName)
				if err != nil {
					_, err = kv.String2Forkable(domainName)
					if err != nil {
						return err
					}
				}
			}
			for _, res := range files {
				if !strings.Contains(res.Name(), domainName) {
					continue
				}
				if removeLatest {
					_maxFrom = max(_maxFrom, res.From)
					_maxTo = max(_maxTo, res.To)
				}
				domainFiles = append(domainFiles, res)
			}
		}
		files = domainFiles
	}
	if stepRange != "" || removeLatest {
		var minS, maxS uint64
		if stepRange != "" {
			parseStep := func(step string) (uint64, uint64, error) {
				var from, to uint64
				if _, err := fmt.Sscanf(step, "%d-%d", &from, &to); err != nil {
					return 0, 0, fmt.Errorf("step expected in format from-to, got %s", step)
				}
				return from, to, nil
			}
			var err error
			minS, maxS, err = parseStep(stepRange)
			if err != nil {
				return err
			}
			removeLatest = false // --step has higher priority
		}

		promptExit := func(s string) (exitNow bool) {
			if !promptUserBeforeDelete {
				return false
			}

		AllowPruneSteps:
			fmt.Printf("\n%s", s)
			var ans uint8
			_, err := fmt.Scanf("%d\n", &ans)
			if err != nil {
				fmt.Printf("err: %v\n", err)
				return true
			}
			switch ans {
			case 1:
				return false
			case 4:
				return true
			default:
				fmt.Printf("invalid input: %d; Just a number 1 or 4 expected.\n", ans)
				goto AllowPruneSteps
			}
		}

		if removeLatest {
			// domain files have higher merge limit, so latest domain may have From < stepFrom but To == stepTo
			q := fmt.Sprintf("remove latest snapshot files (stepFrom>=%d) and files ending at stepTo=%d?\n1) RemoveFile\n4) Exit\n (pick number): ", _maxFrom, _maxTo)
			if promptExit(q) {
				os.Exit(0)
			}
			minS, maxS = _maxFrom, math.MaxUint64
		}

		if minS == maxS {
			q := "remove ALL snapshot files?\n\t1) RemoveFile\n\t4) NONONO (Exit)\n (pick number): "
			if promptExit(q) {
				os.Exit(0)
			}
			minS, maxS = 0, math.MaxUint64

		} else { // prevent all commitment files with trie state from deletion for "compacted" domains case
			hasStateTrie := 0
			for _, file := range commitmentFilesWithState {
				if file.To <= minS {
					hasStateTrie++
					fmt.Println("KEEP   " + file.Path)
				} else {
					fmt.Println("REMOVE " + file.Path)
				}
			}
			if hasStateTrie == 0 && len(commitmentFilesWithState) > 0 {
				fmt.Printf("this will remove ALL commitment files with state trie\n")
				q := "Do that anyway?\n\t1) RemoveFile\n\t4) NONONO (Exit)\n (pick number): "
				if promptExit(q) {
					os.Exit(0)
				}
			}
		}

		for _, res := range files {
			if res.From >= minS && res.To <= maxS {
				toRemove[res.Path] = res
			} else if removeLatest && res.To == _maxTo {
				toRemove[res.Path] = res
			}
		}

		// Second pass: remove any file whose step range is a strict subset of a file
		// already marked for removal, but only within the same domain type.
		// This handles orphaned sub-range files that were constituents of a merged
		// file (e.g., accounts.192-208 is a subset of accounts.192-224).
		// Cross-domain matching is prevented so that, e.g., removing commitment.192-224
		// does not cascade to accounts.192-208 which may be the only copy of that data.
		//
		// A single pass suffices because interval subset containment is transitive:
		// if C ⊂ B and B ⊂ A, then C ⊂ A. Since A (the originally-marked file) is
		// already in toRemove, C will match against A directly without needing B as
		// an intermediate step.
		for _, res := range files {
			if _, alreadyMarked := toRemove[res.Path]; alreadyMarked {
				continue
			}
			for _, marked := range toRemove {
				if res.TypeString == marked.TypeString &&
					res.From >= marked.From && res.To <= marked.To &&
					(res.From != marked.From || res.To != marked.To) {
					toRemove[res.Path] = res
					break
				}
			}
		}
	} else {
		for _, res := range files {
			toRemove[res.Path] = res
		}
	}

	var removed uint64
	for _, res := range toRemove {
		if dryRun {
			fmt.Printf("[dry-run] rm %s\n", res.Path)
			fmt.Printf("[dry-run] rm %s\n", res.Path+".torrent")
			continue
		}
		dir2.RemoveFile(res.Path)
		dir2.RemoveFile(res.Path + ".torrent")
		removed++
	}
	fmt.Printf("removed %d state snapshot segments files\n", removed)

	// Unconditionally remove .tmp files from all snapshot directories.
	// These are artifacts from incomplete/cancelled operations and should always be cleaned up.
	var removedTmp uint64
	for _, dirPath := range []string{dirs.Snap, dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors, dirs.SnapCaplin, dirs.SnapForkable} {
		tmpFiles, err := snaptype.TmpFiles(dirPath)
		if err != nil {
			return err
		}
		for _, tmpFile := range tmpFiles {
			if dryRun {
				fmt.Printf("[dry-run] rm %s\n", tmpFile)
				removedTmp++
				continue
			}
			if err := dir2.RemoveFile(tmpFile); err != nil {
				if !errors.Is(err, fs.ErrNotExist) {
					return err
				}
			}
			removedTmp++
		}
	}
	if removedTmp > 0 {
		fmt.Printf("removed %d .tmp files\n", removedTmp)
	}

	fmt.Printf("\n\nBefore restarting Erigon, run one of:\n  - `integration stage_custom_trace --reset` if deleted domains are handled by stage_custom_trace\n  - `integration stage_exec --reset` otherwise\nThis prunes DB remnants to avoid gaps between snapshots and DB.\n")
	return nil
}

func doRmStateSnapshots(cliCtx *cli.Context) error {
	dirs, l, err := datadir.New(cliCtx.String(utils.DataDirFlag.Name)).MustFlock()
	if err != nil {
		return err
	}
	defer l.Unlock()

	removeLatest := cliCtx.Bool("latest")
	stepRange := cliCtx.String("step")
	domainNames := cliCtx.StringSlice("domain")
	dryRun := cliCtx.Bool("dry-run")
	promptUser := true // CLI should always prompt the user
	return DeleteStateSnapshots(DeleteStateSnapshotsArgs{
		Dirs:                   dirs,
		RemoveLatest:           removeLatest,
		PromptUserBeforeDelete: promptUser,
		DryRun:                 dryRun,
		StepRange:              stepRange,
		DomainNames:            domainNames,
	})
}

func doRollbackSnapshotsToBlock(ctx context.Context, blockNum uint64, prompt bool, dataDir string, logger log.Logger) error {
	dirs, l, err := datadir.New(dataDir).MustFlock()
	if err != nil {
		return err
	}
	defer func() {
		err := l.Unlock()
		if err != nil {
			logger.Error("failed to unlock datadir", "err", err)
		}
	}()
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)
	res, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	br, agg := res.BlockRetire, res.Aggregator
	if err != nil {
		return err
	}
	defer clean()
	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	reader, _ := br.IO()
	txNumReader := reader.TxnumReader()
	toTxNum, err := txNumReader.Min(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	toStep := toTxNum / agg.StepSize()
	var toDelete []string
	for _, dirPath := range []string{dirs.Snap, dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors, dirs.SnapForkable} {
		filePaths, err := dir2.ListFiles(dirPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return err
		}
		for _, filePath := range filePaths {
			parsed, isState, ok := snaptype.ParseFileName("", filePath)
			if !ok {
				continue
			}
			if (isState && parsed.To > toStep) || (!isState && parsed.To > blockNum) {
				logger.Info("adding for deletion", "file", parsed.Path)
				toDelete = append(toDelete, parsed.Path)
			}
		}
	}
	logger.Info("about to delete chaindata and mentioned snapshot files", "toBlock", blockNum, "toTxNum", toTxNum, "toStep", toStep)
	if prompt {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("confirm above? (y/n): ")
		scanner.Scan()
		response := strings.ToLower(strings.TrimSpace(scanner.Text()))
		if response != "y" {
			logger.Info("rollback aborted")
			return nil
		}
	}
	err = dir2.RemoveAll(dirs.Chaindata)
	if err != nil {
		return err
	}
	for _, filePath := range toDelete {
		err = dir2.RemoveFile(filePath)
		if err != nil {
			return err
		}
	}
	logger.Info("rollback completed - deleted chaindata and files", "deletedFiles", toDelete)
	return nil
}

func doBtSearch(cliCtx *cli.Context) error {
	_, l, err := datadir.New(cliCtx.String(utils.DataDirFlag.Name)).MustFlock()
	if err != nil {
		return err
	}
	defer l.Unlock()
	logger := log.Root()

	srcF := cliCtx.String("src")
	dataFilePath := strings.TrimRight(srcF, ".bt") + ".kv"

	runtime.GC()
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info("before open", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	compress := seg.CompressKeys | seg.CompressVals
	kv, idx, err := btindex.OpenBtreeIndexAndDataFile(srcF, dataFilePath, btindex.DefaultBtreeM, compress, false)
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
	logger := log.Root()
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
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	res, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	agg := res.Aggregator
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
	logger := log.Root()

	ctx := cliCtx.Context
	checkStr := cliCtx.String("check")
	var requestedChecks []integrity.Check
	if len(checkStr) > 0 {
		for split := range strings.SplitSeq(checkStr, ",") {
			requestedChecks = append(requestedChecks, integrity.Check(split))
		}

		for _, check := range requestedChecks {
			if slices.Contains(integrity.AllChecks, check) {
				continue
			}

			return fmt.Errorf("requested check %s not found", check)
		}
	} else {
		requestedChecks = integrity.FastChecks
	}

	skipChecks := cliCtx.String("skip-check")
	if len(skipChecks) > 0 {
		skipSet := map[integrity.Check]struct{}{}
		for skipCheck := range strings.SplitSeq(skipChecks, ",") {
			skipSet[integrity.Check(skipCheck)] = struct{}{}
		}
		var finalChecks []integrity.Check
		for _, chk := range requestedChecks {
			if _, skip := skipSet[chk]; skip {
				logger.Info("[integrity] skipping check", "check", chk)
				continue
			}
			finalChecks = append(finalChecks, chk)
		}

		requestedChecks = finalChecks
	}

	failFast := cliCtx.Bool("failFast")
	fromStep := cliCtx.Uint64("fromStep")

	var cache *integrity.IntegrityCache
	if cachePath := cliCtx.String("file-integrity-cache"); cachePath != "" {
		var err error
		cache, err = integrity.LoadIntegrityCache(cachePath)
		if err != nil {
			return err
		}
		defer func() {
			if err := cache.Save(); err != nil {
				logger.Warn("[integrity] failed to save cache", "err", err)
			}
		}()

		// When using cache, verify torrent piece hashes first (unless skipped)
		if !cliCtx.Bool("skip-torrent-verify") {
			dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
			logger.Info("[integrity] verifying torrent piece hashes before integrity checks")
			if err := integrity.VerifyTorrentFiles(ctx, dirs.Snap, failFast, logger); err != nil {
				return fmt.Errorf("torrent verification failed: %w", err)
			}
		}
	}

	var seed int64
	if cliCtx.IsSet("seed") {
		seed = cliCtx.Int64("seed")
	} else {
		seed = time.Now().UnixNano()
	}
	sampleRatio := cliCtx.Float64("sample")
	if err := integrity.ValidateSampleRatio(sampleRatio); err != nil {
		return err
	}
	sc, err := integrity.NewSamplerCfg(seed, sampleRatio)
	if err != nil {
		return err
	}

	logger.Info("[integrity] starting", "seed", sc.Seed, "sampleRatio", sc.SampleRatio, "checks", requestedChecks)

	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	res, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	borSnaps, blockRetire, agg := res.BorSnaps, res.BlockRetire, res.Aggregator
	if err != nil {
		return err
	}
	defer clean()

	defer blockRetire.MadvNormal().DisableReadAhead()
	defer agg.MadvNormal().DisableReadAhead()

	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	defer db.Close()

	blockReader, _ := blockRetire.IO()
	heimdallStore, _ := blockRetire.BorStore()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(1)
	for _, chk := range requestedChecks {
		chk := chk
		g.Go(func() error {
			logger.Info("[integrity] starting", "check", chk)
			if err := func() error {
				switch chk {
				case integrity.BlocksTxnID:
					if err := blockReader.(*freezeblocks.BlockReader).IntegrityTxnID(failFast); err != nil {
						return err
					}
				case integrity.HeaderNoGaps:
					if err := integrity.NoGapsInCanonicalHeaders(ctx, db, blockReader, failFast); err != nil {
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
					if !CheckBorChain(chainConfig.ChainName) {
						logger.Info("BorEvents skipped because not bor chain")
						return nil
					}
					snapshots := blockReader.BorSnapshots().(*heimdall.RoSnapshots)
					if err := bridge.ValidateBorEvents(ctx, db, blockReader, snapshots, 0, 0, failFast); err != nil {
						return err
					}
				case integrity.BorSpans:
					if !CheckBorChain(chainConfig.ChainName) {
						logger.Info("BorSpans skipped because not bor chain")
						return nil
					}
					if err := heimdall.ValidateBorSpans(ctx, logger, dirs, heimdallStore, borSnaps, failFast); err != nil {
						return err
					}
				case integrity.BorCheckpoints:
					if !CheckBorChain(chainConfig.ChainName) {
						logger.Info("BorCheckpoints skipped because not bor chain")
						return nil
					}
					if err := heimdall.ValidateBorCheckpoints(ctx, logger, dirs, heimdallStore, borSnaps, failFast); err != nil {
						return err
					}
				case integrity.ReceiptsNoDups:
					if err := integrity.CheckReceiptsNoDups(ctx, sc, db, blockReader, failFast); err != nil {
						return err
					}
				case integrity.RCacheNoDups:
					if err := integrity.CheckRCacheNoDups(ctx, sc, db, blockReader, failFast); err != nil {
						return err
					}
				case integrity.Publishable:
					if err := doPublishable(cliCtx, chainDB); err != nil {
						return err
					}
				case integrity.CommitmentRoot:
					if err := integrity.CheckCommitmentRoot(ctx, db, blockReader, failFast, logger); err != nil {
						return err
					}
				case integrity.CommitmentKvi:
					scCopy := sc
					scCopy.SampleRatio = 0 // Sudeep will try to speedup it different way: by use `cache`
					if err := integrity.CheckCommitmentKvi(ctx, scCopy, db, cache, failFast, logger); err != nil {
						return err
					}
				case integrity.CommitmentKvDeref:
					if err := integrity.CheckCommitmentKvDeref(ctx, db, cache, failFast, logger); err != nil {
						return err
					}
				case integrity.CommitmentHistVal:
					scCopy := sc
					scCopy.SampleRatio /= 100 // it's very slow check
					if err := integrity.CheckCommitmentHistVal(ctx, scCopy, db, blockReader, failFast, logger); err != nil {
						return err
					}
				case integrity.StateRootVerifyByHistory:
					to, err := stateProgress(ctx, db, blockReader.TxnumReader())
					if err != nil {
						return err
					}
					scCopy := sc
					scCopy.SampleRatio /= 100 // it's very slow check
					if err := integrity.CheckCommitmentHistAtBlkRange(ctx, scCopy, db, blockReader, 1, to+1, failFast, logger); err != nil {
						return err
					}
				case integrity.StateVerify:
					if err := integrity.CheckStateVerify(ctx, db, failFast, fromStep, logger); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unknown check: %s", chk)
				}
				return nil
			}(); err != nil {
				return fmt.Errorf("%s: %w", chk, err)
			}
			return nil
		})
	}

	return g.Wait()
}

// stateProgress returns the latest block number covered by state snapshots,
// derived from the aggregator's EndTxNumMinimax. This may differ from the block
// files progress — block snapshots and state snapshots advance independently.
// Use this as the upper bound for state-history integrity commands.
func stateProgress(ctx context.Context, db kv.TemporalRoDB, txNumsReader rawdbv3.TxNumsReader) (uint64, error) {
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	aggMax := agg.EndTxNumMinimax()
	roTx, err := db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer roTx.Rollback()
	blockNum, _, err := txNumsReader.FindBlockNum(ctx, roTx, aggMax)
	if err != nil {
		return 0, err
	}
	return blockNum, nil
}

func doCheckCommitmentHistAtBlk(cliCtx *cli.Context, logger log.Logger) error {
	ctx := cliCtx.Context
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false /*keepBlocks*/, true /*produceE2*/, true /*produceE3*/, chainConfig.ChainName)
	res, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	blockRetire, agg := res.BlockRetire, res.Aggregator
	if err != nil {
		return err
	}
	defer clean()
	defer blockRetire.MadvNormal().DisableReadAhead()
	defer agg.MadvNormal().DisableReadAhead()
	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	defer db.Close()
	blockReader, _ := blockRetire.IO()
	blockNum := cliCtx.Uint64("block")
	if err = integrity.CheckCommitmentHistAtBlk(ctx, db, blockReader, blockNum, log.LvlInfo, logger); err != nil {
		return fmt.Errorf("checkCommitmentHistAtBlk: %d, %w", blockNum, err)
	}
	return nil
}

func doCheckStateRootByHistory(cliCtx *cli.Context, logger log.Logger) error {
	ctx := cliCtx.Context
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false /*keepBlocks*/, true /*produceE2*/, true /*produceE3*/, chainConfig.ChainName)
	res, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	blockRetire, agg := res.BlockRetire, res.Aggregator
	if err != nil {
		return err
	}
	defer clean()
	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	defer db.Close()
	blockReader, _ := blockRetire.IO()
	from := cliCtx.Uint64("from")
	to := cliCtx.Uint64("to")
	if !cliCtx.IsSet("to") {
		latestBlock, err := stateProgress(ctx, db, blockReader.TxnumReader())
		if err != nil {
			return err
		}
		to = latestBlock + 1 // exclusive upper bound
		logger.Info("[check-commitment-hist-at-blk-range] auto-detected --to", "to", to)
	}
	var seed int64
	if cliCtx.IsSet("seed") {
		seed = cliCtx.Int64("seed")
	} else {
		seed = time.Now().UnixNano()
	}
	sampleRatio := cliCtx.Float64("sample")
	sc, err := integrity.NewSamplerCfg(seed, sampleRatio)
	if err != nil {
		return err
	}
	logger.Info("[check-commitment-hist-at-blk-range] sampling config", "seed", sc.Seed, "sampleRatio", sc.SampleRatio)
	return integrity.CheckCommitmentHistAtBlkRange(ctx, sc, db, blockReader, from, to, true /*failFast*/, logger)
}

func doVerifyState(cliCtx *cli.Context, logger log.Logger) error {
	ctx := cliCtx.Context
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))

	// Open MDBX without Accede so it creates the DB if needed (memState-only setups have no chaindata).
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	chainDB := mdbx.New(dbcfg.ChainDB, logger).Path(dirs.Chaindata).RoTxsLimiter(limiterB).MustOpen()
	defer chainDB.Close()

	agg := openAgg(ctx, dirs, chainDB, logger)
	defer agg.Close()
	defer agg.MadvNormal().DisableReadAhead()
	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	defer db.Close()
	failFast := cliCtx.Bool("failFast")
	fromStep := cliCtx.Uint64("from-step")
	return integrity.CheckStateVerify(ctx, db, failFast, fromStep, logger)
}

func doVerifyHistory(cliCtx *cli.Context, logger log.Logger) error {
	ctx := cliCtx.Context
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))

	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	chainDB := mdbx.New(dbcfg.ChainDB, logger).Path(dirs.Chaindata).RoTxsLimiter(limiterB).MustOpen()
	defer chainDB.Close()

	chainConfig := fromdb.ChainConfig(chainDB)

	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)
	snaps, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	if err != nil {
		return fmt.Errorf("verify-history: open snaps: %w", err)
	}
	defer clean()

	blockReader := freezeblocks.NewBlockReader(snaps.BlockSnaps, snaps.BorSnaps)

	agg := snaps.Aggregator
	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	defer db.Close()

	engine := rulesconfig.CreateRulesEngineBareBones(ctx, chainConfig, logger)

	failFast := cliCtx.Bool("failFast")
	fromStep := cliCtx.Uint64("from-step")
	workers := cliCtx.Int("workers")
	if workers <= 0 {
		workers = max(runtime.NumCPU()/2, 1)
	}

	verifier := verify.NewHistoryVerifier(blockReader, chainConfig, engine, workers, logger)
	stepSize := agg.StepSize()

	// Iterate domain files to find history ranges to verify.
	// We use AccountsDomain files as the canonical list of step ranges,
	// but verify all domains (accounts, storage, code) for each range.
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	aggTx := state.AggTx(tx)
	files := aggTx.Files(kv.AccountsDomain)

	// Collect file ranges to verify.
	type fileRange struct {
		step       uint64
		startTxNum uint64
		endTxNum   uint64
	}
	var ranges []fileRange
	for _, file := range files {
		if !strings.HasSuffix(file.Fullpath(), ".kv") {
			continue
		}
		startTxNum := file.StartRootNum()
		fileStep := startTxNum / stepSize
		if fileStep < fromStep {
			continue
		}
		// Skip base file — it covers the full history from genesis.
		if startTxNum == 0 {
			continue
		}
		ranges = append(ranges, fileRange{
			step:       fileStep,
			startTxNum: startTxNum,
			endTxNum:   file.EndRootNum(),
		})
	}

	logger.Info("[verify-history] starting verification",
		"files", len(ranges), "workers", workers)

	var integrityErr error
	for _, r := range ranges {
		logger.Info("[verify-history] verifying file range",
			"step", r.step, "startTxNum", r.startTxNum, "endTxNum", r.endTxNum)

		err := verifier(ctx, db, r.startTxNum, r.endTxNum)
		if err != nil {
			if failFast {
				return err
			}
			logger.Warn("[verify-history] file failed", "step", r.step, "err", err)
			integrityErr = err
		}
	}
	return integrityErr
}

func CheckBorChain(chainName string) bool {
	return slices.Contains([]string{networkname.BorMainnet, networkname.Amoy, networkname.BorE2ETestChain2Val, networkname.BorDevnet}, chainName)
}

func checkIfCaplinSnapshotsPublishable(dirs datadir.Dirs, emptyOk bool) error {
	stateSnapTypes := snapshotsync.MakeCaplinStateSnapshotsTypes(nil)
	caplinSchema := snapshotsync.NewCaplinSchema(dirs, 1000, stateSnapTypes)

	//to := int64(-1)
	for _, snapt := range snaptype.CaplinSnapshotTypes {
		_, _, err := CheckFilesForSchema(caplinSchema.Get(snapt.Enum()), CheckFilesParams{
			checkLastFileTo: -1,
			emptyOk:         emptyOk,
			doesntStartAt0:  snapt.Enum() == snaptype.BlobSidecars.Enum(),
		})
		if err != nil {
			return err
		}
		// if empty {
		// 	continue
		// }

		// to = int64(uto)
	}

	to := int64(-1)
	somethingPresent, somethingEmpty := false, false
	for table := range stateSnapTypes.KeyValueGetters {
		uto, empty, err := CheckFilesForSchema(caplinSchema.GetState(table), CheckFilesParams{
			checkLastFileTo: to,
			emptyOk:         emptyOk,
		})
		if err != nil {
			return err
		}
		somethingPresent = somethingPresent || !empty
		somethingEmpty = somethingEmpty || empty

		to = int64(uto)
	}

	if somethingEmpty && somethingPresent {
		return fmt.Errorf("some state snapshot files are empty while others are present")
	}

	return nil

}

func checkIfBlockSnapshotsPublishable(snapDir string) error {
	var sum uint64
	var maxTo uint64
	verMap := map[string]map[string]version.Versions{
		"headers": {
			"seg": snaptype2.Headers.Versions(),
			"idx": snaptype2.Headers.Indexes()[0].Version,
		},
		"transactions": {
			"seg": snaptype2.Transactions.Versions(),
			"idx": snaptype2.Transactions.Indexes()[0].Version,
		},
		"bodies": {
			"seg": snaptype2.Bodies.Versions(),
			"idx": snaptype2.Bodies.Indexes()[0].Version,
		},
		"transactions-to-block": {
			"idx": snaptype2.Transactions.Indexes()[1].Version,
		},
	}
	// Check block sanity
	if err := filepath.WalkDir(snapDir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) { //it's ok if some file get removed during walk
				return nil
			}
			return err
		}
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
		headerSegVer := res.Version

		if !verMap["headers"]["seg"].Supports(headerSegVer) {
			return fmt.Errorf("expected version %s, filename: %s", verMap["headers"]["seg"].Current.String(), info.Name())
		}
		// check that all files exist
		for _, snapType := range []string{"headers", "transactions", "bodies"} {
			segName := strings.Replace(headerSegName, "headers", snapType, 1)
			segVer := verMap[snapType]["seg"].Current
			segName = strings.Replace(segName, headerSegVer.String(), segVer.String(), 1)
			segNameMasked, err := version.ReplaceVersionWithMask(segName)
			if err != nil {
				return err
			}
			segName, ver, ok, err := version.FindFilesWithVersionsByPattern(filepath.Join(snapDir, segNameMasked))
			if err != nil {
				return fmt.Errorf("finding %s: %w", segNameMasked, err)
			}
			if !ok {
				return fmt.Errorf("missing file-%s", segNameMasked)
			}
			if !verMap[snapType]["seg"].Supports(ver) {
				return fmt.Errorf("expected version %s, filename: %s", verMap[snapType]["seg"].Current.String(), segName)
			}
			// check that the index file exist
			idxName := strings.Replace(segName, ".seg", ".idx", 1)
			idxNameMasked, err := version.ReplaceVersionWithMask(idxName)
			if err != nil {
				return err
			}
			if err := version.CheckIsThereFileWithSupportedVersion(idxNameMasked, verMap[snapType]["idx"].MinSupported); err != nil {
				return fmt.Errorf("index file %s: %w", idxName, err)
			}
			if snapType == "transactions" {
				// check that the tx index file exist
				txIdxName := strings.Replace(segName, "transactions.seg", "transactions-to-block.idx", 1)
				txIdxNameMasked, err := version.ReplaceVersionWithMask(txIdxName)
				if err != nil {
					return err
				}
				if err := version.CheckIsThereFileWithSupportedVersion(txIdxNameMasked, verMap["transactions-to-block"]["idx"].MinSupported); err != nil {
					return fmt.Errorf("index file %s: %w", txIdxName, err)
				}
			}
		}

		maxTo = max(maxTo, res.To)
		return nil
	}); err != nil {
		return err
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
	if sum != maxTo {
		return fmt.Errorf("sum %d != maxTo %d", sum, maxTo)
	}
	// Iterate over all fies in snapDir
	return nil
}

func checkIfStateSnapshotsPublishable(dirs datadir.Dirs, chainDB kv.RoDB) error {
	// Read feature flags from DB
	if chainDB == nil {
		chainDB = dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
		defer chainDB.Close()
	}

	var persistReceiptCache, commitmentHistory bool
	if err := chainDB.View(context.Background(), func(tx kv.Tx) error {
		var err error
		persistReceiptCache, err = kvcfg.PersistReceipts.Enabled(tx)
		if err != nil {
			return fmt.Errorf("failed to read PersistReceipts config: %w", err)
		}
		commitmentHistory, _, err = rawdb.ReadDBCommitmentHistoryEnabled(tx)
		if err != nil {
			return fmt.Errorf("failed to read CommitmentHistory config: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if !persistReceiptCache {
		log.Warn("[integrity] This installation doesn't persist receipts cache; ignoring .rcache checks")
	}
	if !commitmentHistory {
		log.Warn("[integrity] This installation doesn't persist commitment history; ignoring commitment history checks")
	}

	return checkStateSnapshotFiles(dirs, persistReceiptCache, commitmentHistory)
}

var (
	ErrSnapParseFilename   = errors.New("unparseable snapshot filename")
	ErrSnapNoAccountFiles  = errors.New("no account snapshot files found")
	ErrSnapGapAtStart      = errors.New("gap at start of snapshot range")
	ErrSnapOverlap         = errors.New("overlapping snapshot ranges")
	ErrSnapGap             = errors.New("gap in snapshot ranges")
	ErrSnapMissingFile     = errors.New("missing snapshot file")
	ErrSnapMaxStepMismatch = errors.New("max step mismatch across directories")
)

func checkStateSnapshotFiles(dirs datadir.Dirs, persistReceiptCache, commitmentHistory bool) error {
	var maxStepDomain uint64 // across all files in SnapDomain
	var accFiles []snaptype.FileInfo

	if err := filepath.WalkDir(dirs.SnapDomain, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) { //it's ok if some file get removed during walk
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}

		res, _, ok := snaptype.ParseFileName(dirs.SnapDomain, info.Name())
		if !ok {
			return fmt.Errorf("%w: %s", ErrSnapParseFilename, info.Name())
		}
		maxStepDomain = max(maxStepDomain, res.To)

		if !strings.HasSuffix(info.Name(), ".kv") || !strings.Contains(info.Name(), "accounts") {
			return nil
		}

		accFiles = append(accFiles, res)
		return nil
	}); err != nil {
		return err
	}

	sort.Slice(accFiles, func(i, j int) bool {
		return (accFiles[i].From < accFiles[j].From) || (accFiles[i].From == accFiles[j].From && accFiles[i].To < accFiles[j].To)
	})
	if len(accFiles) == 0 {
		return fmt.Errorf("%w (.kv) in %s", ErrSnapNoAccountFiles, dirs.SnapDomain)
	}
	if accFiles[0].From != 0 {
		return fmt.Errorf("%w: state snaps start at (%d-%d), snaptype: accounts", ErrSnapGapAtStart, accFiles[0].From, accFiles[0].To)
	}

	prevFrom, prevTo := accFiles[0].From, accFiles[0].To
	for i := 1; i < len(accFiles); i++ {
		res := accFiles[i]
		if prevFrom == res.From {
			return fmt.Errorf("%w: %s possibly overlapped by %s (maybe run remove_overlaps)", ErrSnapOverlap, accFiles[i-1].Path, res.Path)
		}
		if res.From < prevTo {
			return fmt.Errorf("%w: between %s and %s", ErrSnapOverlap, res.Path, accFiles[i-1].Path)
		}
		if res.From > prevTo {
			return fmt.Errorf("%w: between %s and %s", ErrSnapGap, accFiles[i-1].Path, res.Path)
		}
		prevFrom, prevTo = res.From, res.To
	}

	for _, res := range accFiles {
		// do a range check over all snapshots types (sanitizes domain and history folder)
		accName, err := version.ReplaceVersionWithMask(res.Name())
		if err != nil {
			return fmt.Errorf("%w: failed to replace version in %s: %v", ErrSnapParseFilename, res.Name(), err)
		}
		for snapType := kv.Domain(0); snapType < kv.DomainLen; snapType++ {
			// skip rcache check if this datadir doesn't produce it
			if snapType == kv.RCacheDomain && !persistReceiptCache {
				continue
			}

			schemaVersionMinSup := statecfg.Schema.GetDomainCfg(snapType).GetVersions().Domain.DataKV.MinSupported
			expectedFileName := strings.Replace(accName, "accounts", snapType.String(), 1)
			if err = version.CheckIsThereFileWithSupportedVersion(filepath.Join(dirs.SnapDomain, expectedFileName), schemaVersionMinSup); err != nil {
				return fmt.Errorf("%w: %s at %s: %v", ErrSnapMissingFile, expectedFileName, filepath.Join(dirs.SnapDomain, expectedFileName), err)
			}

			// check that the index file exist
			if statecfg.Schema.GetDomainCfg(snapType).Accessors.Has(statecfg.AccessorBTree) {
				schemaVersionMinSup = statecfg.Schema.GetDomainCfg(snapType).GetVersions().Domain.AccessorBT.MinSupported
				fileName := strings.Replace(expectedFileName, ".kv", ".bt", 1)
				err := version.CheckIsThereFileWithSupportedVersion(filepath.Join(dirs.SnapDomain, fileName), schemaVersionMinSup)
				if err != nil {
					return fmt.Errorf("%w: %s at %s: %v", ErrSnapMissingFile, expectedFileName, filepath.Join(dirs.SnapDomain, fileName), err)
				}
			}
			if statecfg.Schema.GetDomainCfg(snapType).Accessors.Has(statecfg.AccessorExistence) {
				schemaVersionMinSup = statecfg.Schema.GetDomainCfg(snapType).GetVersions().Domain.AccessorKVEI.MinSupported
				fileName := strings.Replace(expectedFileName, ".kv", ".kvei", 1)
				err := version.CheckIsThereFileWithSupportedVersion(filepath.Join(dirs.SnapDomain, fileName), schemaVersionMinSup)
				if err != nil {
					return fmt.Errorf("%w: %s at %s: %v", ErrSnapMissingFile, expectedFileName, filepath.Join(dirs.SnapDomain, fileName), err)
				}
			}
			if statecfg.Schema.GetDomainCfg(snapType).Accessors.Has(statecfg.AccessorHashMap) {
				schemaVersionMinSup = statecfg.Schema.GetDomainCfg(snapType).GetVersions().Domain.AccessorKVI.MinSupported
				fileName := strings.Replace(expectedFileName, ".kv", ".kvi", 1)
				err := version.CheckIsThereFileWithSupportedVersion(filepath.Join(dirs.SnapDomain, fileName), schemaVersionMinSup)
				if err != nil {
					return fmt.Errorf("%w: %s at %s: %v", ErrSnapMissingFile, expectedFileName, filepath.Join(dirs.SnapDomain, fileName), err)
				}
			}
		}
	}

	if maxStepDomain != accFiles[len(accFiles)-1].To {
		return fmt.Errorf("%w: accounts domain max step (=%d) differs from SnapDomain files max step (=%d)", ErrSnapMaxStepMismatch, accFiles[len(accFiles)-1].To, maxStepDomain)
	}

	var maxStepII uint64 // across all files in SnapIdx
	accFiles = accFiles[:0]

	if err := filepath.WalkDir(dirs.SnapIdx, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) { //it's ok if some file get removed during walk
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}

		res, _, ok := snaptype.ParseFileName(dirs.SnapIdx, info.Name())
		if !ok {
			return fmt.Errorf("%w: %s", ErrSnapParseFilename, info.Name())
		}

		maxStepII = max(maxStepII, res.To)

		if !strings.HasSuffix(info.Name(), ".ef") || !strings.Contains(info.Name(), "accounts") {
			return nil
		}

		accFiles = append(accFiles, res)

		return nil
	}); err != nil {
		return err
	}

	sort.Slice(accFiles, func(i, j int) bool {
		return (accFiles[i].From < accFiles[j].From) || (accFiles[i].From == accFiles[j].From && accFiles[i].To < accFiles[j].To)
	})
	if len(accFiles) == 0 {
		return fmt.Errorf("%w (.ef) in %s", ErrSnapNoAccountFiles, dirs.SnapIdx)
	}
	if accFiles[0].From != 0 {
		return fmt.Errorf("%w: state ef snaps start at (%d-%d), snaptype: accounts", ErrSnapGapAtStart, accFiles[0].From, accFiles[0].To)
	}

	prevFrom, prevTo = accFiles[0].From, accFiles[0].To
	for i := 1; i < len(accFiles); i++ {
		res := accFiles[i]
		if prevFrom == res.From {
			return fmt.Errorf("%w: %s possibly overlapped by %s (maybe run remove_overlaps)", ErrSnapOverlap, accFiles[i-1].Path, res.Path)
		}
		if res.From < prevTo {
			return fmt.Errorf("%w: between %s and %s", ErrSnapOverlap, res.Path, accFiles[i-1].Path)
		}
		if res.From > prevTo {
			return fmt.Errorf("%w: between %s and %s", ErrSnapGap, accFiles[i-1].Path, res.Path)
		}

		prevFrom, prevTo = res.From, res.To
	}

	viTypes := []string{"accounts", "storage", "code", "receipt"}
	iiTypes := []string{"accounts", "storage", "code", "receipt", "logtopics", "logaddrs", "tracesfrom", "tracesto"}
	if persistReceiptCache {
		viTypes = append(viTypes, "rcache")
		iiTypes = append(iiTypes, "rcache")
	}
	if commitmentHistory {
		viTypes = append(viTypes, "commitment")
		iiTypes = append(iiTypes, "commitment")
	}
	for _, res := range accFiles {
		accName, err := version.ReplaceVersionWithMask(res.Name())
		if err != nil {
			return fmt.Errorf("%w: failed to replace version in %s: %v", ErrSnapParseFilename, res.Name(), err)
		}
		// do a range check over all snapshots types (sanitizes domain and history folder)
		for _, snapType := range iiTypes {
			versioned, err := statecfg.Schema.GetVersioned(snapType)
			if err != nil {
				return err
			}

			schemaVersionMinSup := versioned.GetVersions().II.DataEF.MinSupported
			expectedFileName := strings.Replace(accName, "accounts", snapType, 1)
			if err = version.CheckIsThereFileWithSupportedVersion(filepath.Join(dirs.SnapIdx, expectedFileName), schemaVersionMinSup); err != nil {
				return fmt.Errorf("%w: %s at %s: %v", ErrSnapMissingFile, expectedFileName, filepath.Join(dirs.SnapIdx, expectedFileName), err)
			}
			// Check accessors
			schemaVersionMinSup = versioned.GetVersions().II.AccessorEFI.MinSupported
			fileName := strings.Replace(expectedFileName, ".ef", ".efi", 1)
			if err = version.CheckIsThereFileWithSupportedVersion(filepath.Join(dirs.SnapAccessors, fileName), schemaVersionMinSup); err != nil {
				return fmt.Errorf("%w: %s at %s: %v", ErrSnapMissingFile, fileName, filepath.Join(dirs.SnapAccessors, fileName), err)
			}
			if !slices.Contains(viTypes, snapType) {
				continue
			}
			schemaVersionMinSup = versioned.GetVersions().Hist.AccessorVI.MinSupported
			fileName = strings.Replace(expectedFileName, ".ef", ".vi", 1)
			if err = version.CheckIsThereFileWithSupportedVersion(filepath.Join(dirs.SnapAccessors, fileName), schemaVersionMinSup); err != nil {
				return fmt.Errorf("%w: %s at %s: %v", ErrSnapMissingFile, fileName, filepath.Join(dirs.SnapAccessors, fileName), err)
			}
			schemaVersionMinSup = versioned.GetVersions().Hist.DataV.MinSupported
			// check that .v
			fileName = strings.Replace(expectedFileName, ".ef", ".v", 1)
			if err = version.CheckIsThereFileWithSupportedVersion(filepath.Join(dirs.SnapHistory, fileName), schemaVersionMinSup); err != nil {
				return fmt.Errorf("%w: %s at %s: %v", ErrSnapMissingFile, fileName, filepath.Join(dirs.SnapHistory, fileName), err)
			}
		}
	}

	if maxStepDomain != accFiles[len(accFiles)-1].To {
		return fmt.Errorf("%w: accounts domain max step (=%d) differs from SnapIdx files max step (=%d)", ErrSnapMaxStepMismatch, accFiles[len(accFiles)-1].To, maxStepDomain)
	}
	return nil
}

func doBlockSnapshotsRangeCheck(snapDir string, suffix string, snapType string) error {
	type interval struct {
		from  uint64
		to    uint64
		fName string
	}

	intervals := []interval{}
	if err := filepath.WalkDir(snapDir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) { //it's ok if some file get removed during walk
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), suffix) || !strings.Contains(info.Name(), snapType+".") {
			return nil
		}
		res, _, ok := snaptype.ParseFileName(snapDir, info.Name())
		if !ok {
			return nil
		}
		intervals = append(intervals, interval{from: res.From, to: res.To, fName: info.Name()})
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].from < intervals[j].from
	})
	if len(intervals) == 0 {
		return fmt.Errorf("no snapshot files found in %s for type: %s", snapDir, snapType)
	}
	if intervals[0].from != 0 {
		return fmt.Errorf("gap at start: snapshots start at (%d-%d). snaptype: %s. files: %s", intervals[0].from, intervals[0].to, snapType, intervals[0].fName)
	}
	// Check that there are no overlaps
	for i := 1; i < len(intervals); i++ {
		if intervals[i].from < intervals[i-1].to {
			return fmt.Errorf("overlap between (%d-%d) and (%d-%d). snaptype: %s. files: %s %s", intervals[i-1].from, intervals[i-1].to, intervals[i].from, intervals[i].to, snapType, intervals[i-1].fName, intervals[i].fName)
		}
	}
	// Check that there are no gaps
	for i := 1; i < len(intervals); i++ {
		if intervals[i].from != intervals[i-1].to {
			return fmt.Errorf("gap between (%d-%d) and (%d-%d). snaptype: %s. files: %s %s", intervals[i-1].from, intervals[i-1].to, intervals[i].from, intervals[i].to, snapType, intervals[i-1].fName, intervals[i].fName)
		}
	}

	return nil

}

func doPublishable(cliCtx *cli.Context, chainDB kv.RoDB) error {
	dat := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	// Check block snapshots sanity
	if err := checkIfBlockSnapshotsPublishable(dat.Snap); err != nil {
		return err
	}
	// Iterate over all fies in dat.Snap
	if err := checkIfStateSnapshotsPublishable(dat, chainDB); err != nil {
		return err
	}
	if err := checkIfCaplinSnapshotsPublishable(dat, true); err != nil {
		return err
	}
	// check if salt-state.txt and salt-blocks.txt exist
	exists, err := dir2.FileExist(filepath.Join(dat.Snap, "salt-state.txt"))
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("missing file %s", filepath.Join(dat.Snap, "salt-state.txt"))
	}

	exists, err = dir2.FileExist(filepath.Join(dat.Snap, "salt-blocks.txt"))
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
	dat, l, err := datadir.New(cliCtx.String(utils.DataDirFlag.Name)).MustFlock()
	if err != nil {
		return err
	}
	defer l.Unlock()
	accessorsDir := dat.SnapAccessors
	domainDir := dat.SnapDomain
	snapDir := dat.Snap

	// Delete accessorsDir
	if err := dir2.RemoveAll(accessorsDir); err != nil {
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
	dir2.RemoveFile(filepath.Join(snapDir, "salt-state.txt"))
	dir2.RemoveFile(filepath.Join(snapDir, "salt-state.txt.torrent"))
	dir2.RemoveFile(filepath.Join(snapDir, "salt-blocks.txt"))
	dir2.RemoveFile(filepath.Join(snapDir, "salt-blocks.txt.torrent"))

	return nil
}

func deleteFilesWithExtensions(dir string, extensions []string) error {
	return filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) { //it's ok if some file get removed during walk
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Check file extensions and delete matching files
		for _, ext := range extensions {
			if strings.HasSuffix(info.Name(), ext) {
				if err := dir2.RemoveFile(path); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func doBlkTxNum(cliCtx *cli.Context) error {
	logger := log.Root()
	defer logger.Info("Done")

	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	blkNumber := cliCtx.Int64("block")
	txNum := cliCtx.Int64("txnum")

	if blkNumber < 0 && txNum < 0 {
		return errors.New("provide atleast one positive value -- either block or txnum")
	}
	if blkNumber >= 0 && txNum >= 0 {
		return errors.New("both block and txnum can't be provided")
	}

	ctx := cliCtx.Context
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	res, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	br, agg := res.BlockRetire, res.Aggregator
	if err != nil {
		return err
	}
	defer clean()

	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	reader, _ := br.IO()
	txNumReader := reader.TxnumReader()

	if blkNumber >= 0 {
		min, err := txNumReader.Min(ctx, tx, uint64(blkNumber))
		if err != nil {
			return err
		}
		max, err := txNumReader.Max(ctx, tx, uint64(blkNumber))
		if err != nil {
			return err
		}
		stepSize := agg.StepSize()
		minStep := min / stepSize
		maxStep := max / stepSize
		logger.Info("out", "block", blkNumber, "min_txnum", min, "max_txnum", max, "min_step", minStep, "max_step", maxStep)
	} else {
		blk, ok, err := txNumReader.FindBlockNum(ctx, tx, uint64(txNum))
		if err != nil {
			return err
		}
		if !ok {
			blk2, txNum2, err := txNumReader.Last(tx)
			if err != nil {
				return err
			}
			logger.Info("didn't find block for txNum", "txNum", txNum, "maxBlock", blk2, "maxTxNum", txNum2)
		}
		logger.Info("out", "txNum", txNum, "block", blk)
	}
	return nil
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
		var keysSize, valsSize datasize.ByteSize
		g := src.MakeGetter()
		for g.HasNext() {
			k, _ := g.Next(nil)
			keysSize += datasize.ByteSize(len(k))
			if g.HasNext() {
				v, _ := g.Next(nil)
				valsSize += datasize.ByteSize(len(v))
			}
		}
		log.Info("meta", "count", src.Count(), "size", datasize.ByteSize(src.Size()).HR(), "keys_size", keysSize.HR(), "vals_size", valsSize.HR(), "serialized_dict", datasize.ByteSize(src.SerializedDictSize()).HR(), "dict_words", src.DictWords(), "name", src.FileName(), "detected_compression_type", seg.DetectCompressType(src.MakeGetter()))
	} else if before, ok := strings.CutSuffix(fname, ".bt"); ok {
		kvFPath := before + ".kv"
		src, err := seg.NewDecompressor(kvFPath)
		if err != nil {
			panic(err)
		}
		defer src.Close()
		bt, err := btindex.OpenBtreeIndexWithDecompressor(fname, btindex.DefaultBtreeM, seg.NewReader(src.MakeGetter(), seg.CompressNone))
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
		log.Info("meta", "sz_total", total.HR(), "sz_offsets", offsets.HR(), "sz_double_ef", ef.HR(), "sz_golombRice", golombRice.HR(), "sz_existence", existence.HR(), "sz_l1", layer1.HR(), "keys_count", idx.KeyCount(), "leaf_size", idx.LeafSize(), "bucket_size", idx.BucketSize(), "enums", idx.Enums())
	}
	return nil
}

func doDecompressSpeed(cliCtx *cli.Context) error {
	logger := log.Root()
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
		//defer decompressor.MadvSequential().DisableReadAhead()

		t := time.Now()
		view, err := decompressor.OpenSequentialView()
		if err != nil {
			panic(err)
		}
		defer view.Close()
		g := view.MakeGetter()
		buf := make([]byte, 0, 16*etl.BufIOSize)
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
		}
		logger.Info("decompress speed", "took", time.Since(t))
	}()
	func() {
		//defer decompressor.MadvSequential().DisableReadAhead()

		t := time.Now()
		view, err := decompressor.OpenSequentialView()
		if err != nil {
			panic(err)
		}
		defer view.Close()
		g := view.MakeGetter()
		for g.HasNext() {
			_, _ = g.Skip()
		}
		log.Info("decompress skip speed", "took", time.Since(t))
	}()
	return nil
}

func doIndicesCommand(cliCtx *cli.Context, dirs datadir.Dirs) error {
	logger := log.Root()
	defer logger.Info("Done")
	ctx := cliCtx.Context

	rebuild := cliCtx.Bool(SnapshotRebuildFlag.Name)
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	if rebuild {
		panic("not implemented")
	}

	if err := freezeblocks.RemoveIncompatibleIndices(dirs); err != nil {
		return err
	}

	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	res, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	caplinSnaps, caplinStateSnaps, br, agg := res.CaplinSnaps, res.CaplinStateSnaps, res.BlockRetire, res.Aggregator
	if err != nil {
		return err
	}
	defer clean()
	agg.PresetOfflineMerge()

	if err := caplinStateSnaps.BuildMissingIndices(ctx, logger); err != nil {
		return err
	}
	if err := br.BuildMissedIndicesIfNeed(ctx, "Indexing", nil); err != nil {
		return err
	}
	if err := caplinSnaps.BuildMissingIndices(ctx, logger); err != nil {
		return err
	}

	temporalDb, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}

	err = temporalDb.BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers())
	if err != nil {
		return err
	}

	return nil
}
func doLS(cliCtx *cli.Context, dirs datadir.Dirs) error {
	logger := log.Root()
	defer logger.Info("Done")
	ctx := cliCtx.Context

	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	cfg := ethconfig.NewSnapCfg(false, true, true, fromdb.ChainConfig(chainDB).ChainName)

	res, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	blockSnaps, borSnaps, caplinSnaps, agg := res.BlockSnaps, res.BorSnaps, res.CaplinSnaps, res.Aggregator
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

type OpenSnapsResult struct {
	BlockSnaps       *freezeblocks.RoSnapshots
	BorSnaps         *heimdall.RoSnapshots
	CaplinSnaps      *freezeblocks.CaplinSnapshots
	CaplinStateSnaps *snapshotsync.CaplinStateSnapshots
	BlockRetire      *freezeblocks.BlockRetire
	Aggregator       *state.Aggregator
	ForkAgg          *state.ForkableAgg
}

func openSnaps(ctx context.Context, cfg ethconfig.BlocksFreezing, dirs datadir.Dirs, chainDB kv.RwDB, logger log.Logger) (
	res OpenSnapsResult,
	clean func(),
	err error,
) {
	if _, err = features.EnableSyncCfg(chainDB, ethconfig.Sync{}); err != nil {
		return
	}

	chainConfig := fromdb.ChainConfig(chainDB)

	res.BlockSnaps = freezeblocks.NewRoSnapshots(cfg, dirs.Snap, logger)
	if err = res.BlockSnaps.OpenFolder(); err != nil {
		return
	}
	res.BlockSnaps.LogStat("block")
	heimdall.RecordWayPoints(true) // needed to load checkpoints and milestones snapshots
	res.BorSnaps = heimdall.NewRoSnapshots(cfg, dirs.Snap, logger)
	if err = res.BorSnaps.OpenFolder(); err != nil {
		return
	}

	var beaconConfig *clparams.BeaconChainConfig
	_, beaconConfig, _, err = clparams.GetConfigsByNetworkName(chainConfig.ChainName)
	if err == nil {
		res.CaplinSnaps = freezeblocks.NewCaplinSnapshots(cfg, beaconConfig, dirs, logger)
		if err = res.CaplinSnaps.OpenFolder(); err != nil {
			return
		}
		res.CaplinSnaps.LogStat("caplin")

		indexDB, err := caplin1.OpenCaplinIndexDb(ctx, dirs.CaplinIndexing)
		if err != nil {
			return res, nil, err
		}

		snTypes := snapshotsync.MakeCaplinStateSnapshotsTypes(indexDB)
		blkFreezeCfg := ethconfig.BlocksFreezing{ChainName: beaconConfig.ConfigName}
		res.CaplinStateSnaps = snapshotsync.NewCaplinStateSnapshots(blkFreezeCfg, beaconConfig, dirs, snTypes, logger)
		if err = res.CaplinStateSnaps.OpenFolder(); err != nil {
			return res, nil, err
		}
		res.CaplinStateSnaps.LogStat("caplin-state")
	}

	//res.BorSnaps.LogStat("bor")
	var bridgeStore bridge.Store
	var heimdallStore heimdall.Store
	if chainConfig.Bor != nil {
		res.BorSnaps.DownloadComplete() // mark as ready
		bridgeStore = bridge.NewSnapshotStore(bridge.NewMdbxStore(dirs.DataDir, logger, true, 0), res.BorSnaps, chainConfig.Bor)
		if err = bridgeStore.Prepare(ctx); err != nil {
			return
		}
		heimdallStore = heimdall.NewSnapshotStore(heimdall.NewMdbxStore(logger, dirs.DataDir, true, 0), res.BorSnaps)
		if err = heimdallStore.Prepare(ctx); err != nil {
			return
		}
	}

	blockReader := freezeblocks.NewBlockReader(res.BlockSnaps, res.BorSnaps)
	blockWriter := blockio.NewBlockWriter()
	blockSnapBuildSema := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))
	res.BlockRetire = freezeblocks.NewBlockRetire(estimate.CompressSnapshot.Workers(), dirs, blockReader, blockWriter, chainDB, heimdallStore, bridgeStore, chainConfig, &ethconfig.Defaults, nil, blockSnapBuildSema, logger)

	res.Aggregator = openAgg(ctx, dirs, chainDB, logger)
	res.Aggregator.SetSnapshotBuildSema(blockSnapBuildSema)

	clean = func() {
		defer res.BlockSnaps.Close()
		defer res.BorSnaps.Close()
		defer res.CaplinSnaps.Close()
		defer res.Aggregator.Close()
	}
	err = chainDB.View(ctx, func(tx kv.Tx) error {
		ac := res.Aggregator.BeginFilesRo()
		defer ac.Close()
		stats.LogStats(ac, tx, logger, func(endTxNumMinimax uint64) (uint64, error) {
			histBlockNumProgress, _, err := blockReader.TxnumReader().FindBlockNum(ctx, tx, endTxNumMinimax)
			return histBlockNumProgress, err
		})
		return nil
	})
	if err != nil {
		return
	}

	return
}

func doUncompress(cliCtx *cli.Context) error {
	_, l, err := datadir.New(cliCtx.String(utils.DataDirFlag.Name)).MustFlock()
	if err != nil {
		return err
	}
	defer l.Unlock()
	args := cliCtx.Args()
	if args.Len() < 1 {
		return errors.New("expecting file path as a first argument")
	}

	decompressor, err := seg.NewDecompressor(args.First())
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.MadvSequential().DisableReadAhead()

	src, cleanup := seg.Decompressor2bufio(decompressor)
	defer cleanup()

	wr := bufio.NewWriterSize(os.Stdout, int(128*datasize.MB))
	defer wr.Flush()
	_, err = io.Copy(wr, src)
	return err
}

func doCompress(cliCtx *cli.Context) error {
	defer func() {
		var m runtime.MemStats
		dbg.ReadMemStats(&m)
		log.Info("done", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	}()

	dirs, lck, err := datadir.New(cliCtx.String(utils.DataDirFlag.Name)).MustFlock()
	if err != nil {
		return err
	}
	defer lck.Unlock()

	logger := log.Root()
	ctx := cliCtx.Context

	args := cliCtx.Args()
	if args.Len() < 1 {
		return errors.New("expecting file path as a first argument")
	}

	dst := args.First()

	src := bufio.NewReaderSize(os.Stdin, int(128*datasize.MB))
	srcF := cliCtx.String("from")
	if srcF != "" {
		decompressor, err := seg.NewDecompressor(srcF)
		if err != nil {
			return err
		}
		defer decompressor.Close()
		defer decompressor.MadvSequential().DisableReadAhead()
		log.Info("[compress] from", "from", srcF)

		var cleanup func()
		src, cleanup = seg.Decompressor2bufio(decompressor)
		defer cleanup()
	}

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

	logger.Info("[compress] file", "datadir", dirs.DataDir, "dst", dst, "cfg", compressCfg, "SnappyEachWord", doSnappyEachWord)
	c, err := seg.NewCompressor(ctx, "compress", dst, dirs.Tmp, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer c.Close()
	w := seg.NewWriter(c, compression)

	var snappyBuf, unSnappyBuf []byte
	var concatBuf []byte
	concatI := 0

	if err := seg.Bufio2compressor(ctx, src, w, func(word []byte) ([]byte, error) {
		if justPrint {
			fmt.Printf("%x\n\n", word)
			return nil, nil
		}

		concatI++
		if concat > 0 {
			if concatI%concat != 0 {
				concatBuf = append(concatBuf, word...)
				return nil, nil
			}
			word = concatBuf
			concatBuf = concatBuf[:0]
		}

		snappyBuf, word = compress.EncodeZstdIfNeed(snappyBuf[:0], word, doSnappyEachWord)
		var err error
		unSnappyBuf, word, err = compress.DecodeZstdIfNeed(unSnappyBuf[:0], word, doUnSnappyEachWord)
		if err != nil {
			return nil, err
		}
		_, _ = snappyBuf, unSnappyBuf
		return word, nil
	}); err != nil {
		return err
	}
	if err := c.Compress(); err != nil {
		return err
	}

	return nil
}

func doRemoveOverlap(cliCtx *cli.Context, dirs datadir.Dirs) error {
	logger := log.Root()
	defer logger.Info("Done")

	db := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()
	chainConfig := fromdb.ChainConfig(db)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)
	ctx := cliCtx.Context

	res, clean, err := openSnaps(ctx, cfg, dirs, db, logger)
	agg := res.Aggregator
	if err != nil {
		return err
	}
	defer clean()

	return agg.RemoveOverlapsAfterMerge(ctx)
}

func doUnmerge(cliCtx *cli.Context, dirs datadir.Dirs) error {
	logger := log.Root()
	defer logger.Info("Done")

	ctx := cliCtx.Context
	sourcefile := cliCtx.String(SnapshotFileFlag.Name)
	sourcefile = filepath.Join(dirs.Snap, sourcefile)

	exists, err := dir2.FileExist(sourcefile)
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

	if info.Type.Enum() == snaptype2.Enums.Headers || info.Type.Enum() == snaptype2.Enums.Bodies {
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
	} else if info.Type.Enum() != snaptype2.Enums.Transactions {
		return fmt.Errorf("unsupported type %s", info.Type.Enum().String())
	} else {
		// tx unmerge
		for ; blockFrom < blockTo; blockFrom += 1000 {
			um_fileinfo := snaptype2.Enums.Bodies.Type().FileInfo(dirs.Snap, blockFrom, blockFrom+1000)
			bodiesSegment, err := seg.NewDecompressor(um_fileinfo.Path)
			if err != nil {
				return err
			}
			defer bodiesSegment.Close()

			_, expectedCount, err := snaptype2.TxsAmountBasedOnBodiesSnapshots(bodiesSegment, um_fileinfo.Len()-1)
			if err != nil {
				return err
			}

			txfileinfo := um_fileinfo.As(snaptype2.Enums.Transactions.Type())
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
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)
	res, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	br := res.BlockRetire
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
	logger := log.Root()
	defer logger.Info("Done")
	ctx := cliCtx.Context

	db := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()
	chainConfig := fromdb.ChainConfig(db)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)

	res, clean, err := openSnaps(ctx, cfg, dirs, db, logger)
	caplinSnaps, br, agg := res.CaplinSnaps, res.BlockRetire, res.Aggregator
	if err != nil {
		return err
	}
	defer clean()

	defer br.MadvNormal().DisableReadAhead()
	defer agg.MadvNormal().DisableReadAhead()

	blockSnapBuildSema := semaphore.NewWeighted(int64(runtime.NumCPU()))
	agg.SetSnapshotBuildSema(blockSnapBuildSema)

	agg.PresetOfflineMerge()
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
		blocksInSnapshots = min(blocksInSnapshots, blockReader.FrozenBorBlocks(false))
	}
	logger.Info("retiring blocks", "from", blocksInSnapshots, "to", to)
	if err := br.RetireBlocks(ctx, blocksInSnapshots, to, log.LvlInfo, downloader.NoopSeederClient{}, nil); err != nil {
		return err
	}

	if err := br.RemoveOverlaps(nil); err != nil {
		return err
	}

	logger.Info("pruning blocks")
	deletedBlocks := math.MaxInt // To pass the first iteration
	allDeletedBlocks := 0
	for deletedBlocks > 0 { // prune happens by small steps, so need many runs
		err = db.UpdateNosync(ctx, func(tx kv.RwTx) error {
			if deletedBlocks, err = br.PruneAncientBlocks(tx, 100, time.Hour); err != nil {
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

	logger.Info("Work on state history snapshots")
	indexWorkers := estimate.IndexSnapshot.Workers()
	if err = agg.BuildMissedAccessors(ctx, indexWorkers); err != nil {
		return err
	}

	txNumsReader := blockReader.TxnumReader()
	var lastTxNum uint64
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		execProgress, _ := stages.GetStageProgress(tx, stages.Execution)
		lastTxNum, err = txNumsReader.Max(ctx, tx, execProgress)
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
	for hasMoreToPrune := true; hasMoreToPrune; {
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			hasMoreToPrune, err = tx.(kv.TemporalRwTx).PruneSmallBatches(ctx, 30*time.Second)
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

func doCompareIdx(cliCtx *cli.Context) error {
	// doesn't compare exact hashes offset,
	// only sizes, counts, offsets, and ordinal lookups.
	logger := log.Root()

	cmpFn := func(f, s uint64, msg string) {
		if f != s {
			panic(fmt.Sprintf("different %s -- first: %d, second: %d", msg, f, s))
		}
	}

	first := cliCtx.Path("first")
	second := cliCtx.Path("second")
	doSizeCheck := !cliCtx.Bool("skip-size-check")

	if doSizeCheck {
		fileInfo1, err := os.Stat(first)
		if err != nil {
			return err
		}
		fileInfo2, err := os.Stat(second)
		if err != nil {
			return err
		}
		cmpFn(uint64(fileInfo1.Size()), uint64(fileInfo2.Size()), "file_sizes")
	}

	firstIdx := recsplit.MustOpen(first)
	secondIdx := recsplit.MustOpen(second)
	defer firstIdx.Close()
	defer secondIdx.Close()

	cmpFn(firstIdx.KeyCount(), secondIdx.KeyCount(), "key_count")
	cmpFn(firstIdx.BaseDataID(), secondIdx.BaseDataID(), "base_data_id")

	if doSizeCheck {
		cmpFn(uint64(firstIdx.LeafSize()), uint64(secondIdx.LeafSize()), "leaf_size")
		cmpFn(uint64(firstIdx.BucketSize()), uint64(secondIdx.BucketSize()), "bucket_size")

		total1, offsets1, ef1, golombRice1, existence1, layer11 := firstIdx.Sizes()
		total2, offsets2, ef2, golombRice2, existence2, layer12 := secondIdx.Sizes()
		cmpFn(total1.Bytes(), total2.Bytes(), "total")
		cmpFn(offsets1.Bytes(), offsets2.Bytes(), "offset")
		cmpFn(ef1.Bytes(), ef2.Bytes(), "ef")
		cmpFn(golombRice1.Bytes(), golombRice2.Bytes(), "golombRice")
		cmpFn(existence1.Bytes(), existence2.Bytes(), "existence")
		cmpFn(layer11.Bytes(), layer12.Bytes(), "layer1")
	}

	firstOffsets := firstIdx.ExtractOffsets()
	secondOffsets := secondIdx.ExtractOffsets()

	for k := range firstOffsets {
		_, ok := secondOffsets[k]
		if !ok {
			logger.Error("offset not found in second file")
			return nil
		}
	}

	for k := range secondOffsets {
		_, ok := firstOffsets[k]
		if !ok {
			logger.Error("offset not found in first file")
			return nil
		}
	}

	if firstIdx.Enums() != secondIdx.Enums() {
		logger.Error("enums value don't match", "first", firstIdx.Enums(), "second", secondIdx.Enums())
		return nil
	}

	if firstIdx.Enums() {
		for i := uint64(0); i < firstIdx.KeyCount(); i++ {
			off1, off2 := firstIdx.OrdinalLookup(i), secondIdx.OrdinalLookup(i)
			cmpFn(off1, off2, fmt.Sprintf("offset_ordinal_%d", i))
		}
	}

	logger.Info("two files are identical")
	return nil
}

func dbCfg(label kv.Label, path string) mdbx.MdbxOpts {
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	return mdbx.New(label, log.New()).Path(path).
		RoTxsLimiter(limiterB).
		Accede(true) // integration tool: open db without creation and without blocking erigon
}
func openAgg(ctx context.Context, dirs datadir.Dirs, chainDB kv.RwDB, logger log.Logger) *state.Aggregator {
	erigonDBSettings, err := state.ResolveErigonDBSettings(dirs, logger, false)
	if err != nil {
		panic(err)
	}
	agg, err := state.New(dirs).SanityOldNaming().Logger(logger).WithErigonDBSettings(erigonDBSettings).Open(ctx, chainDB)
	if err != nil {
		panic(err)
	}
	if err = agg.OpenFolder(); err != nil {
		panic(err)
	}
	return agg
}

// ── du (disk usage) helpers ─────────────────────────────────────────────

// duCategory constants for file classification.
const (
	duCatDomains    = "domains"
	duCatHistory    = "history"
	duCatInvIdx     = "inverted indices"
	duCatAccessors  = "accessors"
	duCatBlocks     = "block segments"
	duCatCaplin     = "caplin"
	duCatCommitHist = "commitment hist"
	duCatRcache     = "rcache"
	duCatForkable   = "forkable"
	duCatOther      = "other"
)

// duFileInfo holds metadata for a single snapshot file.
type duFileInfo struct {
	Path        string
	Name        string
	Size        int64
	Category    string
	Subcategory string // e.g. "accounts", "storage", "headers", "transactions"
	From        uint64
	To          uint64
	IsState     bool // true for state files (history/idx/domain/accessor), false for block segments
}

// duClassifyFile maps a directory base name and file name to a duCategory.
// dir is the immediate parent directory name (e.g. "domain", "history", "idx", "accessor", "caplin").
func duClassifyFile(dir, name string) string {
	lname := strings.ToLower(name)
	ldir := strings.ToLower(dir)

	// rcache files live in domain/, history/, or idx/
	if strings.Contains(lname, "rcache") {
		return duCatRcache
	}

	// commitment history/idx files
	if (ldir == "history" || ldir == "idx") && strings.Contains(lname, "commitment") {
		return duCatCommitHist
	}

	switch ldir {
	case "domain":
		// All domain/ files (including accessors like .kvi, .kvei, .bt) are classified
		// as domains because domain files are never pruned by the snapshot pruning logic.
		return duCatDomains
	case "history":
		return duCatHistory
	case "idx":
		return duCatInvIdx
	case "accessor":
		return duCatAccessors
	case "caplin":
		return duCatCaplin
	case "forkable":
		return duCatForkable
	}

	// Files directly under snapshots/ — only known segment extensions are block segments.
	switch {
	case strings.HasSuffix(lname, ".seg"), strings.HasSuffix(lname, ".idx"), strings.HasSuffix(lname, ".dat"):
		return duCatBlocks
	default:
		return duCatOther
	}
}

// duWalkSnapshots walks all snapshot subdirectories and collects file metadata.
func duWalkSnapshots(dirs datadir.Dirs) ([]duFileInfo, error) {
	// Directories to scan and whether they contain state files.
	type scanDir struct {
		path    string
		isState bool
	}
	scanDirs := []scanDir{
		{dirs.SnapDomain, true},
		{dirs.SnapHistory, true},
		{dirs.SnapIdx, true},
		{dirs.SnapAccessors, true},
		{dirs.SnapCaplin, false},
		{dirs.SnapForkable, false},
		{dirs.Snap, false}, // top-level snapshots/ for block segments
	}

	var files []duFileInfo

	for _, sd := range scanDirs {
		entries, err := os.ReadDir(sd.path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("reading %s: %w", sd.path, err)
		}

		dirBase := filepath.Base(sd.path)

		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}

			fi := duFileInfo{
				Path:     filepath.Join(sd.path, e.Name()),
				Name:     e.Name(),
				Size:     info.Size(),
				Category: duClassifyFile(dirBase, e.Name()),
				IsState:  sd.isState,
			}

			// Parse range and type from filename using the versioned parser.
			parsed, _, ok := snaptype.ParseFileName(sd.path, e.Name())
			if ok {
				fi.From = parsed.From
				fi.To = parsed.To
				fi.Subcategory = parsed.TypeString
			}

			files = append(files, fi)
		}
	}

	return files, nil
}

// duEstimate holds estimated disk usage for a particular node type (archive/full/minimal).
type duEstimate struct {
	Mode        string `json:"mode"`
	TotalBytes  int64  `json:"total_bytes"`
	Delta       int64  `json:"delta"`
	BlocksDesc  string `json:"blocks_desc"`
	HistoryDesc string `json:"history_desc"`
}

// duIsReceiptRelated returns true for files whose pruning follows receipt
// rules rather than general history rules: rcache, logaddrs, logtopics.
func duIsReceiptRelated(f duFileInfo) bool {
	lname := strings.ToLower(f.Name)
	return f.Category == duCatRcache ||
		strings.Contains(lname, "logaddrs") || strings.Contains(lname, "logtopics")
}

// duIsRcacheDomainFile returns true for rcache files in the domain/ directory.
// Domain files are never pruned by the snapshot pruning logic.
func duIsRcacheDomainFile(f duFileInfo) bool {
	// Normalize path separators to handle both Unix and Windows paths in tests and production.
	normalized := filepath.ToSlash(f.Path)
	return f.Category == duCatRcache && strings.Contains(normalized, "/domain/")
}

// duComputeEstimates computes estimated sizes for archive/full/minimal modes
// by summing files that survive each mode's pruning rules.
// maxBlock is the highest block number across all block segment files.
// maxStep is the highest step number across all state files.
// mergeBlock is the chain's merge height (0 if unknown or pre-merge chain);
// full mode prunes pre-merge transaction segments when mergeBlock > 0.
func duComputeEstimates(files []duFileInfo, maxBlock, maxStep, mergeBlock uint64) []duEstimate {
	pruneDistance := uint64(config3.DefaultPruneDistance)
	// State files use step ranges, not block ranges. Convert the block-based
	// prune distance to step units using the observed blocks-per-step ratio.
	var stepPruneDistance uint64
	if maxStep > 0 && maxBlock > 0 {
		// Use a single division to avoid compounding integer truncation:
		// pruneDistance * maxStep / maxBlock instead of pruneDistance / (maxBlock / maxStep).
		stepPruneDistance = pruneDistance * maxStep / maxBlock
	}

	// Convert merge block to step units for receipt-related file pruning.
	var mergeStep uint64
	if mergeBlock > 0 && maxStep > 0 && maxBlock > 0 {
		mergeStep = mergeBlock * maxStep / maxBlock
	}

	var archiveTotal, fullTotal, minimalTotal int64

	for _, f := range files {
		archiveTotal += f.Size

		// Full mode: prunes old history/idx/accessor, commitment hist,
		// pre-merge transaction block segments (EIP-4444), and receipt-related
		// files below merge height.
		includeInFull := true
		switch f.Category {
		case duCatCommitHist:
			includeInFull = false
		case duCatHistory, duCatInvIdx, duCatAccessors:
			if f.IsState && stepPruneDistance > 0 && f.To > 0 && maxStep > stepPruneDistance && f.To <= maxStep-stepPruneDistance {
				if !duIsReceiptRelated(f) {
					includeInFull = false
				}
			}
		}
		if includeInFull && f.Category == duCatBlocks && !f.IsState && mergeBlock > 0 && strings.Contains(strings.ToLower(f.Name), "transactions") {
			if f.From < mergeBlock {
				includeInFull = false
			}
		}
		// Receipt-related state files (rcache hist/idx, logaddrs, logtopics)
		// are pruned below merge step in full mode (DefaultBlocksPruneMode).
		// Domain files are never pruned.
		if includeInFull && duIsReceiptRelated(f) && !duIsRcacheDomainFile(f) && f.IsState {
			if mergeStep > 0 && f.From < mergeStep {
				includeInFull = false
			}
		}

		if includeInFull {
			fullTotal += f.Size
		}

		// Minimal: same as full but also exclude old transaction block segments.
		// Only transaction-related block segments are prunable (headers/bodies are kept).
		includeInMinimal := includeInFull
		if includeInMinimal && f.Category == duCatBlocks && !f.IsState && strings.Contains(strings.ToLower(f.Name), "transactions") {
			if f.To > 0 && maxBlock > pruneDistance && f.To <= maxBlock-pruneDistance {
				includeInMinimal = false
			}
		}
		// In minimal mode, receipt-related state files use block distance
		// pruning (Distance(100k)), which maps to the same stepPruneDistance.
		if includeInMinimal && duIsReceiptRelated(f) && !duIsRcacheDomainFile(f) && f.IsState {
			if stepPruneDistance > 0 && f.To > 0 && maxStep > stepPruneDistance && f.To <= maxStep-stepPruneDistance {
				includeInMinimal = false
			}
		}

		if includeInMinimal {
			minimalTotal += f.Size
		}
	}

	// Describe what full mode keeps for blocks.
	fullBlocksDesc := "all blocks"
	if mergeBlock > 0 {
		fullBlocksDesc = "post-merge blocks"
	}

	historyDesc := fmt.Sprintf("last %s", duFormatNumber(pruneDistance))
	return []duEstimate{
		{Mode: "archive", TotalBytes: archiveTotal, Delta: 0, BlocksDesc: "all blocks", HistoryDesc: "all history"},
		{Mode: "full", TotalBytes: fullTotal, Delta: fullTotal - archiveTotal, BlocksDesc: fullBlocksDesc, HistoryDesc: historyDesc},
		{Mode: "minimal", TotalBytes: minimalTotal, Delta: minimalTotal - archiveTotal, BlocksDesc: fmt.Sprintf("last %s", duFormatNumber(pruneDistance)), HistoryDesc: historyDesc},
	}
}

// duDetectNodeType infers the current node mode from which files are present.
// Archive nodes retain all state history from step 0 (History=MaxUint64).
// Non-archive modes prune old state history, so files near step 0 are absent.
//   - Full: prunes old history and possibly pre-merge transaction segments.
//   - Minimal: prunes both old history and old transaction block segments.
func duDetectNodeType(files []duFileInfo) string {
	hasOldStateHistory := false
	var maxBlock, maxStep uint64

	pruneDistance := uint64(config3.DefaultPruneDistance)

	for _, f := range files {
		// Track max ranges for pruning cutoff calculations.
		if f.IsState && f.To > maxStep {
			maxStep = f.To
		}
		if f.Category == duCatBlocks && !f.IsState {
			if f.To > maxBlock {
				maxBlock = f.To
			}
		}
		// Regular state history files (not commitment hist, not rcache) starting
		// at step 0 indicate archive mode — only archive keeps all history.
		if f.Category == duCatHistory && f.From == 0 && f.To > 0 {
			hasOldStateHistory = true
		}
	}

	// Compute step prune distance to check if chain is old enough for pruning.
	var stepPruneDistance uint64
	if maxStep > 0 && maxBlock > 0 {
		stepPruneDistance = pruneDistance * maxStep / maxBlock
	}

	// Archive: keeps all state history from step 0.
	// Only consider this if the chain is mature enough that non-archive modes
	// would have pruned old history (maxStep > stepPruneDistance).
	if hasOldStateHistory && (stepPruneDistance == 0 || maxStep > stepPruneDistance) {
		return "archive"
	}

	// Check for old transaction block segments — if present, this is full mode
	// (old history pruned but block segments retained).
	if maxBlock > pruneDistance {
		for _, f := range files {
			if f.Category == duCatBlocks && !f.IsState && strings.Contains(strings.ToLower(f.Name), "transactions") &&
				f.To > 0 && f.To <= maxBlock-pruneDistance {
				return "full"
			}
		}
	}

	return "minimal"
}

// duCategoryStat holds aggregated size and file count for one category.
type duCategoryStat struct {
	Bytes int64 `json:"bytes"`
	Files int   `json:"files"`
}

// duResult aggregates all output data for the du command.
type duResult struct {
	Chain           string                               `json:"chain"`
	ConfiguredMode  string                               `json:"configured_mode,omitempty"` // from DB; empty when DB unavailable
	DetectedMode    string                               `json:"detected_mode"`
	BlockRange      [2]uint64                            `json:"block_range"`
	StepRange       [2]uint64                            `json:"step_range"`
	TotalBytes      int64                                `json:"total_bytes"`
	TotalFiles      int                                  `json:"total_files"`
	Categories      map[string]duCategoryStat            `json:"categories"`
	Subcategories   map[string]map[string]duCategoryStat `json:"subcategories,omitempty"` // category → subcategory → stat
	OtherExtensions []string                             `json:"other_extensions,omitempty"`
	Estimates       []duEstimate                         `json:"estimates"`
}

// duAggregateCategories computes per-category byte totals and file counts.
func duAggregateCategories(files []duFileInfo) map[string]duCategoryStat {
	cats := make(map[string]duCategoryStat)
	for _, f := range files {
		s := cats[f.Category]
		s.Bytes += f.Size
		s.Files++
		cats[f.Category] = s
	}
	return cats
}

// duAggregateSubcategories computes per-subcategory stats within each category.
func duAggregateSubcategories(files []duFileInfo) map[string]map[string]duCategoryStat {
	result := make(map[string]map[string]duCategoryStat)
	for _, f := range files {
		if f.Subcategory == "" {
			continue
		}
		subs, ok := result[f.Category]
		if !ok {
			subs = make(map[string]duCategoryStat)
			result[f.Category] = subs
		}
		s := subs[f.Subcategory]
		s.Bytes += f.Size
		s.Files++
		subs[f.Subcategory] = s
	}
	return result
}

// duOtherExtensions returns a sorted list of unique file extensions in the "other" category.
func duOtherExtensions(files []duFileInfo) []string {
	exts := make(map[string]struct{})
	for _, f := range files {
		if f.Category != duCatOther {
			continue
		}
		ext := filepath.Ext(f.Name)
		if ext == "" {
			ext = f.Name // no extension — use full name
		}
		exts[ext] = struct{}{}
	}
	result := make([]string, 0, len(exts))
	for ext := range exts {
		result = append(result, ext)
	}
	sort.Strings(result)
	return result
}

// duFormatSize formats bytes into a human-readable string (e.g., "420.3GB").
// Wraps common.ByteCount with support for negative values (deltas).
func duFormatSize(b int64) string {
	if b < 0 {
		return "-" + common.ByteCount(uint64(-b))
	}
	return common.ByteCount(uint64(b))
}

// duFormatNumber formats an integer with comma separators (e.g., 21500000 → "21,500,000").
func duFormatNumber(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	var buf strings.Builder
	rem := len(s) % 3
	if rem > 0 {
		buf.WriteString(s[:rem])
	}
	for i := rem; i < len(s); i += 3 {
		if buf.Len() > 0 {
			buf.WriteByte('.')
		}
		buf.WriteString(s[i : i+3])
	}
	return buf.String()
}

// duFormatHuman writes the human-readable du output to w.
func duFormatHuman(w io.Writer, result duResult, verbose bool) {
	// Header line: show configured mode from DB when available, always show detected.
	modeStr := result.DetectedMode + " (detected)"
	if result.ConfiguredMode != "" {
		modeStr = result.ConfiguredMode
		if result.ConfiguredMode != result.DetectedMode {
			modeStr += fmt.Sprintf(" (files look like %s)", result.DetectedMode)
		}
	}
	fmt.Fprintf(w, "\n%s | %s | blocks 0–%s | steps 0–%s\n",
		result.Chain, modeStr,
		duFormatNumber(result.BlockRange[1]),
		duFormatNumber(result.StepRange[1]))
	fmt.Fprintf(w, "total: %s (%d files)\n\n", duFormatSize(result.TotalBytes), result.TotalFiles)

	// Breakdown table sorted by size descending.
	type catEntry struct {
		name string
		stat duCategoryStat
	}
	entries := make([]catEntry, 0, len(result.Categories))
	for cat, stat := range result.Categories {
		entries = append(entries, catEntry{cat, stat})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].stat.Bytes != entries[j].stat.Bytes {
			return entries[i].stat.Bytes > entries[j].stat.Bytes
		}
		return entries[i].name < entries[j].name
	})

	fmt.Fprintln(w, "── Breakdown ──────────────────────────────────────────────────")
	for _, e := range entries {
		pct := float64(0)
		if result.TotalBytes > 0 {
			pct = float64(e.stat.Bytes) / float64(result.TotalBytes) * 100
		}
		fmt.Fprintf(w, "  %-20s %10s %9.1f%%  %5d files\n",
			e.name, duFormatSize(e.stat.Bytes), pct, e.stat.Files)

		// Show "other" extensions on next line.
		if e.name == duCatOther && len(result.OtherExtensions) > 0 {
			fmt.Fprintf(w, "    extensions: %s\n", strings.Join(result.OtherExtensions, ", "))
		}

		// Verbose: show subcategory breakdown within each category.
		if verbose {
			if subs, ok := result.Subcategories[e.name]; ok && len(subs) > 1 {
				subEntries := make([]catEntry, 0, len(subs))
				for sub, stat := range subs {
					subEntries = append(subEntries, catEntry{sub, stat})
				}
				sort.Slice(subEntries, func(i, j int) bool {
					if subEntries[i].stat.Bytes != subEntries[j].stat.Bytes {
						return subEntries[i].stat.Bytes > subEntries[j].stat.Bytes
					}
					return subEntries[i].name < subEntries[j].name
				})
				for j, sub := range subEntries {
					subPct := float64(0)
					if e.stat.Bytes > 0 {
						subPct = float64(sub.stat.Bytes) / float64(e.stat.Bytes) * 100
					}
					prefix := "  ├─"
					if j == len(subEntries)-1 {
						prefix = "  └─"
					}
					displayName := sub.name
					if displayName == "transactions-to-block" {
						displayName = "txn2block"
					}
					fmt.Fprintf(w, "%s %-16s %10s %9.1f%%  %5d files\n",
						prefix, displayName, duFormatSize(sub.stat.Bytes), subPct, sub.stat.Files)
				}
				fmt.Fprintln(w)
			}
		}
	}

	// Estimates table.
	fmt.Fprintln(w)
	fmt.Fprintln(w, "── Estimated Size by Node Type ────────────────────────────────")
	for _, est := range result.Estimates {
		deltaStr := "—"
		if est.Delta != 0 {
			deltaStr = duFormatSize(est.Delta)
		}
		fmt.Fprintf(w, "  %-13s %10s %10s    %-15s %s\n",
			est.Mode, duFormatSize(est.TotalBytes), deltaStr,
			est.BlocksDesc, est.HistoryDesc)
	}
}

// duFormatJSON writes the du result as JSON to w.
func duFormatJSON(w io.Writer, result duResult) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// doDU implements the "erigon seg du" subcommand.
func doDU(cliCtx *cli.Context, dirs datadir.Dirs) error {
	// Resolve chain name and configured prune mode from chaindata (best-effort).
	// Use recover because both MustOpen and fromdb.ChainConfig can panic
	// (e.g., DB locked by running node, corrupted/empty chaindata).
	chainName := "unknown"
	var mergeBlock uint64
	var configuredMode string // empty when DB is unavailable
	if _, err := os.Stat(dirs.Chaindata); err == nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Warn("could not read chaindata", "err", r)
				}
			}()
			chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
			defer chainDB.Close()
			cc := fromdb.ChainConfig(chainDB)
			if cc != nil && cc.ChainName != "" {
				chainName = cc.ChainName
			}
			if cc != nil && cc.MergeHeight != nil {
				mergeBlock = *cc.MergeHeight
			}
			pm := fromdb.PruneMode(chainDB)
			configuredMode = pm.String()
		}()
	}

	// Walk snapshot files.
	files, err := duWalkSnapshots(dirs)
	if err != nil {
		return fmt.Errorf("walking snapshots: %w", err)
	}

	// Compute ranges.
	var maxBlock, maxStep uint64
	for _, f := range files {
		if f.IsState {
			if f.To > maxStep {
				maxStep = f.To
			}
		} else if f.Category == duCatBlocks {
			if f.To > maxBlock {
				maxBlock = f.To
			}
		}
	}

	// Build result.
	cats := duAggregateCategories(files)
	estimates := duComputeEstimates(files, maxBlock, maxStep, mergeBlock)

	var totalBytes int64
	var totalFiles int
	for _, s := range cats {
		totalBytes += s.Bytes
		totalFiles += s.Files
	}

	result := duResult{
		Chain:           chainName,
		ConfiguredMode:  configuredMode,
		DetectedMode:    duDetectNodeType(files),
		BlockRange:      [2]uint64{0, maxBlock},
		StepRange:       [2]uint64{0, maxStep},
		TotalBytes:      totalBytes,
		TotalFiles:      totalFiles,
		Categories:      cats,
		Subcategories:   duAggregateSubcategories(files),
		OtherExtensions: duOtherExtensions(files),
		Estimates:       estimates,
	}

	verbose := cliCtx.Bool("v")

	// Output.
	if cliCtx.Bool("json") {
		return duFormatJSON(os.Stdout, result)
	}
	duFormatHuman(os.Stdout, result, verbose)
	return nil
}
