package app

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/urfave/cli/v2"
)

var (
	pruneFlag = cli.BoolFlag{
		Name:     "prune",
		Usage:    "Remove files not described in snapshot set.",
		Value:    false,
		Aliases:  []string{"p"},
		Category: "Reset",
	}
	dryRunFlag = cli.BoolFlag{
		Name:     "dry-run",
		Usage:    "Print files that would be removed, but do not remove them.",
		Value:    false,
		Aliases:  []string{"n"},
		Category: "Reset",
	}
)

func resetCliAction(cliCtx *cli.Context) (err error) {
	// Set logging verbosity. Oof that function signature.
	logger, _, _, _, err := debug.Setup(cliCtx, true)
	if err != nil {
		err = fmt.Errorf("setting up logging: %w", err)
		return
	}
	prune := pruneFlag.Get(cliCtx)
	dryRun := dryRunFlag.Get(cliCtx)
	dataDirPath := cliCtx.String(utils.DataDirFlag.Name)

	configChainName, err := getChainNameFromChainData(cliCtx, logger)
	if err != nil {
		return fmt.Errorf("getting chain name from chaindata: %w", err)
	}
	chain := utils.ChainFlag.Get(cliCtx)
	if cliCtx.IsSet(utils.ChainFlag.Name) {
		if configChainName.Ok && configChainName.Value != chain {
			// Pedantic but interesting.
			logger.Warn("chain name flag and chain config do not match", "flag", chain, "config", configChainName.Value)
		}
		logger.Info("using chain name from flag", "chain", chain)
	} else {
		if !configChainName.Ok {
			return errors.New("chain flag not set and chain name not found in chaindata (reset already occurred or invalid data dir?)")
		}
		chain = configChainName.Unwrap()
		logger.Info("read chain name from config", "chain", chain)
	}

	dirs := datadir.Open(dataDirPath)
	unlock, err := dirs.TryFlock()
	if err != nil {
		return fmt.Errorf("failed to lock data dir %v: %w", dirs.DataDir, err)
	}
	defer unlock()
	err = snapcfg.LoadRemotePreverified(cliCtx.Context)
	if err != nil {
		// TODO: Check if we should continue? What if we ask for a git revision and
		// can't get it? What about a branch? Can we reset to the embedded snapshot hashes?
		return fmt.Errorf("loading remote preverified snapshots: %w", err)
	}
	cfg := snapcfg.KnownCfg(chain)
	// Should we check cfg.Local? We could be resetting to the preverified.toml...?
	logger.Info(
		"Loaded preverified snapshots hashes",
		"len", len(cfg.Preverified.Items),
		"chain", chain,
	)
	removeFunc := func(path string) error {
		return os.Remove(filepath.Join(dirs.Snap, path))
	}
	if dryRun {
		removeFunc = dryRunRemove
	}
	reset := reset{prune: prune}
	logger.Info("Walking snapshots directory", "path", dirs.Snap)
	err = reset.walkSnapshots(logger, dirs.Snap, cfg.Preverified, removeFunc)
	if err != nil {
		err = fmt.Errorf("walking snapshots: %w", err)
		return
	}
	// Remove chaindata last, so that the config is available if there's an error.
	logger.Warn("Removing chaindata dir", "path", dirs.Chaindata)
	if !dryRun {
		err = os.RemoveAll(dirs.Chaindata)
	}
	if err != nil {
		err = fmt.Errorf("removing chaindata dir: %w", err)
		return
	}
	return
}

func getChainNameFromChainData(cliCtx *cli.Context, logger log.Logger) (_ g.Option[string], err error) {
	ctx := cliCtx.Context
	nodeConfig, err := NewNodeConfig(cliCtx, logger)
	if err != nil {
		err = fmt.Errorf("getting node config: %w", err)
		return
	}
	// Why does this fail if we set readonly?
	db, err := node.OpenDatabase(cliCtx.Context, nodeConfig, kv.ChainDB, "", false, logger)
	if err != nil {
		err = fmt.Errorf("opening chaindata database: %w", err)
		return
	}
	defer db.Close()
	var chainCfg *chain.Config
	err = db.View(ctx, func(tx kv.Tx) (err error) {
		genesis, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			err = fmt.Errorf("reading genesis block hash: %w", err)
			return
		}
		// Do we need genesis block hash here?
		chainCfg, err = core.ReadChainConfig(tx, genesis)
		if err != nil {
			err = fmt.Errorf("reading chain config: %w", err)
			return
		}
		return
	})
	if err != nil {
		err = fmt.Errorf("reading chaindata db: %w", err)
		return
	}
	if chainCfg == nil {
		return
	}
	return g.Some(chainCfg.ChainName), nil
}

func dryRunRemove(path string) error {
	fmt.Printf("%v\n", path)
	return nil
}

type reset struct {
	prune bool
}

func (me reset) walkSnapshots(
	logger log.Logger,
	// Could almost pass fs.FS here except metainfo.LoadFromFile expects a string filepath.
	snapDir string,
	preverified snapcfg.Preverified,
	// path is the relative path to the walk root. Called for each file that should be removed.
	// Error is passed back to the walk function.
	remove func(path string) error,
) error {
	return fs.WalkDir(
		os.DirFS(snapDir),
		".",
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				// Our job is to remove anything that shouldn't be here... so if we can't read a dir
				// we are in trouble.
				return fmt.Errorf("error walking path %v: %w", path, err)
			}
			if d.IsDir() {
				return nil
			}
			itemName, isTorrent := strings.CutSuffix(filepath.ToSlash(path), ".torrent")
			item, ok := preverified.Get(itemName)
			if !ok {
				logger.Debug("file not in preverified list", "path", path)
				if me.prune {
					return remove(path)
				} else {
					return nil
				}
			}
			if isTorrent {
				fullPath := filepath.Join(snapDir, path)
				mi, err := metainfo.LoadFromFile(fullPath)
				if err != nil {
					logger.Error("error loading metainfo file", "path", path, "err", err)
					return remove(path)
				}
				if mi.HashInfoBytes().String() == item.Hash {
					logger.Debug("torrent file matches preverified hash", "path", path)
				} else {
					logger.Info("torrent file hash does not match preverified", "path", path, "expected", item.Hash, "actual", mi.HashInfoBytes())
					return remove(path)
				}
			} else {
				// No checks required. Downloader will clobber it into shape after reset on next run.
				logger.Debug("file is in preverified", "path", path)
			}
			return nil
		},
	)
}
