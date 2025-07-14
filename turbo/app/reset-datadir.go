package app

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/urfave/cli/v2"
)

func resetCliAction(cliCtx *cli.Context) (err error) {
	// Set logging verbosity. Oof that function signature.
	logger, _, _, _, err := debug.Setup(cliCtx, true)
	if err != nil {
		err = fmt.Errorf("setting up logging: %w", err)
		return
	}
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	unlock, err := dirs.TryFlock()
	if err != nil {
		return fmt.Errorf("failed to lock data dir %v: %w", dirs.DataDir, err)
	}
	defer unlock()
	log.Warn("removing chaindata dir", "path", dirs.Chaindata)
	err = snapcfg.LoadRemotePreverified(cliCtx.Context)
	if err != nil {
		// TODO: Check if we should continue? What if we ask for a git revision and
		// can't get it? What about a branch? Can we reset to the embedded snapshot hashes?
		return fmt.Errorf("loading remote preverified snapshots: %w", err)
	}
	chain := utils.ChainFlag.Get(cliCtx)
	cfg := snapcfg.KnownCfg(chain)
	// Should we check cfg.Local? We could be resetting to the preverified.toml...?
	log.Info("Loaded preverified snapshots", "len", len(cfg.Preverified.Items))
	log.Info("Walking snapshots directory", "path", dirs.Snap)
	// TODO: Wire in dryRunRemove.
	err = walkSnapshots(logger, dirs.Snap, cfg.Preverified, func(path string) error {
		return os.Remove(filepath.Join(dirs.Snap, path))
	})
	if err != nil {
		err = fmt.Errorf("walking snapshots: %w", err)
		return
	}
	return nil
}

func dryRunRemove(path string) error {
	fmt.Printf("%v\n", path)
	return nil
}

func walkSnapshots(
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
				// Our job is to remove anything that shouldn't be here... so if we can't read a dir we
				// are in trouble.
				return fmt.Errorf("error walking path %q: %w", path, err)
			}
			if d.IsDir() {
				return nil
			}
			itemName, isTorrent := strings.CutSuffix(filepath.ToSlash(path), ".torrent")
			item, ok := preverified.Get(itemName)
			if !ok {
				logger.Debug("file not in preverified list", "path", path)
				return remove(path)
			}
			if isTorrent {
				fullPath := filepath.Join(snapDir, path)
				mi, err := metainfo.LoadFromFile(fullPath)
				if err != nil {
					logger.Error("error loading metainfo file", "path", path, "err", err)
					return remove(path)
				}
				if mi.HashInfoBytes().String() == item.Hash {
					log.Debug("torrent file matches preverified hash", "path", path)
				} else {
					log.Info("torrent file hash does not match preverified", "path", path, "expected", item.Hash, "actual", mi.HashInfoBytes())
					return remove(path)
				}
			} else {
				// No checks required. Downloader will clobber it into shape after reset on next run.
				log.Debug("file is in preverified", "path", path)
			}
			return nil
		},
	)
}
