package app

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/execution/chain"
)

var (
	removeLocalFlag = cli.BoolFlag{
		Name:     "local",
		Usage:    "Remove files not described in snapshot set (probably generated locally).",
		Value:    true,
		Aliases:  []string{"l"},
		Category: "Reset",
	}
	dryRunFlag = cli.BoolFlag{
		Name:     "dry-run",
		Usage:    "Print files that would be removed, but do not remove them.",
		Value:    false,
		Aliases:  []string{"n"},
		Category: "Reset",
	}
	preverifiedFlag = cli.StringFlag{
		Name:     "preverified",
		Category: "Reset",
		Usage:    "preverified to use (remote, local, embedded)",
		Value:    "remote",
	}
)

// Checks if a value was explicitly set in the given CLI command context or any of its parents. In
// urfave/cli@v2, you must check the lineage to see if a flag was set in any context. It may be
// different in v3.
func isSetLineage(cliCtx *cli.Context, flagName string) bool {
	for _, ctx := range cliCtx.Lineage() {
		if ctx.IsSet(flagName) {
			return true
		}
	}
	return false
}

func resetCliAction(cliCtx *cli.Context) (err error) {
	// This is set up in snapshots cli.Command.Before.
	logger := log.Root()
	removeLocal := removeLocalFlag.Get(cliCtx)
	dryRun := dryRunFlag.Get(cliCtx)
	dataDirPath := cliCtx.String(utils.DataDirFlag.Name)
	logger.Info("resetting datadir", "path", dataDirPath)

	dirs := datadir.Open(dataDirPath)

	configChainName, chainNameErr := getChainNameFromChainData(cliCtx, logger, dirs.Chaindata)

	chainName := utils.ChainFlag.Get(cliCtx)
	// Check the lineage, we don't want to use the mainnet default, but due to how urfave/cli@v2
	// works we shouldn't randomly re-add the chain flag in the current command context.
	if isSetLineage(cliCtx, utils.ChainFlag.Name) {
		if configChainName.Ok && configChainName.Value != chainName {
			// Pedantic but interesting.
			logger.Warn("chain name flag and chain config do not match", "flag", chainName, "config", configChainName.Value)
		}
		logger.Info("using chain name from flag", "chain", chainName)
	} else {
		if chainNameErr != nil {
			logger.Warn("error getting chain name from chaindata", "err", chainNameErr)
		}
		if !configChainName.Ok {
			return errors.New(
				"chain flag not set and chain name not found in chaindata. datadir is ready for sync, invalid, or requires chain flag to reset")
		}
		chainName = configChainName.Unwrap()
		logger.Info("read chain name from config", "chain", chainName)
	}

	unlock, err := dirs.TryFlock()
	if err != nil {
		return fmt.Errorf("failed to lock data dir %v: %w", dirs.DataDir, err)
	}
	defer unlock()

	switch value := preverifiedFlag.Get(cliCtx); value {
	case "local":
		os.Setenv(snapcfg.RemotePreverifiedEnvKey, dirs.PreverifiedPath())
		fallthrough
	case "remote":
		err = snapcfg.LoadRemotePreverified(cliCtx.Context)
		if err != nil {
			// TODO: Check if we should continue? What if we ask for a git revision and
			// can't get it? What about a branch? Can we reset to the embedded snapshot hashes?
			return fmt.Errorf("loading remote preverified snapshots: %w", err)
		}
	case "embedded":
		// Should already be loaded.
	default:
		err = fmt.Errorf("invalid preverified flag value %q", value)
		return
	}

	cfg, known := snapcfg.KnownCfg(chainName)
	if !known {
		// Wtf does this even mean?
		return fmt.Errorf("config for chain %v is not known", chainName)
	}
	// Should we check cfg.Local? We could be resetting to the preverifiedFlag.toml...?
	logger.Info(
		"Loaded preverified snapshots hashes",
		"len", len(cfg.Preverified.Items),
		"chain", chainName,
	)

	if dryRun {
		log.Warn("Resetting datadir in dry run mode. Files that would be removed will be printed to stdout.")
	}

	reset := reset{
		fs:                   os.DirFS(dirs.DataDir),
		removeUnknown:        removeLocal,
		logger:               logger,
		preverifiedSnapshots: cfg.Preverified.Items,
		removeFunc: func(path string) error {
			osName := filepath.Join(dirs.DataDir, path)
			if dryRun {
				println(osName)
				return nil
			}
			logger.Debug("Removing datadir file", "name", osName)
			return os.Remove(osName)
		},
		realPath: func(path string) string {
			local, err := filepath.Localize(path)
			panicif.Err(err)
			return filepath.Join(dirs.DataDir, local)
		},
	}
	return reset.run()
}

func (reset *reset) run() (err error) {
	logger := reset.logger
	logger.Info("Resetting snapshots directory", "path", reset.pathForLog(datadir.SnapDir))
	err = reset.walkSnapshots()
	if err != nil {
		// I think this is okay to do, I'm not sure if you can get fs.ErrNotExist from a nested
		// location in WalkDir.
		if errors.Is(err, fs.ErrNotExist) {
			logger.Warn("snapshots directory does not exist")
			err = nil
		}
		if err != nil {
			err = fmt.Errorf("walking snapshots: %w", err)
			return
		}
	}
	logger.Info("Files NOT removed from snapshots directory",
		"torrents", reset.stats.retained.torrentFiles,
		"data", reset.stats.retained.dataFiles)
	logger.Info("Files removed from snapshots directory",
		"torrents", reset.stats.removed.torrentFiles,
		"data", reset.stats.removed.dataFiles)
	// Remove chaindata last, so that the config is available if there's an error.
	if reset.removeLocal {
		for _, extraDir := range []string{
			dbcfg.HeimdallDB,
			dbcfg.PolygonBridgeDB,
		} {
			// Probably shouldn't log these unless they existed, it would confuse the user for
			// unrelated chains.
			err = reset.removeAll(extraDir)
			if err != nil {
				return fmt.Errorf("removing extra dir %q: %w", extraDir, err)
			}
		}
		logger.Info("Removing chaindata dir", "path", reset.pathForLog(dbcfg.ChainDB))
		err = reset.removeAll(dbcfg.ChainDB)
		if err != nil {
			err = fmt.Errorf("removing chaindata dir: %w", err)
			return
		}
	}
	err = reset.removeFunc(datadir.PreverifiedFileName)
	if err == nil {
		logger.Info("Removed snapshots lock file", "path", datadir.PreverifiedFileName)
	} else {
		if !errors.Is(err, fs.ErrNotExist) {
			err = fmt.Errorf("removing snapshot lock file: %w", err)
			return
		}
	}
	logger.Info("Reset complete. Start Erigon as usual, missing files will be downloaded.")
	return nil
}

// Maybe we should return a "preserve" here?
func (reset *reset) removeAllInner(name string, fi fs.FileInfo, links int) error {
	reset.logger.Debug("reset.removeAllInner", "name", name)
	if links > reset.linkLimit {
		return fmt.Errorf("symlink depth exceeded %v", reset.linkLimit)
	}
	switch modeType := fi.Mode().Type(); modeType {
	case fs.ModeDir:
		entries, err := fs.ReadDir(reset.fs, name)
		if err != nil {
			return err
		}
		for _, de := range entries {
			info, err := de.Info()
			if err != nil {
				return err
			}
			err = reset.removeAllInner(path.Join(name, de.Name()), info, links)
			if err != nil {
				return err
			}
		}
		return nil
	case 0:
		return reset.removeFunc(name)
	case fs.ModeSymlink:
		link, err := fs.ReadLink(reset.fs, name)
		if err != nil {
			return err
		}
		target := path.Join(path.Dir(name), link)
		targetInfo, err := fs.Lstat(reset.fs, target)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				reset.logger.Warn("dangling symlink", "name", name, "target", target, "err", err)
				err = nil
			}
			return err
		}
		err = reset.removeAllInner(target, targetInfo, links+1)
		if err != nil {
			return err
		}
		if targetInfo.Mode().IsRegular() {
			// Or check if it exists again (and is not a dir?)
			return reset.removeFunc(name)
		}
		return nil
	default:
		reset.logger.Warn("ignoring unhandled file mode type", "mode type", modeType.String())
		return nil
	}
}

func (reset *reset) removeAll(name string) error {
	reset.logger.Debug("reset.removeAll", "name", name)
	fi, err := fs.Lstat(reset.fs, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = nil
		}
		return err
	}
	err = reset.removeAllInner(name, fi, 0)
	if err != nil {
		return err
	}
	// Not a regular file or directory
	if fi.Mode().Type()&(^fs.ModeDir) == 0 {
		return reset.removeFunc(name)
	}
	return nil
}

// Probably want to render full/real path rather than rooted inside fs.
func (reset *reset) pathForLog(path string) string {
	return path
}

func getChainNameFromChainData(cliCtx *cli.Context, logger log.Logger, chainDataDir string) (_ g.Option[string], err error) {
	_, err = os.Stat(chainDataDir)
	if err != nil {
		return
	}
	ctx := cliCtx.Context
	var db kv.RoDB
	db, err = mdbx.New(dbcfg.ChainDB, logger).Path(chainDataDir).Accede(true).Readonly(true).Open(ctx)
	if err != nil {
		err = fmt.Errorf("opening chaindata database: %w", err)
		return
	}
	defer db.Close()
	var chainCfg *chain.Config
	// See tool.ChainConfigFromDB for another example, but that panics on errors.
	err = db.View(ctx, func(tx kv.Tx) (err error) {
		genesis, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			err = fmt.Errorf("reading genesis block hash: %w", err)
			return
		}
		// Do we need genesis block hash here?
		chainCfg, err = rawdb.ReadChainConfig(tx, genesis)
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

type resetStats struct {
	torrentFiles int
	dataFiles    int
	unknownFiles int
}

type reset struct {
	logger log.Logger
	// path is the relative path to the walk root. Called for each file that should be removed.
	// Error is passed back to the walk function.
	removeFunc           func(path string) error
	realPath             func(path string) string
	fs                   fs.FS
	preverifiedSnapshots snapcfg.PreverifiedItems
	removeUnknown        bool
	removeLocal          bool
	linkLimit            int

	stats struct {
		removed  resetStats
		retained resetStats
	}
}

type resetItemInfo struct {
	path          string
	hash          g.Option[string]
	isTorrent     bool
	inPreverified bool
}

// Walks the given snapshots directory, removing files that are not in the preverifiedFlag set.
func (me *reset) walkSnapshots() (err error) {
	fsys, err := fs.Sub(me.fs, datadir.SnapDir)
	panicif.Err(err)
	return fs.WalkDir(
		fsys,
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
			if path == datadir.PreverifiedFileName {
				return nil
			}
			itemName, _ := strings.CutSuffix(path, ".part")
			itemName, isTorrent := strings.CutSuffix(itemName, ".torrent")
			item, ok := me.preverifiedSnapshots.Get(itemName)
			doRemove := me.decideRemove(resetItemInfo{
				path:          path,
				hash:          func() g.Option[string] { return g.OptionFromTuple(item.Hash, ok) }(),
				isTorrent:     isTorrent,
				inPreverified: ok,
			})
			stats := &me.stats.retained
			if doRemove {
				stats = &me.stats.removed
				err = me.removeFunc(path)
				if err != nil {
					return fmt.Errorf("removing file %v: %w", path, err)
				}
			}
			if isTorrent {
				stats.torrentFiles++
			} else {
				stats.dataFiles++
			}
			return nil
		},
	)
}

// Decides whether to remove a file, and logs the reasoning.
func (me *reset) decideRemove(file resetItemInfo) bool {
	logger := me.logger
	path := file.path
	if !file.inPreverified {
		if !me.removeUnknown {
			logger.Debug("skipping unknown file", "name", path)
		}
		return me.removeUnknown
	}
	// TODO: missing or incorrect torrent delete data file?
	if file.isTorrent {
		mi, err := me.loadMetainfoFromFile(file.path)
		if err != nil {
			logger.Error("error loading metainfo file", "path", path, "err", err)
			return true
		}
		expectedHash := file.hash.Unwrap()
		if mi.HashInfoBytes().String() == expectedHash {
			logger.Debug("torrent file matches preverified hash", "name", path)
			return false
		} else {
			logger.Debug("removing metainfo file with incorrect infohash",
				"name", path,
				"expected", expectedHash,
				"actual", mi.HashInfoBytes())
			return true
		}
	} else {
		// No checks required. Downloader will clobber it into shape after reset on next run.
		logger.Debug("skipping expected snapshot", "name", path)
		return false
	}
}

func (me *reset) loadMetainfoFromFile(path string) (mi *metainfo.MetaInfo, err error) {
	f, err := me.fs.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	var buf bufio.Reader
	buf.Reset(f)
	return metainfo.Load(&buf)
}
