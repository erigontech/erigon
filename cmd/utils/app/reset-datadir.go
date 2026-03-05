package app

import (
	"errors"
	"fmt"
	"os"

	g "github.com/anacrolix/generics"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir/reset"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapcfg"
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

	err = snapcfg.LoadPreverified(cliCtx.Context, PreverifiedFlag.Get(cliCtx), &dirs)
	if err != nil {
		return
	}

	cfg, known := snapcfg.KnownCfg(chainName)
	if !known {
		// Wtf does this even imply?
		return fmt.Errorf("config for chain %v is not known", chainName)
	}
	// Should we check cfg.Local? We could be resetting to the preverified.toml...?
	logger.Info(
		"Loaded preverified snapshots hashes",
		"len", len(cfg.Preverified.Items),
		"chain", chainName,
	)

	if dryRun {
		log.Warn("Resetting datadir in dry run mode. Files that would be removed will be printed to stdout.")
	}

	// Here we intended to have a list of os.Root to restrict deletions. Instead, for now you should
	// do a dry run, and make sure to use good permissioning.
	//datadirOsRoot, err := os.OpenRoot(dirs.DataDir)
	//if err != nil {
	//	return fmt.Errorf("opening datadir: %w", err)
	//}

	r := reset.Reset{
		Dirs:                 &dirs,
		RemoveUnknown:        removeLocal,
		Logger:               logger,
		PreverifiedSnapshots: cfg.Preverified.Items,
		RemoveFunc: func(osName reset.OsFilePath) error {
			if dryRun {
				println(osName)
				return nil
			}
			logger.Debug("Removing datadir file", "name", osName)
			//return datadirOsRoot.Remove(string(osName))
			return dir.RemoveFile(string(osName))
		},
	}
	err = r.Run()
	if err != nil {
		return
	}
	if !dryRun {
		logger.Info("Reset complete. Start Erigon as usual, missing files will be downloaded.")
	}
	return
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
