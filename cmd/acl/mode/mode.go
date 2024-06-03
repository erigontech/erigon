package mode

import (
	"errors"

	"github.com/ledgerwatch/erigon/cmd/snapshots/sync"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/urfave/cli/v2"
)

var (
	mode string // Mode of the ACL
)

var Command = cli.Command{
	Action: run,
	Name:   "mode",
	Usage:  "Set the mode of the ACL",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{
			Name:        "mode",
			Usage:       "Mode of the ACL (whitelist, blacklist or disabled)",
			Destination: &mode,
		},
	},
}

func run(cliCtx *cli.Context) error {
	logger := sync.Logger(cliCtx.Context)

	if !cliCtx.IsSet(utils.DataDirFlag.Name) {
		return errors.New("data directory is not set")
	}

	if mode == "" {
		return errors.New("mode is not set")
	}

	dataDir := cliCtx.String(utils.DataDirFlag.Name)

	logger.Info("Setting mode", "mode", mode, "dataDir", dataDir)

	aclDB, err := txpool.OpenACLDB(cliCtx.Context, dataDir)
	if err != nil {
		logger.Error("Failed to open ACL database", "err", err)
		return err
	}

	if err := txpool.SetMode(cliCtx.Context, aclDB, mode); err != nil {
		logger.Error("Failed to set acl mode", "err", err)
		return err
	}

	logger.Info("ACL Mode set", "mode", mode)

	return nil
}
