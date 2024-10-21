package mode

import (
	"errors"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/urfave/cli/v2"
)

var (
	mode           string // Mode of the ACL
	logCountOutput string // Output for log count
)

var Command = cli.Command{
	Action: run,
	Name:   "mode",
	Usage:  "Set the mode of the ACL",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{
			Name:        "mode",
			Usage:       "Mode of the ACL (allowlist, blocklist or disabled)",
			Destination: &mode,
		},
		&cli.StringFlag{
			Name:        "log_count",
			Usage:       "Number of transactions at startup to log",
			Destination: &logCountOutput,
		},
	},
}

func run(cliCtx *cli.Context) error {
	if !cliCtx.IsSet(utils.DataDirFlag.Name) {
		return errors.New("data directory is not set")
	}

	if mode == "" {
		return errors.New("mode is not set")
	}

	dataDir := cliCtx.String(utils.DataDirFlag.Name)

	log.Info("Setting mode ", "mode - ", mode, "dataDir - ", dataDir, "log_count_output - ", logCountOutput)

	aclDB, err := txpool.OpenACLDB(cliCtx.Context, dataDir)
	if err != nil {
		log.Error("Failed to open ACL database", "err", err)
		return err
	}

	if err := txpool.SetMode(cliCtx.Context, aclDB, mode); err != nil {
		log.Error("Failed to set acl mode", "err", err)
		return err
	}

	if cliCtx.IsSet("log_count") {
		// Assuming you need to store log_count_output in the config table
		if err := txpool.SetLogCount(cliCtx.Context, aclDB, logCountOutput); err != nil {
			log.Error("Failed to set log_count_output", "err", err)
			return err
		}
	}

	return nil
}
