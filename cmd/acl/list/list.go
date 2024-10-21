package list

import (
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/urfave/cli/v2"
)

var Command = cli.Command{
	Action: run,
	Name:   "list",
	Usage:  "List the content at the ACL",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
	},
}

func run(cliCtx *cli.Context) error {
	dataDir := cliCtx.String(utils.DataDirFlag.Name)
	log.Info("Listing ", "dataDir:", dataDir)

	aclDB, err := txpool.OpenACLDB(cliCtx.Context, dataDir)
	if err != nil {
		log.Error("Failed to open ACL database", "err", err)
		return err
	}

	content, _ := txpool.ListContentAtACL(cliCtx.Context, aclDB)
	log.Info(content)
	pts, _ := txpool.LastPolicyTransactions(cliCtx.Context, aclDB)
	if len(pts) == 0 {
		log.Info("No policy transactions found")
		return nil
	}
	for i, pt := range pts {
		log.Info("Policy transaction - ", "index:", i, "pt:", pt.ToString())
	}

	return nil
}
