package stages

import (
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/cmd/diag/flags"
	"github.com/ledgerwatch/erigon/cmd/diag/util"
	"github.com/urfave/cli/v2"
)

var Command = cli.Command{
	Name:      "stages",
	Aliases:   []string{"st"},
	ArgsUsage: "",
	Subcommands: []*cli.Command{
		{
			Name:      "current",
			Aliases:   []string{"c"},
			Action:    printCurentStage,
			Usage:     "print current stage",
			ArgsUsage: "",
			Flags: []cli.Flag{
				&flags.DebugURLFlag,
				&flags.OutputFlag,
			},
		},
	},
	Description: ``,
}

func printCurentStage(cliCtx *cli.Context) error {
	var data diagnostics.SyncStatistics
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + "/debug/snapshot-sync"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return err
	}

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		bytes, err := json.Marshal(data.SyncStages.StagesList)
		if err != nil {
			return err
		}

		fmt.Println(string(bytes))

	case "text":
		fmt.Println("-------------------Stages-------------------")

		for idx, stage := range data.SyncStages.StagesList {
			if idx == int(data.SyncStages.CurrentStage) {
				fmt.Println("[" + stage + "]" + " - Running")
			} else if idx < int(data.SyncStages.CurrentStage) {
				fmt.Println("[" + stage + "]" + " - Completed")
			} else {
				fmt.Println("[" + stage + "]" + " - Queued")
			}
		}
	}

	return nil
}
