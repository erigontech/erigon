package stages

import (
	"github.com/jedib0t/go-pretty/v6/table"
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
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + flags.ApiPath + "/snapshot-sync"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return err
	}

	stagesRows := getStagesRows(data.SyncStages)

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		util.RenderJson(stagesRows)

	case "text":
		util.RenderTableWithHeader(
			"Sync stages:",
			table.Row{"Stage", "Status"},
			stagesRows,
		)
	}

	return nil
}

func getStagesRows(syncStages diagnostics.SyncStages) []table.Row {
	rows := []table.Row{}
	for idx, stage := range syncStages.StagesList {
		row := table.Row{
			stage,
			"Queued",
		}
		if idx == int(syncStages.CurrentStage) {
			row[1] = "Running"
		} else if idx < int(syncStages.CurrentStage) {
			row[1] = "Completed"
		}

		rows = append(rows, row)
	}

	return rows
}
