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
		util.RenderError(err)
		return nil
	}

	stagesRows := getStagesRows(data.SyncStages)

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		util.RenderJson(stagesRows)

	case "text":
		util.RenderTableWithHeader(
			"Sync stages:",
			table.Row{"Stage", "SubStage", "Status"},
			stagesRows,
		)
	}

	return nil
}

func getStagesRows(stages []diagnostics.SyncStage) []table.Row {
	rows := []table.Row{}
	for _, stage := range stages {
		stageRow := table.Row{
			stage.ID,
			"",
			stage.State.String(),
		}
		rows = append(rows, stageRow)

		for _, substage := range stage.SubStages {
			subStageRow := table.Row{
				"",
				substage.ID,
				substage.State.String(),
			}
			rows = append(rows, subStageRow)
		}

		if len(stage.SubStages) != 0 {
			rows = append(rows, table.Row{"", "", ""})
		}
	}

	return rows
}
