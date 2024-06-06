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

	/*testData := []diagnostics.SyncStage{
		{
			ID:    "Snapshots",
			State: diagnostics.Running,
			SubStages: []diagnostics.SyncSubStage{
				{ID: "Download header-chain", State: diagnostics.Running},
				{ID: "Download snapshots", State: diagnostics.Queued},
				{ID: "Indexing", State: diagnostics.Queued},
				{ID: "Fill DB", State: diagnostics.Queued},
			},
		},
		{ID: "BlockHashes", State: diagnostics.Queued},
		{ID: "Senders", State: diagnostics.Queued},
		{ID: "Execution", State: diagnostics.Queued},
		{ID: "HashState", State: diagnostics.Queued},
		{ID: "IntermediateHashes", State: diagnostics.Queued},
		{ID: "CallTraces", State: diagnostics.Queued},
		{ID: "AccountHistoryIndex", State: diagnostics.Queued},
		{ID: "StorageHistoryIndex", State: diagnostics.Queued},
		{ID: "LogIndex", State: diagnostics.Queued},
		{ID: "TxLookup", State: diagnostics.Queued},
		{ID: "Finish", State: diagnostics.Queued},
	}*/

	stagesRows := getStagesRows(data.SyncStages)
	//stagesRows := getStagesRows(testData)

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
