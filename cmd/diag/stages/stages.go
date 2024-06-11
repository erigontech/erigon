package stages

import (
	"sync"
	"time"

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
	Action:    printStages,
	Flags: []cli.Flag{
		&flags.DebugURLFlag,
		&flags.OutputFlag,
		&flags.AutoUpdateFlag,
		&flags.AutoUpdateIntervalFlag,
	},
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
				&flags.AutoUpdateFlag,
				&flags.AutoUpdateIntervalFlag,
			},
		},
	},
	Description: ``,
}

func printStages(cliCtx *cli.Context) error {
	err := printSyncStages(cliCtx, false)
	if err != nil {
		util.RenderError(err)
	}

	return nil
}

func printCurentStage(cliCtx *cli.Context) error {
	err := printSyncStages(cliCtx, true)
	if err != nil {
		util.RenderError(err)
	}

	return nil
}

func printSyncStages(cliCtx *cli.Context, isCurrent bool) error {
	autoupdate := cliCtx.Bool(flags.AutoUpdateFlag.Name)

	if autoupdate {
		interval := time.Duration(cliCtx.Int(flags.AutoUpdateIntervalFlag.Name)) * time.Millisecond
		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Done()

		ticker := time.NewTicker(interval)
		go func() {
			for {
				select {
				case <-ticker.C:
					syncStages, err := querySyncInfo(cliCtx)
					if err == nil {
						var stagesRows []table.Row
						if isCurrent {
							stagesRows = getCurrentStageRow(syncStages)
						} else {
							stagesRows = getStagesRows(syncStages)
						}

						printData(cliCtx, stagesRows)
					}

				case <-cliCtx.Done():
					ticker.Stop()
					return
				}
			}
		}()

		wg.Wait()
	} else {
		syncStages, err := querySyncInfo(cliCtx)
		if err != nil {
			util.RenderError(err)
			return nil
		}

		var stagesRows []table.Row
		if isCurrent {
			stagesRows = getCurrentStageRow(syncStages)
		} else {
			stagesRows = getStagesRows(syncStages)
		}
		printData(cliCtx, stagesRows)
	}

	return nil
}

func querySyncInfo(cliCtx *cli.Context) ([]diagnostics.SyncStage, error) {
	var data diagnostics.SyncStatistics
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + flags.ApiPath + "/snapshot-sync"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return nil, err
	}

	return data.SyncStages, nil
}

func printData(cliCtx *cli.Context, data []table.Row) {
	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		util.RenderJson(data)

	case "text":
		util.RenderTableWithHeader(
			"",
			table.Row{"Stage", "SubStage", "Status"},
			data,
		)
	}
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

func getCurrentStageRow(stages []diagnostics.SyncStage) []table.Row {
	rows := []table.Row{}
	for _, stage := range stages {
		if stage.State == diagnostics.Running {
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

			break
		}
	}

	return rows
}
