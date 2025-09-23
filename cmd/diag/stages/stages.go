// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stages

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/cmd/diag/flags"
	"github.com/erigontech/erigon/cmd/diag/util"
	"github.com/erigontech/erigon/diagnostics/diaglib"
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

	syncStages, err := querySyncInfo(cliCtx)
	if err != nil {
		util.RenderError(err)
		return nil
	} else {
		var stagesRows []table.Row
		if isCurrent {
			stagesRows = getCurrentStageRow(syncStages)
		} else {
			stagesRows = getStagesRows(syncStages)
		}
		printData(cliCtx, stagesRows)
	}

	if autoupdate {
		interval := time.Duration(cliCtx.Int(flags.AutoUpdateIntervalFlag.Name)) * time.Millisecond
		var wg sync.WaitGroup
		wg.Add(1)

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
					} else {
						util.RenderError(err)
						wg.Done()
						return
					}

				case <-cliCtx.Done():
					ticker.Stop()
					wg.Done()
					return
				}
			}
		}()

		wg.Wait()
	}

	return nil
}

func querySyncInfo(cliCtx *cli.Context) ([]diaglib.SyncStage, error) {
	var data []diaglib.SyncStage
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + flags.ApiPath + "/sync-stages"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func printData(cliCtx *cli.Context, data []table.Row) {
	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		util.RenderJson(data)

	case "text":
		util.PrintTable(
			"",
			table.Row{"Stage", "SubStage", "Status", "Time Elapsed", "Progress"},
			data,
			nil,
		)
	}
}

func getStagesRows(stages []diaglib.SyncStage) []table.Row {
	return createSyncStageRows(stages, false)
}

func getCurrentStageRow(stages []diaglib.SyncStage) []table.Row {
	return createSyncStageRows(stages, true)
}

func createSyncStageRows(stages []diaglib.SyncStage, forCurrentStage bool) []table.Row {
	rows := []table.Row{}
	for _, stage := range stages {

		if forCurrentStage {
			if stage.State != diaglib.Running {
				continue
			}
		}

		stageRow := createStageRowFromStage(stage)
		rows = append(rows, stageRow)

		for _, substage := range stage.SubStages {
			subStageRow := createSubStageRowFromSubstageStage(substage)
			rows = append(rows, subStageRow)
		}

		if len(stage.SubStages) != 0 {
			rows = append(rows, table.Row{"", "", "", "", ""})
		}

		if forCurrentStage {
			break
		}

	}

	return rows
}

func createStageRowFromStage(stage diaglib.SyncStage) table.Row {
	return table.Row{
		stage.ID,
		"",
		stage.State.String(),
		stage.Stats.TimeElapsed,
		stage.Stats.Progress,
	}
}

func createSubStageRowFromSubstageStage(substage diaglib.SyncSubStage) table.Row {
	progress := substage.Stats.Progress

	if substage.State == diaglib.Completed {
		progress = "100%"
	} else {
		if substage.ID == "E3 Indexing" {
			if progress == "100%" {
				progress = "> 50%"
			} else {
				prgint := convertProgress(progress)
				progress = strconv.Itoa(prgint/2) + "%"
			}
		}
	}

	return table.Row{
		"",
		substage.ID,
		substage.State.String(),
		substage.Stats.TimeElapsed,
		progress,
	}
}

func convertProgress(progress string) int {
	progress = strings.ReplaceAll(progress, "%", "")
	progressInt, _ := strconv.Atoi(progress)
	return progressInt
}
