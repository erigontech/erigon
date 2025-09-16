package cmd

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/integration/commands"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
)

var (
	datadirCli string
)

var infoCmd = &cobra.Command{
	Use:     "info",
	Short:   "Stages info of Erigon",
	Example: `To start eTUI in the datadir: go run ./cmd/etui info --datadir /path/to/your/datadir`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if datadirCli == "" {
			return fmt.Errorf("--datadir flag is required")
		}
		logger := log.New(context.Background())
		info, err := commands.InfoAllStages(cmd.Context(), logger, datadirCli)
		if err != nil {
			return err
		}
		app := tview.NewApplication()
		flex := tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(tview.NewTextView().SetText("Erigon TUI").SetTextAlign(tview.AlignCenter), 1, 1, false).
			AddItem(tview.NewTextView().SetText(fmt.Sprintf("%+v", info)), 0, 5, false).
			AddItem(tview.NewBox().SetBorder(true).SetTitle("Bottom (5 rows)"), 5, 1, false)

		if err := app.SetRoot(flex, true).EnableMouse(true).SetInputCapture(
			func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyCtrlC || event.Rune() == 'q' {
					app.Stop()
				}
				return event
			}).Run(); err != nil {
			panic(err)
		}
		return nil
	},
}

func init() {
	infoCmd.Flags().StringVar(&datadirCli, "datadir", "", "Directory containing versioned files")
}
