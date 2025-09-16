package cmd

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/integration/commands"
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
			AddItem(tview.NewBox().SetTitle("Erigon TUI").SetBorder(true), 1, 1, false).
			AddItem(tview.NewTextView().SetText(fmt.Sprintf("%+v", info)), 0, 5, false).
			AddItem(tview.NewBox().SetBorder(true).SetTitle("Bottom (5 rows)"), 5, 1, false)
		//view := tview.NewModal().
		//	SetText(.
		//	AddButtons([]string{"Quit", "Cancel"}).
		//	SetDoneFunc(func(buttonIndex int, buttonLabel string) {
		//		if buttonLabel == "Quit" || buttonLabel == "Cancel" {
		//			app.Stop()
		//		}
		//	})
		if err := app.SetRoot(flex, true).EnableMouse(true).Run(); err != nil {
			panic(err)
		}
		return nil
	},
}

func init() {
	infoCmd.Flags().StringVar(&datadirCli, "datadir", "", "Directory containing versioned files")
}
