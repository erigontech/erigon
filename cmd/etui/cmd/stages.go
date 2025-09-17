package cmd

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/etui/internals/tui"
	"github.com/erigontech/erigon/cmd/integration/commands"
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
		infoCh := make(chan *commands.StagesInfo)
		go func() {
			err := commands.InfoAllStages(cmd.Context(), logger, datadirCli, infoCh)
			if err != nil {
				panic(err)
			}
		}()
		err := tui.MakeTUI(infoCh)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	infoCmd.Flags().StringVar(&datadirCli, "datadir", "", "Directory containing versioned files")
}
