package cmd

import (
	"fmt"

	"github.com/erigontech/erigon/cmd/etui/internals/tui"
	"github.com/erigontech/erigon/cmd/integration/commands"
	log "github.com/erigontech/erigon/common/log/v3"
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
		logger := log.New()
		infoCh := make(chan *commands.StagesInfo)
		errCh := make(chan error, 1)
		go func() {
			defer close(infoCh)
			defer func() {
				if r := recover(); r != nil {
					println("recovered from panic: ", r)
				}
			}()
			err := commands.InfoAllStages(cmd.Context(), logger, datadirCli, infoCh)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
		tuiApp := tui.NewTUI(datadirCli)
		err := tuiApp.Run(infoCh, errCh)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	infoCmd.Flags().StringVar(&datadirCli, "datadir", "", "Directory containing versioned files")
}
