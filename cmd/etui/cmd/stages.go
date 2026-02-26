package cmd

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/etui/app"
	"github.com/erigontech/erigon/cmd/integration/commands"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state"
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
			backoff := 5 * time.Second
			for {
				err := commands.InfoAllStages(cmd.Context(), logger, datadirCli, infoCh)
				if err == nil {
					return
				}
				// Salt files missing means OtterSync is still downloading — retry
				if !errors.Is(err, state.ErrCannotStartWithoutSaltFiles) {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				// Don't report to errCh — the downloader widget already shows progress.
				select {
				case <-cmd.Context().Done():
					return
				case <-time.After(backoff):
				}
			}
		}()
		tuiApp := app.New(datadirCli)
		return tuiApp.Run(cmd.Context(), infoCh, errCh)
	},
}

func init() {
	infoCmd.Flags().StringVar(&datadirCli, "datadir", "", "Directory containing versioned files")
}
