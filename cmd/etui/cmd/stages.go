package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/etui/app"
	"github.com/erigontech/erigon/cmd/etui/config"
	"github.com/erigontech/erigon/cmd/etui/datasource"
	log "github.com/erigontech/erigon/common/log/v3"
)

var infoCmd = &cobra.Command{
	Use:        "info",
	Short:      "Stages info of Erigon (deprecated: use 'etui --datadir <path>' instead)",
	Deprecated: "use 'etui --datadir <path>' instead",
	Example:    `etui --datadir /path/to/your/datadir`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if datadirCli == "" {
			return fmt.Errorf("--datadir flag is required")
		}
		opts := app.Options{
			ForceAnalytics: analyticsCli,
			DiagnosticsURL: diagnosticsURLCli,
			Chain:          chainCli,
		}
		return launchTUI(datadirCli, opts, cmd)
	},
}

func init() {
	infoCmd.Flags().StringVar(&datadirCli, "datadir", "", "Directory containing versioned files")
}

// launchTUI is the shared logic for starting the TUI application.
// Used by both the root command and the deprecated info subcommand.
func launchTUI(datadir string, opts app.Options, cmd *cobra.Command) error {
	if _, err := os.Stat(config.ConfigPath(datadir)); os.IsNotExist(err) {
		if nodeAlreadyRunning(datadir, opts.Chain) {
			opts.InitialPage = app.InitialPageNodeInfo
		} else {
			selectedDatadir, setupErr := app.RunSetup(cmd.Context(), datadir, opts)
			switch {
			case setupErr == nil:
				datadir = selectedDatadir
				opts.InitialPage = app.InitialPageNodeInfo
			case errors.Is(setupErr, app.ErrSetupCancelled), errors.Is(setupErr, cmd.Context().Err()):
				return nil
			default:
				return setupErr
			}
		}
	}

	logger := log.New()
	infoCh := make(chan *datasource.StagesInfo)
	errCh := make(chan error, 1)
	go func() {
		defer close(infoCh)
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("panic in InfoAllStages: %v\n%s", r, debug.Stack())
				writeCrashLog(datadir, msg)
				select {
				case errCh <- fmt.Errorf("panic: %v (see etui-crash.log)", r):
				default:
				}
			}
		}()
		const retryInterval = 5 * time.Second
		for {
			if !chaindataReady(datadir) {
				select {
				case <-cmd.Context().Done():
					return
				case <-time.After(retryInterval):
				}
				continue
			}

			err := datasource.InfoAllStages(cmd.Context(), logger, datadir, infoCh)
			if err == nil {
				return // context cancelled or clean exit
			}
			// DB open errors are transient: DB locked, salt files
			// missing, chaindata not yet created, etc. Always retry.
			logger.Warn("InfoAllStages failed, retrying", "err", err)
			select {
			case errCh <- fmt.Errorf("InfoAllStages: %v (retrying in %v)", err, retryInterval):
			default:
			}
			select {
			case <-cmd.Context().Done():
				return
			case <-time.After(retryInterval):
			}
		}
	}()
	tuiApp := app.New(datadir, opts)
	return tuiApp.Run(cmd.Context(), infoCh, errCh)
}

func chaindataReady(datadir string) bool {
	_, err := os.Stat(filepath.Join(datadir, "chaindata", "mdbx.dat"))
	return err == nil
}

func nodeAlreadyRunning(datadir, chain string) bool {
	nodeMgr := datasource.NewNodeManager(datadir, chain)
	nodeMgr.Detect()
	status := nodeMgr.Status()
	return status.State == datasource.NodeRunning ||
		status.State == datasource.NodeStarting ||
		status.State == datasource.NodeStopping
}

// writeCrashLog appends a panic message with stack trace to etui-crash.log in the datadir.
func writeCrashLog(datadir, msg string) {
	logPath := filepath.Join(datadir, "etui-crash.log")
	entry := fmt.Sprintf("[%s] %s\n\n", time.Now().Format(time.RFC3339), msg)
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	f.WriteString(entry)
}
