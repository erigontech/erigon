package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/etui/app"
	"github.com/erigontech/erigon/cmd/etui/config"
)

var (
	datadirCli        string
	analyticsCli      bool
	diagnosticsURLCli string
	chainCli          string
)

var rootCmd = &cobra.Command{
	Use:     "etui",
	Short:   "Erigon TUI — monitor and manage Erigon nodes",
	Version: config.Version,
	Long: `Erigon TUI provides a terminal dashboard for monitoring and managing Erigon nodes.

Run with no flags to use saved configuration or launch the setup wizard.
Use --datadir to specify a data directory directly.
Use --analytics to force monitoring-only mode (no node lifecycle control).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Resolve datadir: CLI flag → global config → default.
		datadir := datadirCli
		if datadir == "" {
			datadir = config.LoadGlobal()
		}
		if datadir == "" {
			datadir = config.DefaultDatadir()
		}

		opts := app.Options{
			ForceAnalytics: analyticsCli,
			DiagnosticsURL: diagnosticsURLCli,
			Chain:          chainCli,
		}
		return launchTUI(datadir, opts, cmd)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVar(&datadirCli, "datadir", "", "Path to Erigon data directory")
	rootCmd.Flags().BoolVar(&analyticsCli, "analytics", false, "Force analytics-only mode (no node management)")
	rootCmd.Flags().StringVar(&diagnosticsURLCli, "diagnostics-url", "", "Override diagnostics HTTP endpoint (e.g. http://localhost:6060)")
	rootCmd.Flags().StringVar(&chainCli, "chain", "", "Override network chain (e.g. mainnet, sepolia, hoodi)")
	rootCmd.AddCommand(infoCmd)
}
