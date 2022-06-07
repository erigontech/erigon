package commands

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

var (
	consensusAddr string // Address of the consensus engine <host>:<port>
	datadirCli    string // Path to the working dir
	config        string // `file:<path>`` to specify config file in file system, `embed:<path>`` to use embedded file, `test` to register test interface and receive config from test driver
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
}

var rootCmd = &cobra.Command{
	Use:   "consensus",
	Short: "consensus is Proof Of Concept for separare consensus engine",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
}

func Execute() {
	ctx, _ := common.RootContext()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withDataDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadirCli, "datadir", paths.DefaultDataDir(), "directory where databases and temporary files are kept")
	must(cmd.MarkFlagDirname("datadir"))
}

func withApiAddr(cmd *cobra.Command) {
	cmd.Flags().StringVar(&consensusAddr, "consensus.api.addr", "localhost:9093", "address to listen to for consensus engine api <host>:<port>")
}

func withConfig(cmd *cobra.Command) {
	cmd.Flags().StringVar(&config, "config", "", "`file:<path>` to specify config file in file system, `embed:<path>` to use embedded file, `test` to register test interface and receive config from test driver")
}

func openDB(path string, logger log.Logger) kv.RwDB {
	return mdbx.NewMDBX(logger).Path(path).MustOpen()
}
