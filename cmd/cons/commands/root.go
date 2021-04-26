package commands

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/paths"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/spf13/cobra"
)

var (
	consensusAddr string // Address of the consensus engine <host>:<port>
	datadir       string // Path to the working dir
	database      string // Type of database (lmdb or mdbx)
	chain         string // Pre-set chain name
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
	ctx, _ := utils.RootContext()
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

func withDatadir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadir, "datadir", paths.DefaultDataDir(), "directory where databases and temporary files are kept")
	must(cmd.MarkFlagDirname("datadir"))
	cmd.Flags().StringVar(&database, "database", "", "lmdb|mdbx")
}

func withApiAddr(cmd *cobra.Command) {
	cmd.Flags().StringVar(&consensusAddr, "consensus.api.addr", "localhost:9093", "address to listen to for consensus engine api <host>:<port>")
}

func withChain(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chain, "chain", "", "pre-set chain name")
}

func openDatabase(path string) *ethdb.ObjectDatabase {
	db := ethdb.NewObjectDatabase(openKV(path, false))
	return db
}

func openKV(path string, exclusive bool) ethdb.RwKV {
	if database == "mdbx" {
		opts := ethdb.NewMDBX().Path(path)
		if exclusive {
			opts = opts.Exclusive()
		}
		return opts.MustOpen()
	}

	opts := ethdb.NewLMDB().Path(path)
	if exclusive {
		opts = opts.Exclusive()
	}
	return opts.MustOpen()
}
