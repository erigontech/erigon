package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/spf13/cobra"
)

var (
	genesisPath string
	genesis     *core.Genesis
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
	rootCmd.PersistentFlags().StringVar(&genesisPath, "genesis", "", "path to genesis.json file")
}

var rootCmd = &cobra.Command{
	Use:   "state",
	Short: "state is a utility for Stateless ethereum clients",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}

		genesis = core.DefaultGenesisBlock()
		if genesisPath != "" {
			genesis = genesisFromFile(genesisPath)
		}
		if chaindata == "" {
			chaindata = path.Join(datadir, "chaindata")
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
}

func genesisFromFile(genesisPath string) *core.Genesis {
	file, err := os.Open(genesisPath)
	if err != nil {
		utils.Fatalf("Failed to read genesis file: %v", err)
	}
	defer file.Close()

	genesis := new(core.Genesis)
	if err := json.NewDecoder(file).Decode(genesis); err != nil {
		utils.Fatalf("invalid genesis file: %v", err)
	}
	return genesis
}

func Execute() {
	ctx, cancel := common.RootContext()
	defer cancel()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
