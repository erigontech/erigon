package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/params"
)

var (
	genesisPath string
	genesis     *core.Genesis
	chainConfig *chain2.Config
)

func init() {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)
	rootCmd.PersistentFlags().StringVar(&genesisPath, "genesis", "", "path to genesis.json file")
}

var rootCmd = &cobra.Command{
	Use:   "state",
	Short: "state is a utility for Stateless ethereum clients",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}

		genesis, chainConfig = getChainGenesisAndConfig()
		if genesisPath != "" {
			genesis = genesisFromFile(genesisPath)
		}
		if genesis.Config != nil && genesis.Config.ChainID.Cmp(chainConfig.ChainID) != 0 {
			utils.Fatalf("provided genesis.json chain configuration is invalid: expected chainId to be %v, got %v",
				chainConfig.ChainID.String(), genesis.Config.ChainID.String())
		}
		// Apply special hacks for BSC params
		if chainConfig.Parlia != nil {
			params.ApplyBinanceSmartChainParams()
		}

		if chaindata == "" {
			chaindata = filepath.Join(datadirCli, "chaindata")
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

func getChainGenesisAndConfig() (genesis *core.Genesis, chainConfig *chain2.Config) {
	if chain == "" {
		genesis, chainConfig = core.DefaultGenesisBlock(), params.MainnetChainConfig
	} else {
		genesis, chainConfig = core.DefaultGenesisBlockByChainName(chain), params.ChainConfigByChainName(chain)
	}
	return genesis, chainConfig
}

func Execute() {
	ctx, cancel := common.RootContext()
	defer cancel()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
