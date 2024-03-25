package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	libchain "github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/terminate"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
)

var (
	genesisPath string
	genesis     *types.Genesis
	chainConfig *libchain.Config
)

func init() {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)
	rootCmd.PersistentFlags().StringVar(&genesisPath, "genesis", "", "path to genesis.json file")
}

var rootCmd = &cobra.Command{
	Use:   "state",
	Short: "state is a utility for Stateless ethereum clients",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		genesis, chainConfig = getChainGenesisAndConfig()
		if genesisPath != "" {
			genesis = genesisFromFile(genesisPath)
		}
		if genesis.Config != nil && genesis.Config.ChainID.Cmp(chainConfig.ChainID) != 0 {
			terminate.Fatalf("provided genesis.json chain configuration is invalid: expected chainId to be %v, got %v",
				chainConfig.ChainID.String(), genesis.Config.ChainID.String())
		}

		if chaindata == "" {
			chaindata = filepath.Join(datadirCli, "chaindata")
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
}

func genesisFromFile(genesisPath string) *types.Genesis {
	file, err := os.Open(genesisPath)
	if err != nil {
		terminate.Fatalf("Failed to read genesis file: %v", err)
	}
	defer file.Close()

	genesis := new(types.Genesis)
	if err := json.NewDecoder(file).Decode(genesis); err != nil {
		terminate.Fatalf("invalid genesis file: %v", err)
	}
	return genesis
}

func getChainGenesisAndConfig() (genesis *types.Genesis, chainConfig *libchain.Config) {
	if chain == "" {
		genesis, chainConfig = core.MainnetGenesisBlock(), params.MainnetChainConfig
	} else {
		genesis, chainConfig = core.GenesisBlockByChainName(chain), params.ChainConfigByChainName(chain)
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
