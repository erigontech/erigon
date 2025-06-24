// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	chain2 "github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/execution/chainspec"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/logging"

	_ "github.com/erigontech/erigon/polygon/chain" // Register Polygon chains
)

var (
	genesisPath string
	genesis     *types.Genesis
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
		genesis, chainConfig = getChainGenesisAndConfig()
		if genesisPath != "" {
			genesis = genesisFromFile(genesisPath)
		}
		if genesis.Config != nil && genesis.Config.ChainID.Cmp(chainConfig.ChainID) != 0 {
			utils.Fatalf("provided genesis.json chain configuration is invalid: expected chainId to be %v, got %v",
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
		utils.Fatalf("Failed to read genesis file: %v", err)
	}
	defer file.Close()

	genesis := new(types.Genesis)
	if err := json.NewDecoder(file).Decode(genesis); err != nil {
		utils.Fatalf("invalid genesis file: %v", err)
	}
	return genesis
}

func getChainGenesisAndConfig() (genesis *types.Genesis, chainConfig *chain2.Config) {
	if chain == "" {
		genesis, chainConfig = chainspec.MainnetGenesisBlock(), params.MainnetChainConfig
	} else {
		genesis, chainConfig = chainspec.GenesisBlockByChainName(chain), params.ChainConfigByChainName(chain)
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
