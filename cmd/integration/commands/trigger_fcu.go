package commands

import (
	"path/filepath"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/node/debug"
	"github.com/spf13/cobra"
)

func init() {
	withDataDir(triggerFCU)

	triggerFCU.Flags().StringVar(&blockHash, "blockhash", "", "head block hash")
	must(triggerFCU.MarkFlagRequired("blockhash"))

	triggerFCU.Flags().StringVar(&engineEndpoint, "engineendpoint", "", "engine API endpoint")
	must(triggerFCU.MarkFlagRequired("engineendpoint"))

	rootCmd.AddCommand(triggerFCU)
}

var blockHash string

var engineEndpoint string

var triggerFCU = &cobra.Command{
	Use:   "trigger_fcu",
	Short: "Trigger forkChoiceUpdate manually for testing purposes",
	Long: `This command simulates a forkChoiceUpdate call from an external CL to a specific block hash.

This is specially useful on devnets where there are network constraints (e.g. few slow peers), so you can test Erigon in isolation without having to rely on an external CL up to a certain known block.
`,
	Args: cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()
		dirs := datadir.New(datadirCli)
		jwt := filepath.Join(dirs.DataDir, "jwt.hex")

		jwtConfig := &httpcfg.HttpCfg{JWTSecretPath: jwt}
		jwtSecret, err := cli.ObtainJWTSecret(jwtConfig, logger)
		if err != nil {
			logger.Error("failed while reading jwt secret", "err", err)
			panic(err)
		}

		engineApiClient, err := engineapi.DialJsonRpcClient(engineEndpoint, jwtSecret, logger)
		if err != nil {
			logger.Error("failed while connecting to engine API endpoint", "err", err)
			panic(err)
		}
		hash := common.HexToHash(blockHash)
		fcu := &engine_types.ForkChoiceState{
			HeadHash:           hash,
			SafeBlockHash:      hash,
			FinalizedBlockHash: hash,
		}
		_, err = engineApiClient.ForkchoiceUpdatedV3(ctx, fcu, nil)
		if err != nil {
			logger.Error("failed while sending forkChoiceUpdate", "err", err)
			panic(err)
		}
	},
}
