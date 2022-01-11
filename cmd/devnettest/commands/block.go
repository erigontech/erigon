package commands

import (
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/spf13/cobra"
)

var devnetSignPrivateKey, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")

var (
	sendAddr  string
	sendValue uint64
	nonce     uint64
)

const (
	gasPrice = 50000
)

func init() {
	sendTxCmd.Flags().StringVar(&sendAddr, "addr", "", "String address to send to")
	sendTxCmd.MarkFlagRequired("addr")
	sendTxCmd.Flags().Uint64Var(&sendValue, "value", 0, "Uint64 Value to send")
	sendTxCmd.Flags().Uint64Var(&nonce, "nonce", 0, "Uint64 nonce")

	rootCmd.AddCommand(sendTxCmd)
}

var sendTxCmd = &cobra.Command{
	Use:   "send-tx",
	Short: "Sends a transaction",
	Args: func(cmd *cobra.Command, args []string) error {
		if sendValue == 0 {
			return fmt.Errorf("value must be > 0")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer clearDevDB()
		}
		toAddress := common.HexToAddress(sendAddr)
		signer := types.LatestSigner(params.AllCliqueProtocolChanges)
		signedTx, _ := types.SignTx(types.NewTransaction(nonce, toAddress, uint256.NewInt(sendValue),
			params.TxGas, uint256.NewInt(gasPrice), nil), *signer, devnetSignPrivateKey)
		requests.SendTx(reqId, &signedTx)
	},
}
