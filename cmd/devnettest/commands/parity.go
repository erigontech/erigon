package commands

import (
	"strings"

	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/common"
	"github.com/spf13/cobra"
)

var (
	offsetAddr string
	quantity   int
)

func init() {
	listStorageKeysCmd.Flags().StringVar(&addr, "addr", "", "String address to list keys")
	listStorageKeysCmd.MarkFlagRequired("addr")
	listStorageKeysCmd.Flags().StringVar(&offsetAddr, "offset", "", "Offset storage key from which the batch should start")
	listStorageKeysCmd.Flags().IntVar(&quantity, "quantity", 10, "Integer number of addresses to display in a batch")
	listStorageKeysCmd.Flags().StringVar(&blockNum, "block", "latest", "Integer block number, or the string 'latest', 'earliest' or 'pending'; now only 'latest' is available")

	rootCmd.AddCommand(listStorageKeysCmd)
}

var listStorageKeysCmd = &cobra.Command{
	Use:   "parity-list",
	Short: "Returns all storage keys of the given address",
	RunE: func(cmd *cobra.Command, args []string) error {
		if clearDev {
			defer clearDevDB()
		}
		toAddress := common.HexToAddress(addr)
		offset := common.Hex2Bytes(strings.TrimSuffix(offsetAddr, "0x"))
		requests.ParityList(reqId, toAddress, quantity, offset, blockNum)
		return nil
	},
}
