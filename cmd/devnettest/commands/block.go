package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/rpc"

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
	sendAddr    string
	sendValue   uint64
	nonce       uint64
	searchBlock bool
)

const (
	gasPrice = 912345678
)

func init() {
	sendTxCmd.Flags().StringVar(&sendAddr, "addr", "", "String address to send to")
	sendTxCmd.MarkFlagRequired("addr")
	sendTxCmd.Flags().Uint64Var(&sendValue, "value", 0, "Uint64 Value to send")
	sendTxCmd.Flags().Uint64Var(&nonce, "nonce", 0, "Uint64 nonce")
	sendTxCmd.Flags().BoolVar(&searchBlock, "search-block", false, "Boolean look for tx in mined blocks")

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
		hash, err := requests.SendTx(reqId, &signedTx)
		if err != nil {
			fmt.Printf("Error trying to send transaction: %v\n", err)
		}

		if searchBlock {
			searchBlockForTx(*hash)
		}
	},
}

func searchBlockForTx(txnHash common.Hash) {
	url := "ws://127.0.0.1:8545"
	client, clientErr := rpc.DialWebsocket(context.Background(), url, "")
	if clientErr != nil {
		fmt.Println("error connecting to socket", clientErr)
		panic(clientErr)
	}
	fmt.Println("Connected to web socket successfully")

	if err := subscribe(client, "eth_newHeads", txnHash); err != nil {
		fmt.Println("error occurred while subscribing", err)
		panic(err)
	}
}

func subscribe(client *rpc.Client, method string, hash common.Hash) error {
	parts := strings.SplitN(method, "_", 2)
	namespace := parts[0]
	method = parts[1]
	ch := make(chan interface{})
	sub, err := client.Subscribe(context.Background(), namespace, ch, []interface{}{method}...)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	blockCount := 0
ForLoop:
	for {
		select {
		case v := <-ch:
			blockCount++
			blockNumber := v.(map[string]interface{})["number"]
			fmt.Printf("Searching for the transaction in block with number: %+v\n", blockNumber)
			foundTx, err := blockHasHash(client, hash, blockNumber.(string))
			if err != nil {
				return err
			}
			if foundTx || blockCount == 128 {
				break ForLoop
			}
		case err := <-sub.Err():
			return err
		}
	}

	return nil

}

type Block struct {
	Number       *hexutil.Big
	Transactions []common.Hash
}

func blockHasHash(client *rpc.Client, hash common.Hash, blockNumber string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var currentBlock Block
	err := client.CallContext(ctx, &currentBlock, "eth_getBlockByNumber", blockNumber, false)
	if err != nil {
		fmt.Println("can't get latest block:", err)
		return false, err
	}

	for _, txnHash := range currentBlock.Transactions {
		if txnHash == hash {
			fmt.Println()
			fmt.Printf("Block with number: %v was mined and included transaction with hash: %v ==> %+v\n", blockNumber, hash, currentBlock)
			fmt.Println()
			return true, nil
		}
	}

	return false, nil
}
