package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/cmd/devnettest/contracts"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"math/big"
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
	txType      string
)

const (
	gasPrice = 912345678
)

func init() {
	sendTxCmd.Flags().StringVar(&txType, "tx-type", "", "type of transaction, specify 'contract' or 'regular'")
	sendTxCmd.MarkFlagRequired("tx-type")

	sendTxCmd.Flags().StringVar(&sendAddr, "addr", "", "String address to send to")
	sendTxCmd.Flags().Uint64Var(&sendValue, "value", 0, "Uint64 Value to send")
	sendTxCmd.Flags().Uint64Var(&nonce, "nonce", 0, "Uint64 nonce")
	sendTxCmd.Flags().BoolVar(&searchBlock, "search-block", false, "Boolean look for tx in mined blocks")

	rootCmd.AddCommand(sendTxCmd)
}

var sendTxCmd = &cobra.Command{
	Use:   "send-tx",
	Short: "Sends a transaction",
	Args: func(cmd *cobra.Command, args []string) error {
		if txType != "regular" && txType != "contract" {
			return fmt.Errorf("tx type to create must either be 'contract' or 'regular'")
		}
		if txType == "regular" {
			fmt.Println(sendAddr)
			fmt.Println(sendValue)
			if sendValue == 0 {
				return fmt.Errorf("value must be > 0")
			}
			if sendAddr == "" {
				return fmt.Errorf("string address to send to must be present")
			}
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer clearDevDB()
		}

		// subscriptionContract is the handler to the contract for further operations
		signedTx, subscriptionContract, transactOpts, err := createTransaction(txType)
		if err != nil {
			panic(err)
		}

		hash, err := requests.SendTx(reqId, signedTx)
		if err != nil {
			panic(err)
		}

		if searchBlock {
			searchBlockForTx(*hash)
		}

		fmt.Printf("subscription contract, transactOps: %+v, %+v\n", subscriptionContract, transactOpts)

		//_, err = subscriptionContract.Fallback(transactOpts, []byte{})
		//if err != nil {
		//	fmt.Printf("error 3: %+v\n", err)
		//	panic(err)
		//}

		// TODO: call eth_getLogs on address and this block number
	},
}

func createTransaction(transactionType string) (*types.Transaction, *contracts.Subscription, *bind.TransactOpts, error) {
	signer := types.LatestSigner(params.AllCliqueProtocolChanges)
	if transactionType == "regular" {
		return createNonContractTx(signer)
	}
	return createContractTx(signer)
}

func createNonContractTx(signer *types.Signer) (*types.Transaction, *contracts.Subscription, *bind.TransactOpts, error) {
	toAddress := common.HexToAddress(sendAddr)
	signedTx, err := types.SignTx(types.NewTransaction(nonce, toAddress, uint256.NewInt(sendValue),
		params.TxGas, uint256.NewInt(gasPrice), nil), *signer, devnetSignPrivateKey)
	if err != nil {
		return nil, nil, nil, err
	}
	return &signedTx, nil, nil, nil
}

func createContractTx(signer *types.Signer) (*types.Transaction, *contracts.Subscription, *bind.TransactOpts, error) {
	const txGas uint64 = 200_000
	gspec := core.DeveloperGenesisBlock(uint64(0), common.HexToAddress("67b1d87101671b127f5f8714789C7192f7ad340e"))
	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, 1_000_000)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(devnetSignPrivateKey, big.NewInt(1337))
	if err != nil {
		return nil, nil, nil, err
	}

	transactOpts.GasLimit = txGas
	transactOpts.GasPrice = big.NewInt(880_000_000)
	// TODO: Get Nonce from account automatically
	transactOpts.Nonce = big.NewInt(int64(nonce))

	// get transaction to sign and contract handler
	_, txToSign, subscriptionContract, err := contracts.DeploySubscription(transactOpts, contractBackend)
	if err != nil {
		return nil, nil, nil, err
	}

	// sign the transaction with the private key
	signedTx, err := types.SignTx(txToSign, *signer, devnetSignPrivateKey)
	if err != nil {
		return nil, nil, nil, err
	}

	return &signedTx, subscriptionContract, transactOpts, nil
}

func searchBlockForTx(txnHash common.Hash) {
	url := "ws://127.0.0.1:8545"
	client, clientErr := rpc.DialWebsocket(context.Background(), url, "")
	if clientErr != nil {
		fmt.Println("error connecting to socket", clientErr)
		panic(clientErr)
	}
	fmt.Println()
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
