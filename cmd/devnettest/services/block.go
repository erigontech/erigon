package services

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/cmd/devnettest/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
)

const (
	gasPrice = 912345678
)

var (
	devnetSignPrivateKey, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")
	signer                  = types.LatestSigner(params.AllCliqueProtocolChanges)
)

type Block struct {
	Number       *hexutil.Big
	Transactions []common.Hash
}

// CreateTransaction returns transaction details depending on what transaction type is given
func CreateTransaction(transactionType, addr string, value, nonce uint64, searchBlock bool) (*types.Transaction, common.Address, *contracts.Subscription, *bind.TransactOpts, error) {
	if transactionType == "regular" {
		tx, address, err := createNonContractTx(addr, value, nonce)
		if err != nil {
			return nil, common.Address{}, nil, nil, fmt.Errorf("failed to create non-contract transaction: %v", err)
		}
		return tx, address, nil, nil, nil
	}
	return createContractTx(nonce)
}

// createNonContractTx takes in a signer and returns the signed transaction and the address receiving the sent value
func createNonContractTx(addr string, value, nonce uint64) (*types.Transaction, common.Address, error) {
	toAddress := common.HexToAddress(addr)
	signedTx, err := types.SignTx(types.NewTransaction(nonce, toAddress, uint256.NewInt(value),
		params.TxGas, uint256.NewInt(gasPrice), nil), *signer, devnetSignPrivateKey)
	if err != nil {
		return nil, toAddress, fmt.Errorf("failed to sign transaction: %v", err)
	}
	return &signedTx, toAddress, nil
}

// createContractTx creates and signs a transaction using the developer address, returns the contract and the signed transaction
func createContractTx(nonce uint64) (*types.Transaction, common.Address, *contracts.Subscription, *bind.TransactOpts, error) {
	gspec := core.DeveloperGenesisBlock(uint64(0), common.HexToAddress("67b1d87101671b127f5f8714789C7192f7ad340e"))
	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, 1_000_000)

	// initialize transactOpts
	transactOpts, err := initializeTransactOps(nonce)
	if err != nil {
		return nil, common.Address{}, nil, nil, fmt.Errorf("failed to initialize transactOpts: %v", err)
	}

	// deploy the contract and get the contract handler
	address, txToSign, subscriptionContract, err := contracts.DeploySubscription(transactOpts, contractBackend)
	if err != nil {
		return nil, common.Address{}, nil, nil, fmt.Errorf("failed to deploy subscription: %v", err)
	}

	// sign the transaction with the private key
	signedTx, err := types.SignTx(txToSign, *signer, devnetSignPrivateKey)
	if err != nil {
		return nil, common.Address{}, nil, nil, fmt.Errorf("failed to sign tx: %v", err)
	}

	return &signedTx, address, subscriptionContract, transactOpts, nil
}

func initializeTransactOps(nonce uint64) (*bind.TransactOpts, error) {
	const txGas uint64 = 200_000
	var chainID = big.NewInt(1337)

	transactOpts, err := bind.NewKeyedTransactorWithChainID(devnetSignPrivateKey, chainID)
	if err != nil {
		return nil, fmt.Errorf("cannot create transactor with chainID %s, error: %v", chainID, err)
	}

	transactOpts.GasLimit = txGas
	transactOpts.GasPrice = big.NewInt(880_000_000)
	// TODO: Get Nonce from account automatically
	transactOpts.Nonce = big.NewInt(int64(nonce))

	return transactOpts, nil
}

// SearchBlockForTx connects the client to a websocket and listens for new heads to search the blocks for a tx hash
func SearchBlockForTx(txnHash common.Hash) (uint64, error) {
	client, clientErr := rpc.DialWebsocket(context.Background(), "ws://127.0.0.1:8545", "")
	if clientErr != nil {
		return 0, fmt.Errorf("failed to dial websocket: %v", clientErr)
	}
	fmt.Println()
	fmt.Println("Connected to web socket successfully")

	blockN, err := subscribe(client, "eth_newHeads", txnHash)
	if err != nil {
		return 0, fmt.Errorf("failed to subscribe to ws: %v", err)
	}

	return blockN, nil
}

// subscribe makes a ws subscription using eth_newHeads
func subscribe(client *rpc.Client, method string, hash common.Hash) (uint64, error) {
	parts := strings.SplitN(method, "_", 2)
	namespace := parts[0]
	method = parts[1]
	ch := make(chan interface{})
	sub, err := client.Subscribe(context.Background(), namespace, ch, []interface{}{method}...)
	if err != nil {
		return uint64(0), fmt.Errorf("client failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	var (
		blockCount int
		blockN     uint64
	)
ForLoop:
	for {
		select {
		case v := <-ch:
			blockCount++
			blockNumber := v.(map[string]interface{})["number"]
			fmt.Printf("Searching for the transaction in block with number: %+v, type: %[1]T\n", blockNumber.(string))
			num, foundTx, err := blockHasHash(client, hash, blockNumber.(string))
			if err != nil {
				return uint64(0), fmt.Errorf("could not verify if current block contains the tx hash: %v", err)
			}
			if foundTx || blockCount == 128 {
				blockN = num
				break ForLoop
			}
		case err := <-sub.Err():
			return uint64(0), fmt.Errorf("subscription error from client: %v", err)
		}
	}

	return blockN, nil

}

// blockHasHash checks if the current block has the transaction hash in its list of transactions
func blockHasHash(client *rpc.Client, hash common.Hash, blockNumber string) (uint64, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var currentBlock Block
	err := client.CallContext(ctx, &currentBlock, "eth_getBlockByNumber", blockNumber, false)
	if err != nil {
		return uint64(0), false, fmt.Errorf("failed to get block by number: %v", err)
	}

	for _, txnHash := range currentBlock.Transactions {
		if txnHash == hash {
			fmt.Println()
			fmt.Printf("Block with number: %v was mined and included transaction with hash: %v ==> %+v\n", blockNumber, hash, currentBlock)
			fmt.Println()
			return requests.HexToInt(blockNumber), true, nil
		}
	}

	return uint64(0), false, nil
}

// EmitEventAndGetLogs emits an event from the contract using the fallback method
func EmitEventAndGetLogs(reqId int, subContract *contracts.Subscription, opts *bind.TransactOpts, address common.Address) error {
	opts.Nonce.Add(opts.Nonce, big.NewInt(1))

	tx, err := subContract.Fallback(opts, []byte{})
	if err != nil {
		return fmt.Errorf("failed to emit event from fallback: %v", err)
	}

	signedTx, err := types.SignTx(tx, *signer, devnetSignPrivateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %v", err)
	}

	hash, err := requests.SendTx(reqId, &signedTx)
	if err != nil {
		return fmt.Errorf("failed to send transaction: %v", err)
	}

	blockN, err := SearchBlockForTx(*hash)
	if err != nil {
		return fmt.Errorf("error searching block for tx: %v", err)
	}

	if err = requests.GetLogs(reqId, blockN, blockN, address); err != nil {
		return fmt.Errorf("failed to get logs: %v", err)
	}

	return nil
}

// ClearDevDB cleans up the dev folder used for the operations
func ClearDevDB() {
	fmt.Printf("Clearing ~/dev\n")
}
