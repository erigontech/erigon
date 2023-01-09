package services

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
)

const gasPrice = 912345678

var (
	signer = types.LatestSigner(params.AllCliqueProtocolChanges)
)

// CreateTransaction creates a transaction depending on the type of transaction being passed
func CreateTransaction(txType models.TransactionType, addr string, value, nonce uint64) (*types.Transaction, common.Address, *contracts.Subscription, *bind.TransactOpts, error) {
	switch txType {
	case models.NonContractTx:
		tx, address, err := createNonContractTx(addr, value, nonce)
		if err != nil {
			return nil, common.Address{}, nil, nil, fmt.Errorf("failed to create non-contract transaction: %v", err)
		}
		return tx, address, nil, nil, nil
	case models.ContractTx:
		return createContractTx(nonce)
	default:
		return nil, common.Address{}, nil, nil, models.ErrInvalidTransactionType
	}
}

// createNonContractTx returns a signed transaction and the recipient address
func createNonContractTx(addr string, value, nonce uint64) (*types.Transaction, common.Address, error) {
	toAddress := common.HexToAddress(addr)

	// create a new transaction using the parameters to send
	transaction := types.NewTransaction(nonce, toAddress, uint256.NewInt(value), params.TxGas, uint256.NewInt(gasPrice), nil)

	// sign the transaction using the developer 0signed private key
	signedTx, err := types.SignTx(transaction, *signer, models.DevSignedPrivateKey)
	if err != nil {
		return nil, common.Address{}, fmt.Errorf("failed to sign non-contract transaction: %v", err)
	}

	return &signedTx, toAddress, nil
}

// createContractTx creates and signs a transaction using the developer address, returns the contract and the signed transaction
func createContractTx(nonce uint64) (*types.Transaction, common.Address, *contracts.Subscription, *bind.TransactOpts, error) {
	// initialize transactOpts
	transactOpts, err := initializeTransactOps(nonce)
	if err != nil {
		return nil, common.Address{}, nil, nil, fmt.Errorf("failed to initialize transactOpts: %v", err)
	}

	// deploy the contract and get the contract handler
	address, txToSign, subscriptionContract, err := contracts.DeploySubscription(transactOpts, models.ContractBackend)
	if err != nil {
		return nil, common.Address{}, nil, nil, fmt.Errorf("failed to deploy subscription: %v", err)
	}

	// sign the transaction with the private key
	signedTx, err := types.SignTx(txToSign, *signer, models.DevSignedPrivateKey)
	if err != nil {
		return nil, common.Address{}, nil, nil, fmt.Errorf("failed to sign tx: %v", err)
	}

	return &signedTx, address, subscriptionContract, transactOpts, nil
}

// initializeTransactOps initializes the transactOpts object for a contract transaction
func initializeTransactOps(nonce uint64) (*bind.TransactOpts, error) {
	var chainID = big.NewInt(1337)

	transactOpts, err := bind.NewKeyedTransactorWithChainID(models.DevSignedPrivateKey, chainID)
	if err != nil {
		return nil, fmt.Errorf("cannot create transactor with chainID %d, error: %v", chainID, err)
	}

	transactOpts.GasLimit = uint64(200_000)
	transactOpts.GasPrice = big.NewInt(880_000_000)
	transactOpts.Nonce = big.NewInt(int64(nonce)) // TODO: Get Nonce from account automatically

	return transactOpts, nil
}

// txHashInBlock checks if the block with block number has the transaction hash in its list of transactions
func txHashInBlock(client *rpc.Client, hashmap map[common.Hash]bool, blockNumber string, txToBlockMap map[common.Hash]string) (uint64, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel() // releases the resources held by the context

	var (
		currBlock models.Block
		numFound  int
	)
	err := client.CallContext(ctx, &currBlock, string(models.ETHGetBlockByNumber), blockNumber, false)
	if err != nil {
		return uint64(0), 0, fmt.Errorf("failed to get block by number: %v", err)
	}

	for _, txnHash := range currBlock.Transactions {
		// check if tx is in the hash set and remove it from the set if it is present
		if _, ok := hashmap[txnHash]; ok {
			numFound++
			fmt.Printf("SUCCESS => Tx with hash %q is in mined block with number %q\n", txnHash, blockNumber)
			// add the block number as an entry to the map
			txToBlockMap[txnHash] = blockNumber
			delete(hashmap, txnHash)
			if len(hashmap) == 0 {
				return devnetutils.HexToInt(blockNumber), numFound, nil
			}
		}
	}

	return uint64(0), 0, nil
}

// EmitFallbackEvent emits an event from the contract using the fallback method
func EmitFallbackEvent(reqId int, subContract *contracts.Subscription, opts *bind.TransactOpts, address common.Address) (*common.Hash, error) {
	fmt.Println("EMITTING EVENT FROM FALLBACK...")

	// adding one to the nonce before initiating another transaction
	opts.Nonce.Add(opts.Nonce, big.NewInt(1))

	tx, err := subContract.Fallback(opts, []byte{})
	if err != nil {
		return nil, fmt.Errorf("failed to emit event from fallback: %v", err)
	}

	signedTx, err := types.SignTx(tx, *signer, models.DevSignedPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign fallback transaction: %v", err)
	}

	hash, err := requests.SendTransaction(models.ReqId, &signedTx)
	if err != nil {
		return nil, fmt.Errorf("failed to send fallback transaction: %v", err)
	}
	fmt.Printf("Tx submitted, adding tx with hash %q to txpool\n", hash)

	// TODO: Get all the logs across the blocks that mined the transactions and check that they are logged

	return hash, nil
}
