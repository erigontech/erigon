package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
)

const (
	recipientAddress        = "0x71562b71999873DB5b286dF957af199Ec94617F7"
	sendValue        uint64 = 10000
)

func callSendTx(value uint64, toAddr, fromAddr string) (*common.Hash, error) {
	fmt.Printf("Sending %d ETH to %q from %q...\n", value, toAddr, fromAddr)

	// get the latest nonce for the next transaction
	nonce, err := services.GetNonce(models.ReqId, common.HexToAddress(fromAddr))
	if err != nil {
		fmt.Printf("failed to get latest nonce: %s\n", err)
		return nil, err
	}

	// create a non-contract transaction and sign it
	signedTx, _, _, _, err := services.CreateTransaction(models.NonContractTx, toAddr, value, nonce)
	if err != nil {
		fmt.Printf("failed to create a transaction: %s\n", err)
		return nil, err
	}

	// send the signed transaction
	hash, err := requests.SendTransaction(models.ReqId, signedTx)
	if err != nil {
		fmt.Printf("failed to send transaction: %s\n", err)
		return nil, err
	}

	fmt.Printf("SUCCESS => Tx submitted, adding tx with hash %q to txpool\n", hash)

	hashes := map[common.Hash]bool{*hash: true}
	if _, err = services.SearchReservesForTransactionHash(hashes); err != nil {
		return nil, fmt.Errorf("failed to call contract tx: %v", err)
	}

	return hash, nil
}

func callContractTx() (*common.Hash, error) {
	// hashset to hold hashes for search after mining
	hashes := make(map[common.Hash]bool)

	// get the latest nonce for the next transaction
	nonce, err := services.GetNonce(models.ReqId, common.HexToAddress(models.DevAddress))
	if err != nil {
		fmt.Printf("failed to get latest nonce: %s\n", err)
		return nil, err
	}

	// subscriptionContract is the handler to the contract for further operations
	signedTx, address, subscriptionContract, transactOpts, err := services.CreateTransaction(models.ContractTx, "", 0, nonce)
	if err != nil {
		fmt.Printf("failed to create transaction: %v\n", err)
		return nil, err
	}

	// send the contract transaction to the node
	hash, err := requests.SendTransaction(models.ReqId, signedTx)
	if err != nil {
		fmt.Printf("failed to send transaction: %v\n", err)
		return nil, err
	}
	hashes[*hash] = true
	fmt.Printf("SUCCESS => Tx submitted, adding tx with hash %q to txpool\n", hash)
	fmt.Println()

	eventHash, err := services.EmitFallbackEvent(models.ReqId, subscriptionContract, transactOpts, address)
	if err != nil {
		fmt.Printf("failed to emit events: %v\n", err)
		return nil, err
	}
	hashes[*eventHash] = true

	txToBlockMap, err := services.SearchReservesForTransactionHash(hashes)
	if err != nil {
		return nil, fmt.Errorf("failed to call contract tx: %v", err)
	}

	blockNum := (*txToBlockMap)[*eventHash]

	block, err := requests.GetBlockByNumber(models.ReqId, devnetutils.HexToInt(blockNum), true)
	if err != nil {
		return nil, err
	}

	expectedLog := devnetutils.BuildLog(*eventHash, blockNum, address,
		devnetutils.GenerateTopic(models.SolContractMethodSignature), hexutil.Bytes{}, hexutil.Uint(1),
		block.Result.Hash, hexutil.Uint(0), false)

	if err = requests.GetAndCompareLogs(models.ReqId, 0, 20, expectedLog); err != nil {
		return nil, fmt.Errorf("failed to get logs: %v", err)
	}

	return hash, nil
}
