package commands

import (
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/common/hexutil"
)

const (
	recipientAddress        = "0x71562b71999873DB5b286dF957af199Ec94617F7"
	sendValue        uint64 = 10000
)

func callSendTx(reqGen *requests.RequestGenerator, value uint64, toAddr, fromAddr string, logger log.Logger) (*libcommon.Hash, error) {
	logger.Info("Sending tx", "value", value, "to", toAddr, "from", fromAddr)

	// get the latest nonce for the next transaction
	nonce, err := services.GetNonce(reqGen, libcommon.HexToAddress(fromAddr), logger)
	if err != nil {
		logger.Error("failed to get latest nonce", "error", err)
		return nil, err
	}

	// create a non-contract transaction and sign it
	signedTx, _, _, _, err := services.CreateTransaction(models.NonContractTx, toAddr, value, nonce)
	if err != nil {
		logger.Error("failed to create a transaction", "error", err)
		return nil, err
	}

	// send the signed transaction
	hash, err := requests.SendTransaction(reqGen, signedTx, logger)
	if err != nil {
		logger.Error("failed to send transaction", "error", err)
		return nil, err
	}

	hashes := map[libcommon.Hash]bool{*hash: true}
	if _, err = services.SearchReservesForTransactionHash(hashes, logger); err != nil {
		return nil, fmt.Errorf("failed to call contract tx: %v", err)
	}

	return hash, nil
}

func callSendTxWithDynamicFee(reqGen *requests.RequestGenerator, toAddr, fromAddr string, logger log.Logger) ([]*libcommon.Hash, error) {
	// get the latest nonce for the next transaction
	nonce, err := services.GetNonce(reqGen, libcommon.HexToAddress(fromAddr), logger)
	if err != nil {
		logger.Error("failed to get latest nonce", "error", err)
		return nil, err
	}

	lowerThanBaseFeeTxs, higherThanBaseFeeTxs, err := services.CreateManyEIP1559TransactionsRefWithBaseFee2(reqGen, toAddr, &nonce, logger)
	if err != nil {
		logger.Error("failed CreateManyEIP1559TransactionsRefWithBaseFee", "error", err)
		return nil, err
	}

	lowerThanBaseFeeHashlist, err := services.SendManyTransactions(reqGen, lowerThanBaseFeeTxs, logger)
	if err != nil {
		logger.Error("failed SendManyTransactions(lowerThanBaseFeeTxs)", "error", err)
		return nil, err
	}

	higherThanBaseFeeHashlist, err := services.SendManyTransactions(reqGen, higherThanBaseFeeTxs, logger)
	if err != nil {
		logger.Error("failed SendManyTransactions(higherThanBaseFeeTxs)", "error", err)
		return nil, err
	}

	services.CheckTxPoolContent(reqGen, 100, 0, 100, logger)

	hashmap := make(map[libcommon.Hash]bool)
	for _, hash := range higherThanBaseFeeHashlist {
		hashmap[*hash] = true
	}

	if _, err = services.SearchReservesForTransactionHash(hashmap, logger); err != nil {
		return nil, fmt.Errorf("failed to call contract tx: %v", err)
	}

	logger.Info("SUCCESS: All transactions in pending pool included in blocks")

	for i := 1; i <= 20; i++ {
		blockNumber, err := requests.BlockNumber(reqGen, logger)
		if err != nil {
			logger.Error("FAILURE => error getting block number", "error", err)
		} else {
			logger.Info("Got block number", "blockNum", blockNumber)
		}
		pendingSize, queuedSize, baseFeeSize, err := requests.TxpoolContent(reqGen, logger)
		if err != nil {
			logger.Error("FAILURE getting txpool content", "error", err)
		} else {
			logger.Info("Txpool subpool sizes", "pending", pendingSize, "queued", queuedSize, "basefee", baseFeeSize)
		}
		time.Sleep(5 * time.Second)
	}

	return append(lowerThanBaseFeeHashlist, higherThanBaseFeeHashlist...), nil
}

func callContractTx(reqGen *requests.RequestGenerator, logger log.Logger) (*libcommon.Hash, error) {
	// hashset to hold hashes for search after mining
	hashes := make(map[libcommon.Hash]bool)
	// get the latest nonce for the next transaction
	nonce, err := services.GetNonce(reqGen, libcommon.HexToAddress(models.DevAddress), logger)
	if err != nil {
		logger.Error("failed to get latest nonce", "error", err)
		return nil, err
	}

	// subscriptionContract is the handler to the contract for further operations
	signedTx, address, subscriptionContract, transactOpts, err := services.CreateTransaction(models.ContractTx, "", 0, nonce)
	if err != nil {
		logger.Error("failed to create transaction", "error", err)
		return nil, err
	}

	// send the contract transaction to the node
	hash, err := requests.SendTransaction(reqGen, signedTx, logger)
	if err != nil {
		logger.Error("failed to send transaction", "error", err)
		return nil, err
	}
	hashes[*hash] = true
	logger.Info("")

	eventHash, err := services.EmitFallbackEvent(reqGen, subscriptionContract, transactOpts, logger)
	if err != nil {
		logger.Error("failed to emit events", "error", err)
		return nil, err
	}
	hashes[*eventHash] = true

	txToBlockMap, err := services.SearchReservesForTransactionHash(hashes, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to call contract tx: %v", err)
	}

	blockNum := (*txToBlockMap)[*eventHash]

	block, err := requests.GetBlockByNumber(reqGen, devnetutils.HexToInt(blockNum), true, logger)
	if err != nil {
		return nil, err
	}

	expectedLog := devnetutils.BuildLog(*eventHash, blockNum, address,
		devnetutils.GenerateTopic(models.SolContractMethodSignature), hexutility.Bytes{}, hexutil.Uint(1),
		block.Result.Hash, hexutil.Uint(0), false)

	if err = requests.GetAndCompareLogs(reqGen, 0, 20, expectedLog, logger); err != nil {
		return nil, fmt.Errorf("failed to get logs: %v", err)
	}

	return hash, nil
}

func makeEIP1559Checks() {
	// run the check for baseFee effect twice

}
