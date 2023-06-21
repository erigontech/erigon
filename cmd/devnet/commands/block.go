package commands

import (
	"context"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/common/hexutil"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(SendTxWithDynamicFee),
		scenarios.StepHandler(AwaitBlocks),
	)
}

func callSendTx(node devnet.Node, value uint64, toAddr, fromAddr string, logger log.Logger) (*libcommon.Hash, error) {
	logger.Info("Sending tx", "value", value, "to", toAddr, "from", fromAddr)

	// get the latest nonce for the next transaction
	nonce, err := services.GetNonce(node, libcommon.HexToAddress(fromAddr))
	if err != nil {
		logger.Error("failed to get latest nonce", "error", err)
		return nil, err
	}

	// create a non-contract transaction and sign it
	signedTx, _, err := services.CreateTransaction(toAddr, value, nonce)
	if err != nil {
		logger.Error("failed to create a transaction", "error", err)
		return nil, err
	}

	// send the signed transaction
	hash, err := node.SendTransaction(signedTx)
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

func SendTxWithDynamicFee(ctx context.Context, toAddr, fromAddr string, amount uint64) ([]*libcommon.Hash, error) {
	// get the latest nonce for the next transaction
	node := devnet.SelectNode(ctx)
	logger := devnet.Logger(ctx)

	nonce, err := services.GetNonce(node, libcommon.HexToAddress(fromAddr))

	if err != nil {
		logger.Error("failed to get latest nonce", "error", err)
		return nil, err
	}

	lowerThanBaseFeeTxs, higherThanBaseFeeTxs, err := services.CreateManyEIP1559TransactionsRefWithBaseFee2(ctx, toAddr, &nonce)
	if err != nil {
		logger.Error("failed CreateManyEIP1559TransactionsRefWithBaseFee", "error", err)
		return nil, err
	}

	higherThanBaseFeeHashlist, err := services.SendManyTransactions(ctx, higherThanBaseFeeTxs)
	if err != nil {
		logger.Error("failed SendManyTransactions(higherThanBaseFeeTxs)", "error", err)
		return nil, err
	}

	lowerThanBaseFeeHashlist, err := services.SendManyTransactions(ctx, lowerThanBaseFeeTxs)

	if err != nil {
		logger.Error("failed SendManyTransactions(lowerThanBaseFeeTxs)", "error", err)
		return nil, err
	}

	services.CheckTxPoolContent(ctx, 100, 0, 100)

	services.CheckTxPoolContent(ctx, -1, -1, -1)

	hashmap := make(map[libcommon.Hash]bool)
	for _, hash := range higherThanBaseFeeHashlist {
		hashmap[*hash] = true
	}

	if _, err = services.SearchReservesForTransactionHash(hashmap, logger); err != nil {
		return nil, fmt.Errorf("failed to call contract tx: %v", err)
	}

	logger.Info("SUCCESS: All transactions in pending pool included in blocks")

	return append(lowerThanBaseFeeHashlist, higherThanBaseFeeHashlist...), nil
}

func AwaitBlocks(ctx context.Context, sleepTime time.Duration) error {
	logger := devnet.Logger(ctx)

	for i := 1; i <= 20; i++ {
		node := devnet.SelectNode(ctx)

		blockNumber, err := node.BlockNumber()

		if err != nil {
			logger.Error("FAILURE => error getting block number", "error", err)
		} else {
			logger.Info("Got block number", "blockNum", blockNumber)
		}

		pendingSize, queuedSize, baseFeeSize, err := node.TxpoolContent()

		if err != nil {
			logger.Error("FAILURE getting txpool content", "error", err)
		} else {
			logger.Info("Txpool subpool sizes", "pending", pendingSize, "queued", queuedSize, "basefee", baseFeeSize)
		}

		time.Sleep(sleepTime)
	}

	return nil
}

func callContractTx(node devnet.Node, logger log.Logger) (*libcommon.Hash, error) {
	// hashset to hold hashes for search after mining
	hashes := make(map[libcommon.Hash]bool)
	// get the latest nonce for the next transaction
	nonce, err := services.GetNonce(node, libcommon.HexToAddress(services.DevAddress))

	if err != nil {
		logger.Error("failed to get latest nonce", "error", err)
		return nil, err
	}

	// subscriptionContract is the handler to the contract for further operations
	signedTx, address, subscriptionContract, transactOpts, err := services.DeploySubsriptionContract(nonce)

	if err != nil {
		logger.Error("failed to create transaction", "error", err)
		return nil, err
	}

	// send the contract transaction to the node
	hash, err := node.SendTransaction(signedTx)

	if err != nil {
		logger.Error("failed to send transaction", "error", err)
		return nil, err
	}

	hashes[*hash] = true
	logger.Info("")

	eventHash, err := services.EmitFallbackEvent(node, subscriptionContract, transactOpts, logger)

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

	block, err := node.GetBlockByNumber(devnetutils.HexToInt(blockNum), true)
	if err != nil {
		return nil, err
	}

	expectedLog := requests.BuildLog(*eventHash, blockNum, address,
		devnetutils.GenerateTopic(services.SolContractMethodSignature), hexutility.Bytes{}, hexutil.Uint(1),
		block.Result.Hash, hexutil.Uint(0), false)

	if err = node.GetAndCompareLogs(0, 20, expectedLog); err != nil {
		return nil, fmt.Errorf("failed to get logs: %v", err)
	}

	return hash, nil
}
