package transactions

import (
	"context"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"math/big"

	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	accounts_steps "github.com/ledgerwatch/erigon/cmd/devnet/accounts/steps"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(CheckTxPoolContent),
		scenarios.StepHandler(SendTxWithDynamicFee),
		scenarios.StepHandler(AwaitBlocks),
	)
}

func CheckTxPoolContent(ctx context.Context, expectedPendingSize, expectedQueuedSize, expectedBaseFeeSize int) {
	pendingSize, queuedSize, baseFeeSize, err := devnet.SelectNode(ctx).TxpoolContent()

	logger := devnet.Logger(ctx)

	if err != nil {
		logger.Error("FAILURE getting txpool content", "error", err)
		return
	}

	if expectedPendingSize >= 0 && pendingSize != expectedPendingSize {
		logger.Error("FAILURE mismatched pending subpool size", "expected", expectedPendingSize, "got", pendingSize)
		return
	}

	if expectedQueuedSize >= 0 && queuedSize != expectedQueuedSize {
		logger.Error("FAILURE mismatched queued subpool size", "expected", expectedQueuedSize, "got", queuedSize)
		return
	}

	if expectedBaseFeeSize >= 0 && baseFeeSize != expectedBaseFeeSize {
		logger.Error("FAILURE mismatched basefee subpool size", "expected", expectedBaseFeeSize, "got", baseFeeSize)
	}

	logger.Info("Subpool sizes", "pending", pendingSize, "queued", queuedSize, "basefee", baseFeeSize)
}

func callSendTx(ctx context.Context, value uint64, toAddr, fromAddr string, logger log.Logger) (*libcommon.Hash, error) {
	logger.Info("Sending tx", "value", value, "to", toAddr, "from", fromAddr)

	// get the latest nonce for the next transaction
	nonce, err := accounts_steps.GetNonce(ctx, libcommon.HexToAddress(fromAddr))

	if err != nil {
		logger.Error("failed to get latest nonce", "error", err)
		return nil, err
	}

	node := devnet.SelectNode(ctx)

	// create a non-contract transaction and sign it
	signedTx, _, err := CreateTransaction(node.ChainID(), toAddr, fromAddr, value, nonce)
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
	if _, err = SearchReservesForTransactionHash(hashes, logger); err != nil {
		return nil, fmt.Errorf("failed to call contract tx: %v", err)
	}

	return hash, nil
}

func SendTxWithDynamicFee(ctx context.Context, to, from string, amount uint64) ([]*libcommon.Hash, error) {
	// get the latest nonce for the next transaction
	logger := devnet.Logger(ctx)

	nonce, err := accounts_steps.GetNonce(ctx, libcommon.HexToAddress(from))

	if err != nil {
		logger.Error("failed to get latest nonce", "error", err)
		return nil, err
	}

	lowerThanBaseFeeTxs, higherThanBaseFeeTxs, err := CreateManyEIP1559TransactionsRefWithBaseFee2(ctx, to, from, &nonce)
	if err != nil {
		logger.Error("failed CreateManyEIP1559TransactionsRefWithBaseFee", "error", err)
		return nil, err
	}

	higherThanBaseFeeHashlist, err := SendManyTransactions(ctx, higherThanBaseFeeTxs)
	if err != nil {
		logger.Error("failed SendManyTransactions(higherThanBaseFeeTxs)", "error", err)
		return nil, err
	}

	lowerThanBaseFeeHashlist, err := SendManyTransactions(ctx, lowerThanBaseFeeTxs)

	if err != nil {
		logger.Error("failed SendManyTransactions(lowerThanBaseFeeTxs)", "error", err)
		return nil, err
	}

	CheckTxPoolContent(ctx, 100, 0, 100)

	CheckTxPoolContent(ctx, -1, -1, -1)

	hashmap := make(map[libcommon.Hash]bool)
	for _, hash := range higherThanBaseFeeHashlist {
		hashmap[*hash] = true
	}

	if _, err = SearchReservesForTransactionHash(hashmap, logger); err != nil {
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

const gasPrice = 912_345_678
const gasAmount = 875_000_000

func CreateManyEIP1559TransactionsRefWithBaseFee(ctx context.Context, to, from string, startingNonce *uint64, logger log.Logger) ([]types.Transaction, []types.Transaction, error) {
	toAddress := libcommon.HexToAddress(to)
	fromAddress := libcommon.HexToAddress(from)

	baseFeePerGas, err := BaseFeeFromBlock(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed BaseFeeFromBlock: %v", err)
	}

	devnet.Logger(ctx).Info("BaseFeePerGas", "val", baseFeePerGas)

	lowerBaseFeeTransactions, higherBaseFeeTransactions, err := signEIP1559TxsLowerAndHigherThanBaseFee2(ctx, 1, 1, baseFeePerGas, startingNonce, toAddress, fromAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsLowerAndHigherThanBaseFee2: %v", err)
	}

	return lowerBaseFeeTransactions, higherBaseFeeTransactions, nil
}

func CreateManyEIP1559TransactionsRefWithBaseFee2(ctx context.Context, to, from string, startingNonce *uint64) ([]types.Transaction, []types.Transaction, error) {
	toAddress := libcommon.HexToAddress(to)
	fromAddress := libcommon.HexToAddress(from)

	baseFeePerGas, err := BaseFeeFromBlock(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed BaseFeeFromBlock: %v", err)
	}

	devnet.Logger(ctx).Info("BaseFeePerGas2", "val", baseFeePerGas)

	lowerBaseFeeTransactions, higherBaseFeeTransactions, err := signEIP1559TxsLowerAndHigherThanBaseFee2(ctx, 100, 100, baseFeePerGas, startingNonce, toAddress, fromAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsLowerAndHigherThanBaseFee2: %v", err)
	}

	return lowerBaseFeeTransactions, higherBaseFeeTransactions, nil
}

// createNonContractTx returns a signed transaction and the recipient address
func CreateTransaction(chainId *big.Int, to, from string, value, nonce uint64) (types.Transaction, libcommon.Address, error) {
	toAddress := libcommon.HexToAddress(to)
	fromAddress := libcommon.HexToAddress(from)
	// create a new transaction using the parameters to send
	transaction := types.NewTransaction(nonce, toAddress, uint256.NewInt(value), params.TxGas, uint256.NewInt(gasPrice), nil)

	// sign the transaction using the developer 0signed private key
	signedTx, err := types.SignTx(transaction, *types.LatestSignerForChainID(chainId), accounts.SigKey(fromAddress))

	if err != nil {
		return nil, libcommon.Address{}, fmt.Errorf("failed to sign non-contract transaction: %v", err)
	}

	return signedTx, toAddress, nil
}

func signEIP1559TxsLowerAndHigherThanBaseFee2(ctx context.Context, amountLower, amountHigher int, baseFeePerGas uint64, nonce *uint64, toAddress libcommon.Address, fromAddress libcommon.Address) ([]types.Transaction, []types.Transaction, error) {
	higherBaseFeeTransactions, err := signEIP1559TxsHigherThanBaseFee(ctx, amountHigher, baseFeePerGas, nonce, toAddress, fromAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsHigherThanBaseFee: %v", err)
	}

	lowerBaseFeeTransactions, err := signEIP1559TxsLowerThanBaseFee(ctx, amountLower, baseFeePerGas, nonce, toAddress, fromAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsLowerThanBaseFee: %v", err)
	}

	return lowerBaseFeeTransactions, higherBaseFeeTransactions, nil
}

// signEIP1559TxsLowerThanBaseFee creates n number of transactions with gasFeeCap lower than baseFeePerGas
func signEIP1559TxsLowerThanBaseFee(ctx context.Context, n int, baseFeePerGas uint64, nonce *uint64, toAddress, fromAddress libcommon.Address) ([]types.Transaction, error) {
	var signedTransactions []types.Transaction

	var (
		minFeeCap = baseFeePerGas - 300_000_000
		maxFeeCap = (baseFeePerGas - 100_000_000) + 1 // we want the value to be inclusive in the random number generation, hence the addition of 1
	)

	node := devnet.SelectNode(ctx)
	signer := *types.LatestSignerForChainID(node.ChainID())
	chainId := *uint256.NewInt(node.ChainID().Uint64())

	for i := 0; i < n; i++ {
		gasFeeCap, err := devnetutils.RandomNumberInRange(minFeeCap, maxFeeCap)

		if err != nil {
			return nil, err
		}

		value, err := devnetutils.RandomNumberInRange(0, 100_000)

		if err != nil {
			return nil, err
		}

		transaction := types.NewEIP1559Transaction(chainId, *nonce, toAddress, uint256.NewInt(value), uint64(210_000), uint256.NewInt(gasPrice), new(uint256.Int), uint256.NewInt(gasFeeCap), nil)

		devnet.Logger(ctx).Info("LOWER", "transaction", i, "nonce", transaction.Nonce, "value", transaction.Value, "feecap", transaction.FeeCap)

		signedTransaction, err := types.SignTx(transaction, signer, accounts.SigKey(fromAddress))

		if err != nil {
			return nil, err
		}

		signedTransactions = append(signedTransactions, signedTransaction)
		*nonce++
	}

	return signedTransactions, nil
}

// signEIP1559TxsHigherThanBaseFee creates amount number of transactions with gasFeeCap higher than baseFeePerGas
func signEIP1559TxsHigherThanBaseFee(ctx context.Context, n int, baseFeePerGas uint64, nonce *uint64, toAddress, fromAddress libcommon.Address) ([]types.Transaction, error) {
	var signedTransactions []types.Transaction

	var (
		minFeeCap = baseFeePerGas
		maxFeeCap = (baseFeePerGas + 100_000_000) + 1 // we want the value to be inclusive in the random number generation, hence the addition of 1
	)

	node := devnet.SelectNode(ctx)
	signer := *types.LatestSignerForChainID(node.ChainID())
	chainId := *uint256.NewInt(node.ChainID().Uint64())

	for i := 0; i < n; i++ {
		gasFeeCap, err := devnetutils.RandomNumberInRange(minFeeCap, maxFeeCap)
		if err != nil {
			return nil, err
		}

		value, err := devnetutils.RandomNumberInRange(0, 100_000)
		if err != nil {
			return nil, err
		}

		transaction := types.NewEIP1559Transaction(chainId, *nonce, toAddress, uint256.NewInt(value), uint64(210_000), uint256.NewInt(gasPrice), new(uint256.Int), uint256.NewInt(gasFeeCap), nil)

		devnet.Logger(ctx).Info("HIGHER", "transaction", i, "nonce", transaction.Nonce, "value", transaction.Value, "feecap", transaction.FeeCap)

		signedTransaction, err := types.SignTx(transaction, signer, accounts.SigKey(fromAddress))
		if err != nil {
			return nil, err
		}

		signedTransactions = append(signedTransactions, signedTransaction)
		*nonce++
	}

	return signedTransactions, nil
}

func BaseFeeFromBlock(ctx context.Context) (uint64, error) {
	var val uint64
	res, err := devnet.SelectNode(ctx).GetBlockByNumberDetails("latest", false)
	if err != nil {
		return 0, fmt.Errorf("failed to get base fee from block: %v\n", err)
	}

	if v, ok := res["baseFeePerGas"]; !ok {
		return val, fmt.Errorf("baseFeePerGas field missing from response")
	} else {
		val = devnetutils.HexToInt(v.(string))
	}

	return val, err
}

func SendManyTransactions(ctx context.Context, signedTransactions []types.Transaction) ([]*libcommon.Hash, error) {
	logger := devnet.Logger(ctx)

	logger.Info("Sending multiple transactions to the txpool...")
	hashes := make([]*libcommon.Hash, len(signedTransactions))

	for idx, tx := range signedTransactions {
		hash, err := devnet.SelectNode(ctx).SendTransaction(tx)
		if err != nil {
			logger.Error("failed SendTransaction", "error", err)
			//return nil, err
		}
		hashes[idx] = hash
	}

	return hashes, nil
}
