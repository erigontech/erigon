// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package transactions

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/devnet/accounts"
	"github.com/erigontech/erigon/cmd/devnet/blocks"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/devnetutils"
	"github.com/erigontech/erigon/cmd/devnet/scenarios"
	"github.com/erigontech/erigon/rpc"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(CheckTxPoolContent),
		scenarios.StepHandler(SendTxWithDynamicFee),
		scenarios.StepHandler(AwaitBlocks),
		scenarios.StepHandler(SendTxLoad),
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
		logger.Debug("FAILURE mismatched pending subpool size", "expected", expectedPendingSize, "got", pendingSize)
		return
	}

	if expectedQueuedSize >= 0 && queuedSize != expectedQueuedSize {
		logger.Error("FAILURE mismatched queued subpool size", "expected", expectedQueuedSize, "got", queuedSize)
		return
	}

	if expectedBaseFeeSize >= 0 && baseFeeSize != expectedBaseFeeSize {
		logger.Debug("FAILURE mismatched basefee subpool size", "expected", expectedBaseFeeSize, "got", baseFeeSize)
	}

	logger.Info("Subpool sizes", "pending", pendingSize, "queued", queuedSize, "basefee", baseFeeSize)
}

func Transfer(ctx context.Context, toAddr, fromAddr string, value uint64, wait bool) (common.Hash, error) {
	logger := devnet.Logger(ctx)

	node := devnet.SelectNode(ctx)

	// create a non-contract transaction and sign it
	signedTx, _, err := CreateTransaction(node, toAddr, fromAddr, value)

	if err != nil {
		logger.Error("failed to create a transaction", "error", err)
		return common.Hash{}, err
	}

	logger.Info("Sending tx", "value", value, "to", toAddr, "from", fromAddr, "tx", signedTx.Hash())

	// send the signed transaction
	hash, err := node.SendTransaction(signedTx)

	if err != nil {
		logger.Error("failed to send transaction", "error", err)
		return common.Hash{}, err
	}

	if wait {
		if _, err = AwaitTransactions(ctx, hash); err != nil {
			return common.Hash{}, fmt.Errorf("failed to call contract tx: %v", err)
		}
	}

	return hash, nil
}

func SendTxWithDynamicFee(ctx context.Context, to, from string, amount uint64) ([]common.Hash, error) {
	// get the latest nonce for the next transaction
	logger := devnet.Logger(ctx)

	lowerThanBaseFeeTxs, higherThanBaseFeeTxs, err := CreateManyEIP1559TransactionsRefWithBaseFee2(ctx, to, from, 200)
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

	CheckTxPoolContent(ctx, len(higherThanBaseFeeHashlist), 0, len(lowerThanBaseFeeHashlist))

	CheckTxPoolContent(ctx, -1, -1, -1)

	if _, err = AwaitTransactions(ctx, higherThanBaseFeeHashlist...); err != nil {
		return nil, fmt.Errorf("failed to call contract tx: %v", err)
	}

	logger.Info("SUCCESS: All transactions in pending pool included in blocks")

	return append(lowerThanBaseFeeHashlist, higherThanBaseFeeHashlist...), nil
}

func SendTxLoad(ctx context.Context, to, from string, amount uint64, txPerSec uint) error {
	logger := devnet.Logger(ctx)

	batchCount := txPerSec / 4

	if batchCount < 1 {
		batchCount = 1
	}

	ms250 := 250 * time.Millisecond

	for {
		start := time.Now()

		tx, err := CreateManyEIP1559TransactionsHigherThanBaseFee(ctx, to, from, int(batchCount))

		if err != nil {
			logger.Error("failed Create Txns", "error", err)
			return err
		}

		_, err = SendManyTransactions(ctx, tx)

		if err != nil {
			logger.Error("failed SendManyTransactions(higherThanBaseFeeTxs)", "error", err)
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}

		duration := time.Since(start)

		if duration < ms250 {
			time.Sleep(ms250 - duration)
		}
	}
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

func CreateManyEIP1559TransactionsRefWithBaseFee(ctx context.Context, to, from string, logger log.Logger) ([]types.Transaction, []types.Transaction, error) {
	toAddress := common.HexToAddress(to)
	fromAddress := common.HexToAddress(from)

	baseFeePerGas, err := blocks.BaseFeeFromBlock(ctx)

	if err != nil {
		return nil, nil, fmt.Errorf("failed BaseFeeFromBlock: %v", err)
	}

	devnet.Logger(ctx).Info("BaseFeePerGas", "val", baseFeePerGas)

	lowerBaseFeeTransactions, higherBaseFeeTransactions, err := signEIP1559TxsLowerAndHigherThanBaseFee2(ctx, 1, 1, baseFeePerGas, toAddress, fromAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsLowerAndHigherThanBaseFee2: %v", err)
	}

	return lowerBaseFeeTransactions, higherBaseFeeTransactions, nil
}

func CreateManyEIP1559TransactionsRefWithBaseFee2(ctx context.Context, to, from string, count int) ([]types.Transaction, []types.Transaction, error) {
	toAddress := common.HexToAddress(to)
	fromAddress := common.HexToAddress(from)

	baseFeePerGas, err := blocks.BaseFeeFromBlock(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed BaseFeeFromBlock: %v", err)
	}

	devnet.Logger(ctx).Info("BaseFeePerGas2", "val", baseFeePerGas)

	lower := count - devnetutils.RandomInt(count)
	higher := count - lower

	lowerBaseFeeTransactions, higherBaseFeeTransactions, err := signEIP1559TxsLowerAndHigherThanBaseFee2(ctx, lower, higher, baseFeePerGas, toAddress, fromAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsLowerAndHigherThanBaseFee2: %v", err)
	}

	return lowerBaseFeeTransactions, higherBaseFeeTransactions, nil
}

func CreateManyEIP1559TransactionsHigherThanBaseFee(ctx context.Context, to, from string, count int) ([]types.Transaction, error) {
	toAddress := common.HexToAddress(to)
	fromAddress := common.HexToAddress(from)

	baseFeePerGas, err := blocks.BaseFeeFromBlock(ctx)

	if err != nil {
		return nil, fmt.Errorf("failed BaseFeeFromBlock: %v", err)
	}

	baseFeePerGas = baseFeePerGas * 2

	devnet.Logger(ctx).Info("BaseFeePerGas2", "val", baseFeePerGas)

	node := devnet.SelectNode(ctx)

	res, err := node.GetTransactionCount(fromAddress, rpc.PendingBlock)

	if err != nil {
		return nil, fmt.Errorf("failed to get transaction count for address 0x%x: %v", fromAddress, err)
	}

	nonce := res.Uint64()

	return signEIP1559TxsHigherThanBaseFee(ctx, count, baseFeePerGas, &nonce, toAddress, fromAddress)
}

// createNonContractTx returns a signed transaction and the recipient address
func CreateTransaction(node devnet.Node, to, from string, value uint64) (types.Transaction, common.Address, error) {
	toAccount := accounts.GetAccount(to)

	var toAddress common.Address

	if toAccount == nil {
		if strings.HasPrefix(to, "0x") {
			toAddress = common.HexToAddress(from)
		} else {
			return nil, common.Address{}, fmt.Errorf("unknown to account: %s", to)
		}
	} else {
		toAddress = toAccount.Address
	}

	fromAccount := accounts.GetAccount(from)

	if fromAccount == nil {
		return nil, common.Address{}, fmt.Errorf("unknown from account: %s", from)
	}

	res, err := node.GetTransactionCount(fromAccount.Address, rpc.PendingBlock)

	if err != nil {
		return nil, common.Address{}, fmt.Errorf("failed to get transaction count for address 0x%x: %v", fromAccount.Address, err)
	}

	// create a new transaction using the parameters to send
	transaction := types.NewTransaction(res.Uint64(), toAddress, uint256.NewInt(value), params.TxGas, uint256.NewInt(gasPrice), nil)

	// sign the transaction using the developer 0signed private key
	signedTx, err := types.SignTx(transaction, *types.LatestSignerForChainID(node.ChainID()), fromAccount.SigKey())

	if err != nil {
		return nil, common.Address{}, fmt.Errorf("failed to sign non-contract transaction: %v", err)
	}

	return signedTx, toAddress, nil
}

func signEIP1559TxsLowerAndHigherThanBaseFee2(ctx context.Context, amountLower, amountHigher int, baseFeePerGas uint64, toAddress common.Address, fromAddress common.Address) ([]types.Transaction, []types.Transaction, error) {
	node := devnet.SelectNode(ctx)

	res, err := node.GetTransactionCount(fromAddress, rpc.PendingBlock)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to get transaction count for address 0x%x: %v", fromAddress, err)
	}

	nonce := res.Uint64()

	higherBaseFeeTransactions, err := signEIP1559TxsHigherThanBaseFee(ctx, amountHigher, baseFeePerGas, &nonce, toAddress, fromAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsHigherThanBaseFee: %v", err)
	}

	lowerBaseFeeTransactions, err := signEIP1559TxsLowerThanBaseFee(ctx, amountLower, baseFeePerGas, &nonce, toAddress, fromAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("failed signEIP1559TxsLowerThanBaseFee: %v", err)
	}

	return lowerBaseFeeTransactions, higherBaseFeeTransactions, nil
}

// signEIP1559TxsLowerThanBaseFee creates n number of transactions with gasFeeCap lower than baseFeePerGas
func signEIP1559TxsLowerThanBaseFee(ctx context.Context, n int, baseFeePerGas uint64, nonce *uint64, toAddress, fromAddress common.Address) ([]types.Transaction, error) {
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

		devnet.Logger(ctx).Trace("LOWER", "transaction", i, "nonce", transaction.Nonce, "value", transaction.Value, "feecap", transaction.FeeCap)

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
func signEIP1559TxsHigherThanBaseFee(ctx context.Context, n int, baseFeePerGas uint64, nonce *uint64, toAddress, fromAddress common.Address) ([]types.Transaction, error) {
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

		devnet.Logger(ctx).Trace("HIGHER", "transaction", i, "nonce", transaction.Nonce, "value", transaction.Value, "feecap", transaction.FeeCap)

		signerKey := accounts.SigKey(fromAddress)
		if signerKey == nil {
			return nil, fmt.Errorf("devnet.signEIP1559TxsHigherThanBaseFee failed to SignTx: private key not found for address %s", fromAddress)
		}

		signedTransaction, err := types.SignTx(transaction, signer, signerKey)
		if err != nil {
			return nil, err
		}

		signedTransactions = append(signedTransactions, signedTransaction)
		*nonce++
	}

	return signedTransactions, nil
}

func SendManyTransactions(ctx context.Context, signedTransactions []types.Transaction) ([]common.Hash, error) {
	logger := devnet.Logger(ctx)

	logger.Info(fmt.Sprintf("Sending %d transactions to the txpool...", len(signedTransactions)))
	hashes := make([]common.Hash, len(signedTransactions))

	for idx, txn := range signedTransactions {
		hash, err := devnet.SelectNode(ctx).SendTransaction(txn)
		if err != nil {
			logger.Error("failed SendTransaction", "error", err)
			return nil, err
		}
		hashes[idx] = hash
	}

	return hashes, nil
}
