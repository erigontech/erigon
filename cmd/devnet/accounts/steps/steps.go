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

package accounts_steps

import (
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/fastjson"
	"github.com/erigontech/erigon/cmd/devnet/accounts"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/scenarios"
	"github.com/erigontech/erigon/cmd/devnet/services"
	"github.com/erigontech/erigon/cmd/devnet/transactions"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/requests"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(CreateAccount),
		scenarios.StepHandler(CreateAccountWithFunds),
		scenarios.StepHandler(SendFunds),
		scenarios.StepHandler(GetBalance),
		scenarios.StepHandler(GetNonce),
	)
}

func CreateAccount(ctx context.Context, name string) (*accounts.Account, error) {
	if account := accounts.GetAccount(name); account != nil {
		return account, nil
	}

	return accounts.NewAccount(name), nil
}

func CreateAccountWithFunds(ctx context.Context, chainName string, name string, ethAmount float64) (*accounts.Account, error) {
	account, err := CreateAccount(ctx, name)

	if err != nil {
		return nil, err
	}

	if _, err = SendFunds(ctx, chainName, name, ethAmount); err != nil {
		return nil, err
	}

	return account, nil
}

func SendFunds(ctx context.Context, chainName string, name string, ethAmount float64) (uint64, error) {
	chainCtx := devnet.WithCurrentNetwork(ctx, chainName)
	faucet := services.Faucet(chainCtx)

	account := accounts.GetAccount(name)

	if account == nil {
		return 0, fmt.Errorf("Unknown account: %s", name)
	}

	facuetStartingBalance, _ := faucet.Balance(chainCtx)

	sent, hash, err := faucet.Send(chainCtx, account, ethAmount)

	if err != nil {
		return 0, err
	}

	blockMap, err := transactions.AwaitTransactions(chainCtx, hash)

	if err != nil {
		return 0, fmt.Errorf("Failed to get transfer tx: %w", err)
	}

	blockNum, _ := blockMap[hash]

	logs, err := faucet.Contract().FilterSent(&bind.FilterOpts{
		Start: blockNum,
		End:   &blockNum,
	})

	if err != nil {
		return 0, fmt.Errorf("Failed to get post transfer logs: %w", err)
	}

	sendConfirmed := false

	for logs.Next() {
		if account.Address != logs.Event.Destination {
			return 0, fmt.Errorf("Unexpected send destination: %s", logs.Event.Destination)
		}

		if sent.Cmp(logs.Event.Amount) != 0 {
			return 0, fmt.Errorf("Unexpected send amount: %s", logs.Event.Amount)
		}

		sendConfirmed = true
	}

	node := devnet.SelectBlockProducer(chainCtx)

	if !sendConfirmed {
		logger := devnet.Logger(chainCtx)

		traceResults, err := node.TraceTransaction(hash)

		if err != nil {
			return 0, fmt.Errorf("Send transaction failure: transaction trace failed: %w", err)
		}

		for _, traceResult := range traceResults {
			accountResult, err := node.DebugAccountAt(traceResult.BlockHash, traceResult.TransactionPosition, faucet.Address())

			if err != nil {
				return 0, fmt.Errorf("Send transaction failure: account debug failed: %w", err)
			}

			logger.Info("Faucet account details", "address", faucet.Address(), "account", accountResult)

			accountCode, err := node.GetCode(faucet.Address(), rpc.AsBlockReference(traceResult.BlockHash))

			if err != nil {
				return 0, fmt.Errorf("Send transaction failure: get account code failed: %w", err)
			}

			logger.Info("Faucet account code", "address", faucet.Address(), "code", accountCode)

			callResults, err := node.TraceCall(rpc.AsBlockReference(blockNum), ethapi.CallArgs{
				From: &traceResult.Action.From,
				To:   &traceResult.Action.To,
				Data: &traceResult.Action.Input,
			}, requests.TraceOpts.StateDiff, requests.TraceOpts.Trace, requests.TraceOpts.VmTrace)

			if err != nil {
				return 0, fmt.Errorf("Send transaction failure: trace call failed: %w", err)
			}

			results, _ := fastjson.MarshalIndent(callResults, "  ", "  ")
			logger.Debug("Send transaction call trace", "hash", hash, "trace", string(results))
		}
	}

	balance, err := faucet.Balance(chainCtx)

	if err != nil {
		return 0, fmt.Errorf("Failed to get post transfer faucet balance: %w", err)
	}

	if balance.Cmp((&big.Int{}).Sub(facuetStartingBalance, sent)) != 0 {
		return 0, fmt.Errorf("Unexpected post transfer faucet balance got: %s:, expected: %s", balance, (&big.Int{}).Sub(facuetStartingBalance, sent))
	}

	balance, err = node.GetBalance(account.Address, rpc.LatestBlock)

	if err != nil {
		return 0, fmt.Errorf("Failed to get post transfer balance: %w", err)
	}

	if balance.Cmp(sent) != 0 {
		return 0, fmt.Errorf("Unexpected post transfer balance got: %s:, expected: %s", balance, sent)
	}

	return balance.Uint64(), nil
}

func GetBalance(ctx context.Context, accountName string, blockNum rpc.BlockNumber) (uint64, error) {
	logger := devnet.Logger(ctx)

	node := devnet.CurrentNode(ctx)

	if node == nil {
		node = devnet.SelectBlockProducer(ctx)
	}

	account := accounts.GetAccount(accountName)

	if account == nil {
		err := fmt.Errorf("Unknown account: %s", accountName)
		logger.Error("FAILURE", "error", err)
		return 0, err
	}

	logger.Info("Getting balance", "address", account.Address)

	bal, err := node.GetBalance(account.Address, rpc.AsBlockReference(blockNum))

	if err != nil {
		logger.Error("FAILURE", "error", err)
		return 0, err
	}

	logger.Info("SUCCESS", "balance", bal)

	return bal.Uint64(), nil
}

func GetNonce(ctx context.Context, address common.Address) (uint64, error) {
	node := devnet.CurrentNode(ctx)

	if node == nil {
		node = devnet.SelectBlockProducer(ctx)
	}

	res, err := node.GetTransactionCount(address, rpc.LatestBlock)

	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count for address 0x%x: %v", address, err)
	}

	return res.Uint64(), nil
}
