package accounts_steps

import (
	"context"
	"encoding/json"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/cmd/devnet/transactions"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
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

	node := devnet.SelectBlockProducer(chainCtx)

	if fbal, err := node.GetBalance(faucet.Source().Address, requests.BlockNumbers.Latest); err == nil {
		fmt.Println(faucet.Source().Address, fbal)
		if fbal.Uint64() > 0 {
			txHash, err := transactions.Transfer(chainCtx, account.Address.Hex(), faucet.Source().Address.Hex(), 100, true)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("TF", txHash)
		}
	}

	balance, _ := node.GetBalance(account.Address, requests.BlockNumbers.Latest)
	fmt.Println(chainName, account.Address, balance)
	balance, _ = faucet.Balance(chainCtx)
	fmt.Println(chainName, faucet.Address(), balance)

	_, hash, err := faucet.Send(chainCtx, account, ethAmount)

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

	//if err != nil {
	//	return 0, fmt.Errorf("Failed to get post transfer logs: %w", err)
	//}

	for logs.Next() {
		fmt.Println(logs.Event.Destination, logs.Event.Amount)
	}

	traceResults, err := node.TraceTransaction(hash)

	if err != nil {
		fmt.Println(err)
	} else {
		for _, traceResult := range traceResults {
			fmt.Printf("%+v [%+v]\n", traceResult, traceResult.Result)

			accountResult, err := node.DebugAccountAt(traceResult.BlockHash, traceResult.TransactionPosition, faucet.Address())

			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(faucet.Address(), accountResult)
			}

			accountCode, err := node.GetCode(faucet.Address(), requests.BlockNumber(traceResult.BlockHash.Hex()))

			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(faucet.Address(), accountCode)
			}

			callResults, err := node.TraceCall(fmt.Sprintf("0x%x", blockNum), ethapi.CallArgs{
				From: &traceResult.Action.From,
				To:   &traceResult.Action.To,
				Data: &traceResult.Action.Input,
			}, requests.TraceOpts.StateDiff, requests.TraceOpts.Trace, requests.TraceOpts.VmTrace)

			if err != nil {
				fmt.Println(err)
			} else {
				results, _ := json.MarshalIndent(callResults, "  ", "  ")
				fmt.Println(string(results))
			}
		}
	}

	balance, err = node.GetBalance(account.Address, requests.BlockNumbers.Latest)
	fmt.Println(chainName, account.Address, balance)
	balance, _ = node.GetBalance(faucet.Address(), requests.BlockNumbers.Latest)
	fmt.Println(chainName, faucet.Address(), balance)

	if err != nil {
		return 0, fmt.Errorf("Failed to get post transfer balance: %w", err)
	}

	return balance.Uint64(), nil
}

func GetBalance(ctx context.Context, accountName string, blockNum requests.BlockNumber) (uint64, error) {
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

	bal, err := node.GetBalance(account.Address, blockNum)

	if err != nil {
		logger.Error("FAILURE", "error", err)
		return 0, err
	}

	logger.Info("SUCCESS", "balance", bal)

	return bal.Uint64(), nil
}

func GetNonce(ctx context.Context, address libcommon.Address) (uint64, error) {
	node := devnet.CurrentNode(ctx)

	if node == nil {
		node = devnet.SelectBlockProducer(ctx)
	}

	res, err := node.GetTransactionCount(address, requests.BlockNumbers.Latest)

	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count for address 0x%x: %v", address, err)
	}

	return res.Uint64(), nil
}
