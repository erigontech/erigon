package accounts_steps

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/cmd/devnet/transactions"
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

	//if fbal, err := faucet.Balance(chainCtx); err == nil {
	//	fmt.Println(faucet.Address(), fbal)
	//}

	_, hash, err := faucet.Send(chainCtx, account.Address, ethAmount)

	if err != nil {
		return 0, err
	}

	if _, err = transactions.AwaitTransactions(chainCtx, hash); err != nil {
		return 0, fmt.Errorf("failed to get transfer tx: %v", err)
	}

	//if fbal, err := faucet.Balance(chainCtx); err == nil {
	//	fmt.Println(faucet.Address(), fbal)
	//}

	node := devnet.SelectBlockProducer(chainCtx)
	balance, err := node.GetBalance(account.Address, requests.BlockNumbers.Latest)
	//fmt.Println(account.Address, balance)

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
