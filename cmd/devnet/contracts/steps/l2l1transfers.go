package contracts_steps

import (
	"context"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/blocks"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/params/networkname"
)

func DeployChildChainSender(ctx context.Context, deployerName string) (context.Context, error) {
	deployer := accounts.GetAccount(deployerName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.BorDevnetChainName)

	auth, backend, err := contracts.DeploymentTransactor(ctx, deployer.Address)

	if err != nil {
		return nil, err
	}

	receiverAddress, _ := scenarios.Param[libcommon.Address](ctx, "rootReceiverAddress")

	waiter, cancel := blocks.BlockWaiter(ctx, contracts.DeploymentChecker)
	defer cancel()

	address, transaction, contract, err := contracts.DeployChildSender(auth, backend, receiverAddress)

	if err != nil {
		return nil, err
	}

	block, err := waiter.Await(transaction.Hash())

	if err != nil {
		return nil, err
	}

	devnet.Logger(ctx).Info("ChildSender deployed", "chain", networkname.BorDevnetChainName, "block", block.Number, "addr", address)

	return scenarios.WithParam(ctx, "childSenderAddress", address).
		WithParam("childSender", contract), nil
}

func DeployRootChainReceiver(ctx context.Context, deployerName string) (context.Context, error) {
	deployer := accounts.GetAccount(deployerName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.DevChainName)

	auth, backend, err := contracts.DeploymentTransactor(ctx, deployer.Address)

	if err != nil {
		return nil, err
	}

	waiter, cancel := blocks.BlockWaiter(ctx, contracts.DeploymentChecker)
	defer cancel()

	heimdall := services.Heimdall(ctx)

	address, transaction, contract, err := contracts.DeployChildSender(auth, backend, heimdall.RootChainAddress())

	if err != nil {
		return nil, err
	}

	block, err := waiter.Await(transaction.Hash())

	if err != nil {
		return nil, err
	}

	devnet.Logger(ctx).Info("RootReceiver deployed", "chain", networkname.BorDevnetChainName, "block", block.Number, "addr", address)

	return scenarios.WithParam(ctx, "rootReceiverAddress", address).
		WithParam("rootReceiver", contract), nil
}
