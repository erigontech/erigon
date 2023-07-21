package contracts_steps

import (
	"context"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/params/networkname"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(DeployChildChainReceiver),
		scenarios.StepHandler(DeployRootChainSender),
		scenarios.StepHandler(ProcessTransfers),
	)
}

func DeployRootChainSender(ctx context.Context, deployerName string) (context.Context, error) {
	deployer := accounts.GetAccount(deployerName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.DevChainName)

	auth, backend, err := contracts.DeploymentTransactor(ctx, deployer.Address)

	if err != nil {
		return nil, err
	}

	childStateReceiver, _ := scenarios.Param[libcommon.Address](ctx, "childStateReceiverAddress")

	heimdall := services.Heimdall(ctx)

	address, _, contract, err := contracts.DeployRootSender(auth, backend, heimdall.StateSenderAddress(), childStateReceiver)

	if err != nil {
		return nil, err
	}

	return scenarios.WithParam(ctx, "rootSenderAddress", address).
		WithParam("rootSender", contract), nil
}

func DeployChildChainReceiver(ctx context.Context, deployerName string) (context.Context, error) {
	deployer := accounts.GetAccount(deployerName)

	address, contract, err := contracts.Deploy(devnet.WithCurrentNetwork(ctx, networkname.BorDevnetChainName), deployer.Address, contracts.DeployChildReceiver)

	if err != nil {
		return nil, err
	}

	return scenarios.WithParam(ctx, "childReceiverAddress", address).
		WithParam("childReceiver", contract), nil
}

func ProcessTransfers(ctx context.Context, sourceName string, numberOfTransfers int, minTransfer int, maxTransfer int) error {
	source := accounts.GetAccount(sourceName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.DevChainName)

	auth, err := contracts.TransactOpts(ctx, source.Address)

	if err != nil {
		return err
	}

	sender, _ := scenarios.Param[*contracts.RootSender](ctx, "rootSender")

	for i := 0; i < numberOfTransfers; i++ {
		amount := accounts.EtherAmount(float64(minTransfer))

		if _, err = sender.SendToChild(auth, amount); err != nil {
			return err
		}

		auth.Nonce = (&big.Int{}).Add(auth.Nonce, big.NewInt(1))
	}

	return nil
}
