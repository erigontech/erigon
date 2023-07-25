package contracts_steps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/blocks"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/event"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(DeployChildChainReceiver),
		scenarios.StepHandler(DeployRootChainSender),
		scenarios.StepHandler(GenerateSyncEvents),
		scenarios.StepHandler(ProcessTransfers),
	)
}

func GenerateSyncEvents(ctx context.Context, senderName string, numberOfTransfers int, minTransfer int, maxTransfer int) error {
	sender := accounts.GetAccount(senderName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.DevChainName)

	auth, err := contracts.TransactOpts(ctx, sender.Address)

	if err != nil {
		return err
	}

	heimdall := services.Heimdall(ctx)

	waiter, cancel := blocks.BlockWaiter(ctx, contracts.DeploymentChecker)
	defer cancel()

	stateSender := heimdall.StateSenderContract()

	receiver, _ := scenarios.Param[libcommon.Address](ctx, "childReceiverAddress")

	Uint256, _ := abi.NewType("uint256", "", nil)
	Address, _ := abi.NewType("address", "", nil)

	args := abi.Arguments{
		{Name: "from", Type: Address},
		{Name: "amount", Type: Uint256},
	}

	//for i := 0; i < numberOfTransfers; i++ {
	sendData, err := args.Pack(sender.Address, big.NewInt(int64(minTransfer)))

	if err != nil {
		return err
	}

	transaction, err := stateSender.SyncState(auth, receiver, sendData)

	if err != nil {
		return err
	}

	block, err := waiter.Await(transaction.Hash())

	if err != nil {
		return fmt.Errorf("Failed to wait for sync block: %w", err)
	}

	blockNum := block.BlockNumber.Uint64()

	logs, err := stateSender.FilterStateSynced(&bind.FilterOpts{
		Start: blockNum,
		End:   &blockNum,
	}, nil, nil)

	if err != nil {
		return fmt.Errorf("Failed to get post sync logs: %w", err)
	}

	sendConfirmed := false

	for logs.Next() {
		if logs.Event.ContractAddress != receiver {
			return fmt.Errorf("Receiver address mismatched: expected: %s, got: %s", receiver, logs.Event.ContractAddress)
		}

		if !bytes.Equal(logs.Event.Data, sendData) {
			return fmt.Errorf("Send data mismatched: expected: %s, got: %s", sendData, logs.Event.Data)
		}

		sendConfirmed = true
	}

	if !sendConfirmed {
		return fmt.Errorf("No post sync log received")
	}

	//	auth.Nonce = (&big.Int{}).Add(auth.Nonce, big.NewInt(1))
	//}

	return nil
}

func DeployRootChainSender(ctx context.Context, deployerName string) (context.Context, error) {
	deployer := accounts.GetAccount(deployerName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.DevChainName)

	auth, backend, err := contracts.DeploymentTransactor(ctx, deployer.Address)

	if err != nil {
		return nil, err
	}

	childStateReceiver, _ := scenarios.Param[libcommon.Address](ctx, "childReceiverAddress")

	heimdall := services.Heimdall(ctx)

	waiter, cancel := blocks.BlockWaiter(ctx, contracts.DeploymentChecker)
	defer cancel()

	address, transaction, contract, err := contracts.DeployRootSender(auth, backend, heimdall.StateSenderAddress(), childStateReceiver)

	if err != nil {
		return nil, err
	}

	if _, err = waiter.Await(transaction.Hash()); err != nil {
		return nil, err
	}

	return scenarios.WithParam(ctx, "rootSenderAddress", address).
		WithParam("rootSender", contract), nil
}

func DeployChildChainReceiver(ctx context.Context, deployerName string) (context.Context, error) {
	deployer := accounts.GetAccount(deployerName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.BorDevnetChainName)

	waiter, cancel := blocks.BlockWaiter(ctx, contracts.DeploymentChecker)
	defer cancel()

	address, transaction, contract, err := contracts.Deploy(ctx, deployer.Address, contracts.DeployChildReceiver)

	if err != nil {
		return nil, err
	}

	if _, err = waiter.Await(transaction.Hash()); err != nil {
		return nil, err
	}

	return scenarios.WithParam(ctx, "childReceiverAddress", address).
		WithParam("childReceiver", contract), nil
}

func ProcessTransfers(ctx context.Context, sourceName string, numberOfTransfers int, minTransfer int, maxTransfer int, wait bool) error {
	source := accounts.GetAccount(sourceName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.DevChainName)

	auth, err := contracts.TransactOpts(ctx, source.Address)

	if err != nil {
		return err
	}

	sender, _ := scenarios.Param[*contracts.RootSender](ctx, "rootSender")
	stateSender := services.Heimdall(ctx).StateSenderContract()

	receiver, _ := scenarios.Param[*contracts.ChildReceiver](ctx, "childReceiver")
	receiverAddress, _ := scenarios.Param[libcommon.Address](ctx, "childReceiverAddress")

	var receiverSubscription event.Subscription
	var receivedChan chan *contracts.ChildReceiverReceived

	if wait {
		receivedChan := make(chan *contracts.ChildReceiverReceived)
		receiverSubscription, err = receiver.WatchReceived(&bind.WatchOpts{}, receivedChan)

		if err != nil {
			return fmt.Errorf("Receiver subscription failed: %w", err)
		}

		defer receiverSubscription.Unsubscribe()
	}

	Uint256, _ := abi.NewType("uint256", "", nil)
	Address, _ := abi.NewType("address", "", nil)

	args := abi.Arguments{
		{Name: "from", Type: Address},
		{Name: "amount", Type: Uint256},
	}

	for i := 0; i < numberOfTransfers; i++ {
		amount := accounts.EtherAmount(float64(minTransfer))

		err = func() error {
			waiter, cancel := blocks.BlockWaiter(ctx, blocks.CompletionChecker)
			defer cancel()

			transaction, err := sender.SendToChild(auth, amount)

			if err != nil {
				return err
			}

			block, terr := waiter.Await(transaction.Hash())

			if terr != nil {
				node := devnet.SelectBlockProducer(ctx)

				traceResults, err := node.TraceTransaction(transaction.Hash())

				if err != nil {
					return fmt.Errorf("Send transaction failure: transaction trace failed: %w", err)
				}

				for _, traceResult := range traceResults {
					callResults, err := node.TraceCall(string(block.BlockNumber), ethapi.CallArgs{
						From: &traceResult.Action.From,
						To:   &traceResult.Action.To,
						Data: &traceResult.Action.Input,
					}, requests.TraceOpts.StateDiff, requests.TraceOpts.Trace, requests.TraceOpts.VmTrace)

					if err != nil {
						return fmt.Errorf("Send transaction failure: trace call failed: %w", err)
					}

					results, _ := json.MarshalIndent(callResults, "  ", "  ")
					fmt.Println(string(results))
				}

				return terr
			}

			blockNum := block.BlockNumber.Uint64()

			logs, err := stateSender.FilterStateSynced(&bind.FilterOpts{
				Start: blockNum,
				End:   &blockNum,
			}, nil, nil)

			if err != nil {
				return fmt.Errorf("Failed to get post sync logs: %w", err)
			}

			for logs.Next() {
				if logs.Event.ContractAddress != receiverAddress {
					return fmt.Errorf("Receiver address mismatched: expected: %s, got: %s", receiverAddress, logs.Event.ContractAddress)
				}

				values, err := args.Unpack(logs.Event.Data)

				if err != nil {
					return fmt.Errorf("Failed unpack log args: %w", err)
				}

				sender, ok := values[0].(libcommon.Address)

				if !ok {
					return fmt.Errorf("Unexpected arg type: expected: %T, got %T", libcommon.Address{}, values[0])
				}

				sentAmount, ok := values[1].(*big.Int)

				if !ok {
					return fmt.Errorf("Unexpected arg type: expected: %T, got %T", &big.Int{}, values[1])
				}

				if sender != source.Address {
					return fmt.Errorf("Unexpected sender: expected: %s, got %s", source.Address, sender)
				}

				if amount.Cmp(sentAmount) != 0 {
					return fmt.Errorf("Unexpected sent amount: expected: %s, got %s", amount, sentAmount)
				}
			}

			return nil
		}()

		if err != nil {
			return err
		}

		auth.Nonce = (&big.Int{}).Add(auth.Nonce, big.NewInt(1))
	}

	if wait {
		receivedCount := 0

		devnet.Logger(ctx).Info("Waiting for receive events")

		for received := range receivedChan {
			fmt.Println(received)
			receivedCount++
			if receivedCount == numberOfTransfers {
				break
			}
		}
	}

	return nil
}
