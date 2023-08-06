package contracts_steps

import (
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
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(DeployChildChainSender),
		scenarios.StepHandler(DeployRootChainReceiver),
		scenarios.StepHandler(ProcessChildTransfers),
	)
}

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

func ProcessChildTransfers(ctx context.Context, sourceName string, numberOfTransfers int, minTransfer int, maxTransfer int) error {
	source := accounts.GetAccount(sourceName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.DevChainName)

	auth, err := contracts.TransactOpts(ctx, source.Address)

	if err != nil {
		return err
	}

	sender, _ := scenarios.Param[*contracts.ChildSender](ctx, "childSender")

	receiver, _ := scenarios.Param[*contracts.RootReceiver](ctx, "rootReceiver")
	receiverAddress, _ := scenarios.Param[libcommon.Address](ctx, "rootReceiverAddress")

	receivedChan := make(chan *contracts.RootReceiverReceived)
	receiverSubscription, err := receiver.WatchReceived(&bind.WatchOpts{}, receivedChan)

	if err != nil {
		return fmt.Errorf("Receiver subscription failed: %w", err)
	}

	defer receiverSubscription.Unsubscribe()

	Uint256, _ := abi.NewType("uint256", "", nil)
	Address, _ := abi.NewType("address", "", nil)

	args := abi.Arguments{
		{Name: "from", Type: Address},
		{Name: "amount", Type: Uint256},
	}

	heimdall := services.Heimdall(ctx)
	proofGenerator := services.ProofGenerator(ctx)

	var sendTxHashes []libcommon.Hash
	var lastTxBlockNum *big.Int
	var receiptTopic libcommon.Hash

	zeroHash := libcommon.Hash{}

	for i := 0; i < numberOfTransfers; i++ {
		amount := accounts.EtherAmount(float64(minTransfer))

		err = func() error {
			waiter, cancel := blocks.BlockWaiter(ctx, blocks.CompletionChecker)
			defer cancel()

			transaction, err := sender.SendToRoot(auth, amount)

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
					callResults, err := node.TraceCall(rpc.AsBlockReference(block.Number), ethapi.CallArgs{
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

			sendTxHashes = append(sendTxHashes, transaction.Hash())
			lastTxBlockNum = block.Number

			blockNum := block.Number.Uint64()

			logs, err := sender.FilterMessageSent(&bind.FilterOpts{
				Start: blockNum,
				End:   &blockNum,
			})

			if err != nil {
				return fmt.Errorf("Failed to get post sync logs: %w", err)
			}

			for logs.Next() {
				values, err := args.Unpack(logs.Event.Message)

				if err != nil {
					return fmt.Errorf("Failed unpack log args: %w", err)
				}

				recceiverAddressValue, ok := values[0].(libcommon.Address)

				if !ok {
					return fmt.Errorf("Unexpected arg type: expected: %T, got %T", libcommon.Address{}, values[0])
				}

				sender, ok := values[1].(libcommon.Address)

				if !ok {
					return fmt.Errorf("Unexpected arg type: expected: %T, got %T", libcommon.Address{}, values[0])
				}

				sentAmount, ok := values[1].(*big.Int)

				if !ok {
					return fmt.Errorf("Unexpected arg type: expected: %T, got %T", &big.Int{}, values[1])
				}

				if recceiverAddressValue != receiverAddress {
					return fmt.Errorf("Unexpected sender: expected: %s, got %s", receiverAddress, recceiverAddressValue)
				}

				if sender != source.Address {
					return fmt.Errorf("Unexpected sender: expected: %s, got %s", source.Address, sender)
				}

				if amount.Cmp(sentAmount) != 0 {
					return fmt.Errorf("Unexpected sent amount: expected: %s, got %s", amount, sentAmount)
				}

				if receiptTopic == zeroHash {
					receiptTopic = logs.Event.Raw.Topics[0]
				}
			}

			return nil
		}()

		if err != nil {
			return err
		}

		auth.Nonce = (&big.Int{}).Add(auth.Nonce, big.NewInt(1))
	}

	devnet.Logger(ctx).Info("Waiting for checkpoint")

	err = heimdall.AwaitCheckpoint(ctx, lastTxBlockNum)

	if err != nil {
		return err
	}

	for _, hash := range sendTxHashes {
		payload, err := proofGenerator.GenerateExitPayload(ctx, hash, receiptTopic, 0)

		waiter, cancel := blocks.BlockWaiter(ctx, blocks.CompletionChecker)
		defer cancel()

		if err != nil {
			return err
		}

		transaction, err := receiver.ReceiveMessage(auth, payload)

		if err != nil {
			return err
		}

		if _, err := waiter.Await(transaction.Hash()); err != nil {
			return err
		}

	}

	receivedCount := 0

	devnet.Logger(ctx).Info("Waiting for receive events")

	for received := range receivedChan {
		if received.Source != source.Address {
			return fmt.Errorf("Source address mismatched: expected: %s, got: %s", source.Address, received.Source)
		}

		if received.Amount.Cmp(accounts.EtherAmount(float64(minTransfer))) != 0 {
			return fmt.Errorf("Amount mismatched: expected: %s, got: %s", accounts.EtherAmount(float64(minTransfer)), received.Amount)
		}

		receivedCount++
		if receivedCount == numberOfTransfers {
			break
		}
	}

	return nil
}
