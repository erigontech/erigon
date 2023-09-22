package contracts_steps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
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
		scenarios.StepHandler(DeployChildChainReceiver),
		scenarios.StepHandler(DeployRootChainSender),
		scenarios.StepHandler(GenerateSyncEvents),
		scenarios.StepHandler(ProcessRootTransfers),
		scenarios.StepHandler(BatchProcessRootTransfers),
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

	stateSender := heimdall.StateSenderContract()

	receiver, _ := scenarios.Param[*contracts.ChildReceiver](ctx, "childReceiver")
	receiverAddress, _ := scenarios.Param[libcommon.Address](ctx, "childReceiverAddress")

	receivedChan := make(chan *contracts.ChildReceiverReceived)
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

	for i := 0; i < numberOfTransfers; i++ {
		err := func() error {
			sendData, err := args.Pack(sender.Address, big.NewInt(int64(minTransfer)))

			if err != nil {
				return err
			}

			waiter, cancel := blocks.BlockWaiter(ctx, contracts.DeploymentChecker)
			defer cancel()

			transaction, err := stateSender.SyncState(auth, receiverAddress, sendData)

			if err != nil {
				return err
			}

			block, err := waiter.Await(transaction.Hash())

			if err != nil {
				return fmt.Errorf("Failed to wait for sync block: %w", err)
			}

			blockNum := block.Number.Uint64()

			logs, err := stateSender.FilterStateSynced(&bind.FilterOpts{
				Start: blockNum,
				End:   &blockNum,
			}, nil, nil)

			if err != nil {
				return fmt.Errorf("Failed to get post sync logs: %w", err)
			}

			sendConfirmed := false

			for logs.Next() {
				if logs.Event.ContractAddress != receiverAddress {
					return fmt.Errorf("Receiver address mismatched: expected: %s, got: %s", receiverAddress, logs.Event.ContractAddress)
				}

				if !bytes.Equal(logs.Event.Data, sendData) {
					return fmt.Errorf("Send data mismatched: expected: %s, got: %s", sendData, logs.Event.Data)
				}

				sendConfirmed = true
			}

			if !sendConfirmed {
				return fmt.Errorf("No post sync log received")
			}

			auth.Nonce = (&big.Int{}).Add(auth.Nonce, big.NewInt(1))

			return nil
		}()

		if err != nil {
			return err
		}
	}

	receivedCount := 0

	devnet.Logger(ctx).Info("Waiting for receive events")

	for received := range receivedChan {
		if received.Source != sender.Address {
			return fmt.Errorf("Source address mismatched: expected: %s, got: %s", sender.Address, received.Source)
		}

		if received.Amount.Cmp(big.NewInt(int64(minTransfer))) != 0 {
			return fmt.Errorf("Amount mismatched: expected: %s, got: %s", big.NewInt(int64(minTransfer)), received.Amount)
		}

		receivedCount++
		if receivedCount == numberOfTransfers {
			break
		}
	}

	return nil
}

func DeployRootChainSender(ctx context.Context, deployerName string) (context.Context, error) {
	deployer := accounts.GetAccount(deployerName)
	ctx = devnet.WithCurrentNetwork(ctx, networkname.DevChainName)

	auth, backend, err := contracts.DeploymentTransactor(ctx, deployer.Address)

	if err != nil {
		return nil, err
	}

	receiverAddress, _ := scenarios.Param[libcommon.Address](ctx, "childReceiverAddress")

	heimdall := services.Heimdall(ctx)

	waiter, cancel := blocks.BlockWaiter(ctx, contracts.DeploymentChecker)
	defer cancel()

	address, transaction, contract, err := contracts.DeployRootSender(auth, backend, heimdall.StateSenderAddress(), receiverAddress)

	if err != nil {
		return nil, err
	}

	block, err := waiter.Await(transaction.Hash())

	if err != nil {
		return nil, err
	}

	devnet.Logger(ctx).Info("RootSender deployed", "chain", networkname.DevChainName, "block", block.Number, "addr", address)

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

	block, err := waiter.Await(transaction.Hash())

	if err != nil {
		return nil, err
	}

	devnet.Logger(ctx).Info("ChildReceiver deployed", "chain", networkname.BorDevnetChainName, "block", block.Number, "addr", address)

	return scenarios.WithParam(ctx, "childReceiverAddress", address).
		WithParam("childReceiver", contract), nil
}

func ProcessRootTransfers(ctx context.Context, sourceName string, numberOfTransfers int, minTransfer int, maxTransfer int) error {
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

	receivedChan := make(chan *contracts.ChildReceiverReceived)
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

			blockNum := block.Number.Uint64()

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

func BatchProcessRootTransfers(ctx context.Context, sourceName string, batches int, transfersPerBatch, minTransfer int, maxTransfer int) error {
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

	receivedChan := make(chan *contracts.ChildReceiverReceived)
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

	for b := 0; b < batches; b++ {

		hashes := make([]libcommon.Hash, transfersPerBatch)

		waiter, cancel := blocks.BlockWaiter(ctx, blocks.CompletionChecker)
		defer cancel()

		amount := accounts.EtherAmount(float64(minTransfer))

		for i := 0; i < transfersPerBatch; i++ {

			transaction, err := sender.SendToChild(auth, amount)

			if err != nil {
				return err
			}

			hashes[i] = transaction.Hash()
			auth.Nonce = (&big.Int{}).Add(auth.Nonce, big.NewInt(1))
		}

		blocks, err := waiter.AwaitMany(hashes...)

		if err != nil {
			return err
		}

		startBlock := uint64(math.MaxUint64)
		endBlock := uint64(0)

		for _, block := range blocks {
			blockNum := block.Number.Uint64()

			if blockNum < startBlock {
				startBlock = blockNum
			}
			if blockNum > endBlock {
				endBlock = blockNum
			}
		}

		logs, err := stateSender.FilterStateSynced(&bind.FilterOpts{
			Start: startBlock,
			End:   &endBlock,
		}, nil, nil)

		if err != nil {
			return fmt.Errorf("Failed to get post sync logs: %w", err)
		}

		receivedCount := 0

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

			receivedCount++
		}

		if receivedCount != transfersPerBatch {
			return fmt.Errorf("Expected %d, got: %d", transfersPerBatch, receivedCount)
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
		if receivedCount == batches*transfersPerBatch {
			break
		}
	}

	return nil
}
