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

package contracts_steps

import (
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/devnet/accounts"
	"github.com/erigontech/erigon/cmd/devnet/contracts"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/devnetutils"
	"github.com/erigontech/erigon/cmd/devnet/scenarios"
	"github.com/erigontech/erigon/cmd/devnet/transactions"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/requests"
)

func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(DeployAndCallLogSubscriber),
	)
}

func DeployAndCallLogSubscriber(ctx context.Context, deployer string) (*common.Hash, error) {
	logger := devnet.Logger(ctx)

	node := devnet.SelectNode(ctx)

	deployerAddress := common.HexToAddress(deployer)

	// subscriptionContract is the handler to the contract for further operations
	tx, address, subscriptionContract, transactOpts, err := DeploySubsriptionContract(node, deployerAddress)

	if err != nil {
		logger.Error("failed to create transaction", "error", err)
		return nil, err
	}

	hash := tx.Hash()

	eventHash, err := EmitFallbackEvent(node, subscriptionContract, transactOpts, logger)

	if err != nil {
		logger.Error("failed to emit events", "error", err)
		return nil, err
	}

	txToBlockMap, err := transactions.AwaitTransactions(ctx, hash, eventHash)

	if err != nil {
		return nil, fmt.Errorf("failed to call contract tx: %v", err)
	}

	blockNum := txToBlockMap[eventHash]

	block, err := node.GetBlockByNumber(ctx, rpc.AsBlockNumber(blockNum), true)

	if err != nil {
		return nil, err
	}

	logs, err := node.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   new(big.Int).SetUint64(blockNum),
		Addresses: []common.Address{address}})

	if err != nil || len(logs) == 0 {
		return nil, fmt.Errorf("failed to get logs: %v", err)
	}

	// compare the log events
	errs, ok := requests.Compare(requests.NewLog(eventHash, blockNum, address,
		devnetutils.GenerateTopic("SubscriptionEvent()"), hexutil.Bytes{}, 1,
		block.Hash, hexutil.Uint(0), false), logs[0])

	if !ok {
		logger.Error("Log result is incorrect", "errors", errs)
		return nil, fmt.Errorf("incorrect logs: %v", errs)
	}

	logger.Info("SUCCESS => Logs compared successfully, no discrepancies")

	return &hash, nil
}

// DeploySubsriptionContract creates and signs a transaction using the developer address, returns the contract and the signed transaction
func DeploySubsriptionContract(node devnet.Node, deployer common.Address) (types.Transaction, common.Address, *contracts.Subscription, *bind.TransactOpts, error) {
	// initialize transactOpts
	transactOpts, err := initializeTransactOps(node, deployer)

	if err != nil {
		return nil, common.Address{}, nil, nil, fmt.Errorf("failed to initialize transactOpts: %v", err)
	}

	// deploy the contract and get the contract handler
	address, tx, subscriptionContract, err := contracts.DeploySubscription(transactOpts, contracts.NewBackend(node))

	if err != nil {
		return nil, common.Address{}, nil, nil, fmt.Errorf("failed to deploy subscription: %v", err)
	}

	return tx, address, subscriptionContract, transactOpts, nil
}

// EmitFallbackEvent emits an event from the contract using the fallback method
func EmitFallbackEvent(node devnet.Node, subContract *contracts.Subscription, opts *bind.TransactOpts, logger log.Logger) (common.Hash, error) {
	logger.Info("EMITTING EVENT FROM FALLBACK...")

	// adding one to the nonce before initiating another transaction
	opts.Nonce.Add(opts.Nonce, big.NewInt(1))

	tx, err := subContract.Fallback(opts, []byte{})
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to emit event from fallback: %v", err)
	}

	return tx.Hash(), nil
}

// initializeTransactOps initializes the transactOpts object for a contract transaction
func initializeTransactOps(node devnet.Node, transactor common.Address) (*bind.TransactOpts, error) {
	count, err := node.GetTransactionCount(transactor, rpc.LatestBlock)

	if err != nil {
		return nil, fmt.Errorf("failed to get transaction count for address 0x%x: %v", transactor, err)
	}

	transactOpts, err := bind.NewKeyedTransactorWithChainID(accounts.SigKey(transactor), node.ChainID())

	if err != nil {
		return nil, fmt.Errorf("cannot create transactor with chainID %d, error: %v", node.ChainID(), err)
	}

	transactOpts.GasLimit = uint64(200_000)
	transactOpts.GasPrice = big.NewInt(880_000_000)
	transactOpts.Nonce = count

	return transactOpts, nil
}
