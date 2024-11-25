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

package contracts

import (
	"context"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/cmd/devnet/accounts"
	"github.com/erigontech/erigon/cmd/devnet/blocks"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/jsonrpc"
)

func TransactOpts(ctx context.Context, sender libcommon.Address) (*bind.TransactOpts, error) {
	node := devnet.SelectNode(ctx)

	transactOpts, err := bind.NewKeyedTransactorWithChainID(accounts.SigKey(sender), node.ChainID())

	if err != nil {
		return nil, err
	}

	count, err := node.GetTransactionCount(sender, rpc.PendingBlock)

	if err != nil {
		return nil, err
	}

	transactOpts.Nonce = count

	return transactOpts, nil
}

func DeploymentTransactor(ctx context.Context, deployer libcommon.Address) (*bind.TransactOpts, bind.ContractBackend, error) {
	node := devnet.SelectNode(ctx)

	transactOpts, err := TransactOpts(ctx, deployer)

	if err != nil {
		return nil, nil, err
	}

	return transactOpts, NewBackend(node), nil
}

func Deploy[C any](ctx context.Context, deployer libcommon.Address, deploy func(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *C, error)) (libcommon.Address, types.Transaction, *C, error) {
	transactOpts, err := bind.NewKeyedTransactorWithChainID(accounts.SigKey(deployer), devnet.CurrentChainID(ctx))

	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	return DeployWithOps[C](ctx, transactOpts, deploy)
}

func DeployWithOps[C any](ctx context.Context, auth *bind.TransactOpts, deploy func(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *C, error)) (libcommon.Address, types.Transaction, *C, error) {
	node := devnet.SelectNode(ctx)

	count, err := node.GetTransactionCount(auth.From, rpc.PendingBlock)

	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	auth.Nonce = count

	// deploy the contract and get the contract handler
	address, tx, contract, err := deploy(auth, NewBackend(node))

	return address, tx, contract, err
}

var DeploymentChecker = blocks.BlockHandlerFunc(
	func(ctx context.Context, node devnet.Node, block *requests.Block, transaction *jsonrpc.RPCTransaction) error {
		if err := blocks.CompletionChecker(ctx, node, block, transaction); err != nil {
			return nil
		}

		return nil
	})
