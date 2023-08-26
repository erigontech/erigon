package contracts

import (
	"context"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/blocks"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
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
