package accounts

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/blocks"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
)

type Faucet struct {
	sync.Mutex
	chainName       string
	source          *accounts.Account
	transactOpts    *bind.TransactOpts
	contractAddress libcommon.Address
	contract        *contracts.Faucet
	deployer        *deployer
}

type deployer struct {
	sync.WaitGroup
	faucet *Faucet
}

func (d *deployer) deploy(ctx context.Context, node devnet.Node) {
	logger := devnet.Logger(ctx)

	// deploy the contract and get the contract handler
	deployCtx := devnet.WithCurrentNode(ctx, node)

	waiter, deployCancel := blocks.BlockWaiter(deployCtx, contracts.DeploymentChecker)
	defer deployCancel()

	address, transaction, contract, err := contracts.DeployWithOps(deployCtx, d.faucet.transactOpts, contracts.DeployFaucet)

	if err != nil {
		d.faucet.Lock()
		defer d.faucet.Unlock()

		d.faucet.deployer = nil
		d.faucet.transactOpts = nil
		logger.Error("failed to deploy faucet", "chain", d.faucet.chainName, "err", err)
		return
	}

	block, err := waiter.Await(transaction.Hash())

	if err != nil {
		d.faucet.Lock()
		defer d.faucet.Unlock()

		d.faucet.deployer = nil
		d.faucet.transactOpts = nil
		logger.Error("failed to deploy faucet", "chain", d.faucet.chainName, "err", err)
		return
	}

	logger.Info("Faucet deployed", "chain", d.faucet.chainName, "block", block.BlockNumber, "addr", address)

	d.faucet.contractAddress = address
	d.faucet.contract = contract

	// make the amount received a fraction of the source
	//if sbal, err := node.GetBalance(f.source, requests.BlockNumbers.Latest); err == nil {
	//	fmt.Println(f.source, sbal)
	//}

	waiter, receiveCancel := blocks.BlockWaiter(deployCtx, blocks.CompletionChecker)
	defer receiveCancel()

	received, receiveHash, err := d.faucet.Receive(deployCtx, d.faucet.source, 20000)

	if err != nil {
		logger.Error("Failed to receive faucet funds", "err", err)
		return
	}

	block, err = waiter.Await(receiveHash)

	if err != nil {
		d.faucet.Lock()
		defer d.faucet.Unlock()

		d.faucet.deployer = nil
		d.faucet.transactOpts = nil
		logger.Error("failed to deploy faucet", "chain", d.faucet.chainName, "err", err)
		return
	}

	logger.Info("Faucet funded", "chain", d.faucet.chainName, "block", block.BlockNumber, "addr", address, "received", received)

	d.faucet.Lock()
	defer d.faucet.Unlock()
	d.faucet.deployer = nil
	d.Done()
}

func NewFaucet(chainName string, source *accounts.Account) *Faucet {
	return &Faucet{
		chainName: chainName,
		source:    source,
	}
}

func (f *Faucet) Start(context context.Context) error {
	return nil
}

func (f *Faucet) Stop() {}

func (f *Faucet) Address() libcommon.Address {
	return f.contractAddress
}

func (f *Faucet) Contract() *contracts.Faucet {
	return f.contract
}

func (f *Faucet) Source() *accounts.Account {
	return f.source
}

func (f *Faucet) Balance(ctx context.Context) (*big.Int, error) {
	f.Lock()
	deployer := f.deployer
	f.Unlock()

	if deployer != nil {
		f.deployer.Wait()
	}

	node := devnet.SelectBlockProducer(devnet.WithCurrentNetwork(ctx, f.chainName))
	return node.GetBalance(f.contractAddress, requests.BlockNumbers.Latest)
}

func (f *Faucet) Send(ctx context.Context, destination *accounts.Account, eth float64) (*big.Int, libcommon.Hash, error) {
	f.Lock()
	deployer := f.deployer
	f.Unlock()

	if deployer != nil {
		f.deployer.Wait()
	}

	if f.transactOpts == nil {
		return nil, libcommon.Hash{}, fmt.Errorf("Faucet not initialized")
	}

	node := devnet.SelectNode(ctx)

	count, err := node.GetTransactionCount(f.source.Address, requests.BlockNumbers.Pending)

	if err != nil {
		return nil, libcommon.Hash{}, err
	}

	f.transactOpts.Nonce = count

	amount := accounts.EtherAmount(eth)
	trn, err := f.contract.Send(f.transactOpts, destination.Address, amount)

	if err != nil {
		return nil, libcommon.Hash{}, err
	}

	return amount, trn.Hash(), err
}

func (f *Faucet) Receive(ctx context.Context, source *accounts.Account, eth float64) (*big.Int, libcommon.Hash, error) {
	node := devnet.SelectNode(ctx)

	transactOpts, err := bind.NewKeyedTransactorWithChainID(source.SigKey(), node.ChainID())

	if err != nil {
		return nil, libcommon.Hash{}, err
	}

	count, err := node.GetTransactionCount(f.source.Address, requests.BlockNumbers.Pending)

	if err != nil {
		return nil, libcommon.Hash{}, err
	}

	transactOpts.Nonce = count

	transactOpts.Value = accounts.EtherAmount(eth)

	trn, err := (&contracts.FaucetRaw{Contract: f.contract}).Transfer(transactOpts)

	if err != nil {
		return nil, libcommon.Hash{}, err
	}

	return transactOpts.Value, trn.Hash(), nil
}

func (f *Faucet) NodeCreated(ctx context.Context, node devnet.Node) {
}

func (f *Faucet) NodeStarted(ctx context.Context, node devnet.Node) {
	logger := devnet.Logger(ctx)

	if strings.HasPrefix(node.Name(), f.chainName) && node.IsBlockProducer() {
		f.Lock()
		defer f.Unlock()

		if f.transactOpts != nil {
			return
		}

		var err error

		f.transactOpts, err = bind.NewKeyedTransactorWithChainID(f.source.SigKey(), node.ChainID())

		if err != nil {
			logger.Error("failed to get transaction ops", "address", f.source, "err", err)
			return
		}

		f.deployer = &deployer{
			faucet: f,
		}

		f.deployer.Add(1)

		go f.deployer.deploy(ctx, node)
	}
}
