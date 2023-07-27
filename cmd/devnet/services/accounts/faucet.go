package accounts

import (
	"context"
	"math/big"
	"strings"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	//"github.com/ledgerwatch/erigon/cmd/devnet/transactions"
)

type Faucet struct {
	sync.Mutex
	chainName       string
	source          libcommon.Address
	transactOpts    *bind.TransactOpts
	contractAddress libcommon.Address
	contract        *contracts.Faucet
}

func NewFaucet(chainName string, source libcommon.Address) *Faucet {
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

func (f *Faucet) Balance(ctx context.Context) (*big.Int, error) {
	node := devnet.SelectBlockProducer(devnet.WithCurrentNetwork(ctx, f.chainName))
	return node.GetBalance(f.contractAddress, requests.BlockNumbers.Latest)
}

func (f *Faucet) Send(ctx context.Context, destination libcommon.Address, eth float64) (*big.Int, libcommon.Hash, error) {
	node := devnet.SelectNode(ctx)

	count, err := node.GetTransactionCount(f.source, requests.BlockNumbers.Pending)

	if err != nil {
		return nil, libcommon.Hash{}, err
	}

	f.transactOpts.Nonce = count

	amount := accounts.EtherAmount(eth)
	trn, err := f.contract.Send(f.transactOpts, destination, amount)
	return amount, trn.Hash(), err
}

func (f *Faucet) Receive(ctx context.Context, source libcommon.Address, eth float64) (*big.Int, libcommon.Hash, error) {
	node := devnet.SelectNode(ctx)

	transactOpts, err := bind.NewKeyedTransactorWithChainID(accounts.SigKey(source), node.ChainID())

	if err != nil {
		return nil, libcommon.Hash{}, err
	}

	count, err := node.GetTransactionCount(f.source, requests.BlockNumbers.Latest)

	if err != nil {
		return nil, libcommon.Hash{}, err
	}

	f.transactOpts.Nonce = count

	transactOpts.Value = accounts.EtherAmount(eth)
	transactOpts.GasLimit = uint64(200_000)
	transactOpts.GasPrice = big.NewInt(880_000_000)

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

		f.transactOpts, err = bind.NewKeyedTransactorWithChainID(accounts.SigKey(f.source), node.ChainID())

		if err != nil {
			logger.Error("failed to get transaction ops", "address", f.source, "err", err)
			return
		}

		go func() {
			// deploy the contract and get the contract handler
			deployCtx := devnet.WithCurrentNode(ctx, node)

			address, contract, err := contracts.DeployWithOps(deployCtx, f.transactOpts, contracts.DeployFaucet)

			if err != nil {
				f.Lock()
				defer f.Unlock()

				f.transactOpts = nil
				logger.Error("failed to deploy faucet", "err", err)
				return
			}

			logger.Info("Faucet deployed", "addr", address)

			f.contractAddress = address
			f.contract = contract

			// make the amount received a fraction of the source
			//if sbal, err := node.GetBalance(f.source, requests.BlockNumbers.Latest); err == nil {
			//	fmt.Println(f.source, sbal)
			//}

			_, _ /*hash*/, err = f.Receive(deployCtx, f.source, 20000)

			if err != nil {
				logger.Error("failed to deploy faucet", "err", err)
				return
			}

			//TODO do we need this if so we need to resolve dependencies
			//if _, err = transactions.AwaitTransactions(ctx, hash); err != nil {
			//	logger.Error("failed to call contract tx", "err", err)
			//}
		}()
	}
}
