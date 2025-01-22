package contracts

import (
	"context"
	"math/big"

	ethereum "github.com/erigontech/erigon"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/core/types"
)

var _ bind.ContractBackend = Backend{}

type Backend struct {
	caller     bind.ContractCaller
	transactor bind.ContractTransactor
	filterer   bind.ContractFilterer
}

func NewBackend(c bind.ContractCaller, t bind.ContractTransactor, f bind.ContractFilterer) Backend {
	return Backend{
		caller:     c,
		transactor: t,
		filterer:   f,
	}
}

func (b Backend) CodeAt(ctx context.Context, contract libcommon.Address, blockNumber *big.Int) ([]byte, error) {
	return b.caller.CodeAt(ctx, contract, blockNumber)
}

func (b Backend) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return b.caller.CallContract(ctx, call, blockNumber)
}

func (b Backend) PendingCodeAt(ctx context.Context, account libcommon.Address) ([]byte, error) {
	return b.transactor.PendingCodeAt(ctx, account)
}

func (b Backend) PendingNonceAt(ctx context.Context, account libcommon.Address) (uint64, error) {
	return b.transactor.PendingNonceAt(ctx, account)
}

func (b Backend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return b.transactor.SuggestGasPrice(ctx)
}

func (b Backend) EstimateGas(ctx context.Context, call ethereum.CallMsg) (gas uint64, err error) {
	return b.transactor.EstimateGas(ctx, call)
}

func (b Backend) SendTransaction(ctx context.Context, txn types.Transaction) error {
	return b.transactor.SendTransaction(ctx, txn)
}

func (b Backend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return b.filterer.FilterLogs(ctx, query)
}

func (b Backend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return b.filterer.SubscribeFilterLogs(ctx, query, ch)
}

var _ bind.ContractCaller = Caller{}

type Caller struct {
	db kv.TemporalRoDB
}

func (c Caller) CodeAt(ctx context.Context, contract libcommon.Address, blockNumber *big.Int) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (c Caller) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

var _ bind.ContractCaller = UnimplementedCaller{}

type UnimplementedCaller struct{}

func (c UnimplementedCaller) CodeAt(context.Context, libcommon.Address, *big.Int) ([]byte, error) {
	panic("not implemented UnimplementedCaller.CodeAt")
}

func (c UnimplementedCaller) CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error) {
	panic("not implemented UnimplementedCaller.CallContract")
}

var _ bind.ContractTransactor = UnimplementedTransactor{}

type UnimplementedTransactor struct{}

func (t UnimplementedTransactor) PendingCodeAt(context.Context, libcommon.Address) ([]byte, error) {
	panic("not implemented UnimplementedTransactor.PendingCodeAt")
}

func (t UnimplementedTransactor) PendingNonceAt(context.Context, libcommon.Address) (uint64, error) {
	panic("not implemented UnimplementedTransactor.PendingNonceAt")
}

func (t UnimplementedTransactor) SuggestGasPrice(context.Context) (*big.Int, error) {
	panic("not implemented UnimplementedTransactor.SuggestGasPrice")
}

func (t UnimplementedTransactor) EstimateGas(context.Context, ethereum.CallMsg) (gas uint64, err error) {
	panic("not implemented UnimplementedTransactor.EstimateGas")
}

func (t UnimplementedTransactor) SendTransaction(context.Context, types.Transaction) error {
	panic("not implemented UnimplementedTransactor.SendTransaction")
}

var _ bind.ContractFilterer = UnimplementedFilterer{}

type UnimplementedFilterer struct{}

func (f UnimplementedFilterer) FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
	panic("not implemented UnimplementedFilterer.FilterLogs")
}

func (f UnimplementedFilterer) SubscribeFilterLogs(context.Context, ethereum.FilterQuery, chan<- types.Log) (ethereum.Subscription, error) {
	panic("not implemented UnimplementedFilterer.SubscribeFilterLogs")
}
