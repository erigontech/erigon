package contracts

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	ethereum "github.com/erigontech/erigon"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/filters"
	"github.com/erigontech/erigon/event"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/adapter/ethapi"
	"github.com/erigontech/erigon/turbo/jsonrpc"
)

var _ bind.ContractBackend = Backend{}

type Backend struct {
	api jsonrpc.EthAPI
}

func NewDirectBackend(api jsonrpc.EthAPI) Backend {
	return Backend{
		api: api,
	}
}

func (b Backend) CodeAt(ctx context.Context, contract libcommon.Address, blockNum *big.Int) ([]byte, error) {
	return b.api.GetCode(ctx, contract, BlockNumArg(blockNum))
}

func (b Backend) CallContract(ctx context.Context, callMsg ethereum.CallMsg, blockNum *big.Int) ([]byte, error) {
	return b.api.Call(ctx, CallArgsFromCallMsg(callMsg), BlockNumArg(blockNum), nil)
}

func (b Backend) PendingCodeAt(ctx context.Context, account libcommon.Address) ([]byte, error) {
	return b.api.GetCode(ctx, account, PendingBlockNumArg())
}

func (b Backend) PendingNonceAt(ctx context.Context, account libcommon.Address) (uint64, error) {
	count, err := b.api.GetTransactionCount(ctx, account, PendingBlockNumArg())
	if err != nil {
		return 0, err
	}

	return uint64(*count), nil
}

func (b Backend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	price, err := b.api.GasPrice(ctx)
	if err != nil {
		return nil, err
	}

	return price.ToInt(), nil
}

func (b Backend) EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error) {
	callArgs := CallArgsFromCallMsg(call)
	gas, err := b.api.EstimateGas(ctx, &callArgs, nil, nil)
	if err != nil {
		return 0, err
	}

	return gas.Uint64(), nil
}

func (b Backend) SendTransaction(ctx context.Context, txn types.Transaction) error {
	var buf bytes.Buffer
	err := txn.MarshalBinary(&buf)
	if err != nil {
		return err
	}

	_, err = b.api.SendRawTransaction(ctx, buf.Bytes())
	return err
}

func (b Backend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	logs, err := b.api.GetLogs(ctx, filters.FilterCriteria(query))
	if err != nil {
		return nil, err
	}

	res := make([]types.Log, len(logs))
	for i, log := range logs {
		res[i] = *log
	}

	return res, nil
}

func (b Backend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	resc, closec := make(chan any), make(chan any)
	ctx = rpc.ContextWithNotifier(ctx, rpc.NewLocalNotifier("eth", resc, closec))
	_, err := b.api.Logs(ctx, filters.FilterCriteria(query))
	if err != nil {
		return nil, err
	}

	sub := event.NewSubscription(func(quit <-chan struct{}) error {
		for {
			select {
			case <-quit:
				close(closec)
				return nil
			case res := <-resc:
				log, ok := res.(*types.Log)
				if !ok {
					return fmt.Errorf("unexpected type %T in SubscribeFilterLogs", res)
				}

				ch <- *log
			}
		}
	})

	return sub, nil
}

func BlockNumArg(blockNum *big.Int) rpc.BlockNumberOrHash {
	var blockRef rpc.BlockReference
	if blockNum == nil {
		blockRef = rpc.LatestBlock
	} else {
		blockRef = rpc.AsBlockReference(blockNum)
	}

	return rpc.BlockNumberOrHash(blockRef)
}

func PendingBlockNumArg() rpc.BlockNumberOrHash {
	return rpc.BlockNumberOrHash(rpc.PendingBlock)
}

func CallArgsFromCallMsg(callMsg ethereum.CallMsg) ethapi.CallArgs {
	var emptyAddress libcommon.Address
	var from *libcommon.Address
	if callMsg.From != emptyAddress {
		from = &callMsg.From
	}

	var gas *hexutil.Uint64
	if callMsg.Gas != 0 {
		gas = (*hexutil.Uint64)(&callMsg.Gas)
	}

	var gasPrice *hexutil.Big
	if callMsg.GasPrice != nil {
		gasPrice = (*hexutil.Big)(callMsg.GasPrice.ToBig())
	}

	var feeCap *hexutil.Big
	if callMsg.FeeCap != nil {
		feeCap = (*hexutil.Big)(callMsg.FeeCap.ToBig())
	}

	var maxFeePerBlobGas *hexutil.Big
	if callMsg.MaxFeePerBlobGas != nil {
		maxFeePerBlobGas = (*hexutil.Big)(callMsg.MaxFeePerBlobGas.ToBig())
	}

	var value *hexutil.Big
	if callMsg.Value != nil {
		value = (*hexutil.Big)(callMsg.Value.ToBig())
	}

	var data *hexutility.Bytes
	if callMsg.Data != nil {
		b := hexutility.Bytes(callMsg.Data)
		data = &b
	}

	var accessList *types.AccessList
	if callMsg.AccessList != nil {
		accessList = &callMsg.AccessList
	}

	return ethapi.CallArgs{
		From:             from,
		To:               callMsg.To,
		Gas:              gas,
		GasPrice:         gasPrice,
		MaxFeePerGas:     feeCap,
		MaxFeePerBlobGas: maxFeePerBlobGas,
		Value:            value,
		Data:             data,
		AccessList:       accessList,
	}
}
