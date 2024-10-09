package requests

import (
	"context"
	"errors"
	"math/big"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
)

var ErrNotImplemented = errors.New("not implemented")

type NopRequestGenerator struct {
}

func (n NopRequestGenerator) PingErigonRpc() PingResult {
	return PingResult{}
}

func (n NopRequestGenerator) GetBalance(address libcommon.Address, blockRef rpc.BlockReference) (*big.Int, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) AdminNodeInfo() (p2p.NodeInfo, error) {
	return p2p.NodeInfo{}, ErrNotImplemented
}

func (n NopRequestGenerator) GetBlockByNumber(ctx context.Context, blockNum rpc.BlockNumber, withTxs bool) (*Block, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) GetTransactionByHash(hash libcommon.Hash) (*jsonrpc.RPCTransaction, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) GetTransactionReceipt(ctx context.Context, hash libcommon.Hash) (*types.Receipt, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) TraceTransaction(hash libcommon.Hash) ([]TransactionTrace, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) GetTransactionCount(address libcommon.Address, blockRef rpc.BlockReference) (*big.Int, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) BlockNumber() (uint64, error) {
	return 0, ErrNotImplemented
}

func (n NopRequestGenerator) SendTransaction(signedTx types.Transaction) (libcommon.Hash, error) {
	return libcommon.Hash{}, ErrNotImplemented
}

func (n NopRequestGenerator) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) Subscribe(ctx context.Context, method SubMethod, subChan interface{}, args ...interface{}) (ethereum.Subscription, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) TxpoolContent() (int, int, int, error) {
	return 0, 0, 0, ErrNotImplemented
}

func (n NopRequestGenerator) Call(args ethapi.CallArgs, blockRef rpc.BlockReference, overrides *ethapi.StateOverrides) ([]byte, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) TraceCall(blockRef rpc.BlockReference, args ethapi.CallArgs, traceOpts ...TraceOpt) (*TraceCallResult, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) DebugAccountAt(blockHash libcommon.Hash, txIndex uint64, account libcommon.Address) (*AccountResult, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) GetCode(address libcommon.Address, blockRef rpc.BlockReference) (hexutility.Bytes, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) EstimateGas(args ethereum.CallMsg, blockNum BlockNumber) (uint64, error) {
	return 0, ErrNotImplemented
}

func (n NopRequestGenerator) GasPrice() (*big.Int, error) {
	return nil, ErrNotImplemented
}

func (n NopRequestGenerator) GetRootHash(ctx context.Context, startBlock uint64, endBlock uint64) (libcommon.Hash, error) {
	return libcommon.Hash{}, ErrNotImplemented
}
