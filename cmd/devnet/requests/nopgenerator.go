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

package requests

import (
	"context"
	"errors"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	ethereum "github.com/erigontech/erigon/v3"
	"github.com/erigontech/erigon/v3/core/types"
	"github.com/erigontech/erigon/v3/p2p"
	"github.com/erigontech/erigon/v3/rpc"
	"github.com/erigontech/erigon/v3/turbo/adapter/ethapi"
	"github.com/erigontech/erigon/v3/turbo/jsonrpc"
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
