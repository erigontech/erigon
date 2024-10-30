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
	"fmt"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon/v3/rpc"
)

type DebugAccountAt struct {
	CommonResponse
	Result AccountResult `json:"result"`
}

// AccountResult is the result struct for GetProof
type AccountResult struct {
	Address      libcommon.Address `json:"address"`
	AccountProof []string          `json:"accountProof"`
	Balance      *hexutil.Big      `json:"balance"`
	CodeHash     libcommon.Hash    `json:"codeHash"`
	Code         hexutility.Bytes  `json:"code"`
	Nonce        hexutil.Uint64    `json:"nonce"`
	StorageHash  libcommon.Hash    `json:"storageHash"`
	StorageProof []StorageResult   `json:"storageProof"`
}

type StorageResult struct {
	Key   string       `json:"key"`
	Value *hexutil.Big `json:"value"`
	Proof []string     `json:"proof"`
}

func (reqGen *requestGenerator) GetCode(address libcommon.Address, blockRef rpc.BlockReference) (hexutility.Bytes, error) {
	var result hexutility.Bytes

	if err := reqGen.rpcCall(context.Background(), &result, Methods.ETHGetCode, address, blockRef); err != nil {
		return nil, err
	}

	return result, nil
}

func (reqGen *requestGenerator) GetBalance(address libcommon.Address, blockRef rpc.BlockReference) (*big.Int, error) {
	var result hexutil.Big

	if err := reqGen.rpcCall(context.Background(), &result, Methods.ETHGetBalance, address, blockRef); err != nil {
		return nil, err
	}

	return result.ToInt(), nil
}

func (reqGen *requestGenerator) GetTransactionCount(address libcommon.Address, blockRef rpc.BlockReference) (*big.Int, error) {
	var result hexutil.Big

	if err := reqGen.rpcCall(context.Background(), &result, Methods.ETHGetTransactionCount, address, blockRef); err != nil {
		return nil, err
	}

	return result.ToInt(), nil
}

func (reqGen *requestGenerator) DebugAccountAt(blockHash libcommon.Hash, txIndex uint64, account libcommon.Address) (*AccountResult, error) {
	var b DebugAccountAt

	method, body := reqGen.debugAccountAt(blockHash, txIndex, account)
	if res := reqGen.rpcCallJSON(method, body, &b); res.Err != nil {
		return nil, fmt.Errorf("failed to get account: %v", res.Err)
	}

	if b.Error != nil {
		return nil, fmt.Errorf("failed to get account: rpc failed: %w", b.Error)
	}

	return &b.Result, nil
}

func (req *requestGenerator) debugAccountAt(blockHash libcommon.Hash, txIndex uint64, account libcommon.Address) (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":["0x%x",%d, "0x%x"],"id":%d}`
	return Methods.DebugAccountAt, fmt.Sprintf(template, Methods.DebugAccountAt, blockHash, txIndex, account, req.reqID)
}
