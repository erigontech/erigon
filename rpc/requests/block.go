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
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	hexutil2 "github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/fastjson"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

type BlockNumber string

func (bn BlockNumber) Uint64() uint64 {
	if b, ok := math.ParseBig256(string(bn)); ok {
		return b.Uint64()
	}

	return 0
}

func AsBlockNumber(n *big.Int) BlockNumber {
	return BlockNumber(hexutil2.EncodeBig(n))
}

var BlockNumbers = struct {
	// Latest is the parameter for the latest block
	Latest BlockNumber
	// Earliest is the parameter for the earliest block
	Earliest BlockNumber
	// Pending is the parameter for the pending block
	Pending BlockNumber
}{
	Latest:   "latest",
	Earliest: "earliest",
	Pending:  "pending",
}

type BlockWithTxHashes struct {
	*types.Header
	Hash              common.Hash `json:"hash"`
	TransactionHashes []common.Hash
}

func (b *BlockWithTxHashes) UnmarshalJSON(input []byte) error {
	var header types.Header
	if err := fastjson.Unmarshal(input, &header); err != nil {
		return err
	}

	var bd struct {
		Hash              common.Hash   `json:"hash"`
		TransactionHashes []common.Hash `json:"transactions"`
	}
	if err := fastjson.Unmarshal(input, &bd); err != nil {
		return err
	}

	b.Header = &header
	b.Hash = bd.Hash
	b.TransactionHashes = bd.TransactionHashes

	return nil
}

type Block struct {
	BlockWithTxHashes
	Transactions []*ethapi.RPCTransaction `json:"transactions"`
}

func (b *Block) UnmarshalJSON(input []byte) error {
	var header types.Header
	if err := fastjson.Unmarshal(input, &header); err != nil {
		return err
	}

	var bd struct {
		Hash         common.Hash              `json:"hash"`
		Transactions []*ethapi.RPCTransaction `json:"transactions"`
	}
	if err := fastjson.Unmarshal(input, &bd); err != nil {
		return err
	}

	b.Header = &header
	b.Hash = bd.Hash
	b.Transactions = bd.Transactions

	if bd.Transactions != nil {
		b.TransactionHashes = make([]common.Hash, len(b.Transactions))
		for _, t := range bd.Transactions {
			b.TransactionHashes = append(b.TransactionHashes, t.Hash)
		}
	}

	return nil
}

type EthGetTransactionCount struct {
	CommonResponse
	Result hexutil2.Uint64 `json:"result"`
}

func (reqGen *requestGenerator) BlockNumber() (uint64, error) {
	var result hexutil2.Uint64

	if err := reqGen.rpcCall(context.Background(), &result, Methods.ETHBlockNumber); err != nil {
		return 0, err
	}

	return uint64(result), nil
}

func (reqGen *requestGenerator) GetBlockByNumber(ctx context.Context, blockNum rpc.BlockNumber, withTxs bool) (*Block, error) {
	var result Block
	var err error

	if withTxs {
		err = reqGen.rpcCall(ctx, &result, Methods.ETHGetBlockByNumber, blockNum, withTxs)
	} else {
		err = reqGen.rpcCall(ctx, &result.BlockWithTxHashes, Methods.ETHGetBlockByNumber, blockNum, withTxs)
	}

	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (req *requestGenerator) GetRootHash(ctx context.Context, startBlock uint64, endBlock uint64) (common.Hash, error) {
	var result string

	if err := req.rpcCall(ctx, &result, Methods.BorGetRootHash, startBlock, endBlock); err != nil {
		return common.Hash{}, err
	}

	return common.HexToHash(result), nil
}
