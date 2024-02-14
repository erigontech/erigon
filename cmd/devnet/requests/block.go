package requests

import (
	"context"
	"encoding/json"
	"math/big"

	hexutil2 "github.com/ledgerwatch/erigon-lib/common/hexutil"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
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
	Hash              libcommon.Hash `json:"hash"`
	TransactionHashes []libcommon.Hash
}

func (b *BlockWithTxHashes) UnmarshalJSON(input []byte) error {
	var header types.Header
	if err := json.Unmarshal(input, &header); err != nil {
		return err
	}

	var bd struct {
		Hash              libcommon.Hash   `json:"hash"`
		TransactionHashes []libcommon.Hash `json:"transactions"`
	}
	if err := json.Unmarshal(input, &bd); err != nil {
		return err
	}

	b.Header = &header
	b.Hash = bd.Hash
	b.TransactionHashes = bd.TransactionHashes

	return nil
}

type Block struct {
	BlockWithTxHashes
	Transactions []*jsonrpc.RPCTransaction `json:"transactions"`
}

func (b *Block) UnmarshalJSON(input []byte) error {
	var header types.Header
	if err := json.Unmarshal(input, &header); err != nil {
		return err
	}

	var bd struct {
		Hash         libcommon.Hash            `json:"hash"`
		Transactions []*jsonrpc.RPCTransaction `json:"transactions"`
	}
	if err := json.Unmarshal(input, &bd); err != nil {
		return err
	}

	b.Header = &header
	b.Hash = bd.Hash
	b.Transactions = bd.Transactions

	if bd.Transactions != nil {
		b.TransactionHashes = make([]libcommon.Hash, len(b.Transactions))
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

func (req *requestGenerator) GetRootHash(ctx context.Context, startBlock uint64, endBlock uint64) (libcommon.Hash, error) {
	var result string

	if err := req.rpcCall(ctx, &result, Methods.BorGetRootHash, startBlock, endBlock); err != nil {
		return libcommon.Hash{}, err
	}

	return libcommon.HexToHash(result), nil
}
