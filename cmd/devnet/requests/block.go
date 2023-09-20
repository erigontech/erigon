package requests

import (
	"encoding/json"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
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
	return BlockNumber(hexutil.EncodeBig(n))
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

type Block struct {
	*types.Header
	Hash         libcommon.Hash            `json:"hash"`
	Transactions []*jsonrpc.RPCTransaction `json:"transactions"`
}

func (b *Block) UnmarshalJSON(input []byte) error {
	type body struct {
		Hash         libcommon.Hash            `json:"hash"`
		Transactions []*jsonrpc.RPCTransaction `json:"transactions"`
	}

	bd := body{}

	if err := json.Unmarshal(input, &bd); err != nil {
		return err
	}

	header := types.Header{}

	if err := json.Unmarshal(input, &header); err != nil {
		return err
	}

	b.Header = &header
	b.Hash = bd.Hash
	b.Transactions = bd.Transactions
	return nil
}

type EthGetTransactionCount struct {
	CommonResponse
	Result hexutil.Uint64 `json:"result"`
}

func (reqGen *requestGenerator) BlockNumber() (uint64, error) {
	var result hexutil.Uint64

	if err := reqGen.callCli(&result, Methods.ETHBlockNumber); err != nil {
		return 0, err
	}

	return uint64(result), nil
}

func (reqGen *requestGenerator) GetBlockByNumber(blockNum rpc.BlockNumber, withTxs bool) (*Block, error) {
	var result Block

	if err := reqGen.callCli(&result, Methods.ETHGetBlockByNumber, blockNum, withTxs); err != nil {
		return nil, err
	}

	return &result, nil
}

func (req *requestGenerator) GetRootHash(startBlock uint64, endBlock uint64) (libcommon.Hash, error) {
	var result string

	if err := req.callCli(&result, Methods.BorGetRootHash, startBlock, endBlock); err != nil {
		return libcommon.Hash{}, err
	}

	return libcommon.HexToHash(result), nil
}
