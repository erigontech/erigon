package commands

import (
	"math/big"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	types2 "github.com/gateway-fm/cdk-erigon-lib/types"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/types"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
)

func (api *BaseAPI) SetL2RpcUrl(url string) {
	api.l2RpcUrl = url
}

func (api *BaseAPI) GetL2RpcUrl() string {
	if len(api.l2RpcUrl) == 0 {
		panic("L2RpcUrl is not set")
	}
	return api.l2RpcUrl
}

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash        *common.Hash       `json:"blockHash"`
	BlockNumber      *hexutil.Big       `json:"blockNumber"`
	From             common.Address     `json:"from"`
	Gas              hexutil.Uint64     `json:"gas"`
	GasPrice         *hexutil.Big       `json:"gasPrice,omitempty"`
	Tip              *hexutil.Big       `json:"maxPriorityFeePerGas,omitempty"`
	FeeCap           *hexutil.Big       `json:"maxFeePerGas,omitempty"`
	Hash             common.Hash        `json:"hash"`
	Input            hexutility.Bytes   `json:"input"`
	Nonce            hexutil.Uint64     `json:"nonce"`
	To               *common.Address    `json:"to"`
	TransactionIndex *hexutil.Uint64    `json:"transactionIndex"`
	Value            *hexutil.Big       `json:"value"`
	Type             hexutil.Uint64     `json:"type"`
	Accesses         *types2.AccessList `json:"accessList,omitempty"`
	ChainID          *hexutil.Big       `json:"chainId,omitempty"`
	V                *hexutil.Big       `json:"v"`
	R                *hexutil.Big       `json:"r"`
	S                *hexutil.Big       `json:"s"`
	L2Hash           *common.Hash       `json:"l2Hash,omitempty"`
}

// newRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCTransaction_zkevm(tx types.Transaction, blockHash common.Hash, blockNumber uint64, index uint64, baseFee *big.Int, includeL2TxHash bool) *RPCTransaction {
	result := newRPCTransaction(tx, blockHash, blockNumber, index, baseFee)

	if includeL2TxHash {
		l2TxHash, err := zktx.ComputeL2TxHash(
			tx.GetChainID().ToBig(),
			tx.GetValue(),
			tx.GetPrice(),
			tx.GetNonce(),
			tx.GetGas(),
			tx.GetTo(),
			&result.From,
			tx.GetData(),
		)
		if err == nil {
			result.L2Hash = &l2TxHash
		}
	}

	return result
}

// newRPCPendingTransaction returns a pending transaction that will serialize to the RPC representation
func newRPCPendingTransaction_zkevm(tx types.Transaction, current *types.Header, config *chain.Config, includeL2TxHash bool) *RPCTransaction {
	var baseFee *big.Int
	if current != nil {
		baseFee = misc.CalcBaseFeeZk(config, current)
	}
	return newRPCTransaction_zkevm(tx, common.Hash{}, 0, 0, baseFee, includeL2TxHash)
}
