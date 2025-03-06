package jsonrpc

import (
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/consensus/misc"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/adapter/ethapi"
	zktx "github.com/erigontech/erigon/zk/tx"
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

func (api *BaseAPI) SetGasless(gasless bool) {
	api.gasless = gasless
}

// newRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCTransaction_zkevm(tx types.Transaction, blockHash common.Hash, blockNumber uint64, index uint64, baseFee *big.Int, includeL2TxHash bool) *ethapi.RPCTransaction {
	result := NewRPCTransaction(tx, blockHash, blockNumber, index, baseFee)

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
func newRPCPendingTransaction_zkevm(tx types.Transaction, current *types.Header, config *chain.Config, includeL2TxHash bool) *ethapi.RPCTransaction {
	var baseFee *big.Int
	if current != nil {
		baseFee = misc.CalcBaseFeeZk(config, current)
	}
	return newRPCTransaction_zkevm(tx, common.Hash{}, 0, 0, baseFee, includeL2TxHash)
}
