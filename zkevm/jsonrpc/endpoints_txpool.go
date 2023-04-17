package jsonrpc

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
)

// TxPoolEndpoints is the txpool jsonrpc endpoint
type TxPoolEndpoints struct{}

type contentResponse struct {
	Pending map[common.Address]map[uint64]*txPoolTransaction `json:"pending"`
	Queued  map[common.Address]map[uint64]*txPoolTransaction `json:"queued"`
}

type txPoolTransaction struct {
	Nonce       types.ArgUint64 `json:"nonce"`
	GasPrice    types.ArgBig    `json:"gasPrice"`
	Gas         types.ArgUint64 `json:"gas"`
	To          *common.Address `json:"to"`
	Value       types.ArgBig    `json:"value"`
	Input       types.ArgBytes  `json:"input"`
	Hash        common.Hash     `json:"hash"`
	From        common.Address  `json:"from"`
	BlockHash   common.Hash     `json:"blockHash"`
	BlockNumber interface{}     `json:"blockNumber"`
	TxIndex     interface{}     `json:"transactionIndex"`
}

// Content creates a response for txpool_content request.
// See https://geth.ethereum.org/docs/rpc/ns-txpool#txpool_content.
func (e *TxPoolEndpoints) Content() (interface{}, types.Error) {
	resp := contentResponse{
		Pending: make(map[common.Address]map[uint64]*txPoolTransaction),
		Queued:  make(map[common.Address]map[uint64]*txPoolTransaction),
	}

	return resp, nil
}
