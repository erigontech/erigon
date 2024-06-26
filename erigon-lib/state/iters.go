package state

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
)

//go:generate mockgen -typed=true -destination=./iters_mock.go -package=state . CanonicalsReader
type CanonicalsReader interface {
	// TxnIdsOfCanonicalBlocks - for given canonical blocks range returns non-canonical txnIds (not txNums)
	// [fromTxNum, toTxNum)
	// To get all canonical blocks, use fromTxNum=0, toTxNum=-1
	// For reverse iteration use order.Desc and fromTxNum=-1, toTxNum=-1
	TxnIdsOfCanonicalBlocks(tx kv.Tx, fromTxNum, toTxNum int, asc order.By, limit int) (iter.U64, error)
	BaseTxnID(tx kv.Tx, blockNum uint64, blockHash common.Hash) (kv.TxnId, error)
	TxNum2ID(tx kv.Tx, blockNum uint64, blockHash common.Hash, txNum uint64) (kv.TxnId, error)
	LastFrozenTxNum(tx kv.Tx) (kv.TxnId, error)
}
