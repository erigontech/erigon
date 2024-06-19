package state

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
)

//go:generate mockgen -typed=true -destination=./iters_mock.go -package=state . IterFactory
type IterFactory interface {
	// TxnIdsOfCanonicalBlocks - for given canonical blocks range returns non-canonical txnIds (not txNums)
	// [fromTxNum, toTxNum)
	// To get all canonical blocks, use fromTxNum=0, toTxNum=-1
	// For reverse iteration use order.Desc and fromTxNum=-1, toTxNum=-1
	TxnIdsOfCanonicalBlocks(tx kv.Tx, fromTxNum, toTxNum int, asc order.By, limit int) (iter.U64, error)

	//TxsAmountBasedOnBodiesSnapshots(txnum uint64) (iter.U64, error)
}
