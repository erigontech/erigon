package exec22

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
)

// ReadWriteSet contains ReadSet, WriteSet and BalanceIncrease of a transaction,
// which is processed by a single thread that writes into the ReconState1 and
// flushes to the database
type TxTask struct {
	TxNum           uint64
	BlockNum        uint64
	Rules           *chain.Rules
	Header          *types.Header
	Txs             types.Transactions
	Uncles          []*types.Header
	Coinbase        libcommon.Address
	Withdrawals     types.Withdrawals
	BlockHash       libcommon.Hash
	Sender          *libcommon.Address
	SkipAnalysis    bool
	TxIndex         int // -1 for block initialisation
	Final           bool
	Tx              types.Transaction
	GetHashFn       func(n uint64) libcommon.Hash
	TxAsMessage     types.Message
	EvmBlockContext evmtypes.BlockContext

	BalanceIncreaseSet map[libcommon.Address]uint256.Int
	ReadLists          map[string]*KvList
	WriteLists         map[string]*KvList
	AccountPrevs       map[string][]byte
	AccountDels        map[string]*accounts.Account
	StoragePrevs       map[string][]byte
	CodePrevs          map[string]uint64
	ResultsSize        int64
	Error              error
	Logs               []*types.Log
	TraceFroms         map[libcommon.Address]struct{}
	TraceTos           map[libcommon.Address]struct{}

	UsedGas uint64
}

type TxTaskQueue []*TxTask

func (h TxTaskQueue) Len() int {
	return len(h)
}

func (h TxTaskQueue) Less(i, j int) bool {
	return h[i].TxNum < h[j].TxNum
}

func (h TxTaskQueue) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TxTaskQueue) Push(a interface{}) {
	*h = append(*h, a.(*TxTask))
}

func (h *TxTaskQueue) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

// KvList sort.Interface to sort write list by keys
type KvList struct {
	Keys []string
	Vals [][]byte
}

func (l KvList) Len() int {
	return len(l.Keys)
}

func (l KvList) Less(i, j int) bool {
	return l.Keys[i] < l.Keys[j]
}

func (l *KvList) Swap(i, j int) {
	l.Keys[i], l.Keys[j] = l.Keys[j], l.Keys[i]
	l.Vals[i], l.Vals[j] = l.Vals[j], l.Vals[i]
}
