package rpchelper

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

var (
	// keccak256("Transfer(address,address,uint256)")
	transferTopic = common.HexToHash("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	// ERC-7528
	transferAddress = common.HexToAddress("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE")
)

// LogTracer is a simple tracing utility that records all the logs and ether transfers. Transfers are recorded as if they
// were logs. Transfer events include:
// - tx value
// - call value
// - self-destructs
//
// The log format for a transfer is:
// - address: transferAddress
// - data: Value
// - topics:
//   - Transfer(address,address,uint256)
//   - Sender address
//   - Recipient address
type LogTracer struct {
	// logs keeps logs for all open call frames.
	// This lets us clear logs for failed calls.
	logs           [][]*types.Log
	count          int
	traceTransfers bool
	blockNumber    uint64
	blockHash      common.Hash
	txHash         common.Hash
	txIdx          uint
}

func NewLogTracer(traceTransfers bool, blockNumber uint64, blockHash, txHash common.Hash, txIndex uint) *LogTracer {
	return &LogTracer{
		traceTransfers: traceTransfers,
		blockNumber:    blockNumber,
		blockHash:      blockHash,
		txHash:         txHash,
		txIdx:          txIndex,
	}
}

func (t *LogTracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnEnter: t.onEnter,
		OnExit:  t.onExit,
		OnLog:   t.onLog,
	}
}

func (t *LogTracer) onEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
	t.logs = append(t.logs, make([]*types.Log, 0))
	if vm.OpCode(typ) != vm.DELEGATECALL && !value.IsZero() {
		t.captureTransfer(from, to, &value)
	}
}

func (t *LogTracer) onExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if depth == 0 {
		t.onEnd(reverted)
		return
	}
	size := len(t.logs)
	if size <= 1 {
		return
	}
	// pop call
	call := t.logs[size-1]
	t.logs = t.logs[:size-1]
	size--

	// Clear logs if call failed.
	if !reverted {
		t.logs[size-1] = append(t.logs[size-1], call...)
	}
}

func (t *LogTracer) onEnd(reverted bool) {
	if reverted {
		t.logs[0] = nil
	}
}

func (t *LogTracer) onLog(log *types.Log) {
	t.captureLog(log.Address, log.Topics, log.Data)
}

func (t *LogTracer) captureLog(address common.Address, topics []common.Hash, data []byte) {
	t.logs[len(t.logs)-1] = append(t.logs[len(t.logs)-1], &types.Log{
		Address:     address,
		Topics:      topics,
		Data:        data,
		BlockNumber: t.blockNumber,
		BlockHash:   t.blockHash,
		TxHash:      t.txHash,
		TxIndex:     t.txIdx,
		Index:       uint(t.count),
	})
	t.count++
}

func (t *LogTracer) captureTransfer(from, to common.Address, value *uint256.Int) {
	if !t.traceTransfers {
		return
	}
	topics := []common.Hash{
		transferTopic,
		common.BytesToHash(from.Bytes()),
		common.BytesToHash(to.Bytes()),
	}
	t.captureLog(transferAddress, topics, common.BigToHash(value.ToBig()).Bytes())
}

// Reset prepares the LogTracer for the next transaction.
func (t *LogTracer) Reset(txHash common.Hash, txIdx uint) {
	t.logs = nil
	t.txHash = txHash
	t.txIdx = txIdx
}

func (t *LogTracer) Logs() []*types.Log {
	if len(t.logs) == 0 {
		return []*types.Log{}
	}
	return t.logs[0]
}
