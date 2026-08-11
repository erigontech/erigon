package vm

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// benchmarkOpLog isolates the LOG opcode handler (makeLog) emit path: pop
// topics + mem range, copy log data, and hand the log to the IntraBlockState.
// Reset each iteration keeps the log buffer at steady-state capacity so the
// measured allocs are makeLog's own (topics slice + data copy), not buffer growth.
func benchmarkOpLog(b *testing.B, numTopics, dataLen int) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Release(false)
	evm := NewEVM(evmtypes.BlockContext{BlockNumber: 1}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
	to := accounts.InternAddress(common.Address{0x01})
	cc := &CallContext{Contract: *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, to, uint256.Int{})}
	cc.Memory.Resize(uint64(dataLen))
	logFn := makeLog(numTopics)
	pc := uint64(0)
	topic := *uint256.NewInt(0x42)
	mStart := uint256.Int{}
	mSize := *uint256.NewInt(uint64(dataLen))

	b.ReportAllocs()
	ibs.SetTxContext(1, 0)
	for b.Loop() {
		for range numTopics {
			cc.Stack.push(topic)
		}
		cc.Stack.push(mSize)
		cc.Stack.push(mStart)
		logFn(pc, evm, cc)
		ibs.Reset()
	}
}

func BenchmarkOpLog0(b *testing.B) { benchmarkOpLog(b, 0, 96) }
func BenchmarkOpLog2(b *testing.B) { benchmarkOpLog(b, 2, 96) }
func BenchmarkOpLog3(b *testing.B) { benchmarkOpLog(b, 3, 96) }
func BenchmarkOpLog4(b *testing.B) { benchmarkOpLog(b, 4, 96) }
