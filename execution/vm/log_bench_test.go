package vm

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func BenchmarkLog3(b *testing.B) {
	for _, size := range []int{32, 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
			scope := &CallContext{Contract: *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})}
			scope.Memory.Resize(uint64(size + 6))
			op := makeLog(3)
			var offset uint64
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				ibs.SetTxContext(1, 0)
				for range 3 {
					scope.Stack.push(uint256.Int{})
				}
				scope.Stack.push(*uint256.NewInt(uint64(size)))
				scope.Stack.push(*uint256.NewInt(offset))
				if _, _, err := op(0, evm, scope); err != nil {
					b.Fatal(err)
				}
				ibs.Reset()
				offset = (offset + 1) % 7
			}
		})
	}
}

func BenchmarkLog3Interpreter(b *testing.B) {
	for _, size := range []int{32, 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.TestChainOsakaConfig, Config{})
			contract := NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
			contract.Code = []byte{
				byte(PUSH3), byte(size >> 16), byte(size >> 8), byte(size), byte(PUSH0),
				byte(DUP1), byte(DUP1), byte(DUP1), byte(DUP5),
				byte(PUSH1), 7, byte(GAS), byte(MOD), byte(LOG3), byte(STOP),
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				ibs.SetTxContext(1, 0)
				if _, _, _, err := evm.Run(*contract, mdgas.MdGas{Execution: 16_777_216}, nil, false); err != nil {
					b.Fatal(err)
				}
				ibs.Reset()
			}
		})
	}
}
