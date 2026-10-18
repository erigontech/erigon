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

func BenchmarkExpSmallOperands(b *testing.B) {
	for _, base := range []uint64{3, 5, 7, 11, 13, 136279841} {
		for _, exponent := range []uint64{3, 5, 7, 11, 13} {
			b.Run(fmt.Sprintf("%d^%d", base, exponent), func(b *testing.B) {
				opBenchmark(b, opExp, fmt.Sprintf("%016x", exponent), fmt.Sprintf("%016x", base))
			})
		}
	}
}

func BenchmarkExpInterpreter(b *testing.B) {
	for _, base := range []uint64{3, 13, 136279841} {
		b.Run(fmt.Sprint(base), func(b *testing.B) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.TestChainOsakaConfig, Config{})
			contract := NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
			for range 256 {
				contract.Code = append(contract.Code, byte(PUSH1), 13, byte(PUSH4), byte(base>>24), byte(base>>16), byte(base>>8), byte(base), byte(EXP), byte(POP))
			}
			contract.Code = append(contract.Code, byte(STOP))
			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := evm.Run(*contract, mdgas.MdGas{Execution: 1_000_000}, nil, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
