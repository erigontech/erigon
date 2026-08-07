package benchmark

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// BenchmarkAddressDiversity measures repeated BALANCE over n distinct warm
// accounts, the way an airdrop or batch contract walks a holder list. n
// straddles the EVM address intern table, so the widest case is the
// conflict-miss path the call benchmarks never reach — they touch a handful of
// addresses and hit every time.
func BenchmarkAddressDiversity(b *testing.B) {
	for _, n := range []int{16, 256, 1024} {
		p, lbl := program.New().Jumpdest()
		addrs := make([]common.Address, n)
		for i := range addrs {
			addrs[i] = common.BytesToAddress(crypto.Keccak256([]byte{byte(i), byte(i >> 8)}))
			p.Push(addrs[i]).Op(vm.BALANCE, vm.POP)
		}
		code := p.Jump(lbl).Bytes()

		b.Run(fmt.Sprintf("%daccounts", n), func(b *testing.B) {
			b.ReportAllocs()
			vmenv := benchConfig(b, 100_000_000)
			statedb := vmenv.IntraBlockState()
			deployContract(statedb, addrContract, code)
			for i, a := range addrs {
				addr := accounts.InternAddress(a)
				statedb.CreateAccount(addr, false)
				statedb.SetBalance(addr, *uint256.NewInt(uint64(i) + 1), 0)
			}
			callOOG(b, vmenv, addrContract)
			for b.Loop() {
				callOOG(b, vmenv, addrContract)
			}
		})
	}
}
