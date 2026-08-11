package benchmark

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// BenchmarkSLOADCold measures cold SLOAD (2100 gas each, EIP-2929).
// Pre-populates N storage slots and reads them in a single call.
// Uses PushSnapshot/RevertToSnapshot to ensure slots are cold each iteration.
func BenchmarkSLOADCold(b *testing.B) {
	for _, n := range []int{10, 50, 100, 500} {
		// Build code: for each slot i, PUSH i, SLOAD, POP
		p := program.New()
		slots := make(map[uint256.Int]uint256.Int, n)
		for i := range n {
			key := uint256.NewInt(uint64(i))
			slots[*key] = *uint256.NewInt(0xDEAD)
			p.Push(i).Op(vm.SLOAD, vm.POP)
		}
		// STOP at the end
		code := p.Op(vm.STOP).Bytes()

		b.Run(fmt.Sprintf("%dslots", n), func(b *testing.B) {
			b.ReportAllocs()
			// Gas: 2100 per cold SLOAD + overhead
			vmenv := benchConfig(b, uint64(n)*2200+100_000)
			statedb := vmenv.IntraBlockState()
			deployContract(statedb, addrContract, code)
			setStorage(statedb, addrContract, slots)
			callComplete(b, vmenv, addrContract, nil)
			for b.Loop() {
				callComplete(b, vmenv, addrContract, nil)
			}
		})
	}
}

// BenchmarkSLOADWarm measures warm SLOAD (100 gas each, EIP-2929).
// Reads the same N slots in a loop so after the first iteration they're warm.
func BenchmarkSLOADWarm(b *testing.B) {
	for _, n := range []int{10, 50, 100, 500} {
		// Build code: JUMPDEST, for each slot: PUSH i, SLOAD, POP, then JUMP back
		p, lbl := program.New().Jumpdest()
		slots := make(map[uint256.Int]uint256.Int, n)
		for i := range n {
			key := uint256.NewInt(uint64(i))
			slots[*key] = *uint256.NewInt(0xDEAD)
			p.Push(i).Op(vm.SLOAD, vm.POP)
		}
		code := p.Jump(lbl).Bytes()

		b.Run(fmt.Sprintf("%dslots", n), func(b *testing.B) {
			b.ReportAllocs()
			vmenv := benchConfig(b, 100_000_000)
			statedb := vmenv.IntraBlockState()
			deployContract(statedb, addrContract, code)
			setStorage(statedb, addrContract, slots)
			callOOG(b, vmenv, addrContract)
			for b.Loop() {
				callOOG(b, vmenv, addrContract)
			}
		})
	}
}

// BenchmarkSSTORE measures SSTORE cost for different state transitions.
// Each sub-benchmark uses PushSnapshot/RevertToSnapshot to restore storage
// between iterations, ensuring every iteration measures the intended transition.
func BenchmarkSSTORE(b *testing.B) {
	// zero-to-nonzero: most expensive (20k gas) — fresh slot
	b.Run("zero-to-nonzero", func(b *testing.B) {
		// Write to slot 0..N, each costs 20k gas. Linear code, no loop (each slot fresh).
		const n = 100
		p := program.New()
		for i := range n {
			p.Sstore(i, 0xBEEF)
		}
		code := p.Op(vm.STOP).Bytes()

		b.ReportAllocs()
		vmenv := benchConfig(b, uint64(n)*22_100+100_000)
		statedb := vmenv.IntraBlockState()
		deployContract(statedb, addrContract, code)
		callComplete(b, vmenv, addrContract, nil)
		for b.Loop() {
			callComplete(b, vmenv, addrContract, nil)
		}
	})

	// nonzero-to-nonzero: common DeFi path (5k gas) — balance updates
	b.Run("nonzero-to-nonzero", func(b *testing.B) {
		const n = 100
		p := program.New()
		slots := make(map[uint256.Int]uint256.Int, n)
		for i := range n {
			key := uint256.NewInt(uint64(i))
			slots[*key] = *uint256.NewInt(1000) // pre-existing value
			p.Sstore(i, 2000)                   // overwrite
		}
		code := p.Op(vm.STOP).Bytes()

		b.ReportAllocs()
		vmenv := benchConfig(b, uint64(n)*5200+100_000)
		statedb := vmenv.IntraBlockState()
		deployContract(statedb, addrContract, code)
		setStorage(statedb, addrContract, slots)
		callComplete(b, vmenv, addrContract, nil)
		for b.Loop() {
			callComplete(b, vmenv, addrContract, nil)
		}
	})

	// nonzero-to-zero: refund path
	b.Run("nonzero-to-zero", func(b *testing.B) {
		const n = 100
		p := program.New()
		slots := make(map[uint256.Int]uint256.Int, n)
		for i := range n {
			key := uint256.NewInt(uint64(i))
			slots[*key] = *uint256.NewInt(1000)
			p.Sstore(i, 0) // clear
		}
		code := p.Op(vm.STOP).Bytes()

		b.ReportAllocs()
		vmenv := benchConfig(b, uint64(n)*5200+100_000)
		statedb := vmenv.IntraBlockState()
		deployContract(statedb, addrContract, code)
		setStorage(statedb, addrContract, slots)
		callComplete(b, vmenv, addrContract, nil)
		for b.Loop() {
			callComplete(b, vmenv, addrContract, nil)
		}
	})
}

// BenchmarkTransientStorage measures TLOAD/TSTORE (EIP-1153) performance.
func BenchmarkTransientStorage(b *testing.B) {
	for _, n := range []int{10, 100, 500} {
		// Loop: TSTORE N slots then TLOAD them all
		p, lbl := program.New().Jumpdest()
		for i := range n {
			p.Tstore(i, 0xCAFE)
		}
		for i := range n {
			p.Push(i).Op(vm.TLOAD, vm.POP)
		}
		code := p.Jump(lbl).Bytes()

		b.Run(fmt.Sprintf("%dslots", n), func(b *testing.B) {
			b.ReportAllocs()
			vmenv := benchConfig(b, 100_000_000)
			statedb := vmenv.IntraBlockState()
			deployContract(statedb, addrContract, code)
			callOOG(b, vmenv, addrContract)
			for b.Loop() {
				callOOG(b, vmenv, addrContract)
			}
		})
	}
}

// BenchmarkStorageDiversity measures many unique cold slot accesses (simulates balances mapping).
// Uses PushSnapshot/RevertToSnapshot to ensure slots are cold each iteration.
func BenchmarkStorageDiversity(b *testing.B) {
	for _, n := range []int{100, 1000} {
		// Pre-populate N slots, then read them all in one call
		p := program.New()
		slots := make(map[uint256.Int]uint256.Int, n)
		for i := range n {
			key := uint256.NewInt(uint64(i + 1000)) // offset to avoid slot 0
			slots[*key] = *uint256.NewInt(uint64(i * 100))
			p.Push(i+1000).Op(vm.SLOAD, vm.POP)
		}
		code := p.Op(vm.STOP).Bytes()

		b.Run(fmt.Sprintf("%dslots", n), func(b *testing.B) {
			b.ReportAllocs()
			vmenv := benchConfig(b, uint64(n)*2200+100_000)
			statedb := vmenv.IntraBlockState()
			deployContract(statedb, addrContract, code)
			setStorage(statedb, addrContract, slots)
			callComplete(b, vmenv, addrContract, nil)
			for b.Loop() {
				callComplete(b, vmenv, addrContract, nil)
			}
		})
	}
}

// BenchmarkAddressDiversity measures repeated BALANCE over n distinct warm
// accounts, the way an airdrop or batch contract walks a holder list. The sweep
// assumes the EVM interns addresses through a 256-entry table: 16 fits, 256
// fills it and 1024 forces the conflict-miss path the call benchmarks never
// reach — they touch a handful of addresses and hit every time.
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
				require.NoError(b, statedb.CreateAccount(addr, false))
				require.NoError(b, statedb.SetBalance(addr, *uint256.NewInt(uint64(i) + 1), 0))
			}
			callOOG(b, vmenv, addrContract)
			for b.Loop() {
				callOOG(b, vmenv, addrContract)
			}
		})
	}
}
