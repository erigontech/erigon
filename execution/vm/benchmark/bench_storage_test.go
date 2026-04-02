package benchmark

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

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
		for i := 0; i < n; i++ {
			key := uint256.NewInt(uint64(i))
			slots[*key] = *uint256.NewInt(0xDEAD)
			p.Push(i).Op(vm.SLOAD, vm.POP)
		}
		// STOP at the end
		code := p.Op(vm.STOP).Bytes()

		b.Run(fmt.Sprintf("%dslots", n), func(b *testing.B) {
			b.ReportAllocs()
			// Gas: 2100 per cold SLOAD + overhead
			cfg, statedb := benchConfig(b, uint64(n)*2200+100_000)
			deployContract(statedb, addrContract, code)
			setStorage(statedb, addrContract, slots)
			for b.Loop() {
				snap := statedb.PushSnapshot()
				prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
				statedb.RevertToSnapshot(snap, nil)
				statedb.PopSnapshot(snap)
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
		for i := 0; i < n; i++ {
			key := uint256.NewInt(uint64(i))
			slots[*key] = *uint256.NewInt(0xDEAD)
			p.Push(i).Op(vm.SLOAD, vm.POP)
		}
		code := p.Jump(lbl).Bytes()

		b.Run(fmt.Sprintf("%dslots", n), func(b *testing.B) {
			b.ReportAllocs()
			cfg, statedb := benchConfig(b, 100_000_000)
			deployContract(statedb, addrContract, code)
			setStorage(statedb, addrContract, slots)
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
			for b.Loop() {
				prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
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
		for i := 0; i < n; i++ {
			p.Sstore(i, 0xBEEF)
		}
		code := p.Op(vm.STOP).Bytes()

		b.ReportAllocs()
		cfg, statedb := benchConfig(b, uint64(n)*22_100+100_000)
		deployContract(statedb, addrContract, code)
		for b.Loop() {
			snap := statedb.PushSnapshot()
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
			statedb.RevertToSnapshot(snap, nil)
			statedb.PopSnapshot(snap)
		}
	})

	// nonzero-to-nonzero: common DeFi path (5k gas) — balance updates
	b.Run("nonzero-to-nonzero", func(b *testing.B) {
		const n = 100
		p := program.New()
		slots := make(map[uint256.Int]uint256.Int, n)
		for i := 0; i < n; i++ {
			key := uint256.NewInt(uint64(i))
			slots[*key] = *uint256.NewInt(1000) // pre-existing value
			p.Sstore(i, 2000)                   // overwrite
		}
		code := p.Op(vm.STOP).Bytes()

		b.ReportAllocs()
		cfg, statedb := benchConfig(b, uint64(n)*5200+100_000)
		deployContract(statedb, addrContract, code)
		setStorage(statedb, addrContract, slots)
		for b.Loop() {
			snap := statedb.PushSnapshot()
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
			statedb.RevertToSnapshot(snap, nil)
			statedb.PopSnapshot(snap)
		}
	})

	// nonzero-to-zero: refund path
	b.Run("nonzero-to-zero", func(b *testing.B) {
		const n = 100
		p := program.New()
		slots := make(map[uint256.Int]uint256.Int, n)
		for i := 0; i < n; i++ {
			key := uint256.NewInt(uint64(i))
			slots[*key] = *uint256.NewInt(1000)
			p.Sstore(i, 0) // clear
		}
		code := p.Op(vm.STOP).Bytes()

		b.ReportAllocs()
		cfg, statedb := benchConfig(b, uint64(n)*5200+100_000)
		deployContract(statedb, addrContract, code)
		setStorage(statedb, addrContract, slots)
		for b.Loop() {
			snap := statedb.PushSnapshot()
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
			statedb.RevertToSnapshot(snap, nil)
			statedb.PopSnapshot(snap)
		}
	})
}

// BenchmarkTransientStorage measures TLOAD/TSTORE (EIP-1153) performance.
func BenchmarkTransientStorage(b *testing.B) {
	for _, n := range []int{10, 100, 500} {
		// Loop: TSTORE N slots then TLOAD them all
		p, lbl := program.New().Jumpdest()
		for i := 0; i < n; i++ {
			p.Tstore(i, 0xCAFE)
		}
		for i := 0; i < n; i++ {
			p.Push(i).Op(vm.TLOAD, vm.POP)
		}
		code := p.Jump(lbl).Bytes()

		b.Run(fmt.Sprintf("%dslots", n), func(b *testing.B) {
			b.ReportAllocs()
			cfg, statedb := benchConfig(b, 100_000_000)
			deployContract(statedb, addrContract, code)
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
			for b.Loop() {
				prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
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
		for i := 0; i < n; i++ {
			key := uint256.NewInt(uint64(i + 1000)) // offset to avoid slot 0
			slots[*key] = *uint256.NewInt(uint64(i * 100))
			p.Push(i+1000).Op(vm.SLOAD, vm.POP)
		}
		code := p.Op(vm.STOP).Bytes()

		b.Run(fmt.Sprintf("%dslots", n), func(b *testing.B) {
			b.ReportAllocs()
			cfg, statedb := benchConfig(b, uint64(n)*2200+100_000)
			deployContract(statedb, addrContract, code)
			setStorage(statedb, addrContract, slots)
			for b.Loop() {
				snap := statedb.PushSnapshot()
				prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
				statedb.RevertToSnapshot(snap, nil)
				statedb.PopSnapshot(snap)
			}
		})
	}
}
