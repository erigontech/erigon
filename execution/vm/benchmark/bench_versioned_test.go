package benchmark

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/execution/vm/runtime"
)

// configBuilder builds a runtime.Config + state for one benchmark.
type configBuilder func(b *testing.B, gasLimit uint64) (*runtime.Config, *state.IntraBlockState)

// stateReaders is the plain-vs-versioned A/B. "plain" is what every other EVM
// benchmark uses (reads via NewReaderV3). "versioned" routes reads through the
// VersionedStateReader / VersionMap OCC path that the default parallel executor
// uses, so the delta between the two isolates the per-read overhead ParallelExec
// pays but the existing benchmarks do not measure.
var stateReaders = []struct {
	name  string
	build configBuilder
}{
	{"plain", benchConfig},
	{"versioned", benchConfigVersioned},
}

// BenchmarkVersionedSLOADWarm runs the warm-SLOAD workload through both the plain
// and versioned readers.
func BenchmarkVersionedSLOADWarm(b *testing.B) {
	for _, n := range []int{50, 500} {
		p, lbl := program.New().Jumpdest()
		slots := make(map[uint256.Int]uint256.Int, n)
		for i := range n {
			slots[*uint256.NewInt(uint64(i))] = *uint256.NewInt(0xDEAD)
			p.Push(i).Op(vm.SLOAD, vm.POP)
		}
		code := p.Jump(lbl).Bytes()

		for _, r := range stateReaders {
			b.Run(fmt.Sprintf("%dslots/%s", n, r.name), func(b *testing.B) {
				b.ReportAllocs()
				cfg, statedb := r.build(b, 100_000_000)
				deployContract(statedb, addrContract, code)
				setStorage(statedb, addrContract, slots)
				prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
				for b.Loop() {
					prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
				}
			})
		}
	}
}

// BenchmarkVersionedSSTORE runs the zero-to-nonzero SSTORE workload through both
// readers, exercising the write path under the version map.
func BenchmarkVersionedSSTORE(b *testing.B) {
	const n = 100
	p := program.New()
	for i := range n {
		p.Sstore(i, 0xBEEF)
	}
	code := p.Op(vm.STOP).Bytes()

	for _, r := range stateReaders {
		b.Run(r.name, func(b *testing.B) {
			b.ReportAllocs()
			cfg, statedb := r.build(b, uint64(n)*22_100+100_000)
			deployContract(statedb, addrContract, code)
			for b.Loop() {
				snap := statedb.PushSnapshot()
				prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
				statedb.RevertToSnapshot(snap, nil)
				statedb.PopSnapshot(snap)
			}
		})
	}
}
