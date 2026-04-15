package benchmark

import (
	"fmt"
	"testing"

	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// BenchmarkPureArithmetic measures raw opcode dispatch speed for arithmetic ops.
// This is a tight ADD loop that runs until OOG, testing the interpreter's main
// dispatch loop in interpreter.go:Run().
func BenchmarkPureArithmetic(b *testing.B) {
	for _, gas := range []uint64{1_000_000, 10_000_000, 100_000_000} {
		name := formatGas(gas)
		// JUMPDEST, PUSH1 1, PUSH1 2, ADD, POP, JUMP(0)
		p, lbl := program.New().Jumpdest()
		code := p.Push(1).Push(2).Op(vm.ADD, vm.POP).Jump(lbl).Bytes()

		b.Run("add/"+name, func(b *testing.B) {
			b.ReportAllocs()
			cfg, statedb := benchConfig(b, gas)
			deployContract(statedb, addrContract, code)
			// Warmup
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
			for b.Loop() {
				prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
			}
		})
	}

	// MUL loop
	p, lbl := program.New().Jumpdest()
	mulCode := p.Push(3).Push(7).Op(vm.MUL, vm.POP).Jump(lbl).Bytes()
	b.Run("mul/100M", func(b *testing.B) {
		b.ReportAllocs()
		cfg, statedb := benchConfig(b, 100_000_000)
		deployContract(statedb, addrContract, mulCode)
		prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
		for b.Loop() {
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
		}
	})
}

// BenchmarkStackOps measures DUP/SWAP intensive workloads.
func BenchmarkStackOps(b *testing.B) {
	// DUP1..DUP8, SWAP1..SWAP4, POP×8 — exercises stack bounds checking
	p, lbl := program.New().Jumpdest()
	code := p.Push(0x42).
		Op(vm.DUP1, vm.DUP2, vm.DUP3, vm.DUP4, vm.DUP5, vm.DUP6, vm.DUP7, vm.DUP8).
		Op(vm.SWAP1, vm.SWAP2, vm.SWAP3, vm.SWAP4).
		Op(vm.POP, vm.POP, vm.POP, vm.POP, vm.POP, vm.POP, vm.POP, vm.POP, vm.POP).
		Jump(lbl).Bytes()

	b.Run("dup-swap/100M", func(b *testing.B) {
		b.ReportAllocs()
		cfg, statedb := benchConfig(b, 100_000_000)
		deployContract(statedb, addrContract, code)
		prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
		for b.Loop() {
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
		}
	})
}

// BenchmarkMemoryOps measures MSTORE/MLOAD with growing memory.
func BenchmarkMemoryOps(b *testing.B) {
	// Loop: MSTORE at offset 0, MLOAD from offset 0, POP result
	p, lbl := program.New().Jumpdest()
	code := p.Push(0xDEAD).Push(0).Op(vm.MSTORE).
		Push(0).Op(vm.MLOAD, vm.POP).
		Jump(lbl).Bytes()

	b.Run("mstore-mload/100M", func(b *testing.B) {
		b.ReportAllocs()
		cfg, statedb := benchConfig(b, 100_000_000)
		deployContract(statedb, addrContract, code)
		prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
		for b.Loop() {
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
		}
	})

	// Growing memory: PUSH 0 as initial offset, then loop: MSTORE, ADD 32
	// Bytecode: PUSH1 0, JUMPDEST, PUSH1 0xFF, DUP2, MSTORE, PUSH1 32, ADD, JUMP
	p2 := program.New().Push(0) // initial offset on stack
	jdPos := p2.Size()          // position of JUMPDEST
	growCode := p2.Op(vm.JUMPDEST).
		Push(0xFF).    // value
		Op(vm.DUP2).   // offset
		Op(vm.MSTORE). // MSTORE(offset, value)
		Push(32).      // 32
		Op(vm.ADD).    // offset += 32
		Jump(jdPos).   // jump back to JUMPDEST
		Bytes()

	b.Run("mstore-growing/10M", func(b *testing.B) {
		b.ReportAllocs()
		cfg, statedb := benchConfig(b, 10_000_000)
		deployContract(statedb, addrContract, growCode)
		prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
		for b.Loop() {
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
		}
	})
}

// BenchmarkKeccak256 measures SHA3/KECCAK256 on varying input sizes.
func BenchmarkKeccak256(b *testing.B) {
	for _, size := range []int{32, 256, 4096} {
		// Store data in memory, then SHA3(0, size) in a loop
		p, lbl := program.New().Jumpdest()
		code := p.Push(size).Push(0).Op(vm.KECCAK256, vm.POP).Jump(lbl).Bytes()

		b.Run(formatSize(size)+"/100M", func(b *testing.B) {
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

// BenchmarkMixedCompute simulates a realistic opcode mix based on mainnet profiling:
// ~60% stack ops, ~20% arithmetic, ~10% memory, ~10% control flow.
func BenchmarkMixedCompute(b *testing.B) {
	p, lbl := program.New().Jumpdest()
	code := p.
		// Arithmetic (20%)
		Push(42).Push(17).Op(vm.ADD).Push(3).Op(vm.MUL).
		// Stack ops (60%)
		Op(vm.DUP1, vm.DUP2, vm.SWAP1, vm.SWAP2).
		Op(vm.DUP1, vm.DUP2, vm.SWAP1).
		// Memory (10%)
		Push(0).Op(vm.MSTORE).
		Push(0).Op(vm.MLOAD).
		// Clean up and control flow (10%)
		Op(vm.POP, vm.POP, vm.POP, vm.POP, vm.POP).
		Jump(lbl).Bytes()

	b.Run("mixed/100M", func(b *testing.B) {
		b.ReportAllocs()
		cfg, statedb := benchConfig(b, 100_000_000)
		deployContract(statedb, addrContract, code)
		prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
		for b.Loop() {
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
		}
	})
}

func formatGas(gas uint64) string {
	switch {
	case gas >= 1_000_000_000:
		return fmt.Sprintf("%dB", gas/1_000_000_000)
	case gas >= 1_000_000:
		return fmt.Sprintf("%dM", gas/1_000_000)
	default:
		return fmt.Sprintf("%d", gas)
	}
}

func formatSize(size int) string {
	switch {
	case size >= 1024:
		return fmt.Sprintf("%dKB", size/1024)
	default:
		return fmt.Sprintf("%dB", size)
	}
}
