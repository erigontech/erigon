package benchmark

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// transferEventSig is the keccak256 of "Transfer(address,address,uint256)"
var transferEventSig = common.HexToHash("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

// ERC-20 storage layout (simplified):
//   slot 0 = balances[from]
//   slot 1 = balances[to]
//   slot 2 = allowance[from][spender]
// Real ERC-20 uses keccak256(addr . slot) but for benchmarking the storage
// access pattern matters, not the slot derivation.

// BenchmarkERC20Transfer simulates an ERC-20 transfer:
// SLOAD(from_balance), SLOAD(to_balance), SUB, ADD, SSTORE×2, LOG3
func BenchmarkERC20Transfer(b *testing.B) {
	// Build bytecode for: transfer(from, to, amount)
	// slot 0 = from balance, slot 1 = to balance
	// SLOAD 0 → fromBal, PUSH amount, SWAP1, SUB → newFromBal
	// SSTORE(0, newFromBal)
	// SLOAD 1 → toBal, PUSH amount, ADD → newToBal
	// SSTORE(1, newToBal)
	// LOG3 with Transfer event topic
	p, lbl := program.New().Jumpdest()
	code := p.
		// SLOAD from_balance
		Push(0).Op(vm.SLOAD).  // [fromBal]
		Push(100).             // [fromBal, 100]
		Op(vm.SWAP1, vm.SUB).  // [fromBal-100]
		Push(0).Op(vm.SSTORE). // SSTORE(0, fromBal-100)
		// SLOAD to_balance
		Push(1).Op(vm.SLOAD).  // [toBal]
		Push(100).             // [toBal, 100]
		Op(vm.ADD).            // [toBal+100]
		Push(1).Op(vm.SSTORE). // SSTORE(1, toBal+100)
		// LOG3: Transfer(from, to, amount) — 3 topics + 32 bytes data
		Push(100).Push(0).Op(vm.MSTORE). // store amount in memory
		Push(0xDEAD).                    // topic2 (to)
		Push(0xCAFE).                    // topic1 (from)
		Push(transferEventSig).          // Transfer event sig
		Push(32).                        // data size
		Push(0).                         // data offset
		Op(vm.LOG3).
		Jump(lbl).Bytes()

	slots := map[uint256.Int]uint256.Int{
		*uint256.NewInt(0): *uint256.NewInt(1_000_000), // from balance
		*uint256.NewInt(1): *uint256.NewInt(500_000),   // to balance
	}

	b.Run("transfer/100M", func(b *testing.B) {
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

// BenchmarkERC20TransferFrom adds allowance check (extra SLOAD + SSTORE).
func BenchmarkERC20TransferFrom(b *testing.B) {
	p, lbl := program.New().Jumpdest()
	code := p.
		// Check allowance: SLOAD(2)
		Push(2).Op(vm.SLOAD).  // [allowance]
		Push(100).             // [allowance, 100]
		Op(vm.SWAP1, vm.SUB).  // [allowance-100]
		Push(2).Op(vm.SSTORE). // update allowance
		// Transfer: same as above
		Push(0).Op(vm.SLOAD).
		Push(100).Op(vm.SWAP1, vm.SUB).
		Push(0).Op(vm.SSTORE).
		Push(1).Op(vm.SLOAD).
		Push(100).Op(vm.ADD).
		Push(1).Op(vm.SSTORE).
		// LOG3
		Push(100).Push(0).Op(vm.MSTORE).
		Push(0xDEAD).
		Push(0xCAFE).
		Push(transferEventSig).
		Push(32).Push(0).Op(vm.LOG3).
		Jump(lbl).Bytes()

	slots := map[uint256.Int]uint256.Int{
		*uint256.NewInt(0): *uint256.NewInt(1_000_000),
		*uint256.NewInt(1): *uint256.NewInt(500_000),
		*uint256.NewInt(2): *uint256.NewInt(10_000_000), // allowance
	}

	b.Run("transferFrom/100M", func(b *testing.B) {
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

// BenchmarkERC20BalanceOf measures a pure read path: SLOAD + RETURN.
func BenchmarkERC20BalanceOf(b *testing.B) {
	p, lbl := program.New().Jumpdest()
	code := p.
		Push(0).Op(vm.SLOAD). // balances[addr]
		Op(vm.POP).
		Jump(lbl).Bytes()

	slots := map[uint256.Int]uint256.Int{
		*uint256.NewInt(0): *uint256.NewInt(1_000_000),
	}

	b.Run("balanceOf/100M", func(b *testing.B) {
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

// BenchmarkERC20BatchTransfers simulates N sequential transfers to different
// addresses within a single call, measuring cold→warm slot amortization.
func BenchmarkERC20BatchTransfers(b *testing.B) {
	for _, n := range []int{5, 10, 50} {
		// Each transfer: SLOAD(from), SUB, SSTORE(from), SLOAD(to_i), ADD, SSTORE(to_i)
		// from = slot 0, to_i = slot i+1
		p := program.New()
		slots := map[uint256.Int]uint256.Int{
			*uint256.NewInt(0): *uint256.NewInt(uint64(n) * 1000), // from balance
		}
		for i := 0; i < n; i++ {
			toSlot := uint64(i + 1)
			slots[*uint256.NewInt(toSlot)] = *uint256.NewInt(0) // to balance starts at 0
			p.
				Push(0).Op(vm.SLOAD).           // from balance
				Push(100).Op(vm.SWAP1, vm.SUB). // subtract
				Push(0).Op(vm.SSTORE).          // write back
				Push(toSlot).Op(vm.SLOAD).      // to balance
				Push(100).Op(vm.ADD).           // add
				Push(toSlot).Op(vm.SSTORE)      // write back
		}
		code := p.Op(vm.STOP).Bytes()

		b.Run(fmt.Sprintf("batch-%d", n), func(b *testing.B) {
			b.ReportAllocs()
			// Each transfer ~= 2 SLOAD + 2 SSTORE
			gas := uint64(n)*30_000 + 100_000
			cfg, statedb := benchConfig(b, gas)
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
