package benchmark

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// BenchmarkNestedStaticCalls measures STATICCALL overhead at various depths.
// Each contract does minimal work (PUSH+POP) then STATICCALLs the next.
func BenchmarkNestedStaticCalls(b *testing.B) {
	for _, depth := range []int{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("depth-%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			cfg, statedb := benchConfig(b, 100_000_000)

			// Deploy depth chain: addr[0] → addr[1] → ... → addr[depth-1]
			addrs := makeAddrs(depth)
			deployChain(statedb, addrs, func(p *program.Program, next common.Address) *program.Program {
				return p.StaticCall(nil, next, 0, 0, 0, 0)
			})

			// Entry point loops: JUMPDEST, STATICCALL(addr[0]), POP, JUMP
			entry, lbl := program.New().Jumpdest()
			entryCode := entry.
				StaticCall(nil, addrs[0].raw, 0, 0, 0, 0).
				Op(vm.POP).
				Jump(lbl).Bytes()
			deployContract(statedb, addrContract, entryCode)

			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
			for b.Loop() {
				prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
			}
		})
	}
}

// BenchmarkDelegateCallProxy measures DELEGATECALL proxy layers.
func BenchmarkDelegateCallProxy(b *testing.B) {
	for _, layers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("%d-layers", layers), func(b *testing.B) {
			b.ReportAllocs()
			cfg, statedb := benchConfig(b, 100_000_000)

			addrs := makeAddrs(layers)
			deployChain(statedb, addrs, func(p *program.Program, next common.Address) *program.Program {
				return p.DelegateCall(nil, next, 0, 0, 0, 0)
			})

			// Entry: loop calling first proxy
			entry, lbl := program.New().Jumpdest()
			entryCode := entry.
				DelegateCall(nil, addrs[0].raw, 0, 0, 0, 0).
				Op(vm.POP).
				Jump(lbl).Bytes()
			deployContract(statedb, addrContract, entryCode)

			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
			for b.Loop() {
				prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
			}
		})
	}
}

// BenchmarkCallWithValue measures CALL with ETH value transfer vs without.
func BenchmarkCallWithValue(b *testing.B) {
	// Target contract: just STOP
	targetCode := program.New().Op(vm.STOP).Bytes()

	b.Run("no-value", func(b *testing.B) {
		b.ReportAllocs()
		cfg, statedb := benchConfig(b, 100_000_000)

		deployContract(statedb, addrPair, targetCode)
		// Caller loops: CALL with value=0
		p, lbl := program.New().Jumpdest()
		code := p.Call(nil, rawPair, 0, 0, 0, 0, 0).Op(vm.POP).Jump(lbl).Bytes()
		deployContract(statedb, addrContract, code)

		prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
		for b.Loop() {
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
		}
	})

	b.Run("with-value", func(b *testing.B) {
		b.ReportAllocs()
		cfg, statedb := benchConfig(b, 100_000_000)

		deployContract(statedb, addrPair, targetCode)
		// Give caller ETH
		deployContractWithBalance(statedb, addrContract, nil, uint256.NewInt(1_000_000_000))
		// Caller loops: CALL with value=1 wei
		p, lbl := program.New().Jumpdest()
		code := p.Call(nil, rawPair, 1, 0, 0, 0, 0).Op(vm.POP).Jump(lbl).Bytes()
		deployContractWithBalance(statedb, addrContract, code, uint256.NewInt(1_000_000_000))

		prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
		for b.Loop() {
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
		}
	})
}

// BenchmarkDeFiSwapChain simulates a simplified Uniswap-style swap:
// Router → Pair (getReserves + swap) → TokenA.transfer + TokenB.transfer
func BenchmarkDeFiSwapChain(b *testing.B) {
	b.Run("swap/100M", func(b *testing.B) {
		b.ReportAllocs()
		cfg, statedb := benchConfig(b, 100_000_000)

		deployDeFiContracts(statedb)

		// Entry contract loops: CALL router
		entry, lbl := program.New().Jumpdest()
		entryCode := entry.
			Call(nil, rawRouter, 0, 0, 0, 0, 0).
			Op(vm.POP).
			Jump(lbl).Bytes()
		deployContract(statedb, addrContract, entryCode)

		prepareAndCall(cfg, addrContract, nil) //nolint:errcheck // OOG is expected termination for looping benchmarks
		for b.Loop() {
			prepareAndCall(cfg, addrContract, nil) //nolint:errcheck
		}
	})
}

// --- helpers ---

type chainAddr struct {
	interned accounts.Address
	raw      common.Address
}

func makeAddrs(n int) []chainAddr {
	addrs := make([]chainAddr, n)
	for i := range n {
		raw := common.HexToAddress("0x6000") // base
		raw[19] = byte(i + 1)                // unique last byte
		addrs[i] = chainAddr{
			interned: accounts.InternAddress(raw),
			raw:      raw,
		}
	}
	return addrs
}

// chainCallFn builds the intermediate-node bytecode for a call chain.
// It receives a fresh program and the next contract's address, and must return the program
// after emitting the call opcode (without the trailing POP+STOP, which deployChain appends).
type chainCallFn func(p *program.Program, next common.Address) *program.Program

// deployChain deploys a call chain where each intermediate contract invokes the next
// using the call opcode produced by build. The leaf contract does PUSH+POP+STOP.
func deployChain(statedb *state.IntraBlockState, addrs []chainAddr, build chainCallFn) {
	for i, a := range addrs {
		var code []byte
		if i == len(addrs)-1 {
			// Leaf: minimal work
			code = program.New().Push(42).Op(vm.POP, vm.STOP).Bytes()
		} else {
			// Intermediate: call next, POP result, STOP
			code = build(program.New(), addrs[i+1].raw).Op(vm.POP, vm.STOP).Bytes()
		}
		deployContract(statedb, a.interned, code)
	}
}

// deployDeFiContracts sets up a simplified DeFi swap topology:
//
//	Router: STATICCALL Pair (getReserves), CALL TokenA.transfer, CALL TokenB.transfer
//	Pair: SLOAD reserve0, SLOAD reserve1, RETURN (simplified getReserves)
//	TokenA/B: SLOAD balance(from), SLOAD balance(to), SSTORE×2, LOG3 (transfer)
func deployDeFiContracts(statedb *state.IntraBlockState) {
	// Pair contract: load 2 reserves, return them
	pairCode := program.New().
		Push(0).Op(vm.SLOAD). // reserve0
		Push(1).Op(vm.SLOAD). // reserve1
		Op(vm.POP, vm.POP, vm.STOP).
		Bytes()
	deployContract(statedb, addrPair, pairCode)
	setStorage(statedb, addrPair, map[uint256.Int]uint256.Int{
		*uint256.NewInt(0): *uint256.NewInt(1_000_000), // reserve0
		*uint256.NewInt(1): *uint256.NewInt(2_000_000), // reserve1
	})

	// Token contracts: SLOAD(0), SLOAD(1), SSTORE(0, bal-100), SSTORE(1, bal+100), LOG3
	tokenCode := program.New().
		Push(0).Op(vm.SLOAD).
		Push(100).Op(vm.SWAP1, vm.SUB).
		Push(0).Op(vm.SSTORE).
		Push(1).Op(vm.SLOAD).
		Push(100).Op(vm.ADD).
		Push(1).Op(vm.SSTORE).
		// LOG3
		Push(100).Push(0).Op(vm.MSTORE).
		Push(0xDEAD).Push(0xCAFE).Push(transferEventSig).
		Push(32).Push(0).Op(vm.LOG3).
		Op(vm.STOP).Bytes()

	for _, addr := range []accounts.Address{addrTokenA, addrTokenB} {
		deployContract(statedb, addr, tokenCode)
		setStorage(statedb, addr, map[uint256.Int]uint256.Int{
			*uint256.NewInt(0): *uint256.NewInt(500_000),
			*uint256.NewInt(1): *uint256.NewInt(500_000),
		})
	}

	// Router: STATICCALL pair, CALL tokenA, CALL tokenB
	routerCode := program.New().
		StaticCall(nil, rawPair, 0, 0, 0, 0).Op(vm.POP).
		Call(nil, rawTokenA, 0, 0, 0, 0, 0).Op(vm.POP).
		Call(nil, rawTokenB, 0, 0, 0, 0, 0).Op(vm.POP).
		Op(vm.STOP).Bytes()
	deployContract(statedb, addrRouter, routerCode)
}
