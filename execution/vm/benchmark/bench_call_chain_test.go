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

// Cross-contract call addresses (raw, for Push)
var (
	rawRouter = common.HexToAddress("0x5001")
	rawPair   = common.HexToAddress("0x5002")
	rawTokenA = common.HexToAddress("0x5003")
	rawTokenB = common.HexToAddress("0x5004")
)

// BenchmarkNestedStaticCalls measures STATICCALL overhead at various depths.
// Each contract does minimal work (PUSH+POP) then STATICCALLs the next.
func BenchmarkNestedStaticCalls(b *testing.B) {
	for _, depth := range []int{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("depth-%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			vmenv := benchConfig(b, 100_000_000)
			statedb := vmenv.IntraBlockState()

			// Deploy depth chain: addr[0] → addr[1] → ... → addr[depth-1]
			addrs := makeAddrs(depth)
			deployCallChain(b, statedb, addrs) // staticcall chain

			// Entry point loops: JUMPDEST, STATICCALL(addr[0]), POP, JUMP
			entry, lbl := program.New().Jumpdest()
			entryCode := entry.
				StaticCall(nil, addrs[0].raw, 0, 0, 0, 0).
				Op(vm.POP).
				Jump(lbl).Bytes()
			deployContract(b, statedb, addrContract, entryCode)

			callOOG(b, vmenv, addrContract)
			for b.Loop() {
				callOOG(b, vmenv, addrContract)
			}
		})
	}
}

// BenchmarkDelegateCallProxy measures DELEGATECALL proxy layers.
func BenchmarkDelegateCallProxy(b *testing.B) {
	for _, layers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("%d-layers", layers), func(b *testing.B) {
			b.ReportAllocs()
			vmenv := benchConfig(b, 100_000_000)
			statedb := vmenv.IntraBlockState()

			addrs := makeAddrs(layers)
			deployDelegateChain(b, statedb, addrs)

			// Entry: loop calling first proxy
			entry, lbl := program.New().Jumpdest()
			entryCode := entry.
				DelegateCall(nil, addrs[0].raw, 0, 0, 0, 0).
				Op(vm.POP).
				Jump(lbl).Bytes()
			deployContract(b, statedb, addrContract, entryCode)

			callOOG(b, vmenv, addrContract)
			for b.Loop() {
				callOOG(b, vmenv, addrContract)
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
		vmenv := benchConfig(b, 100_000_000)
		statedb := vmenv.IntraBlockState()

		deployContract(b, statedb, addrPair, targetCode)
		// Caller loops: CALL with value=0
		p, lbl := program.New().Jumpdest()
		code := p.Call(nil, rawPair, 0, 0, 0, 0, 0).Op(vm.POP).Jump(lbl).Bytes()
		deployContract(b, statedb, addrContract, code)

		callOOG(b, vmenv, addrContract)
		for b.Loop() {
			callOOG(b, vmenv, addrContract)
		}
	})

	b.Run("with-value", func(b *testing.B) {
		b.ReportAllocs()
		vmenv := benchConfig(b, 100_000_000)
		statedb := vmenv.IntraBlockState()

		deployContract(b, statedb, addrPair, targetCode)
		// Caller loops: CALL with value=1 wei
		p, lbl := program.New().Jumpdest()
		code := p.Call(nil, rawPair, 1, 0, 0, 0, 0).Op(vm.POP).Jump(lbl).Bytes()
		deployContractWithBalance(b, statedb, addrContract, code, uint256.NewInt(1_000_000_000))

		callOOG(b, vmenv, addrContract)
		for b.Loop() {
			callOOG(b, vmenv, addrContract)
		}
	})
}

// BenchmarkDeFiSwapChain simulates a simplified Uniswap-style swap:
// Router → Pair (getReserves + swap) → TokenA.transfer + TokenB.transfer
func BenchmarkDeFiSwapChain(b *testing.B) {
	b.Run("swap/100M", func(b *testing.B) {
		b.ReportAllocs()
		vmenv := benchConfig(b, 100_000_000)
		statedb := vmenv.IntraBlockState()

		deployDeFiContracts(b, statedb)

		// Entry contract loops: CALL router
		entry, lbl := program.New().Jumpdest()
		entryCode := entry.
			Call(nil, rawRouter, 0, 0, 0, 0, 0).
			Op(vm.POP).
			Jump(lbl).Bytes()
		deployContract(b, statedb, addrContract, entryCode)

		callOOG(b, vmenv, addrContract)
		for b.Loop() {
			callOOG(b, vmenv, addrContract)
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

// deployCallChain deploys a chain where each contract STATICCALLs the next.
// The last contract just does PUSH+POP+STOP.
func deployCallChain(b testing.TB, statedb *state.IntraBlockState, addrs []chainAddr) {
	for i, a := range addrs {
		if i == len(addrs)-1 {
			// Leaf: minimal work
			code := program.New().Push(42).Op(vm.POP, vm.STOP).Bytes()
			deployContract(b, statedb, a.interned, code)
		} else {
			// Intermediate: STATICCALL next, POP result, STOP
			p := program.New()
			code := p.StaticCall(nil, addrs[i+1].raw, 0, 0, 0, 0).Op(vm.POP, vm.STOP).Bytes()
			deployContract(b, statedb, a.interned, code)
		}
	}
}

// deployDelegateChain deploys a chain where each contract DELEGATECALLs the next.
func deployDelegateChain(b testing.TB, statedb *state.IntraBlockState, addrs []chainAddr) {
	for i, a := range addrs {
		if i == len(addrs)-1 {
			code := program.New().Push(42).Op(vm.POP, vm.STOP).Bytes()
			deployContract(b, statedb, a.interned, code)
		} else {
			p := program.New()
			code := p.DelegateCall(nil, addrs[i+1].raw, 0, 0, 0, 0).Op(vm.POP, vm.STOP).Bytes()
			deployContract(b, statedb, a.interned, code)
		}
	}
}

// deployDeFiContracts sets up a simplified DeFi swap topology:
//
//	Router: STATICCALL Pair (getReserves), CALL TokenA.transfer, CALL TokenB.transfer
//	Pair: SLOAD reserve0, SLOAD reserve1, RETURN (simplified getReserves)
//	TokenA/B: SLOAD balance(from), SLOAD balance(to), SSTORE×2, LOG3 (transfer)
func deployDeFiContracts(b testing.TB, statedb *state.IntraBlockState) {
	// Pair contract: load 2 reserves, return them
	pairCode := program.New().
		Push(0).Op(vm.SLOAD). // reserve0
		Push(1).Op(vm.SLOAD). // reserve1
		Op(vm.POP, vm.POP, vm.STOP).
		Bytes()
	deployContract(b, statedb, addrPair, pairCode)
	setStorage(b, statedb, addrPair, map[uint256.Int]uint256.Int{
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
		deployContract(b, statedb, addr, tokenCode)
		setStorage(b, statedb, addr, map[uint256.Int]uint256.Int{
			*uint256.NewInt(0): *uint256.NewInt(1_000_000_000),
			*uint256.NewInt(1): *uint256.NewInt(1_000_000_000),
		})
	}

	// Router: STATICCALL pair, CALL tokenA, CALL tokenB
	routerCode := program.New().
		StaticCall(nil, rawPair, 0, 0, 0, 0).Op(vm.POP).
		Call(nil, rawTokenA, 0, 0, 0, 0, 0).Op(vm.POP).
		Call(nil, rawTokenB, 0, 0, 0, 0, 0).Op(vm.POP).
		Op(vm.STOP).Bytes()
	deployContract(b, statedb, addrRouter, routerCode)
}

// BenchmarkDeepStacks recurses into itself until gas runs out, with a nearly
// full stack in every frame. Ported from go-ethereum's BenchmarkLargeDeepStacks
// and BenchmarkShortDeepStacks. It measures CallContext acquisition at depth,
// where every context carries a 32 KB Stack.
func BenchmarkDeepStacks(b *testing.B) {
	for _, pushes := range []int{8, 512} {
		b.Run(fmt.Sprintf("pushes-%d", pushes), func(b *testing.B) {
			b.ReportAllocs()
			vmenv := benchConfig(b, 10_000_000)
			statedb := vmenv.IntraBlockState()

			code := make([]byte, pushes)
			for i := range code {
				code[i] = byte(vm.PUSH0)
			}
			code = append(code, byte(vm.ADDRESS), byte(vm.GAS), byte(vm.CALL))
			deployContract(b, statedb, addrContract, code)

			callComplete(b, vmenv, addrContract, nil)
			for b.Loop() {
				callComplete(b, vmenv, addrContract, nil)
			}
		})
	}
}

// BenchmarkDeepCallsWithMemory runs a STATICCALL chain where every frame
// expands memory before calling the next, so all frames hold memory at once.
// The two shallow cases match mainnet call trees, where a DeFi router or
// aggregator nests under twenty frames and each ABI-encodes a few KB; the
// deep case is a stress shape that mainnet gas costs do not reach.
func BenchmarkDeepCallsWithMemory(b *testing.B) {
	for _, tc := range []struct {
		name  string
		depth int
		words int
	}{
		{"defi/depth-8/1KiB", 8, 32},
		{"aggregator/depth-16/4KiB", 16, 128},
		{"stress/depth-64/8KiB", 64, 256},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			vmenv := benchConfig(b, 30_000_000)
			statedb := vmenv.IntraBlockState()

			addrs := makeAddrs(tc.depth)
			for i, a := range addrs {
				var next *common.Address
				if i < len(addrs)-1 {
					next = &addrs[i+1].raw
				}
				deployContract(b, statedb, a.interned, memChainCode(next, tc.words))
			}

			callComplete(b, vmenv, addrs[0].interned, nil)
			for b.Loop() {
				callComplete(b, vmenv, addrs[0].interned, nil)
			}
		})
	}
}

// memChainCode fills words of memory a word at a time, the way solidity grows
// its free memory pointer, then hands the region to next as calldata. A leaf
// (next == nil) returns instead of calling on.
func memChainCode(next *common.Address, words int) []byte {
	p := program.New()
	for w := range words {
		p.Push(0xFF).Push(w * 32).Op(vm.MSTORE)
	}
	if next == nil {
		return p.Return(0, 32).Bytes()
	}
	return p.StaticCall(nil, *next, 0, words*32, 0, 32).Op(vm.POP, vm.STOP).Bytes()
}
